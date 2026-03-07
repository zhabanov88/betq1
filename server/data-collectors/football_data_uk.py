"""
football-data.co.uk Collector
Downloads historical match + odds data from football-data.co.uk
Covers: 30+ leagues, 1993-present
Data: Results, odds from B365/Pinnacle/William Hill/Bet365/etc
"""
import requests, csv, io, clickhouse_connect, os, time
from datetime import datetime

CH_HOST = os.getenv('CH_HOST','localhost')
CH_PORT = int(os.getenv('CH_PORT',8123))
CH_USER = os.getenv('CH_USER','default')
CH_PASS = os.getenv('CH_PASS','')
CH_DB   = os.getenv('CH_DB','betquant')

LEAGUES = {
    # England
    'E0': 'EPL', 'E1': 'Championship', 'E2': 'League One', 'E3': 'League Two',
    # Spain
    'SP1': 'La Liga', 'SP2': 'La Liga 2',
    # Germany
    'D1': 'Bundesliga', 'D2': '2. Bundesliga',
    # Italy
    'I1': 'Serie A', 'I2': 'Serie B',
    # France
    'F1': 'Ligue 1', 'F2': 'Ligue 2',
    # Netherlands
    'N1': 'Eredivisie',
    # Portugal
    'P1': 'Primeira Liga',
    # Scotland
    'SC0': 'Scottish Premiership',
    # Belgium
    'B1': 'First Division A',
    # Turkey
    'T1': 'Super Lig',
    # Greece
    'G1': 'Super League',
}

SEASONS = ['9394','9495','9596','9697','9798','9899','9900',
           '0001','0102','0203','0304','0405','0506','0607','0708','0809',
           '0910','1011','1112','1213','1314','1415','1516','1617','1718',
           '1819','1920','2021','2122','2223','2324']

BASE_URL = 'https://www.football-data.co.uk/mmz4281/{season}/{league}.csv'

def get_client():
    return clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB)

def ensure_tables(client):
    client.command('''
    CREATE TABLE IF NOT EXISTS matches (
        id UUID DEFAULT generateUUIDv4(),
        date Date,
        league String,
        season String,
        home_team String,
        away_team String,
        home_goals UInt8,
        away_goals UInt8,
        result FixedString(1),
        ht_home_goals UInt8,
        ht_away_goals UInt8,
        home_shots UInt8,
        away_shots UInt8,
        home_shots_on_target UInt8,
        away_shots_on_target UInt8,
        home_corners UInt8,
        away_corners UInt8,
        home_fouls UInt8,
        away_fouls UInt8,
        home_yellow UInt8,
        away_yellow UInt8,
        home_red UInt8,
        away_red UInt8,
        source String DEFAULT 'football-data.co.uk'
    ) ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY (date, league, home_team)
    ''')
    
    client.command('''
    CREATE TABLE IF NOT EXISTS odds (
        match_id String,
        date Date,
        league String,
        home_team String,
        away_team String,
        bookmaker String,
        market String,
        odds_home Float32,
        odds_draw Float32,
        odds_away Float32,
        odds_over Float32,
        odds_under Float32,
        odds_btts_yes Float32,
        odds_btts_no Float32,
        closing_home Float32,
        closing_draw Float32,
        closing_away Float32
    ) ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY (date, league, bookmaker)
    ''')

FIELD_MAP = {
    'B365H':'B365_home','B365D':'B365_draw','B365A':'B365_away',
    'PSH':'Pinnacle_home','PSD':'Pinnacle_draw','PSA':'Pinnacle_away',
    'BWH':'BetWin_home','BWD':'BetWin_draw','BWA':'BetWin_away',
    'WHH':'WilliamHill_home','WHD':'WilliamHill_draw','WHA':'WilliamHill_away',
    'VCH':'VCBet_home','VCD':'VCBet_draw','VCA':'VCBet_away',
    'GB>2.5':'generic_over','GB<2.5':'generic_under',
    'BbAv>2.5':'BetBrain_over','BbAv<2.5':'BetBrain_under',
    'BbAvH':'BetBrain_home','BbAvD':'BetBrain_draw','BbAvA':'BetBrain_away',
}

def collect_league(league_code, season, client):
    url = BASE_URL.format(season=season, league=league_code)
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return 0
        
        rows_m, rows_o = [], []
        reader = csv.DictReader(io.StringIO(r.text))
        
        for row in reader:
            if not row.get('Date') or not row.get('HomeTeam'):
                continue
            
            try:
                parts = row['Date'].split('/')
                if len(parts[2]) == 2:
                    year = '20' + parts[2] if int(parts[2]) < 50 else '19' + parts[2]
                else:
                    year = parts[2]
                date_str = f"{year}-{parts[1]}-{parts[0]}"
            except:
                continue
            
            match_id = f"{date_str}_{league_code}_{row.get('HomeTeam','')}_{row.get('AwayTeam','')}"
            
            def safe_int(v, default=0):
                try: return int(v) if v and v.strip() else default
                except: return default
            
            rows_m.append({
                'date': date_str, 'league': LEAGUES.get(league_code, league_code),
                'season': season, 'home_team': row.get('HomeTeam',''),
                'away_team': row.get('AwayTeam',''),
                'home_goals': safe_int(row.get('FTHG') or row.get('HG')),
                'away_goals': safe_int(row.get('FTAG') or row.get('AG')),
                'result': row.get('FTR') or row.get('Res','?'),
                'ht_home_goals': safe_int(row.get('HTHG')),
                'ht_away_goals': safe_int(row.get('HTAG')),
                'home_shots': safe_int(row.get('HS')),
                'away_shots': safe_int(row.get('AS')),
                'home_shots_on_target': safe_int(row.get('HST')),
                'away_shots_on_target': safe_int(row.get('AST')),
                'home_corners': safe_int(row.get('HC')),
                'away_corners': safe_int(row.get('AC')),
                'home_fouls': safe_int(row.get('HF')),
                'away_fouls': safe_int(row.get('AF')),
                'home_yellow': safe_int(row.get('HY')),
                'away_yellow': safe_int(row.get('AY')),
                'home_red': safe_int(row.get('HR')),
                'away_red': safe_int(row.get('AR')),
            })
            
            # Extract odds from multiple bookmakers
            bookmakers = {
                'B365': ('B365H','B365D','B365A'),
                'Pinnacle': ('PSH','PSD','PSA'),
                'WilliamHill': ('WHH','WHD','WHA'),
                'BetWin': ('BWH','BWD','BWA'),
                'Interwetten': ('IWH','IWD','IWA'),
            }
            
            def safe_float(v):
                try: return float(v) if v and v.strip() else 0.0
                except: return 0.0
            
            for bk, (h,d,a) in bookmakers.items():
                oh, od, oa = safe_float(row.get(h)), safe_float(row.get(d)), safe_float(row.get(a))
                if oh > 0:
                    rows_o.append({
                        'match_id': match_id, 'date': date_str,
                        'league': LEAGUES.get(league_code, league_code),
                        'home_team': row.get('HomeTeam',''), 'away_team': row.get('AwayTeam',''),
                        'bookmaker': bk, 'market': '1X2',
                        'odds_home': oh, 'odds_draw': od, 'odds_away': oa,
                        'odds_over': safe_float(row.get('BbAv>2.5') or row.get('GB>2.5')),
                        'odds_under': safe_float(row.get('BbAv<2.5') or row.get('GB<2.5')),
                        'odds_btts_yes': 0.0, 'odds_btts_no': 0.0,
                        'closing_home': safe_float(row.get('BbClH') or row.get(h)),
                        'closing_draw': safe_float(row.get('BbClD') or row.get(d)),
                        'closing_away': safe_float(row.get('BbClA') or row.get(a)),
                    })
        
        if rows_m:
            client.insert('matches', rows_m, column_names=list(rows_m[0].keys()))
        if rows_o:
            client.insert('odds', rows_o, column_names=list(rows_o[0].keys()))
        
        return len(rows_m)
    
    except Exception as e:
        print(f"  ERROR {league_code}/{season}: {e}")
        return 0

def main(leagues=None, seasons=None):
    client = get_client()
    ensure_tables(client)
    
    target_leagues = leagues or list(LEAGUES.keys())
    target_seasons = seasons or SEASONS[-10:]  # Last 10 seasons by default
    
    total = 0
    for league in target_leagues:
        for season in target_seasons:
            n = collect_league(league, season, client)
            if n > 0:
                print(f"  ✓ {LEAGUES.get(league,league)} {season}: {n} matches")
                total += n
            time.sleep(0.3)
    
    print(f"\nTotal collected: {total} matches")
    client.close()

if __name__ == '__main__':
    import sys
    leagues = sys.argv[1].split(',') if len(sys.argv)>1 else None
    seasons = sys.argv[2].split(',') if len(sys.argv)>2 else None
    main(leagues, seasons)
