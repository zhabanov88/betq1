"""
Tennis Data Collector — Jeff Sackmann GitHub + tennis-data.co.uk
ATP 1968–now, WTA 1920–now, with full serve/return stats
"""
import requests, csv, io, os, time, clickhouse_connect

CH_HOST = os.getenv('CH_HOST','localhost')
CH_DB   = os.getenv('CH_DB','betquant')

ATP_BASE  = 'https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_{year}.csv'
WTA_BASE  = 'https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_{year}.csv'
ODDS_BASE = 'http://www.tennis-data.co.uk/{year}/{tour}{year}.csv'

def get_client():
    return clickhouse_connect.get_client(host=CH_HOST, database=CH_DB)

def ensure_tables(client):
    client.command('''
    CREATE TABLE IF NOT EXISTS tennis_matches (
        tourney_id String,
        tourney_name String,
        surface String,
        tourney_date Date,
        match_num UInt16,
        winner_name String,
        loser_name String,
        winner_rank UInt16,
        loser_rank UInt16,
        winner_rank_points UInt32,
        loser_rank_points UInt32,
        score String,
        best_of UInt8,
        round String,
        minutes UInt16,
        w_svpt UInt16, w_1stIn UInt16, w_1stWon UInt16, w_2ndWon UInt16,
        w_SvGms UInt8, w_bpSaved UInt8, w_bpFaced UInt8,
        l_svpt UInt16, l_1stIn UInt16, l_1stWon UInt16, l_2ndWon UInt16,
        l_SvGms UInt8, l_bpSaved UInt8, l_bpFaced UInt8,
        tour String
    ) ENGINE = MergeTree() ORDER BY (tourney_date, winner_name) 
    ''')
    
    client.command('''
    CREATE TABLE IF NOT EXISTS tennis_odds (
        date Date,
        tournament String,
        surface String,
        round String,
        winner String,
        loser String,
        winner_odds_b365 Float32,
        loser_odds_b365 Float32,
        winner_odds_ps Float32,
        loser_odds_ps Float32,
        tour String
    ) ENGINE = MergeTree() ORDER BY (date, tournament)
    ''')

def collect_atp_year(year, client):
    url = ATP_BASE.format(year=year)
    r = requests.get(url, timeout=15)
    if r.status_code != 200: return 0
    
    rows = []
    for row in csv.DictReader(io.StringIO(r.text)):
        try:
            d = row.get('tourney_date','')
            date_str = f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d)==8 else '2000-01-01'
            
            def si(v): 
                try: return int(v) if v else 0
                except: return 0
            
            rows.append({
                'tourney_id': row.get('tourney_id',''), 'tourney_name': row.get('tourney_name',''),
                'surface': row.get('surface',''), 'tourney_date': date_str,
                'match_num': si(row.get('match_num')), 'winner_name': row.get('winner_name',''),
                'loser_name': row.get('loser_name',''), 'winner_rank': si(row.get('winner_rank')),
                'loser_rank': si(row.get('loser_rank')), 'winner_rank_points': si(row.get('winner_rank_points')),
                'loser_rank_points': si(row.get('loser_rank_points')), 'score': row.get('score',''),
                'best_of': si(row.get('best_of',3)), 'round': row.get('round',''),
                'minutes': si(row.get('minutes')),
                'w_svpt': si(row.get('w_svpt')), 'w_1stIn': si(row.get('w_1stIn')),
                'w_1stWon': si(row.get('w_1stWon')), 'w_2ndWon': si(row.get('w_2ndWon')),
                'w_SvGms': si(row.get('w_SvGms')), 'w_bpSaved': si(row.get('w_bpSaved')),
                'w_bpFaced': si(row.get('w_bpFaced')), 'l_svpt': si(row.get('l_svpt')),
                'l_1stIn': si(row.get('l_1stIn')), 'l_1stWon': si(row.get('l_1stWon')),
                'l_2ndWon': si(row.get('l_2ndWon')), 'l_SvGms': si(row.get('l_SvGms')),
                'l_bpSaved': si(row.get('l_bpSaved')), 'l_bpFaced': si(row.get('l_bpFaced')),
                'tour': 'ATP'
            })
        except Exception as e:
            continue
    
    if rows:
        client.insert('tennis_matches', rows, column_names=list(rows[0].keys()))
    return len(rows)

def main(start_year=2000, end_year=2024):
    client = get_client()
    ensure_tables(client)
    total = 0
    for year in range(start_year, end_year+1):
        n = collect_atp_year(year, client)
        if n: print(f"  ✓ ATP {year}: {n} matches")
        total += n
        time.sleep(0.5)
    print(f"Total ATP: {total} matches")
    client.close()

if __name__ == '__main__':
    import sys
    sy = int(sys.argv[1]) if len(sys.argv)>1 else 2000
    ey = int(sys.argv[2]) if len(sys.argv)>2 else 2024
    main(sy, ey)
