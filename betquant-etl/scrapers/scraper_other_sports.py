#!/usr/bin/env python3
"""
BetQuant ETL — Tennis / Basketball / Baseball Scraper
Sources:
  Tennis:
    • Jeff Sackmann ATP/WTA datasets (github.com/JeffSackmann) — бесплатно, 40+ лет данных
      Включает: результаты, счёт по сетам, статистику подач/приёма, коэффициенты
  Basketball:
    • basketball-reference.com (scraping) — NBA box scores
    • balldontlie.io FREE API — NBA stats (без ключа: 30 запросов/мин)
  Baseball:
    • Sean Lahman Baseball Database (CSV, открытые данные)
    • retrosheet.org — play-by-play MLB (бесплатно)
"""

import urllib.request
import urllib.error
import csv
import json
import io
import time
import sys
import argparse
import hashlib
import zipfile
from datetime import datetime, date, timedelta
from collections import defaultdict

# ── Helpers (те же что в других скриптах) ─────────────────────────────

def fetch_text(url, timeout=30, retries=2, encoding='utf-8'):
    headers = {'User-Agent': 'BetQuant-ETL/1.0'}
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                try:
                    return raw.decode(encoding)
                except UnicodeDecodeError:
                    return raw.decode('latin-1')
        except urllib.error.HTTPError as e:
            if e.code == 404: return None
            if attempt == retries-1: raise
            time.sleep(2)
        except Exception:
            if attempt == retries-1: raise
            time.sleep(2)
    return None

def fetch_json(url, timeout=30, retries=2):
    text = fetch_text(url, timeout, retries)
    return json.loads(text) if text else None

def fetch_bytes(url, timeout=60):
    headers = {'User-Agent': 'BetQuant-ETL/1.0'}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def safe_int(v, d=0):
    try: return int(float(v)) if v not in (None, '', 'NA', 'N/A') else d
    except: return d

def safe_float(v, d=0.0):
    try: return float(v) if v not in (None, '', 'NA', 'N/A') else d
    except: return d

def make_id(*parts):
    return hashlib.md5('|'.join(str(p) for p in parts).encode()).hexdigest()[:16]

class ClickHouseClient:
    def __init__(self, host, db, user='default', pw=''):
        self.host = host.rstrip('/')
        self.db = db
        self.user = user
        self.pw = pw

    def query(self, sql):
        url = f"{self.host}/?database={self.db}&user={self.user}"
        if self.pw: url += f"&password={self.pw}"
        req = urllib.request.Request(url, data=sql.encode(),
                                     headers={'Content-Type': 'application/octet-stream'})
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read().decode('utf-8')

    def insert(self, table, rows):
        if not rows: return 0
        lines = '\n'.join(json.dumps(r, default=str) for r in rows)
        self.query(f"INSERT INTO {self.db}.{table} FORMAT JSONEachRow\n{lines}")
        return len(rows)

    def log(self, sport, source, league, season, n, status, msg=''):
        self.insert('etl_log', [{'sport': sport, 'source': source, 'league': league,
                                  'season': season, 'rows_loaded': n,
                                  'status': status, 'message': msg[:400]}])

    def count(self, table):
        return int(self.query(f"SELECT count() FROM {self.db}.{table}").strip())


# ═══════════════════════════════════════════════════════════════════════
#  ТЕННИС — Jeff Sackmann datasets
#  github.com/JeffSackmann/tennis_atp  (ATP)
#  github.com/JeffSackmann/tennis_wta  (WTA)
# ═══════════════════════════════════════════════════════════════════════

SACKMANN_BASE_ATP = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
SACKMANN_BASE_WTA = "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master"

TENNIS_COL_MAP = {
    'tourney_id': 'tournament', 'tourney_name': 'tournament',
    'surface': 'surface', 'tourney_date': 'date',
    'winner_name': 'winner', 'loser_name': 'loser',
    'winner_id': 'winner_id', 'loser_id': 'loser_id',
    'score': 'score', 'best_of': 'best_of',
    'round': 'round', 'minutes': 'minutes',
    'winner_rank': 'winner_rank', 'loser_rank': 'loser_rank',
    'winner_rank_points': 'winner_rank_pts', 'loser_rank_points': 'loser_rank_pts',
    'winner_seed': 'winner_seed', 'loser_seed': 'loser_seed',
    'w_ace': 'w_aces', 'l_ace': 'l_aces',
    'w_df': 'w_df', 'l_df': 'l_df',
    'w_svpt': 'w_svpt', 'l_svpt': 'l_svpt',
    'w_1stIn': 'w_1st_in', 'l_1stIn': 'l_1st_in',
    'w_1stWon': 'w_1st_won', 'l_1stWon': 'l_1st_won',
    'w_2ndWon': 'w_2nd_won', 'l_2ndWon': 'l_2nd_won',
    'w_bpSaved': 'w_bp_saved', 'l_bpSaved': 'l_bp_saved',
    'w_bpFaced': 'w_bp_faced', 'l_bpFaced': 'l_bp_faced',
    # Odds (разные букмекеры в разных файлах)
    'B365W': 'b365_winner', 'B365L': 'b365_loser',
    'PSW':   'ps_winner',   'PSL':   'ps_loser',
    'MaxW':  'max_winner',  'MaxL':  'max_loser',
    'AvgW':  'avg_winner',  'AvgL':  'avg_loser',
}

def parse_tennis_date(s):
    if not s or len(str(s)) < 8:
        return None
    s = str(s).strip()
    try:
        return datetime.strptime(s[:8], '%Y%m%d').strftime('%Y-%m-%d')
    except ValueError:
        return None

def parse_sackmann_csv(text, tour, year):
    if not text:
        return []
    rows = []
    try:
        reader = csv.DictReader(io.StringIO(text))
    except Exception:
        return []

    for raw in reader:
        winner = raw.get('winner_name', '').strip()
        loser  = raw.get('loser_name', '').strip()
        score  = raw.get('score', '').strip()
        if not winner or not loser:
            continue

        dt = parse_tennis_date(raw.get('tourney_date', ''))
        if not dt:
            continue

        # Пропускаем незаверш. матчи? Нет — retired тоже интересны
        retired = 1 if 'RET' in score or 'W/O' in score else 0

        sets = score.replace('RET', '').strip()
        # Считаем сыгранные сеты
        set_count = len([s for s in sets.split() if '-' in s])

        row = {
            'match_id':   make_id(dt, winner, loser, raw.get('tourney_id', '')),
            'source':     'sackmann',
            'date':       dt,
            'tournament': raw.get('tourney_name', '').strip(),
            'surface':    raw.get('surface', '').strip(),
            'tour':       tour,
            'round':      raw.get('round', '').strip(),
            'best_of':    safe_int(raw.get('best_of', 3)),
            'indoor':     0,
            'winner':     winner,
            'loser':      loser,
            'winner_id':  str(raw.get('winner_id', '')),
            'loser_id':   str(raw.get('loser_id', '')),
            'score':      score[:100],
            'sets_played':set_count,
            'retired':    retired,
            'winner_rank':     safe_int(raw.get('winner_rank', 0)),
            'loser_rank':      safe_int(raw.get('loser_rank', 0)),
            'winner_rank_pts': safe_int(raw.get('winner_rank_points', 0)),
            'loser_rank_pts':  safe_int(raw.get('loser_rank_points', 0)),
            'winner_seed':     safe_int(raw.get('winner_seed', 0)),
            'loser_seed':      safe_int(raw.get('loser_seed', 0)),
            'w_aces':     safe_int(raw.get('w_ace', 0)),
            'l_aces':     safe_int(raw.get('l_ace', 0)),
            'w_df':       safe_int(raw.get('w_df', 0)),
            'l_df':       safe_int(raw.get('l_df', 0)),
            'w_svpt':     safe_int(raw.get('w_svpt', 0)),
            'l_svpt':     safe_int(raw.get('l_svpt', 0)),
            'w_1st_in':   safe_int(raw.get('w_1stIn', 0)),
            'l_1st_in':   safe_int(raw.get('l_1stIn', 0)),
            'w_1st_won':  safe_int(raw.get('w_1stWon', 0)),
            'l_1st_won':  safe_int(raw.get('l_1stWon', 0)),
            'w_2nd_won':  safe_int(raw.get('w_2ndWon', 0)),
            'l_2nd_won':  safe_int(raw.get('l_2ndWon', 0)),
            'w_bp_saved': safe_int(raw.get('w_bpSaved', 0)),
            'l_bp_saved': safe_int(raw.get('l_bpSaved', 0)),
            'w_bp_faced': safe_int(raw.get('w_bpFaced', 0)),
            'l_bp_faced': safe_int(raw.get('l_bpFaced', 0)),
            'minutes':    safe_int(raw.get('minutes', 0)),
            'b365_winner':safe_float(raw.get('B365W', 0)),
            'b365_loser': safe_float(raw.get('B365L', 0)),
            'ps_winner':  safe_float(raw.get('PSW', 0)),
            'ps_loser':   safe_float(raw.get('PSL', 0)),
            'max_winner': safe_float(raw.get('MaxW', 0)),
            'max_loser':  safe_float(raw.get('MaxL', 0)),
        }
        rows.append(row)

    return rows

def scrape_tennis(ch, years_back=5):
    print("\n🎾 ТЕННИС (Sackmann ATP+WTA datasets)")
    total = 0
    current_year = date.today().year

    for tour, base_url in [('ATP', SACKMANN_BASE_ATP), ('WTA', SACKMANN_BASE_WTA)]:
        for year in range(current_year - years_back, current_year + 1):
            url = f"{base_url}/atp_matches_{year}.csv" if tour == 'ATP' \
                  else f"{base_url}/wta_matches_{year}.csv"
            print(f"  [{tour}] {year} → {url}")
            try:
                text = fetch_text(url, timeout=30)
                if not text:
                    print(f"    ✗ не найдено")
                    continue
                rows = parse_sackmann_csv(text, tour, year)
                if rows:
                    ch.insert('tennis_matches', rows)
                    ch.log('tennis', 'sackmann', tour, str(year), len(rows), 'ok')
                    print(f"    ✓ {len(rows)} матчей")
                    total += len(rows)
                time.sleep(0.5)
            except Exception as e:
                print(f"    ✗ {e}")

        # Также challengers (ATP только)
        if tour == 'ATP':
            for year in range(current_year - years_back, current_year + 1):
                url = f"{base_url}/atp_matches_qual_chall_{year}.csv"
                try:
                    text = fetch_text(url, timeout=20)
                    if not text:
                        continue
                    rows = parse_sackmann_csv(text, 'Challenger', year)
                    if rows:
                        ch.insert('tennis_matches', rows)
                        total += len(rows)
                    time.sleep(0.3)
                except Exception:
                    pass

    print(f"✅ Теннис: {total:,} матчей загружено")
    return total


# ═══════════════════════════════════════════════════════════════════════
#  БАСКЕТБОЛ — balldontlie.io (NBA, бесплатный API, без ключа)
#  Rate limit: 30 req/min на free tier
# ═══════════════════════════════════════════════════════════════════════

BDL_BASE = "https://api.balldontlie.io/v1"

def fetch_bdl(endpoint, params=''):
    import os
    url = f"{BDL_BASE}/{endpoint}?{params}&per_page=100"
    headers = {'User-Agent': 'BetQuant-ETL/1.0'}
    key = os.getenv('BDL_API_KEY', '')
    if key:
        headers['Authorization'] = key
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode('utf-8'))

def scrape_nba_games(ch, seasons_back=3):
    """
    Загружает NBA матчи через balldontlie.io
    Бесплатный тир: без API ключа, 30 req/min
    """
    print("\n🏀 БАСКЕТБОЛ NBA (balldontlie.io)")
    total = 0
    current_year = date.today().year

    for season in range(current_year - seasons_back, current_year):
        print(f"  NBA сезон {season}...")
        cursor = None
        page_count = 0
        season_rows = []

        while True:
            try:
                params = f"seasons[]={season}"
                if cursor:
                    params += f"&cursor={cursor}"
                data = fetch_bdl('games', params)
                games = data.get('data', [])
                cursor = data.get('meta', {}).get('next_cursor')

                for g in games:
                    if g.get('status') != 'Final':
                        continue

                    home = g.get('home_team', {})
                    away = g.get('visitor_team', {})
                    dt   = g.get('date', '')[:10]
                    hpts = safe_int(g.get('home_team_score', 0))
                    apts = safe_int(g.get('visitor_team_score', 0))

                    if not dt or (hpts == 0 and apts == 0):
                        continue

                    result = 'H' if hpts > apts else ('A' if apts > hpts else 'D')

                    row = {
                        'match_id':  str(g.get('id', make_id(dt, home.get('full_name', ''), away.get('full_name', '')))),
                        'source':    'balldontlie',
                        'date':      dt,
                        'season':    f"{season}-{str(season+1)[-2:]}",
                        'league':    'NBA',
                        'home_team': home.get('full_name', home.get('name', '')),
                        'away_team': away.get('full_name', away.get('name', '')),
                        'home_pts':  hpts,
                        'away_pts':  apts,
                        'result':    result,
                        # Поквартально если доступно
                        'home_pts_q1': 0, 'away_pts_q1': 0,
                        'home_pts_q2': 0, 'away_pts_q2': 0,
                        'home_pts_q3': 0, 'away_pts_q3': 0,
                        'home_pts_q4': 0, 'away_pts_q4': 0,
                        'went_to_ot': 0,
                    }
                    season_rows.append(row)

                page_count += 1
                if not cursor or page_count > 150:
                    break

                time.sleep(2.1)  # 30 req/min = 2 сек между запросами

            except urllib.error.HTTPError as e:
                if e.code == 429:
                    print(f"    Rate limit, ждём 60 сек...")
                    time.sleep(60)
                    continue
                break
            except Exception as e:
                print(f"    ✗ {e}")
                break

        if season_rows:
            ch.insert('basketball_matches', season_rows)
            ch.log('basketball', 'balldontlie', 'NBA', str(season), len(season_rows), 'ok')
            print(f"    ✓ NBA {season}: {len(season_rows)} матчей")
            total += len(season_rows)

    return total

def scrape_nba_stats(ch, seasons_back=2):
    """
    Загружает детальную статистику команд по матчам NBA
    (FG%, 3P%, FT%, REB, AST, TOV, STL, BLK)
    """
    print("\n🏀 NBA Team Stats (balldontlie.io)")
    total = 0
    current_year = date.today().year

    for season in range(current_year - seasons_back, current_year):
        cursor = None
        batch = []

        while True:
            try:
                params = f"seasons[]={season}"
                if cursor:
                    params += f"&cursor={cursor}"
                data = fetch_bdl('stats', params)
                items = data.get('data', [])
                cursor = data.get('meta', {}).get('next_cursor')

                for s in items:
                    game = s.get('game', {})
                    team = s.get('team', {})
                    # Эти данные — индивидуальные, нам нужны командные
                    # balldontlie возвращает по игроку, мы агрегируем на уровне матча
                    # Для упрощения — skip player stats, только матчи

                if not cursor:
                    break
                time.sleep(2.1)

            except Exception:
                break

    return total


# ═══════════════════════════════════════════════════════════════════════
#  БЕЙСБОЛ — Sean Lahman Database (открытые данные)
#  http://www.seanlahman.com/baseball-archive/statistics/
# ═══════════════════════════════════════════════════════════════════════

LAHMAN_BASE = "https://github.com/chadwickbureau/baseballdatabank/raw/master/core"

def scrape_baseball_games(ch, years_back=5):
    """
    Загружает результаты MLB игр из Lahman/Chadwick Baseball Databank
    """
    print("\n⚾ БЕЙСБОЛ MLB (Chadwick Databank)")
    current_year = date.today().year
    total = 0

    # Games table
    url = f"{LAHMAN_BASE}/Games.csv"
    print(f"  Загружаем {url}...")
    try:
        text = fetch_text(url, timeout=60)
        if not text:
            print("  ✗ Games.csv недоступен")
            return 0

        reader = csv.DictReader(io.StringIO(text))
        rows   = []

        for raw in reader:
            year = safe_int(raw.get('yearID', 0))
            if year < current_year - years_back:
                continue

            date_str = f"{year}-{raw.get('month', '01').zfill(2)}-{raw.get('day', '01').zfill(2)}"
            home = raw.get('teamIDhome', '').strip()
            away = raw.get('teamIDvisit', '').strip()
            if not home or not away:
                continue

            hr = safe_int(raw.get('Hruns', raw.get('HRuns', raw.get('homeScore', 0))))
            ar = safe_int(raw.get('Vruns', raw.get('VRuns', raw.get('visitorScore', 0))))
            result = 'H' if hr > ar else ('A' if ar > hr else 'D')

            rows.append({
                'match_id':  make_id(date_str, home, away, str(year)),
                'date':      date_str,
                'season':    str(year),
                'league':    raw.get('lgIDhome', 'MLB'),
                'home_team': home,
                'away_team': away,
                'home_runs': hr,
                'away_runs': ar,
                'home_hits': safe_int(raw.get('Hhits', 0)),
                'away_hits': safe_int(raw.get('Vhits', 0)),
                'home_errors': safe_int(raw.get('Herrors', 0)),
                'away_errors': safe_int(raw.get('Verrors', 0)),
                'innings':   safe_int(raw.get('innings', 9)) or 9,
                'result':    result,
            })

            if len(rows) >= 1000:
                ch.insert('baseball_matches', rows)
                total += len(rows)
                rows = []

        if rows:
            ch.insert('baseball_matches', rows)
            total += len(rows)

        ch.log('baseball', 'chadwick', 'MLB', f'{current_year-years_back}-{current_year}', total, 'ok')
        print(f"  ✓ MLB: {total:,} матчей загружено")

    except Exception as e:
        print(f"  ✗ {e}")

    return total


# ═══════════════════════════════════════════════════════════════════════
#  РЕГБИ — открытые данные
# ═══════════════════════════════════════════════════════════════════════

RUGBY_URLS = {
    'Six Nations': 'https://raw.githubusercontent.com/openfootball/rugby.json/master/en.json',
    'Rugby World Cup': 'https://raw.githubusercontent.com/openfootball/world-cup.json/master/rugby.json',
}

def scrape_rugby(ch):
    """Загружает данные регби из open datasets"""
    print("\n🏉 РЕГБИ (openfootball)")
    total = 0

    for tournament, url in RUGBY_URLS.items():
        try:
            data = fetch_json(url, timeout=20)
            if not data:
                continue
            rows = []
            for round_data in data.get('rounds', []):
                for m in round_data.get('matches', []):
                    dt = m.get('date', '')
                    team1 = m.get('team1', {})
                    team2 = m.get('team2', {})
                    score = m.get('score', {})
                    if not dt or not team1 or not team2:
                        continue
                    h_pts = safe_int(score.get('ft', [0, 0])[0] if isinstance(score.get('ft'), list) else 0)
                    a_pts = safe_int(score.get('ft', [0, 0])[1] if isinstance(score.get('ft'), list) else 0)
                    rows.append({
                        'match_id':  make_id(dt, str(team1), str(team2), tournament),
                        'date':      dt,
                        'season':    dt[:4],
                        'league':    tournament,
                        'home_team': str(team1.get('name', team1)) if isinstance(team1, dict) else str(team1),
                        'away_team': str(team2.get('name', team2)) if isinstance(team2, dict) else str(team2),
                        'home_pts':  h_pts,
                        'away_pts':  a_pts,
                        'result':    'H' if h_pts > a_pts else ('A' if a_pts > h_pts else 'D'),
                    })
            if rows:
                # Сохраняем в basketball_matches (структура аналогична)
                ch.insert('basketball_matches', rows)
                total += len(rows)
                print(f"  ✓ {tournament}: {len(rows)} матчей")
        except Exception as e:
            print(f"  ✗ {tournament}: {e}")

    return total


# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='BetQuant Other Sports ETL')
    parser.add_argument('--years',    type=int, default=5)
    parser.add_argument('--ch-host', default='http://localhost:8123')
    parser.add_argument('--ch-db',   default='betquant')
    parser.add_argument('--ch-user', default='default')
    parser.add_argument('--ch-pass', default='')
    parser.add_argument('--skip-tennis',     action='store_true')
    parser.add_argument('--skip-basketball', action='store_true')
    parser.add_argument('--skip-baseball',   action='store_true')
    args = parser.parse_args()

    ch = ClickHouseClient(args.ch_host, args.ch_db, args.ch_user, args.ch_pass)
    try:
        ch.query("SELECT 1")
        print("✅ ClickHouse подключён")
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)

    totals = {}

    if not args.skip_tennis:
        totals['tennis'] = scrape_tennis(ch, args.years)

    if not args.skip_basketball:
        totals['basketball'] = scrape_nba_games(ch, min(args.years, 3))

    if not args.skip_baseball:
        totals['baseball'] = scrape_baseball_games(ch, args.years)

    scrape_rugby(ch)

    print(f"\n{'='*60}")
    print(f"📈 ИТОГ:")
    for sport, n in totals.items():
        print(f"  {sport:<20}: {n:>8,}")
    try:
        for t in ['tennis_matches', 'basketball_matches', 'baseball_matches']:
            print(f"  CH {t:<22}: {ch.count(t):>8,}")
    except Exception:
        pass

if __name__ == '__main__':
    main()
