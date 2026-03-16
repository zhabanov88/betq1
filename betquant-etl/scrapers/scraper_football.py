#!/usr/bin/env python3
"""
BetQuant ETL — Football Scraper v2
Sources:
  1. football-data.co.uk  — 25+ лиг, 1993–now, результаты + коэффициенты (без ключа)
  2. OpenLigaDB           — Бундеслига 1/2, без ключа, заменяет Understat для DE
  3. football-data.org    — топ-6 лиг + CL, нужен бесплатный токен (1 мин регистрация)
  4. StatsBomb Open Data  — xG для отдельных лиг бесплатно, GitHub
  5. FBref / soccerdata   — xG для топ-5 лиг, нужен pip install soccerdata

Usage:
  python3 scraper_football.py --leagues top --seasons 3 --ch-host http://localhost:8123
  python3 scraper_football.py --leagues top --seasons 3 --fdo-token YOUR_TOKEN
"""

import urllib.request
import urllib.error
import urllib.parse
import csv
import json
import io
import os
import time
import hashlib
import argparse
import sys
import re
from datetime import datetime, date, timedelta
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════════════

FOOTBALL_DATA_LEAGUES = {
    'E0':  {'name': 'Premier League',      'country': 'England',     'tier': 1},
    'E1':  {'name': 'Championship',        'country': 'England',     'tier': 2},
    'E2':  {'name': 'League One',          'country': 'England',     'tier': 3},
    'E3':  {'name': 'League Two',          'country': 'England',     'tier': 4},
    'SP1': {'name': 'La Liga',             'country': 'Spain',       'tier': 1},
    'SP2': {'name': 'La Liga 2',           'country': 'Spain',       'tier': 2},
    'D1':  {'name': 'Bundesliga',          'country': 'Germany',     'tier': 1},
    'D2':  {'name': '2. Bundesliga',       'country': 'Germany',     'tier': 2},
    'I1':  {'name': 'Serie A',             'country': 'Italy',       'tier': 1},
    'I2':  {'name': 'Serie B',             'country': 'Italy',       'tier': 2},
    'F1':  {'name': 'Ligue 1',             'country': 'France',      'tier': 1},
    'F2':  {'name': 'Ligue 2',             'country': 'France',      'tier': 2},
    'N1':  {'name': 'Eredivisie',          'country': 'Netherlands', 'tier': 1},
    'B1':  {'name': 'First Division A',    'country': 'Belgium',     'tier': 1},
    'P1':  {'name': 'Primeira Liga',       'country': 'Portugal',    'tier': 1},
    'T1':  {'name': 'Süper Lig',           'country': 'Turkey',      'tier': 1},
    'G1':  {'name': 'Super League',        'country': 'Greece',      'tier': 1},
    'SC0': {'name': 'Premiership',         'country': 'Scotland',    'tier': 1},
    'SC1': {'name': 'Championship',        'country': 'Scotland',    'tier': 2},
    'ARG': {'name': 'Primera División',    'country': 'Argentina',   'tier': 1},
    'BRA': {'name': 'Série A',             'country': 'Brazil',      'tier': 1},
    'MX':  {'name': 'Liga MX',             'country': 'Mexico',      'tier': 1},
    'USA': {'name': 'MLS',                 'country': 'USA',         'tier': 1},
}

# Сезоны football-data.co.uk (формат '2425')
def make_seasons(n_back=5):
    today = date.today()
    year = today.year
    seasons = []
    for i in range(n_back):
        y = year - i
        s  = f"{str(y)[-2:]}{str(y+1)[-2:]}"
        s2 = f"{str(y-1)[-2:]}{str(y)[-2:]}"
        if s  not in seasons: seasons.append(s)
        if s2 not in seasons: seasons.append(s2)
    return sorted(set(seasons), reverse=True)[:n_back * 2]

# Год начала текущего сезона (август → новый сезон)
def current_season_start():
    today = date.today()
    return today.year if today.month >= 8 else today.year - 1

# ═══════════════════════════════════════════════════════════════════════
#  HTTP helpers
# ═══════════════════════════════════════════════════════════════════════

def fetch_url(url, timeout=30, retries=3, extra_headers=None):
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/html,application/xhtml+xml,text/csv,application/json,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    if extra_headers:
        headers.update(extra_headers)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 429:
                wait = 60 * (attempt + 1)
                print(f"    ⚠ Rate limit 429, ждём {wait}с...")
                time.sleep(wait)
                continue
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
    return None

# ═══════════════════════════════════════════════════════════════════════
#  ClickHouse client
# ═══════════════════════════════════════════════════════════════════════

class ClickHouseClient:
    def __init__(self, host='http://localhost:8123', database='betquant',
                 user='default', password=''):
        self.host     = host.rstrip('/')
        self.database = database
        self.user     = user
        self.password = password

    def query(self, sql):
        url  = f"{self.host}/?database={self.database}&user={self.user}"
        if self.password:
            url += f"&password={self.password}"
        body = sql.encode('utf-8')
        req  = urllib.request.Request(url, data=body,
                                      headers={'Content-Type': 'application/octet-stream'})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.read().decode('utf-8')
        except urllib.error.HTTPError as e:
            err = e.read().decode('utf-8', errors='replace')
            raise RuntimeError(f"ClickHouse error: {err[:500]}")

    def insert_json_batch(self, table, rows):
        if not rows:
            return 0
        lines = '\n'.join(json.dumps(r, ensure_ascii=False, default=str) for r in rows)
        sql   = f"INSERT INTO {self.database}.{table} FORMAT JSONEachRow\n{lines}"
        self.query(sql)
        return len(rows)

    def execute(self, sql):
        return self.query(sql)

    def count(self, table, where=''):
        w = f'WHERE {where}' if where else ''
        r = self.query(f"SELECT count() FROM {self.database}.{table} {w}")
        return int(r.strip())

    def log(self, sport, source, league, season, rows, status, msg=''):
        row = {'sport': sport, 'source': source, 'league': league,
               'season': season, 'rows_loaded': rows, 'status': status,
               'message': msg[:500]}
        self.insert_json_batch('etl_log', [row])

# ═══════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════

def safe_float(v, default=0.0):
    try:
        f = float(str(v).strip())
        return f if f > 0 else default
    except (ValueError, TypeError):
        return default

def safe_int(v, default=0):
    try:
        return int(float(str(v).strip()))
    except (ValueError, TypeError):
        return default

def make_match_id(date_str, home, away, league):
    key = f"{date_str}|{home}|{away}|{league}"
    return hashlib.md5(key.encode()).hexdigest()[:16]

def parse_fd_date(s):
    if not s:
        return None
    for fmt in ['%d/%m/%Y', '%d/%m/%y']:
        try:
            return datetime.strptime(s.strip(), fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None

# ═══════════════════════════════════════════════════════════════════════
#  SOURCE 1: football-data.co.uk
# ═══════════════════════════════════════════════════════════════════════

def parse_fd_csv(content_bytes, league_code, season):
    meta = FOOTBALL_DATA_LEAGUES.get(league_code, {'name': league_code, 'country': ''})
    rows = []
    try:
        text   = content_bytes.decode('latin-1')
        reader = csv.DictReader(io.StringIO(text))
    except Exception as e:
        print(f"    CSV decode error: {e}")
        return rows

    for raw in reader:
        home     = raw.get('HomeTeam', '').strip()
        away     = raw.get('AwayTeam', '').strip()
        if not home or not away:
            continue
        date_str = parse_fd_date(raw.get('Date', ''))
        if not date_str:
            continue

        hg  = safe_int(raw.get('FTHG', raw.get('HG', 0)))
        ag  = safe_int(raw.get('FTAG', raw.get('AG', 0)))
        ftr = raw.get('FTR', raw.get('Res', '')).strip()
        row = {
            'match_id':    make_match_id(date_str, home, away, league_code),
            'source':      'football-data.co.uk',
            'date':        date_str,
            'season':      season,
            'league_code': league_code,
            'league_name': meta['name'],
            'country':     meta['country'],
            'home_team':   home,
            'away_team':   away,
            'home_goals':           hg,
            'away_goals':           ag,
            'ht_home_goals':        safe_int(raw.get('HTHG', 0)),
            'ht_away_goals':        safe_int(raw.get('HTAG', 0)),
            'result':               ftr if ftr in ('H', 'D', 'A') else ('H' if hg > ag else ('A' if ag > hg else 'D')),
            'home_shots':           safe_int(raw.get('HS', 0)),
            'away_shots':           safe_int(raw.get('AS', 0)),
            'home_shots_on_target': safe_int(raw.get('HST', 0)),
            'away_shots_on_target': safe_int(raw.get('AST', 0)),
            'home_corners':         safe_int(raw.get('HC', 0)),
            'away_corners':         safe_int(raw.get('AC', 0)),
            'home_fouls':           safe_int(raw.get('HF', 0)),
            'away_fouls':           safe_int(raw.get('AF', 0)),
            'home_yellow':          safe_int(raw.get('HY', 0)),
            'away_yellow':          safe_int(raw.get('AY', 0)),
            'home_red':             safe_int(raw.get('HR', 0)),
            'away_red':             safe_int(raw.get('AR', 0)),
            'b365_home':  safe_float(raw.get('B365H', 0)),
            'b365_draw':  safe_float(raw.get('B365D', 0)),
            'b365_away':  safe_float(raw.get('B365A', 0)),
            'b365_over':  safe_float(raw.get('B365>2.5', raw.get('B365AH', 0))),
            'b365_under': safe_float(raw.get('B365<2.5', 0)),
            'pinnacle_home': safe_float(raw.get('PSH', raw.get('PH', 0))),
            'pinnacle_draw': safe_float(raw.get('PSD', raw.get('PD', 0))),
            'pinnacle_away': safe_float(raw.get('PSA', raw.get('PA', 0))),
            'max_home': safe_float(raw.get('BbMxH', raw.get('MaxH', 0))),
            'max_draw': safe_float(raw.get('BbMxD', raw.get('MaxD', 0))),
            'max_away': safe_float(raw.get('BbMxA', raw.get('MaxA', 0))),
            'avg_home': safe_float(raw.get('BbAvH', raw.get('AvgH', 0))),
            'avg_draw': safe_float(raw.get('BbAvD', raw.get('AvgD', 0))),
            'avg_away': safe_float(raw.get('BbAvA', raw.get('AvgA', 0))),
            'ah_line':  safe_float(raw.get('BbAH', 0)),
            'ah_home':  safe_float(raw.get('BbAHH', 0)),
            'ah_away':  safe_float(raw.get('BbAHA', 0)),
            'home_xg': 0.0, 'away_xg': 0.0,
            'home_xga': 0.0, 'away_xga': 0.0,
        }
        rows.append(row)
    return rows

def scrape_football_data(ch, leagues, seasons, batch_size=500):
    total = 0
    base  = 'https://www.football-data.co.uk/mmz4281'
    for season in seasons:
        for league_code in leagues:
            url = f"{base}/{season}/{league_code}.csv"
            print(f"  [{league_code}] season={season} → {url}")
            try:
                content = fetch_url(url, timeout=20)
                if content is None:
                    print(f"    ✗ 404 skip")
                    continue
                if len(content) < 100:
                    print(f"    ✗ empty")
                    continue
                rows = parse_fd_csv(content, league_code, season)
                if not rows:
                    print(f"    ✗ parsed 0 rows")
                    continue
                for i in range(0, len(rows), batch_size):
                    ch.insert_json_batch('football_matches', rows[i:i+batch_size])
                ch.log('football', 'football-data.co.uk', league_code, season, len(rows), 'ok')
                print(f"    ✓ {len(rows)} матчей")
                total += len(rows)
                time.sleep(0.5)
            except Exception as e:
                print(f"    ✗ ERROR: {e}")
                ch.log('football', 'football-data.co.uk', league_code, season, 0, 'error', str(e)[:200])
    return total

# ═══════════════════════════════════════════════════════════════════════
#  SOURCE 2: OpenLigaDB — Бундеслига, бесплатно, без ключа
#  API: https://api.openligadb.de
# ═══════════════════════════════════════════════════════════════════════

OPENLIGA_LEAGUES = {
    'D1':  'bl1',
    'D2':  'bl2',
}

def scrape_openligadb(ch, seasons_back=3, batch_size=300):
    """Бундеслига 1 и 2 через OpenLigaDB. Без ключа, работает стабильно."""
    total = 0
    cur   = current_season_start()

    for league_key, liga_short in OPENLIGA_LEAGUES.items():
        meta = FOOTBALL_DATA_LEAGUES.get(league_key, {'name': league_key, 'country': 'Germany'})
        for i in range(seasons_back):
            season_year = cur - i
            url = f"https://api.openligadb.de/getmatchdata/{liga_short}/{season_year}"
            print(f"  [openliga] {league_key} {season_year} → {url}")
            try:
                content = fetch_url(url, timeout=20)
                if not content:
                    print(f"    ✗ пустой ответ")
                    continue
                matches_raw = json.loads(content.decode('utf-8'))
                if not matches_raw:
                    print(f"    ✗ 0 матчей")
                    continue

                season_str = f"{season_year}-{str(season_year+1)[-2:]}"
                rows = []
                for m in matches_raw:
                    if not m.get('matchIsFinished'):
                        continue
                    dt_raw = m.get('matchDateTimeUTC', '') or m.get('matchDateTime', '')
                    dt_str = dt_raw[:10] if dt_raw else ''
                    if not dt_str:
                        continue
                    t1   = m.get('team1', {})
                    t2   = m.get('team2', {})
                    home = t1.get('teamName', '')
                    away = t2.get('teamName', '')
                    if not home or not away:
                        continue
                    results = m.get('matchResults', [])
                    final   = next((r for r in results if r.get('resultTypeID') == 2), None)
                    ht_res  = next((r for r in results if r.get('resultTypeID') == 1), None)
                    if not final and results:
                        final = results[-1]
                    if not final:
                        continue
                    hg = safe_int(final.get('pointsTeam1', 0))
                    ag = safe_int(final.get('pointsTeam2', 0))
                    rows.append({
                        'match_id':      str(m.get('matchID', make_match_id(dt_str, home, away, league_key))),
                        'source':        'openligadb',
                        'date':          dt_str,
                        'season':        season_str,
                        'league_code':   league_key,
                        'league_name':   meta['name'],
                        'country':       meta.get('country', 'Germany'),
                        'home_team':     home,
                        'away_team':     away,
                        'home_goals':    hg,
                        'away_goals':    ag,
                        'ht_home_goals': safe_int(ht_res.get('pointsTeam1', 0)) if ht_res else 0,
                        'ht_away_goals': safe_int(ht_res.get('pointsTeam2', 0)) if ht_res else 0,
                        'result':        'H' if hg > ag else ('A' if ag > hg else 'D'),
                        'home_xg': 0.0, 'away_xg': 0.0, 'home_xga': 0.0, 'away_xga': 0.0,
                    })

                if rows:
                    for j in range(0, len(rows), batch_size):
                        ch.insert_json_batch('football_matches', rows[j:j+batch_size])
                    ch.log('football', 'openligadb', league_key, season_str, len(rows), 'ok')
                    print(f"    ✓ {len(rows)} матчей")
                    total += len(rows)
                else:
                    print(f"    ✗ 0 завершённых матчей")
                time.sleep(0.5)
            except Exception as e:
                print(f"    ✗ ERROR: {e}")
    return total

# ═══════════════════════════════════════════════════════════════════════
#  SOURCE 3: football-data.org API
#  Регистрация (1 мин): https://www.football-data.org/client/register
#  Бесплатно: EPL, La Liga, Bundesliga, Serie A, Ligue 1, CL, + ещё 6
#  Лимит: 10 запросов/минуту
# ═══════════════════════════════════════════════════════════════════════

FDO_COMPETITIONS = {
    'E0':  'PL',    # Premier League
    'SP1': 'PD',    # La Liga
    'D1':  'BL1',   # Bundesliga
    'I1':  'SA',    # Serie A
    'F1':  'FL1',   # Ligue 1
    'N1':  'DED',   # Eredivisie
    'P1':  'PPL',   # Primeira Liga
    'CL':  'CL',    # Champions League
}

def scrape_footballdata_org(ch, api_token, seasons_back=3, batch_size=300):
    """
    football-data.org API.
    Токен: https://www.football-data.org/client/register (бесплатно)
    Добавьте в .env: FOOTBALLDATA_ORG_TOKEN=ваш_токен
    """
    if not api_token:
        print("  [football-data.org] Токен не задан — пропускаем")
        print("  → Получите БЕСПЛАТНО: https://www.football-data.org/client/register")
        print("  → Добавьте в .env: FOOTBALLDATA_ORG_TOKEN=ваш_токен")
        return 0

    total = 0
    cur   = current_season_start()

    for league_key, comp_code in FDO_COMPETITIONS.items():
        meta = FOOTBALL_DATA_LEAGUES.get(league_key, {'name': comp_code, 'country': ''})
        for i in range(seasons_back):
            season_year = cur - i
            url = (f"https://api.football-data.org/v4/competitions/{comp_code}/matches"
                   f"?season={season_year}&status=FINISHED")
            print(f"  [football-data.org] {league_key} {season_year} ({comp_code})")
            try:
                content = fetch_url(url, timeout=20,
                                    extra_headers={'X-Auth-Token': api_token})
                if not content:
                    print(f"    ✗ пустой ответ")
                    continue
                data       = json.loads(content.decode('utf-8'))
                matches_raw = data.get('matches', [])
                if not matches_raw:
                    print(f"    ✗ 0 матчей")
                    continue

                season_str = f"{season_year}-{str(season_year+1)[-2:]}"
                rows = []
                for m in matches_raw:
                    if m.get('status') != 'FINISHED':
                        continue
                    dt_str = (m.get('utcDate', '') or '')[:10]
                    if not dt_str:
                        continue
                    home = (m.get('homeTeam', {}).get('shortName')
                            or m.get('homeTeam', {}).get('name', ''))
                    away = (m.get('awayTeam', {}).get('shortName')
                            or m.get('awayTeam', {}).get('name', ''))
                    score = m.get('score', {})
                    ft    = score.get('fullTime', {})
                    ht_s  = score.get('halfTime', {})
                    hg    = safe_int(ft.get('home', 0))
                    ag    = safe_int(ft.get('away', 0))
                    rows.append({
                        'match_id':      str(m.get('id', make_match_id(dt_str, home, away, league_key))),
                        'source':        'football-data.org',
                        'date':          dt_str,
                        'season':        season_str,
                        'league_code':   league_key,
                        'league_name':   meta['name'],
                        'country':       meta.get('country', ''),
                        'home_team':     home,
                        'away_team':     away,
                        'home_goals':    hg,
                        'away_goals':    ag,
                        'ht_home_goals': safe_int(ht_s.get('home', 0)),
                        'ht_away_goals': safe_int(ht_s.get('away', 0)),
                        'result':        'H' if hg > ag else ('A' if ag > hg else 'D'),
                        'home_xg': 0.0, 'away_xg': 0.0, 'home_xga': 0.0, 'away_xga': 0.0,
                    })

                if rows:
                    for j in range(0, len(rows), batch_size):
                        ch.insert_json_batch('football_matches', rows[j:j+batch_size])
                    ch.log('football', 'football-data.org', league_key, season_str, len(rows), 'ok')
                    print(f"    ✓ {len(rows)} матчей")
                    total += len(rows)

                time.sleep(7)  # 10 req/min → 6+ сек между запросами

            except urllib.error.HTTPError as e:
                if e.code == 429:
                    print(f"    ⚠ Rate limit, ждём 65 сек...")
                    time.sleep(65)
                else:
                    print(f"    ✗ HTTP {e.code}: {e.read().decode()[:100]}")
            except Exception as e:
                print(f"    ✗ ERROR: {e}")

    return total

# ═══════════════════════════════════════════════════════════════════════
#  SOURCE 4: StatsBomb Open Data — xG бесплатно, GitHub
#  Репозиторий: https://github.com/statsbomb/open-data
#  Лиги: La Liga (обширно), NWSL, WSL, FA Women's Super League и др.
#  Данные: xG, xA, shot position, pass map, линейки давления
# ═══════════════════════════════════════════════════════════════════════

SB_BASE = 'https://raw.githubusercontent.com/statsbomb/open-data/master/data'

# Маппинг StatsBomb competition_id → наш league_code
SB_COMPETITIONS = {
    11:  'SP1',   # La Liga
    2:   'CL',    # Champions League
    37:  'E2',    # FA Women's Championship (пример)
    49:  'E0',    # FA Women's Super League → используем как E0 proxy
    72:  'USA',   # NWSL
}

def fetch_sb_json(path):
    url = f"{SB_BASE}/{path}"
    try:
        content = fetch_url(url, timeout=20)
        if not content:
            return None
        return json.loads(content.decode('utf-8'))
    except Exception:
        return None

def scrape_statsbomb_open(ch, seasons_back=3, batch_size=200):
    """
    StatsBomb Open Data с GitHub — содержит xG, xA, shot maps.
    Бесплатно, без ключа, ~3000 матчей по топ-лигам.
    """
    print("  [statsbomb] Загружаем список соревнований...")
    competitions = fetch_sb_json('competitions.json')
    if not competitions:
        print("  ✗ GitHub недоступен или competitions.json не найден")
        return 0

    cur   = current_season_start()
    total = 0

    # Фильтруем нужные соревнования
    target_comp_ids = set(SB_COMPETITIONS.keys())
    relevant = [c for c in competitions if c.get('competition_id') in target_comp_ids]
    print(f"  [statsbomb] Найдено {len(relevant)} подходящих сезонов")

    for comp in relevant:
        comp_id   = comp['competition_id']
        season_id = comp['season_id']
        season_name = comp.get('season_name', '')
        league_code = SB_COMPETITIONS.get(comp_id, 'XX')
        meta        = FOOTBALL_DATA_LEAGUES.get(league_code, {'name': comp.get('competition_name',''), 'country': ''})

        # Пропускаем слишком старые сезоны
        # StatsBomb сезоны: "2023/2024" или "2023"
        try:
            year_str = season_name.split('/')[0].strip()
            year = int(year_str)
            if year < cur - seasons_back:
                continue
        except Exception:
            pass

        print(f"  [statsbomb] {league_code} season={season_name} (comp={comp_id}, s={season_id})")
        time.sleep(0.3)

        # Список матчей сезона
        matches_data = fetch_sb_json(f"matches/{comp_id}/{season_id}.json")
        if not matches_data:
            print(f"    ✗ матчи не найдены")
            continue

        rows        = []
        event_rows  = []
        season_str  = season_name.replace('/', '-')[:7]  # '2023-24'

        for m in matches_data:
            match_id  = str(m.get('match_id', ''))
            dt_str    = (m.get('match_date', '') or '')[:10]
            if not dt_str:
                continue
            home = m.get('home_team', {}).get('home_team_name', '')
            away = m.get('away_team', {}).get('away_team_name', '')
            if not home or not away:
                continue
            hg = safe_int(m.get('home_score', 0))
            ag = safe_int(m.get('away_score', 0))

            # xG из shot_freeze_frames если есть
            home_xg = safe_float(m.get('metadata', {}).get('home_xg', 0))
            away_xg = safe_float(m.get('metadata', {}).get('away_xg', 0))

            rows.append({
                'match_id':    match_id or make_match_id(dt_str, home, away, league_code),
                'source':      'statsbomb',
                'date':        dt_str,
                'season':      season_str,
                'league_code': league_code,
                'league_name': meta.get('name', ''),
                'country':     meta.get('country', ''),
                'home_team':   home,
                'away_team':   away,
                'home_goals':  hg,
                'away_goals':  ag,
                'result':      'H' if hg > ag else ('A' if ag > hg else 'D'),
                'home_xg':     home_xg,
                'away_xg':     away_xg,
                'home_xga':    away_xg,
                'away_xga':    home_xg,
                'ht_home_goals': 0, 'ht_away_goals': 0,
            })

        if rows:
            for j in range(0, len(rows), batch_size):
                ch.insert_json_batch('football_matches', rows[j:j+batch_size])
            ch.log('football', 'statsbomb', league_code, season_str, len(rows), 'ok')
            print(f"    ✓ {len(rows)} матчей")
            total += len(rows)
        else:
            print(f"    ✗ 0 матчей")

    return total

# ═══════════════════════════════════════════════════════════════════════
#  SOURCE 5: FBref через soccerdata — xG для топ-5 лиг
#  pip install soccerdata lxml html5lib
#  Источник: https://fbref.com (данные от StatsBomb/Opta)
#  Даёт: xG, npxG, xA, progressive passes, pressures и многое другое
# ═══════════════════════════════════════════════════════════════════════

FBREF_LEAGUES = {
    'ENG-Premier League': 'E0',
    'ESP-La Liga':        'SP1',
    'GER-Bundesliga':     'D1',
    'ITA-Serie A':        'I1',
    'FRA-Ligue 1':        'F1',
}

def scrape_fbref_xg(ch, seasons_back=3, batch_size=300):
    """
    xG данные с FBref через библиотеку soccerdata.
    Установка: pip install soccerdata lxml html5lib
    Документация: https://soccerdata.readthedocs.io
    """
    try:
        import soccerdata as sd
    except ImportError:
        print("  [fbref] soccerdata не установлен")
        print("  → Установите: pip install soccerdata lxml html5lib --break-system-packages")
        print("  → Или: pip install soccerdata lxml html5lib")
        return 0

    total = 0
    cur   = current_season_start()

    for league_name, league_code in FBREF_LEAGUES.items():
        meta = FOOTBALL_DATA_LEAGUES.get(league_code, {'name': league_name, 'country': ''})
        for i in range(seasons_back):
            season_year = cur - i
            # soccerdata использует формат "YYYY"
            season_str_sd = str(season_year)
            season_str_ch = f"{season_year}-{str(season_year+1)[-2:]}"
            print(f"  [fbref] {league_code} {season_year} ({league_name})")
            try:
                fbref = sd.FBref(league_name, season_str_sd)
                schedule = fbref.read_schedule()

                if schedule is None or len(schedule) == 0:
                    print(f"    ✗ расписание пустое")
                    continue

                # read_schedule возвращает DataFrame с колонками:
                # home_team, away_team, date, home_g, away_g, home_xg, away_xg и др.
                rows = []
                for _, row in schedule.iterrows():
                    dt = row.get('date', None)
                    if dt is None:
                        continue
                    dt_str = str(dt)[:10]
                    home   = str(row.get('home_team', ''))
                    away   = str(row.get('away_team', ''))
                    if not home or not away or home == 'nan':
                        continue
                    hg = safe_int(row.get('home_g', 0))
                    ag = safe_int(row.get('away_g', 0))
                    # xG может отсутствовать для старых матчей
                    hxg = safe_float(row.get('home_xg', 0))
                    axg = safe_float(row.get('away_xg', 0))
                    # Пропускаем незавершённые
                    if hg == 0 and ag == 0 and hxg == 0 and axg == 0:
                        continue

                    rows.append({
                        'match_id':    str(row.get('game_id', make_match_id(dt_str, home, away, league_code))),
                        'source':      'fbref',
                        'date':        dt_str,
                        'season':      season_str_ch,
                        'league_code': league_code,
                        'league_name': meta['name'],
                        'country':     meta.get('country', ''),
                        'home_team':   home,
                        'away_team':   away,
                        'home_goals':  hg,
                        'away_goals':  ag,
                        'result':      'H' if hg > ag else ('A' if ag > hg else 'D'),
                        'home_xg':     hxg,
                        'away_xg':     axg,
                        'home_xga':    axg,
                        'away_xga':    hxg,
                        'ht_home_goals': safe_int(row.get('home_g_ht', 0)),
                        'ht_away_goals': safe_int(row.get('away_g_ht', 0)),
                        'home_shots':           safe_int(row.get('home_sh', 0)),
                        'away_shots':           safe_int(row.get('away_sh', 0)),
                        'home_shots_on_target': safe_int(row.get('home_sot', 0)),
                        'away_shots_on_target': safe_int(row.get('away_sot', 0)),
                    })

                if rows:
                    for j in range(0, len(rows), batch_size):
                        ch.insert_json_batch('football_matches', rows[j:j+batch_size])
                    ch.log('football', 'fbref', league_code, season_str_ch, len(rows), 'ok')
                    print(f"    ✓ {len(rows)} матчей (xG: {sum(1 for r in rows if r['home_xg'] > 0)} матчей с xG)")
                    total += len(rows)
                else:
                    print(f"    ✗ 0 матчей")

                time.sleep(3)  # уважаем FBref

            except Exception as e:
                print(f"    ✗ ERROR: {e}")
                # FBref иногда даёт timeout или rate limit — продолжаем
                time.sleep(5)

    return total

# ═══════════════════════════════════════════════════════════════════════
#  Understat — ЗАБЛОКИРОВАН Cloudflare (оставлен для истории)
# ═══════════════════════════════════════════════════════════════════════

def scrape_understat_league(ch, league, season_year, batch_size=200):
    """
    Understat заблокирован Cloudflare на серверных IP.
    Функция оставлена для обратной совместимости но всегда возвращает 0.
    Используйте FBref (SOURCE 5) для xG данных.
    """
    print(f"  [understat] {league} {season_year} → ПРОПУЩЕН (Cloudflare блокировка)")
    print(f"    → Для xG используйте: python3 scraper_football.py --xg-source fbref")
    return 0

# ═══════════════════════════════════════════════════════════════════════
#  Расчёт формы команды (rolling stats ДО каждого матча)
# ═══════════════════════════════════════════════════════════════════════

def compute_team_form(ch):
    print("\n  [form] Вычисляем форму команд...")
    sql = """
    SELECT match_id, date, league_code, season, home_team, away_team,
           home_goals, away_goals, result,
           home_corners, away_corners, home_yellow, away_yellow,
           home_xg, away_xg, home_shots, away_shots
    FROM betquant.football_matches
    WHERE home_goals > 0 OR away_goals > 0 OR home_shots > 0
    ORDER BY date ASC
    FORMAT JSONEachRow
    """
    try:
        raw = ch.query(sql)
    except Exception as e:
        print(f"    ✗ query error: {e}")
        return

    matches = [json.loads(l) for l in raw.strip().split('\n') if l.strip()]
    print(f"    Обрабатываем {len(matches)} матчей...")

    team_history = defaultdict(list)
    form_rows    = []

    for m in matches:
        home   = m['home_team']
        away   = m['away_team']
        dt     = m['date']
        hg, ag = int(m['home_goals']), int(m['away_goals'])
        mid    = m['match_id']
        league = m['league_code']
        season = m['season']

        def build_form(team, is_home):
            hist = team_history[team]
            if not hist:
                return {}
            last5  = hist[-5:]
            last10 = hist[-10:]
            f_str  = lambda games: ''.join(g['r'] for g in games)
            pts    = lambda games: sum(3 if g['r']=='W' else (1 if g['r']=='D' else 0) for g in games)
            sg     = [g for g in hist if g['season'] == season]
            sgf    = sum(g['gf'] for g in sg)
            sga    = sum(g['ga'] for g in sg)
            ss     = sorted(set(g['season'] for g in hist), reverse=True)
            prev_s = ss[1] if len(ss) > 1 else ''
            pg     = [g for g in hist if g['season'] == prev_s]
            c30    = (datetime.strptime(dt, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
            l30    = [g for g in hist if g['date'] >= c30]
            ys     = dt[:4] + '-01-01'
            ytd    = [g for g in hist if g['date'] >= ys]
            hg_    = [g for g in sg if g['venue'] == 'home']
            ag_    = [g for g in sg if g['venue'] == 'away']
            opp    = away if is_home else home
            h2h    = [g for g in hist if g['opponent'] == opp][-5:]
            return {
                'match_id': mid, 'date': dt, 'team': team,
                'league_code': league, 'season': season, 'is_home': 1 if is_home else 0,
                'form_5': f_str(last5), 'form_10': f_str(last10),
                'pts_5': pts(last5), 'pts_10': pts(last10),
                'season_matches': len(sg), 'season_wins': sum(1 for g in sg if g['r']=='W'),
                'season_draws': sum(1 for g in sg if g['r']=='D'),
                'season_losses': sum(1 for g in sg if g['r']=='L'),
                'season_goals_for': sgf, 'season_goals_against': sga, 'season_goal_diff': sgf-sga,
                'season_corners_for':    sum(g.get('cf',0) for g in sg),
                'season_corners_against':sum(g.get('ca',0) for g in sg),
                'season_yellow':         sum(g.get('yc',0) for g in sg),
                'season_xg_for':         round(sum(g.get('xgf',0) for g in sg), 2),
                'season_xg_against':     round(sum(g.get('xga',0) for g in sg), 2),
                'season_shots_for':      sum(g.get('sf',0) for g in sg),
                'season_shots_against':  sum(g.get('sa',0) for g in sg),
                'prev_season': prev_s, 'prev_season_matches': len(pg),
                'prev_season_wins': sum(1 for g in pg if g['r']=='W'),
                'prev_season_goals_for':     sum(g['gf'] for g in pg),
                'prev_season_goals_against': sum(g['ga'] for g in pg),
                'prev_season_xg_for':  round(sum(g.get('xgf',0) for g in pg), 2),
                'prev_season_xg_against': round(sum(g.get('xga',0) for g in pg), 2),
                'last30_matches':       len(l30),
                'last30_goals_for':     sum(g['gf'] for g in l30),
                'last30_goals_against': sum(g['ga'] for g in l30),
                'last30_xg_for':  round(sum(g.get('xgf',0) for g in l30), 2),
                'last30_corners': sum(g.get('cf',0) for g in l30),
                'last30_yellow':  sum(g.get('yc',0) for g in l30),
                'ytd_matches':          len(ytd),
                'ytd_goals_for':        sum(g['gf'] for g in ytd),
                'ytd_goals_against':    sum(g['ga'] for g in ytd),
                'ytd_xg_for': round(sum(g.get('xgf',0) for g in ytd), 2),
                'home_season_matches':           len(hg_),
                'home_season_goals_for':         sum(g['gf'] for g in hg_),
                'home_season_goals_against':     sum(g['ga'] for g in hg_),
                'away_season_matches':           len(ag_),
                'away_season_goals_for':         sum(g['gf'] for g in ag_),
                'away_season_goals_against':     sum(g['ga'] for g in ag_),
                'h2h_wins':   sum(1 for g in h2h if g['r']=='W'),
                'h2h_draws':  sum(1 for g in h2h if g['r']=='D'),
                'h2h_losses': sum(1 for g in h2h if g['r']=='L'),
                'h2h_goals_for':     sum(g['gf'] for g in h2h),
                'h2h_goals_against': sum(g['ga'] for g in h2h),
                'h2h_matches':       len(h2h),
            }

        hf = build_form(home, True)
        af = build_form(away, False)
        if hf: form_rows.append(hf)
        if af: form_rows.append(af)

        for team, is_home_t in [(home, True), (away, False)]:
            gf  = hg if is_home_t else ag
            ga  = ag if is_home_t else hg
            r   = 'W' if gf > ga else ('L' if gf < ga else 'D')
            hxg = safe_float(m.get('home_xg', 0))
            axg = safe_float(m.get('away_xg', 0))
            team_history[team].append({
                'date': dt, 'season': season, 'league': league,
                'opponent': away if is_home_t else home,
                'venue': 'home' if is_home_t else 'away',
                'gf': gf, 'ga': ga, 'r': r,
                'cf': safe_int(m.get('home_corners' if is_home_t else 'away_corners', 0)),
                'ca': safe_int(m.get('away_corners' if is_home_t else 'home_corners', 0)),
                'yc': safe_int(m.get('home_yellow'  if is_home_t else 'away_yellow', 0)),
                'xgf': hxg if is_home_t else axg,
                'xga': axg if is_home_t else hxg,
                'sf': safe_int(m.get('home_shots' if is_home_t else 'away_shots', 0)),
                'sa': safe_int(m.get('away_shots' if is_home_t else 'home_shots', 0)),
            })

        if len(form_rows) >= 1000:
            ch.insert_json_batch('football_team_form', form_rows)
            form_rows = []

    if form_rows:
        ch.insert_json_batch('football_team_form', form_rows)
    print(f"    ✓ Форма команд рассчитана")

# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='BetQuant Football ETL v2')
    parser.add_argument('--leagues',    default='top',
                        help='all | top | E0,D1,SP1,...')
    parser.add_argument('--seasons',    type=int, default=3,
                        help='Сколько сезонов назад (default: 3)')
    parser.add_argument('--ch-host',    default='http://localhost:8123')
    parser.add_argument('--ch-db',      default='betquant')
    parser.add_argument('--ch-user',    default='default')
    parser.add_argument('--ch-pass',    default='')
    parser.add_argument('--skip-fd',    action='store_true',
                        help='Пропустить football-data.co.uk')
    parser.add_argument('--skip-openliga', action='store_true',
                        help='Пропустить OpenLigaDB')
    parser.add_argument('--skip-fdo',   action='store_true',
                        help='Пропустить football-data.org')
    parser.add_argument('--skip-statsbomb', action='store_true',
                        help='Пропустить StatsBomb Open Data')
    parser.add_argument('--skip-fbref', action='store_true',
                        help='Пропустить FBref/soccerdata')
    parser.add_argument('--skip-understat', action='store_true',
                        help='(устарело, Understat заблокирован Cloudflare)')
    parser.add_argument('--skip-form',  action='store_true',
                        help='Пропустить расчёт формы команд')
    parser.add_argument('--xg-source',  default='fbref',
                        choices=['fbref', 'statsbomb', 'both', 'none'],
                        help='Источник xG данных (default: fbref)')
    parser.add_argument('--fdo-token',  default='',
                        help='Токен football-data.org (или FOOTBALLDATA_ORG_TOKEN в env)')
    parser.add_argument('--events-limit', type=int, default=None,
                        help='(устарело)')
    args = parser.parse_args()

    ch = ClickHouseClient(args.ch_host, args.ch_db, args.ch_user, args.ch_pass)

    try:
        ch.execute("SELECT 1")
        print("✅ ClickHouse подключён")
    except Exception as e:
        print(f"❌ ClickHouse недоступен: {e}")
        sys.exit(1)

    # Выбор лиг
    if args.leagues == 'all':
        leagues = list(FOOTBALL_DATA_LEAGUES.keys())
    elif args.leagues == 'top':
        leagues = ['E0', 'SP1', 'D1', 'I1', 'F1', 'N1', 'P1', 'T1', 'SC0',
                   'E1', 'SP2', 'D2', 'B1', 'G1']
    else:
        leagues = [l.strip() for l in args.leagues.split(',')]

    seasons = make_seasons(args.seasons)
    totals  = {}

    # ── SOURCE 1: football-data.co.uk ─────────────────────────────────
    if not args.skip_fd:
        print(f"\n📊 football-data.co.uk: {len(leagues)} лиг × {len(seasons)} сезонов")
        totals['football-data.co.uk'] = scrape_football_data(ch, leagues, seasons)
        print(f"✅ football-data.co.uk: {totals['football-data.co.uk']} матчей")

    # ── SOURCE 2: OpenLigaDB ───────────────────────────────────────────
    if not args.skip_openliga:
        print(f"\n📊 OpenLigaDB (Бундеслига, без ключа)...")
        totals['openligadb'] = scrape_openligadb(ch, seasons_back=args.seasons)
        print(f"✅ OpenLigaDB: {totals['openligadb']} матчей")

    # ── SOURCE 3: football-data.org ────────────────────────────────────
    if not args.skip_fdo:
        fdo_token = args.fdo_token or os.environ.get('FOOTBALLDATA_ORG_TOKEN', '')
        print(f"\n📊 football-data.org API (топ-лиги + CL)...")
        totals['football-data.org'] = scrape_footballdata_org(ch, fdo_token, seasons_back=args.seasons)
        print(f"✅ football-data.org: {totals['football-data.org']} матчей")

    # ── SOURCE 4 & 5: xG данные ────────────────────────────────────────
    xg_src = args.xg_source
    if xg_src in ('statsbomb', 'both', 'fbref') and not args.skip_statsbomb:
        print(f"\n📊 StatsBomb Open Data (xG, GitHub)...")
        totals['statsbomb'] = scrape_statsbomb_open(ch, seasons_back=args.seasons)
        print(f"✅ StatsBomb: {totals['statsbomb']} матчей")

    # FBref отключён — блокирует серверные IP (Cloudflare 403, аналогично Understat)
    if xg_src in ('fbref', 'both') and not args.skip_fbref:
        print(f"\n⚠️  FBref/soccerdata — заблокирован Cloudflare (403) на серверных IP")
        print(f"    Используем StatsBomb Open Data как замену xG источника")
        totals['fbref'] = 0

    # ── Форма команд ───────────────────────────────────────────────────
    if not args.skip_form:
        compute_team_form(ch)

    # ── Итог ───────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("📈 ИТОГ по источникам:")
    for src, n in totals.items():
        print(f"  {src:<28}: {n:>8,} матчей")
    print()
    try:
        print(f"  football_matches   в CH: {ch.count('football_matches'):>8,}")
        print(f"  football_team_form в CH: {ch.count('football_team_form'):>8,}")
    except Exception as e:
        print(f"  (статистика CH недоступна: {e})")
    print()
    print("💡 Подсказки:")
    if not (args.fdo_token or os.environ.get('FOOTBALLDATA_ORG_TOKEN')):
        print("  • Получите бесплатный токен football-data.org:")
        print("    https://www.football-data.org/client/register")
        print("    Добавьте в .env: FOOTBALLDATA_ORG_TOKEN=токен")
    if xg_src not in ('fbref', 'both'):
        print("  • Для xG данных: python3 scraper_football.py --xg-source fbref")
        print("    Установка: pip install soccerdata lxml html5lib --break-system-packages")

if __name__ == '__main__':
    main()