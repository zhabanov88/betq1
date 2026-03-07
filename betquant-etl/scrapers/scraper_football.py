#!/usr/bin/env python3
"""
BetQuant ETL — Football Scraper
Sources:
  1. football-data.co.uk — 25+ лиг, с 1993 года, результаты + коэффициенты + базовая статистика
  2. understat.com      — 6 топ-лиг, с 2014, xG + продвинутые метрики + поминутные удары

Usage:
  python3 scraper_football.py --leagues all --seasons 3 --ch-host http://localhost:8123
"""

import urllib.request
import urllib.error
import csv
import json
import io
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

# football-data.co.uk — коды лиг
FOOTBALL_DATA_LEAGUES = {
    # Англия
    'E0': {'name': 'Premier League',     'country': 'England', 'tier': 1},
    'E1': {'name': 'Championship',       'country': 'England', 'tier': 2},
    'E2': {'name': 'League One',         'country': 'England', 'tier': 3},
    'E3': {'name': 'League Two',         'country': 'England', 'tier': 4},
    # Испания
    'SP1': {'name': 'La Liga',           'country': 'Spain',   'tier': 1},
    'SP2': {'name': 'La Liga 2',         'country': 'Spain',   'tier': 2},
    # Германия
    'D1': {'name': 'Bundesliga',         'country': 'Germany', 'tier': 1},
    'D2': {'name': '2. Bundesliga',      'country': 'Germany', 'tier': 2},
    # Италия
    'I1': {'name': 'Serie A',            'country': 'Italy',   'tier': 1},
    'I2': {'name': 'Serie B',            'country': 'Italy',   'tier': 2},
    # Франция
    'F1': {'name': 'Ligue 1',            'country': 'France',  'tier': 1},
    'F2': {'name': 'Ligue 2',            'country': 'France',  'tier': 2},
    # Нидерланды
    'N1': {'name': 'Eredivisie',         'country': 'Netherlands', 'tier': 1},
    # Бельгия
    'B1': {'name': 'First Division A',   'country': 'Belgium', 'tier': 1},
    # Португалия
    'P1': {'name': 'Primeira Liga',      'country': 'Portugal','tier': 1},
    # Турция
    'T1': {'name': 'Süper Lig',          'country': 'Turkey',  'tier': 1},
    # Греция
    'G1': {'name': 'Super League',       'country': 'Greece',  'tier': 1},
    # Шотландия
    'SC0': {'name': 'Premiership',       'country': 'Scotland','tier': 1},
    'SC1': {'name': 'Championship',      'country': 'Scotland','tier': 2},
    # Другие
    'ARG': {'name': 'Primera División',  'country': 'Argentina','tier': 1},
    'BRA': {'name': 'Série A',           'country': 'Brazil',  'tier': 1},
    'MX':  {'name': 'Liga MX',           'country': 'Mexico',  'tier': 1},
    'USA': {'name': 'MLS',               'country': 'USA',     'tier': 1},
}

# understat — только 6 лиг
UNDERSTAT_LEAGUES = {
    'EPL':        {'fd_code': 'E0',  'name': 'Premier League'},
    'La_liga':    {'fd_code': 'SP1', 'name': 'La Liga'},
    'Bundesliga': {'fd_code': 'D1',  'name': 'Bundesliga'},
    'Serie_A':    {'fd_code': 'I1',  'name': 'Serie A'},
    'Ligue_1':    {'fd_code': 'F1',  'name': 'Ligue 1'},
    'RFPL':       {'fd_code': 'R1',  'name': 'Russian Premier League'},
}

# Сезоны football-data.co.uk
def make_seasons(n_back=5):
    """Возвращает список сезонов вида ['2425','2324','2223',...]"""
    today = date.today()
    year = today.year
    seasons = []
    for i in range(n_back):
        y = year - i
        # сезон 2024-25 → '2425'
        s = f"{str(y)[-2:]}{str(y+1)[-2:]}"
        seasons.append(s)
        # также прошлый сезон
        s2 = f"{str(y-1)[-2:]}{str(y)[-2:]}"
        if s2 not in seasons:
            seasons.append(s2)
    return sorted(set(seasons), reverse=True)[:n_back*2]

# ═══════════════════════════════════════════════════════════════════════
#  HTTP helpers
# ═══════════════════════════════════════════════════════════════════════

def fetch_url(url, timeout=30, retries=3):
    headers = {
        'User-Agent': 'Mozilla/5.0 (BetQuant-ETL/1.0; research purposes)',
        'Accept': 'text/html,application/xhtml+xml,text/csv,*/*',
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None  # файл не существует
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
        self.host = host.rstrip('/')
        self.database = database
        self.user = user
        self.password = password

    def query(self, sql, data=None):
        url = f"{self.host}/?database={self.database}&user={self.user}"
        if self.password:
            url += f"&password={self.password}"
        body = sql.encode('utf-8') if data is None else data
        req = urllib.request.Request(
            url,
            data=body if data is None else (sql.encode() + b'\n' + data),
            headers={'Content-Type': 'application/octet-stream'}
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.read().decode('utf-8')
        except urllib.error.HTTPError as e:
            err = e.read().decode('utf-8', errors='replace')
            raise RuntimeError(f"ClickHouse error: {err[:500]}")

    def insert_json_batch(self, table, rows):
        """Вставляет список словарей через JSONEachRow"""
        if not rows:
            return 0
        lines = '\n'.join(json.dumps(r, ensure_ascii=False, default=str) for r in rows)
        sql = f"INSERT INTO {self.database}.{table} FORMAT JSONEachRow\n"
        self.query(sql, (sql + lines).encode('utf-8'))
        return len(rows)

    def execute(self, sql):
        return self.query(sql)

    def count(self, table, where=''):
        w = f'WHERE {where}' if where else ''
        r = self.query(f"SELECT count() FROM {self.database}.{table} {w}")
        return int(r.strip())

    def log(self, sport, source, league, season, rows, status, msg=''):
        row = {
            'sport': sport, 'source': source, 'league': league,
            'season': season, 'rows_loaded': rows, 'status': status, 'message': msg[:500]
        }
        self.insert_json_batch('etl_log', [row])

# ═══════════════════════════════════════════════════════════════════════
#  football-data.co.uk parser
# ═══════════════════════════════════════════════════════════════════════

# Маппинг колонок football-data.co.uk → наши поля
FD_COL_MAP = {
    'Date': 'date', 'Time': 'time',
    'HomeTeam': 'home_team', 'AwayTeam': 'away_team',
    'FTHG': 'home_goals', 'FTAG': 'away_goals', 'FTR': 'result',
    'HTHG': 'ht_home_goals', 'HTAG': 'ht_away_goals',
    'HS': 'home_shots', 'AS': 'away_shots',
    'HST': 'home_shots_on_target', 'AST': 'away_shots_on_target',
    'HC': 'home_corners', 'AC': 'away_corners',
    'HF': 'home_fouls', 'AF': 'away_fouls',
    'HY': 'home_yellow', 'AY': 'away_yellow',
    'HR': 'home_red', 'AR': 'away_red',
    # Коэффициенты
    'B365H': 'b365_home', 'B365D': 'b365_draw', 'B365A': 'b365_away',
    'B365>2.5': 'b365_over', 'B365<2.5': 'b365_under',
    'PSH': 'pinnacle_home', 'PSD': 'pinnacle_draw', 'PSA': 'pinnacle_away',
    'BbMxH': 'max_home', 'BbMxD': 'max_draw', 'BbMxA': 'max_away',
    'BbAvH': 'avg_home', 'BbAvD': 'avg_draw', 'BbAvA': 'avg_away',
    'BbAH': 'ah_line', 'BbAHH': 'ah_home', 'BbAHA': 'ah_away',
}

def parse_fd_date(s):
    """'20/08/2023' или '20/08/23' → 'YYYY-MM-DD'"""
    if not s:
        return None
    for fmt in ['%d/%m/%Y', '%d/%m/%y']:
        try:
            return datetime.strptime(s.strip(), fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None

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

def parse_fd_csv(content_bytes, league_code, season):
    """Парсит CSV от football-data.co.uk, возвращает список dict для CH"""
    meta = FOOTBALL_DATA_LEAGUES.get(league_code, {'name': league_code, 'country': ''})
    rows = []

    try:
        text = content_bytes.decode('latin-1')
        reader = csv.DictReader(io.StringIO(text))
    except Exception as e:
        print(f"    CSV decode error: {e}")
        return rows

    for raw in reader:
        # Пропускаем строки без команд
        home = raw.get('HomeTeam', '').strip()
        away = raw.get('AwayTeam', '').strip()
        if not home or not away:
            continue

        date_str = parse_fd_date(raw.get('Date', ''))
        if not date_str:
            continue

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
        }

        # Результат
        row['home_goals']    = safe_int(raw.get('FTHG', raw.get('HG', 0)))
        row['away_goals']    = safe_int(raw.get('FTAG', raw.get('AG', 0)))
        row['ht_home_goals'] = safe_int(raw.get('HTHG', 0))
        row['ht_away_goals'] = safe_int(raw.get('HTAG', 0))
        ftr = raw.get('FTR', raw.get('Res', '')).strip()
        row['result'] = ftr if ftr in ('H', 'D', 'A') else 'H'

        # Статистика матча
        row['home_shots']           = safe_int(raw.get('HS', 0))
        row['away_shots']           = safe_int(raw.get('AS', 0))
        row['home_shots_on_target'] = safe_int(raw.get('HST', 0))
        row['away_shots_on_target'] = safe_int(raw.get('AST', 0))
        row['home_corners']         = safe_int(raw.get('HC', 0))
        row['away_corners']         = safe_int(raw.get('AC', 0))
        row['home_fouls']           = safe_int(raw.get('HF', 0))
        row['away_fouls']           = safe_int(raw.get('AF', 0))
        row['home_yellow']          = safe_int(raw.get('HY', 0))
        row['away_yellow']          = safe_int(raw.get('AY', 0))
        row['home_red']             = safe_int(raw.get('HR', 0))
        row['away_red']             = safe_int(raw.get('AR', 0))

        # Коэффициенты
        row['b365_home'] = safe_float(raw.get('B365H', 0))
        row['b365_draw'] = safe_float(raw.get('B365D', 0))
        row['b365_away'] = safe_float(raw.get('B365A', 0))
        row['b365_over'] = safe_float(raw.get('B365>2.5', raw.get('B365AH', 0)))
        row['b365_under']= safe_float(raw.get('B365<2.5', 0))
        row['pinnacle_home'] = safe_float(raw.get('PSH', raw.get('PH', 0)))
        row['pinnacle_draw'] = safe_float(raw.get('PSD', raw.get('PD', 0)))
        row['pinnacle_away'] = safe_float(raw.get('PSA', raw.get('PA', 0)))
        row['max_home'] = safe_float(raw.get('BbMxH', raw.get('MaxH', 0)))
        row['max_draw'] = safe_float(raw.get('BbMxD', raw.get('MaxD', 0)))
        row['max_away'] = safe_float(raw.get('BbMxA', raw.get('MaxA', 0)))
        row['avg_home'] = safe_float(raw.get('BbAvH', raw.get('AvgH', 0)))
        row['avg_draw'] = safe_float(raw.get('BbAvD', raw.get('AvgD', 0)))
        row['avg_away'] = safe_float(raw.get('BbAvA', raw.get('AvgA', 0)))
        row['ah_line']  = safe_float(raw.get('BbAH', 0))
        row['ah_home']  = safe_float(raw.get('BbAHH', 0))
        row['ah_away']  = safe_float(raw.get('BbAHA', 0))

        # Defaults для xG (заполним из understat)
        row['home_xg']  = 0.0
        row['away_xg']  = 0.0
        row['home_xga'] = 0.0
        row['away_xga'] = 0.0

        rows.append(row)

    return rows

def scrape_football_data(ch, leagues, seasons, batch_size=500):
    """Скачивает данные с football-data.co.uk"""
    total = 0
    base = 'https://www.football-data.co.uk/mmz4281'

    for season in seasons:
        for league_code in leagues:
            url = f"{base}/{season}/{league_code}.csv"
            print(f"  [{league_code}] season={season} → {url}")
            try:
                content = fetch_url(url, timeout=20)
                if content is None:
                    print(f"    ✗ 404 skip")
                    ch.log('football', 'football-data.co.uk', league_code, season, 0, 'skip', '404')
                    continue
                if len(content) < 100:
                    print(f"    ✗ empty")
                    continue

                rows = parse_fd_csv(content, league_code, season)
                if not rows:
                    print(f"    ✗ parsed 0 rows")
                    continue

                # Вставляем батчами
                for i in range(0, len(rows), batch_size):
                    ch.insert_json_batch('football_matches', rows[i:i+batch_size])

                ch.log('football', 'football-data.co.uk', league_code, season, len(rows), 'ok')
                print(f"    ✓ {len(rows)} матчей загружено")
                total += len(rows)
                time.sleep(0.5)  # уважаем сервер

            except Exception as e:
                print(f"    ✗ ERROR: {e}")
                ch.log('football', 'football-data.co.uk', league_code, season, 0, 'error', str(e)[:200])

    return total

# ═══════════════════════════════════════════════════════════════════════
#  Understat scraper — xG + события матча
# ═══════════════════════════════════════════════════════════════════════

def extract_json_from_script(html, var_name):
    """Извлекает JSON из <script> understat страницы"""
    pattern = rf"var {var_name}\s*=\s*JSON\.parse\('(.+?)'\)"
    m = re.search(pattern, html, re.DOTALL)
    if not m:
        return None
    raw = m.group(1)
    # unescape
    raw = raw.replace("\\'", "'").replace('\\"', '"').replace('\\\\', '\\')
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Попробуем другой паттерн
        try:
            raw2 = bytes(raw, 'utf-8').decode('unicode_escape')
            return json.loads(raw2)
        except Exception:
            return None

def scrape_understat_league(ch, league, season_year, batch_size=200):
    """
    Скачивает все матчи лиги с understat за сезон
    season_year: 2023 для сезона 2023-24
    """
    url = f"https://understat.com/league/{league}/{season_year}"
    print(f"  [understat] {league} {season_year} → {url}")

    try:
        content = fetch_url(url, timeout=30)
        if not content:
            print(f"    ✗ empty response")
            return 0
        html = content.decode('utf-8', errors='replace')
    except Exception as e:
        print(f"    ✗ fetch error: {e}")
        return 0

    # Парсим datesData — список матчей с xG
    data = extract_json_from_script(html, 'datesData')
    if not data:
        print(f"    ✗ datesData not found")
        return 0

    meta = UNDERSTAT_LEAGUES.get(league, {'fd_code': league, 'name': league})
    season_str = f"{season_year}-{str(season_year+1)[-2:]}"
    matches = []
    match_ids = []

    for m in data:
        if not m.get('isResult'):
            continue

        h_goals = safe_int(m.get('goals', {}).get('h', 0))
        a_goals = safe_int(m.get('goals', {}).get('a', 0))
        h_xg    = safe_float(m.get('xG', {}).get('h', 0))
        a_xg    = safe_float(m.get('xG', {}).get('a', 0))
        h_team  = m.get('h', {}).get('title', '')
        a_team  = m.get('a', {}).get('title', '')
        dt_str  = m.get('datetime', '')[:10]
        m_id    = str(m.get('id', ''))

        if not h_team or not a_team or not dt_str:
            continue

        result = 'H' if h_goals > a_goals else ('A' if a_goals > h_goals else 'D')

        row = {
            'match_id':    m_id or make_match_id(dt_str, h_team, a_team, meta['fd_code']),
            'source':      'understat',
            'date':        dt_str,
            'season':      season_str,
            'league_code': meta['fd_code'],
            'league_name': meta['name'],
            'country':     '',
            'home_team':   h_team,
            'away_team':   a_team,
            'home_goals':  h_goals,
            'away_goals':  a_goals,
            'result':      result,
            'home_xg':     h_xg,
            'away_xg':     a_xg,
            'home_xga':    a_xg,   # xGA для home = xG allowed = xG away
            'away_xga':    h_xg,
            'forecast_win':  safe_float(m.get('forecast', {}).get('w', 0)),
            'forecast_draw': safe_float(m.get('forecast', {}).get('d', 0)),
            'forecast_loss': safe_float(m.get('forecast', {}).get('l', 0)),
        }
        matches.append(row)
        match_ids.append((m_id, dt_str, h_team, a_team))

    if matches:
        for i in range(0, len(matches), batch_size):
            ch.insert_json_batch('football_matches', matches[i:i+batch_size])
        ch.log('football', 'understat', league, season_str, len(matches), 'ok')
        print(f"    ✓ {len(matches)} матчей с xG")

    # Скачиваем детальные события (удары) для каждого матча
    events_total = scrape_understat_match_events(ch, match_ids, meta)

    return len(matches)

def scrape_understat_match_events(ch, match_ids, meta, max_matches=None, delay=1.5):
    """
    Скачивает поминутные удары/голы для каждого матча с understat
    Лимитируем max_matches чтобы не перегружать сервер
    """
    total_events = 0
    to_process = match_ids[:max_matches] if max_matches else match_ids

    print(f"    → Загружаю события для {len(to_process)} матчей...")

    for idx, (mid, dt_str, h_team, a_team) in enumerate(to_process):
        if not mid or mid == '0':
            continue

        url = f"https://understat.com/match/{mid}"
        try:
            content = fetch_url(url, timeout=20)
            if not content:
                continue
            html = content.decode('utf-8', errors='replace')

            shot_data = extract_json_from_script(html, 'shotsData')
            if not shot_data:
                continue

            events = []
            h_score = 0
            a_score = 0

            for side in ['h', 'a']:
                for shot in shot_data.get(side, []):
                    is_goal = shot.get('result') == 'Goal'
                    if is_goal:
                        if side == 'h':
                            h_score += 1
                        else:
                            a_score += 1
                    events.append({
                        'match_id':   mid,
                        'date':       dt_str,
                        'league_code': meta.get('fd_code', ''),
                        'minute':     safe_int(shot.get('minute', 0)),
                        'event_type': 'goal' if is_goal else 'shot',
                        'team':       side,
                        'team_name':  h_team if side == 'h' else a_team,
                        'player':     shot.get('player', ''),
                        'player_id':  str(shot.get('player_id', '')),
                        'xg_shot':    safe_float(shot.get('xG', 0)),
                        'x_coord':    safe_float(shot.get('X', 0)),
                        'y_coord':    safe_float(shot.get('Y', 0)),
                        'situation':  shot.get('situation', ''),
                        'shot_type':  shot.get('shotType', ''),
                        'home_score': h_score,
                        'away_score': a_score,
                    })

            if events:
                ch.insert_json_batch('football_events', events)
                total_events += len(events)

            if (idx + 1) % 20 == 0:
                print(f"      {idx+1}/{len(to_process)} матчей обработано, событий: {total_events}")

            time.sleep(delay)

        except Exception as e:
            # Не прерываем всё из-за одного матча
            pass

    print(f"    ✓ Событий загружено: {total_events}")
    return total_events

# ═══════════════════════════════════════════════════════════════════════
#  Расчёт формы команды (rolling stats ДО каждого матча)
# ═══════════════════════════════════════════════════════════════════════

def compute_team_form(ch):
    """
    Вычисляет rolling stats для каждой команды перед каждым матчем
    и загружает в football_team_form
    """
    print("\n  [form] Вычисляем форму команд...")

    # Получаем все матчи отсортированные по дате
    sql = """
    SELECT match_id, date, league_code, season, home_team, away_team,
           home_goals, away_goals, result,
           home_corners, away_corners, home_yellow, away_yellow,
           home_xg, away_xg, home_shots, away_shots
    FROM betquant.football_matches
    WHERE home_goals > 0 OR away_goals > 0 OR home_shots > 0
    ORDER BY date ASC
    """
    try:
        raw = ch.query(sql + " FORMAT JSONEachRow")
    except Exception as e:
        print(f"    ✗ query error: {e}")
        return

    matches = [json.loads(l) for l in raw.strip().split('\n') if l.strip()]
    print(f"    Обрабатываем {len(matches)} матчей...")

    # Группируем по команде
    # team_history[team] = список матчей в хронологическом порядке
    team_history = defaultdict(list)

    form_rows = []

    for m in matches:
        home = m['home_team']
        away = m['away_team']
        dt   = m['date']
        hg, ag = int(m['home_goals']), int(m['away_goals'])
        mid  = m['match_id']
        league = m['league_code']
        season = m['season']

        def build_form(team, is_home):
            hist = team_history[team]
            if not hist:
                return {}

            # Последние 5 и 10 матчей
            last5  = hist[-5:]
            last10 = hist[-10:]

            def form_str(games):
                return ''.join(g['r'] for g in games)

            def pts(games):
                return sum(3 if g['r']=='W' else (1 if g['r']=='D' else 0) for g in games)

            # Текущий сезон (все матчи в этом сезоне)
            season_games = [g for g in hist if g['season'] == season]
            sgf = sum(g['gf'] for g in season_games)
            sga = sum(g['ga'] for g in season_games)

            # Прошлый сезон
            seasons_sorted = sorted(set(g['season'] for g in hist), reverse=True)
            prev_s = seasons_sorted[1] if len(seasons_sorted) > 1 else ''
            prev_games = [g for g in hist if g['season'] == prev_s]

            # Последние 30 дней
            cutoff_30 = (datetime.strptime(dt, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
            last30 = [g for g in hist if g['date'] >= cutoff_30]

            # Календарный год
            year_start = dt[:4] + '-01-01'
            ytd = [g for g in hist if g['date'] >= year_start]

            # Дома/гость
            home_games = [g for g in season_games if g['venue'] == 'home']
            away_games = [g for g in season_games if g['venue'] == 'away']

            # H2H
            opp = away if is_home else home
            h2h = [g for g in hist if g['opponent'] == opp][-5:]

            return {
                'match_id':    mid,
                'date':        dt,
                'team':        team,
                'league_code': league,
                'season':      season,
                'is_home':     1 if is_home else 0,
                'form_5':      form_str(last5),
                'form_10':     form_str(last10),
                'pts_5':       pts(last5),
                'pts_10':      pts(last10),

                'season_matches':        len(season_games),
                'season_wins':           sum(1 for g in season_games if g['r']=='W'),
                'season_draws':          sum(1 for g in season_games if g['r']=='D'),
                'season_losses':         sum(1 for g in season_games if g['r']=='L'),
                'season_goals_for':      sgf,
                'season_goals_against':  sga,
                'season_goal_diff':      sgf - sga,
                'season_corners_for':    sum(g.get('cf', 0) for g in season_games),
                'season_corners_against':sum(g.get('ca', 0) for g in season_games),
                'season_yellow':         sum(g.get('yc', 0) for g in season_games),
                'season_xg_for':         round(sum(g.get('xgf', 0) for g in season_games), 2),
                'season_xg_against':     round(sum(g.get('xga', 0) for g in season_games), 2),
                'season_shots_for':      sum(g.get('sf', 0) for g in season_games),
                'season_shots_against':  sum(g.get('sa', 0) for g in season_games),

                'prev_season':               prev_s,
                'prev_season_matches':        len(prev_games),
                'prev_season_wins':           sum(1 for g in prev_games if g['r']=='W'),
                'prev_season_goals_for':      sum(g['gf'] for g in prev_games),
                'prev_season_goals_against':  sum(g['ga'] for g in prev_games),
                'prev_season_xg_for':         round(sum(g.get('xgf', 0) for g in prev_games), 2),
                'prev_season_xg_against':     round(sum(g.get('xga', 0) for g in prev_games), 2),

                'last30_matches':       len(last30),
                'last30_goals_for':     sum(g['gf'] for g in last30),
                'last30_goals_against': sum(g['ga'] for g in last30),
                'last30_xg_for':        round(sum(g.get('xgf', 0) for g in last30), 2),
                'last30_corners':       sum(g.get('cf', 0) for g in last30),
                'last30_yellow':        sum(g.get('yc', 0) for g in last30),

                'ytd_matches':          len(ytd),
                'ytd_goals_for':        sum(g['gf'] for g in ytd),
                'ytd_goals_against':    sum(g['ga'] for g in ytd),
                'ytd_xg_for':           round(sum(g.get('xgf', 0) for g in ytd), 2),

                'home_season_matches':           len(home_games),
                'home_season_goals_for':         sum(g['gf'] for g in home_games),
                'home_season_goals_against':     sum(g['ga'] for g in home_games),
                'away_season_matches':           len(away_games),
                'away_season_goals_for':         sum(g['gf'] for g in away_games),
                'away_season_goals_against':     sum(g['ga'] for g in away_games),

                'h2h_wins':           sum(1 for g in h2h if g['r']=='W'),
                'h2h_draws':          sum(1 for g in h2h if g['r']=='D'),
                'h2h_losses':         sum(1 for g in h2h if g['r']=='L'),
                'h2h_goals_for':      sum(g['gf'] for g in h2h),
                'h2h_goals_against':  sum(g['ga'] for g in h2h),
                'h2h_matches':        len(h2h),
            }

        # Строим форму ДО матча
        home_form = build_form(home, True)
        away_form = build_form(away, False)
        if home_form: form_rows.append(home_form)
        if away_form: form_rows.append(away_form)

        # Добавляем этот матч в историю команд
        for team, is_home_team in [(home, True), (away, False)]:
            gf = hg if is_home_team else ag
            ga = ag if is_home_team else hg
            r  = ('W' if gf > ga else ('L' if gf < ga else 'D'))
            hxg = safe_float(m.get('home_xg', 0))
            axg = safe_float(m.get('away_xg', 0))
            entry = {
                'date':     dt,
                'season':   season,
                'league':   league,
                'opponent': away if is_home_team else home,
                'venue':    'home' if is_home_team else 'away',
                'gf': gf, 'ga': ga, 'r': r,
                'cf': safe_int(m.get('home_corners' if is_home_team else 'away_corners', 0)),
                'ca': safe_int(m.get('away_corners' if is_home_team else 'home_corners', 0)),
                'yc': safe_int(m.get('home_yellow' if is_home_team else 'away_yellow', 0)),
                'xgf': hxg if is_home_team else axg,
                'xga': axg if is_home_team else hxg,
                'sf': safe_int(m.get('home_shots' if is_home_team else 'away_shots', 0)),
                'sa': safe_int(m.get('away_shots' if is_home_team else 'home_shots', 0)),
            }
            team_history[team].append(entry)

        # Батчевая запись
        if len(form_rows) >= 1000:
            ch.insert_json_batch('football_team_form', form_rows)
            form_rows = []

    # Остаток
    if form_rows:
        ch.insert_json_batch('football_team_form', form_rows)

    print(f"    ✓ Форма команд рассчитана")

# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='BetQuant Football ETL')
    parser.add_argument('--leagues',   default='top',
                        help='all | top | E0,D1,SP1,... (через запятую)')
    parser.add_argument('--seasons',   type=int, default=3,
                        help='Сколько сезонов назад загружать (default: 3)')
    parser.add_argument('--ch-host',   default='http://localhost:8123')
    parser.add_argument('--ch-db',     default='betquant')
    parser.add_argument('--ch-user',   default='default')
    parser.add_argument('--ch-pass',   default='')
    parser.add_argument('--skip-understat', action='store_true')
    parser.add_argument('--skip-form',      action='store_true')
    parser.add_argument('--events-limit',   type=int, default=None,
                        help='Макс матчей для парсинга событий с understat (default: все)')
    args = parser.parse_args()

    ch = ClickHouseClient(args.ch_host, args.ch_db, args.ch_user, args.ch_pass)

    # Проверяем связь
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
    print(f"\n📊 Football-data.co.uk: {len(leagues)} лиг × {len(seasons)} сезонов")
    print(f"   Лиги: {', '.join(leagues)}")
    print(f"   Сезоны: {', '.join(seasons[:6])}...")

    # ── football-data.co.uk ──────────────────────────────────────────
    total_fd = scrape_football_data(ch, leagues, seasons)
    print(f"\n✅ football-data.co.uk: {total_fd} матчей загружено")

    # ── understat ────────────────────────────────────────────────────
    if not args.skip_understat:
        print(f"\n📊 Understat: 6 лиг × {args.seasons} сезонов")
        total_us = 0
        today = date.today()
        for season_year in range(today.year - args.seasons, today.year + 1):
            for league in UNDERSTAT_LEAGUES:
                try:
                    n = scrape_understat_league(ch, league, season_year)
                    total_us += n
                    time.sleep(2)
                except Exception as e:
                    print(f"  ✗ {league} {season_year}: {e}")
        print(f"\n✅ Understat: {total_us} матчей с xG загружено")

    # ── Форма команд ─────────────────────────────────────────────────
    if not args.skip_form:
        compute_team_form(ch)

    # ── Итоговая статистика ─────────────────────────────────────────
    print("\n" + "="*60)
    print("📈 ИТОГ:")
    try:
        print(f"  football_matches:   {ch.count('football_matches'):>8,}")
        print(f"  football_events:    {ch.count('football_events'):>8,}")
        print(f"  football_team_form: {ch.count('football_team_form'):>8,}")
    except Exception as e:
        print(f"  (статистика недоступна: {e})")

if __name__ == '__main__':
    main()
