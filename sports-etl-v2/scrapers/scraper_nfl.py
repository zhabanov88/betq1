"""
BetQuant — NFL (American Football) ETL Scraper
Источник: nflverse / nfl_data_py — открытые данные, PbP back to 1999
372 поля на каждое игровое действие + командная статистика
"""

import time, json, hashlib, logging, requests
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s [NFL] %(message)s')
log = logging.getLogger(__name__)

NFLVERSE_BASE = 'https://github.com/nflverse/nflverse-data/releases/download'

def insert_batch(ch_url, db, table, rows, batch=5000):
    if not rows: return 0
    total = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        lines = [json.dumps(r, ensure_ascii=False, default=str) for r in chunk]
        url = f"{ch_url}/?query=INSERT+INTO+{db}.{table}+FORMAT+JSONEachRow"
        try:
            r = requests.post(url, data='\n'.join(lines).encode(), timeout=180)
            r.raise_for_status()
            total += len(chunk)
        except Exception as e:
            log.error(f"CH insert ({table}): {e}")
    return total

def safe_get(url, stream=False):
    for i in range(3):
        try:
            r = requests.get(url, timeout=120, stream=stream,
                             headers={'User-Agent': 'BetQuant-ETL/2.0'})
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning(f"GET {url} attempt {i+1}: {e}")
            time.sleep(3*(i+1))
    return None

def mid(h, a, dt): return hashlib.md5(f"{dt}|{h}|{a}".encode()).hexdigest()[:16]

def flt(v, default=0.0):
    try: return float(v) if v is not None and str(v).strip() not in ('', 'NA', 'nan', 'None') else default
    except: return default

def nt(v, default=0):
    try: return int(float(v)) if v is not None and str(v).strip() not in ('', 'NA', 'nan', 'None') else default
    except: return default


# ── Скачиваем CSV данные из nflverse releases ───────────────────────────────

def download_nflverse_csv(season: int, data_type: str) -> Optional[list]:
    """
    data_type: 'pbp' | 'player_stats' | 'schedules'
    """
    urls = {
        'pbp':          f"{NFLVERSE_BASE}/pbp/play_by_play_{season}.csv.gz",
        'player_stats': f"{NFLVERSE_BASE}/player_stats/player_stats_{season}.csv",
        'schedules':    f"{NFLVERSE_BASE}/schedules/schedules_{season}.csv",
    }
    url = urls.get(data_type)
    if not url: return None

    log.info(f"  Downloading {data_type} {season}...")
    r = safe_get(url)
    if not r: return None

    import io, csv, gzip
    content = r.content
    try:
        if url.endswith('.gz'):
            content = gzip.decompress(content)
        reader = csv.DictReader(io.StringIO(content.decode('utf-8', errors='replace')))
        return list(reader)
    except Exception as e:
        log.error(f"CSV parse error: {e}")
        return None


# ── Парсинг расписания → game rows ──────────────────────────────────────────

def parse_schedules(rows: list, season: int) -> list:
    """Формирует базовые game rows из расписания"""
    games = []
    for r in rows:
        if r.get('game_type', 'REG') not in ('REG', 'POST', 'CON', 'SB', 'DIV', 'WC'):
            continue
        home = r.get('home_team', '')
        away = r.get('away_team', '')
        dt   = (r.get('gameday','') or r.get('game_date','') or '')[:10]
        if not home or not away or not dt: continue

        game = {
            'game_id':     r.get('game_id', mid(home, away, dt)),
            'source':      'nflverse',
            'season':      season,
            'season_type': r.get('game_type', 'REG'),
            'week':        nt(r.get('week', 0)),
            'date':        dt,
            'home_team':   home,
            'away_team':   away,
            'venue':       r.get('stadium', '')[:80],
            'roof':        r.get('roof', ''),
            'surface':     r.get('surface', ''),
            'temp':        nt(r.get('temp', -99)) if r.get('temp') else -99,
            'wind':        nt(r.get('wind', 0)),
            'div_game':    nt(r.get('div_game', 0)),
            'home_score':  nt(r.get('home_score', 0)),
            'away_score':  nt(r.get('away_score', 0)),
            'result':      'H' if nt(r.get('home_score',0)) > nt(r.get('away_score',0))
                           else ('A' if nt(r.get('away_score',0)) > nt(r.get('home_score',0)) else 'T'),
            'spread':      flt(r.get('spread_line', 0)),
            'total_line':  flt(r.get('total_line', 0)),
            # Quarters populated from PbP aggregate
            'home_q1': 0, 'away_q1': 0,
            'home_q2': 0, 'away_q2': 0,
            'home_q3': 0, 'away_q3': 0,
            'home_q4': 0, 'away_q4': 0,
            'home_ot': 0, 'away_ot': 0,
            'went_to_ot': 0,
            'home_h1': 0, 'away_h1': 0,
            'home_h2': 0, 'away_h2': 0,
            # Stats fields (will be populated from PbP aggregate)
            'home_pass_att': 0, 'away_pass_att': 0,
            'home_pass_cmp': 0, 'away_pass_cmp': 0,
            'home_pass_yds': 0, 'away_pass_yds': 0,
            'home_pass_tds': 0, 'away_pass_tds': 0,
            'home_pass_int': 0, 'away_pass_int': 0,
            'home_pass_sacks': 0, 'away_pass_sacks': 0,
            'home_rush_att': 0, 'away_rush_att': 0,
            'home_rush_yds': 0, 'away_rush_yds': 0,
            'home_rush_tds': 0, 'away_rush_tds': 0,
            'home_total_yds': 0, 'away_total_yds': 0,
            'home_plays': 0, 'away_plays': 0,
            'home_first_downs': 0, 'away_first_downs': 0,
            'home_third_att': 0, 'away_third_att': 0,
            'home_third_cmp': 0, 'away_third_cmp': 0,
            'home_fumbles': 0, 'away_fumbles': 0,
            'home_fumbles_lost': 0, 'away_fumbles_lost': 0,
            'home_interceptions': 0, 'away_interceptions': 0,
            'home_turnovers': 0, 'away_turnovers': 0,
            'home_penalties': 0, 'away_penalties': 0,
            'home_penalty_yds': 0, 'away_penalty_yds': 0,
            'home_epa_total': 0.0, 'away_epa_total': 0.0,
            'home_epa_per_play': 0.0, 'away_epa_per_play': 0.0,
            'home_epa_pass': 0.0, 'away_epa_pass': 0.0,
            'home_epa_rush': 0.0, 'away_epa_rush': 0.0,
            'home_success_rate': 0.0, 'away_success_rate': 0.0,
            'home_wp_pregame': flt(r.get('home_moneyline', 0)),
            'away_wp_pregame': flt(r.get('away_moneyline', 0)),
        }
        games.append(game)
    return games


# ── Агрегация PbP → командные метрики ───────────────────────────────────────

def aggregate_pbp_to_games(pbp_rows: list, games_map: dict) -> list:
    """
    Считает командные метрики из PbP и обновляет game rows.
    games_map: {game_id: game_row}
    """
    from collections import defaultdict

    # Аккумуляторы {game_id: {team: {...}}}
    acc = defaultdict(lambda: defaultdict(lambda: {
        'plays': 0, 'pass_att': 0, 'pass_cmp': 0, 'pass_yds': 0,
        'pass_tds': 0, 'pass_int': 0, 'sacks': 0, 'sack_yds': 0,
        'rush_att': 0, 'rush_yds': 0, 'rush_tds': 0,
        'total_yds': 0, 'first_downs': 0,
        'third_att': 0, 'third_cmp': 0,
        'fourth_att': 0, 'fourth_cmp': 0,
        'fumbles': 0, 'fumbles_lost': 0, 'int': 0,
        'penalties': 0, 'penalty_yds': 0,
        'epa': 0.0, 'epa_pass': 0.0, 'epa_rush': 0.0,
        'success': 0, 'success_plays': 0,
        'q1_pts': 0, 'q2_pts': 0, 'q3_pts': 0, 'q4_pts': 0, 'ot_pts': 0,
    }))

    score_tracker = {}  # game_id → {home: score, away: score, quarter: q}

    for row in pbp_rows:
        gid   = row.get('game_id', '')
        pteam = row.get('posteam', '')
        play_type = row.get('play_type', '')
        if not gid or not pteam or play_type in ('', 'no_play', 'kickoff', 'punt', 'qb_kneel'):
            continue

        a = acc[gid][pteam]
        a['plays']       += 1
        a['total_yds']   += nt(row.get('yards_gained', 0))
        a['first_downs'] += nt(row.get('first_down', 0))
        a['fumbles']     += nt(row.get('fumble', 0))
        a['fumbles_lost'] += nt(row.get('fumble_lost', 0))

        if play_type == 'pass':
            a['pass_att']  += 1
            a['pass_cmp']  += nt(row.get('complete_pass', 0))
            a['pass_yds']  += nt(row.get('yards_gained', 0))
            a['pass_tds']  += nt(row.get('pass_touchdown', 0))
            a['pass_int']  += nt(row.get('interception', 0))
            a['sacks']     += nt(row.get('sack', 0))
            a['epa_pass']  += flt(row.get('epa', 0))
        elif play_type == 'run':
            a['rush_att']  += 1
            a['rush_yds']  += nt(row.get('yards_gained', 0))
            a['rush_tds']  += nt(row.get('rush_touchdown', 0))
            a['epa_rush']  += flt(row.get('epa', 0))

        a['epa'] += flt(row.get('epa', 0))
        if nt(row.get('success', 0)): a['success'] += 1; a['success_plays'] += 1

        # Penalties
        if nt(row.get('penalty', 0)):
            a['penalties']   += 1
            a['penalty_yds'] += abs(nt(row.get('penalty_yards', 0)))

        # 3rd / 4th down
        if nt(row.get('down', 0)) == 3:
            a['third_att'] += 1
            a['third_cmp'] += nt(row.get('third_down_converted', 0))

        # Quarter scoring (using score_differential changes)
        qtr = nt(row.get('qtr', 1))
        gm = games_map.get(gid)
        if gm:
            home = gm['home_team']
            away = gm['away_team']
            if gid not in score_tracker:
                score_tracker[gid] = {'home': 0, 'away': 0, 'qtr': 1}

    # Apply aggregated stats to game rows
    updated = []
    for gid, game in games_map.items():
        home = game['home_team']
        away = game['away_team']
        h = acc[gid].get(home, {})
        a = acc[gid].get(away, {})

        def upd(game, prefix, team_acc):
            game[f'{prefix}_plays']       = team_acc.get('plays', 0)
            game[f'{prefix}_pass_att']    = team_acc.get('pass_att', 0)
            game[f'{prefix}_pass_cmp']    = team_acc.get('pass_cmp', 0)
            game[f'{prefix}_pass_cmp_pct']= round(team_acc.get('pass_cmp',0)/max(team_acc.get('pass_att',1),1)*100,1)
            game[f'{prefix}_pass_yds']    = team_acc.get('pass_yds', 0)
            game[f'{prefix}_pass_tds']    = team_acc.get('pass_tds', 0)
            game[f'{prefix}_pass_int']    = team_acc.get('pass_int', 0)
            game[f'{prefix}_pass_sacks']  = team_acc.get('sacks', 0)
            game[f'{prefix}_rush_att']    = team_acc.get('rush_att', 0)
            game[f'{prefix}_rush_yds']    = team_acc.get('rush_yds', 0)
            game[f'{prefix}_rush_tds']    = team_acc.get('rush_tds', 0)
            game[f'{prefix}_rush_ypa']    = round(team_acc.get('rush_yds',0)/max(team_acc.get('rush_att',1),1),1)
            game[f'{prefix}_total_yds']   = team_acc.get('total_yds', 0)
            game[f'{prefix}_first_downs'] = team_acc.get('first_downs', 0)
            game[f'{prefix}_third_att']   = team_acc.get('third_att', 0)
            game[f'{prefix}_third_cmp']   = team_acc.get('third_cmp', 0)
            game[f'{prefix}_third_pct']   = round(team_acc.get('third_cmp',0)/max(team_acc.get('third_att',1),1)*100,1)
            game[f'{prefix}_fumbles']     = team_acc.get('fumbles', 0)
            game[f'{prefix}_fumbles_lost']= team_acc.get('fumbles_lost', 0)
            game[f'{prefix}_interceptions'] = team_acc.get('pass_int', 0)
            game[f'{prefix}_turnovers']   = team_acc.get('fumbles_lost',0) + team_acc.get('pass_int',0)
            game[f'{prefix}_penalties']   = team_acc.get('penalties', 0)
            game[f'{prefix}_penalty_yds'] = team_acc.get('penalty_yds', 0)
            game[f'{prefix}_epa_total']   = round(team_acc.get('epa', 0.0), 3)
            plays_n = max(team_acc.get('plays', 1), 1)
            game[f'{prefix}_epa_per_play']= round(team_acc.get('epa', 0.0) / plays_n, 4)
            game[f'{prefix}_epa_pass']    = round(team_acc.get('epa_pass', 0.0), 3)
            game[f'{prefix}_epa_rush']    = round(team_acc.get('epa_rush', 0.0), 3)
            game[f'{prefix}_success_rate']= round(team_acc.get('success',0)/plays_n*100, 1)
            game[f'{prefix}_ypp']         = round(team_acc.get('total_yds',0)/plays_n, 2)

        upd(game, 'home', h)
        upd(game, 'away', a)
        updated.append(game)

    return updated


# ── Парсинг PbP rows ──────────────────────────────────────────────────────

def parse_pbp_rows(pbp_rows: list, season: int, max_rows: int = 500000) -> list:
    """Конвертирует nflverse CSV rows в нашу схему nfl_pbp"""
    result = []
    for i, r in enumerate(pbp_rows[:max_rows]):
        try:
            dt = (r.get('game_date','') or r.get('game_id','')[:10] or '1970-01-01')[:10]
            row = {
                'play_id':   nt(r.get('play_id', i)),
                'game_id':   r.get('game_id', ''),
                'date':      dt,
                'season':    season,
                'season_type': r.get('season_type', 'REG'),
                'week':      nt(r.get('week', 0)),
                'home_team': r.get('home_team', ''),
                'away_team': r.get('away_team', ''),
                'posteam':   r.get('posteam', ''),
                'defteam':   r.get('defteam', ''),
                'side_of_field': r.get('side_of_field', ''),
                'yardline_100': nt(r.get('yardline_100', 50)),
                'game_date': dt,
                'quarter_seconds_remaining': nt(r.get('quarter_seconds_remaining', 0)),
                'half_seconds_remaining':    nt(r.get('half_seconds_remaining', 0)),
                'game_seconds_remaining':    nt(r.get('game_seconds_remaining', 0)),
                'game_half': r.get('game_half', ''),
                'qtr':       nt(r.get('qtr', 1)),
                'down':      nt(r.get('down', 0)),
                'goal_to_go': nt(r.get('goal_to_go', 0)),
                'ydstogo':   nt(r.get('ydstogo', 0)),
                'ydsnet':    nt(r.get('ydsnet', 0)),
                'play_type': r.get('play_type', ''),
                'yards_gained': nt(r.get('yards_gained', 0)),
                'touchdown':    nt(r.get('touchdown', 0)),
                'fumble':       nt(r.get('fumble', 0)),
                'fumble_lost':  nt(r.get('fumble_lost', 0)),
                'interception': nt(r.get('interception', 0)),
                'sack':         nt(r.get('sack', 0)),
                'complete_pass':   nt(r.get('complete_pass', 0)),
                'incomplete_pass': nt(r.get('incomplete_pass', 0)),
                'pass_touchdown':  nt(r.get('pass_touchdown', 0)),
                'rush_touchdown':  nt(r.get('rush_touchdown', 0)),
                'return_touchdown': nt(r.get('return_touchdown', 0)),
                'field_goal_attempt': nt(r.get('field_goal_attempt', 0)),
                'field_goal_result':  r.get('field_goal_result', ''),
                'kick_distance':  nt(r.get('kick_distance', 0)),
                'extra_point_attempt': nt(r.get('extra_point_attempt', 0)),
                'extra_point_result':  r.get('extra_point_result', ''),
                'two_point_attempt':   nt(r.get('two_point_attempt', 0)),
                'two_point_conv_result': r.get('two_point_conv_result', ''),
                'penalty':       nt(r.get('penalty', 0)),
                'penalty_type':  r.get('penalty_type', ''),
                'penalty_yards': nt(r.get('penalty_yards', 0)),
                'first_down':    nt(r.get('first_down', 0)),
                'third_down_converted': nt(r.get('third_down_converted', 0)),
                'third_down_failed':    nt(r.get('third_down_failed', 0)),
                'fourth_down_converted': nt(r.get('fourth_down_converted', 0)),
                'fourth_down_failed':    nt(r.get('fourth_down_failed', 0)),
                'passer_id': r.get('passer_player_id', ''),
                'passer':    r.get('passer_player_name', '')[:60],
                'rusher_id': r.get('rusher_player_id', ''),
                'rusher':    r.get('rusher_player_name', '')[:60],
                'receiver_id': r.get('receiver_player_id', ''),
                'receiver':    r.get('receiver_player_name', '')[:60],
                'air_yards':   nt(r.get('air_yards', 0)),
                'yards_after_catch': flt(r.get('yards_after_catch', 0)),
                'pass_location': r.get('pass_location', ''),
                'pass_length':   r.get('pass_length', ''),
                'run_location':  r.get('run_location', ''),
                'run_gap':       r.get('run_gap', ''),
                # EPA model fields
                'ep':    flt(r.get('ep', 0)),
                'epa':   flt(r.get('epa', 0)),
                'wp':    flt(r.get('wp', 0)),
                'wpa':   flt(r.get('wpa', 0)),
                'air_epa':      flt(r.get('air_epa', 0)),
                'yac_epa':      flt(r.get('yac_epa', 0)),
                'comp_air_epa': flt(r.get('comp_air_epa', 0)),
                'comp_yac_epa': flt(r.get('comp_yac_epa', 0)),
                'cp':    flt(r.get('cp', 0)),
                'cpoe':  flt(r.get('cpoe', 0)),
                'success': nt(r.get('success', 0)),
                'xpass':   flt(r.get('xpass', 0)),
                'pass_oe': flt(r.get('pass_oe', 0)),
                'posteam_score':      nt(r.get('posteam_score', 0)),
                'defteam_score':      nt(r.get('defteam_score', 0)),
                'score_differential': nt(r.get('score_differential', 0)),
                'posteam_score_post': nt(r.get('posteam_score_post', 0)),
                'defteam_score_post': nt(r.get('defteam_score_post', 0)),
            }
            result.append(row)
        except Exception as e:
            log.debug(f"PbP row parse: {e}")
    return result


# ── Парсинг Player Stats ─────────────────────────────────────────────────────

def parse_player_stats(rows: list, season: int) -> list:
    result = []
    for r in rows:
        try:
            dt = r.get('game_date', r.get('week', ''))
            row = {
                'player_id':   r.get('player_id', ''),
                'player_name': r.get('player_display_name', r.get('player_name',''))[:80],
                'position':    r.get('position', ''),
                'team':        r.get('recent_team', r.get('team','')),
                'opponent':    r.get('opponent_team', ''),
                'season':      season,
                'week':        nt(r.get('week', 0)),
                'date':        str(dt)[:10] if dt else '1970-01-01',
                'game_id':     r.get('game_id', ''),
                'is_home':     1,
                # Passing
                'completions':   nt(r.get('completions', 0)),
                'attempts':      nt(r.get('attempts', 0)),
                'passing_yards': nt(r.get('passing_yards', 0)),
                'passing_tds':   nt(r.get('passing_tds', 0)),
                'interceptions': nt(r.get('interceptions', 0)),
                'sacks':         nt(r.get('sacks', 0)),
                'sack_yards':    nt(r.get('sack_yards', 0)),
                'sack_fumbles':  nt(r.get('sack_fumbles', 0)),
                'sack_fumbles_lost': nt(r.get('sack_fumbles_lost', 0)),
                'passing_air_yards': nt(r.get('passing_air_yards', 0)),
                'passing_yards_after_catch': nt(r.get('passing_yards_after_catch', 0)),
                'passing_first_downs': nt(r.get('passing_first_downs', 0)),
                'passing_epa':   flt(r.get('passing_epa', 0)),
                'dakota':        flt(r.get('dakota', 0)),
                'pacr':          flt(r.get('pacr', 0)),
                # Rushing
                'carries':       nt(r.get('carries', 0)),
                'rushing_yards': nt(r.get('rushing_yards', 0)),
                'rushing_tds':   nt(r.get('rushing_tds', 0)),
                'rushing_fumbles': nt(r.get('rushing_fumbles', 0)),
                'rushing_fumbles_lost': nt(r.get('rushing_fumbles_lost', 0)),
                'rushing_first_downs': nt(r.get('rushing_first_downs', 0)),
                'rushing_epa':   flt(r.get('rushing_epa', 0)),
                # Receiving
                'receptions':    nt(r.get('receptions', 0)),
                'targets':       nt(r.get('targets', 0)),
                'receiving_yards': nt(r.get('receiving_yards', 0)),
                'receiving_tds':   nt(r.get('receiving_tds', 0)),
                'receiving_fumbles': nt(r.get('receiving_fumbles', 0)),
                'receiving_fumbles_lost': nt(r.get('receiving_fumbles_lost', 0)),
                'receiving_air_yards': nt(r.get('receiving_air_yards', 0)),
                'receiving_yards_after_catch': nt(r.get('receiving_yards_after_catch', 0)),
                'receiving_first_downs': nt(r.get('receiving_first_downs', 0)),
                'receiving_epa': flt(r.get('receiving_epa', 0)),
                'racr':          flt(r.get('racr', 0)),
                'target_share':  flt(r.get('target_share', 0)),
                'air_yards_share': flt(r.get('air_yards_share', 0)),
                'wopr':          flt(r.get('wopr', 0)),
                # Special
                'special_teams_tds': nt(r.get('special_teams_tds', 0)),
                'fantasy_points':    flt(r.get('fantasy_points', 0)),
                'fantasy_points_ppr': flt(r.get('fantasy_points_ppr', 0)),
            }
            result.append(row)
        except Exception as e:
            log.debug(f"Player stat row: {e}")
    return result


# ── Главный скрапер ──────────────────────────────────────────────────────────

def scrape_nfl(ch_url: str, db: str, seasons_back: int = 3,
               load_pbp: bool = True, load_players: bool = True):
    current_year = datetime.now().year
    start = current_year - seasons_back
    seasons = list(range(start, current_year + 1))

    log.info(f"=== NFL Scraper: seasons {seasons} ===")
    total_games = 0
    total_plays  = 0
    total_players = 0

    for season in seasons:
        log.info(f"Season {season}...")

        # 1. Schedules (meta + final scores)
        sched_rows = download_nflverse_csv(season, 'schedules')
        if sched_rows:
            games = parse_schedules(sched_rows, season)
            log.info(f"  {len(games)} games in schedule")

            # 2. PbP for stats aggregation
            if load_pbp:
                pbp_rows = download_nflverse_csv(season, 'pbp')
                if pbp_rows:
                    log.info(f"  {len(pbp_rows)} PbP rows, aggregating...")
                    games_map = {g['game_id']: g for g in games}
                    games = aggregate_pbp_to_games(pbp_rows, games_map)

                    # Insert PbP
                    pbp_parsed = parse_pbp_rows(pbp_rows, season)
                    n_plays = insert_batch(ch_url, db, 'nfl_pbp', pbp_parsed)
                    total_plays += n_plays
                    log.info(f"  PbP: {n_plays} plays inserted")

            # Insert games
            n = insert_batch(ch_url, db, 'nfl_games', games)
            total_games += n
            log.info(f"  Games: {n} inserted")

        # 3. Player stats
        if load_players:
            ps_rows = download_nflverse_csv(season, 'player_stats')
            if ps_rows:
                ps = parse_player_stats(ps_rows, season)
                n = insert_batch(ch_url, db, 'nfl_player_stats', ps)
                total_players += n
                log.info(f"  Player stats: {n} rows inserted")

        time.sleep(2.0)

    log.info(f"=== NFL DONE: {total_games} games, {total_plays} plays, {total_players} player rows ===")
    return total_games


if __name__ == '__main__':
    import sys
    scrape_nfl(
        sys.argv[1] if len(sys.argv)>1 else 'http://localhost:8123',
        sys.argv[2] if len(sys.argv)>2 else 'betquant',
        int(sys.argv[3]) if len(sys.argv)>3 else 3,
    )
