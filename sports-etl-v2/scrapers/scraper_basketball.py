"""
BetQuant — Basketball ETL Scraper (NBA максимальная детализация)
Источники:
  - stats.nba.com (nba_api) — box score, PbP, player stats, quarter stats
  - balldontlie.io — backup + EuroLeague
  - basketball-reference.com via scraping — historical player data
"""

import time
import json
import hashlib
import logging
import requests
from datetime import datetime, date, timedelta
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s [BBALL] %(message)s')
log = logging.getLogger(__name__)

BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Host': 'stats.nba.com',
    'Origin': 'https://www.nba.com',
    'Referer': 'https://www.nba.com/',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true',
}

NBA_BASE = 'https://stats.nba.com/stats'

# ─────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────

def safe_get(url: str, params: dict = None, retries: int = 3, wait: float = 2.0) -> Optional[dict]:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=BASE_HEADERS, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(wait * (attempt + 1))
    return None

def nba_season_str(year: int) -> str:
    """2023 → '2023-24'"""
    return f"{year}-{str(year+1)[2:]}"

def match_id(home: str, away: str, dt: str) -> str:
    raw = f"{dt}|{home}|{away}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]

def insert_batch(ch_url: str, db: str, table: str, rows: list):
    if not rows: return 0
    lines = [json.dumps(r, ensure_ascii=False, default=str) for r in rows]
    data = '\n'.join(lines)
    url = f"{ch_url}/?query=INSERT+INTO+{db}.{table}+FORMAT+JSONEachRow"
    try:
        r = requests.post(url, data=data.encode('utf-8'),
                          headers={'Content-Type': 'application/x-ndjson'}, timeout=60)
        r.raise_for_status()
        return len(rows)
    except Exception as e:
        log.error(f"CH insert error ({table}): {e}")
        return 0


# ─────────────────────────────────────────────────────────────
# Скорборды: все игры сезона из stats.nba.com
# ─────────────────────────────────────────────────────────────

def get_nba_game_ids(season_year: int) -> list:
    """Возвращает список game_id для всего регулярного сезона"""
    season = nba_season_str(season_year)
    season_type = 'Regular+Season'
    url = f"{NBA_BASE}/leaguegamelog"
    params = {
        'Counter': 1000, 'DateFrom': '', 'DateTo': '',
        'Direction': 'ASC', 'LeagueID': '00',
        'PlayerOrTeam': 'T', 'Season': season,
        'SeasonType': 'Regular Season', 'Sorter': 'DATE',
    }
    data = safe_get(url, params=params)
    if not data:
        return []
    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    game_id_idx = headers.index('GAME_ID')
    seen = set()
    game_ids = []
    for row in rows:
        gid = row[game_id_idx]
        if gid not in seen:
            seen.add(gid)
            game_ids.append(gid)
    log.info(f"  Season {season}: {len(game_ids)} games found")
    return game_ids


# ─────────────────────────────────────────────────────────────
# Box Score — командная статистика по четвертям
# ─────────────────────────────────────────────────────────────

def parse_boxscore(game_id: str, season: str) -> tuple:
    """
    Returns (match_row, quarter_rows, player_rows)
    Использует Advanced + Traditional + Scoring boxscore
    """
    # Traditional boxscore
    url_trad = f"{NBA_BASE}/boxscoretraditionalv2"
    url_adv  = f"{NBA_BASE}/boxscoreadvancedv2"
    url_scor = f"{NBA_BASE}/boxscorescoringv2"
    url_misc = f"{NBA_BASE}/boxscoremiscv2"
    url_sum  = f"{NBA_BASE}/boxscoresummaryv2"

    params = {'EndPeriod': 10, 'EndRange': 28800, 'GameID': game_id,
              'RangeType': 0, 'StartPeriod': 1, 'StartRange': 0}

    data_trad = safe_get(url_trad, params=params)
    data_sum  = safe_get(url_sum,  params={'GameID': game_id})
    time.sleep(0.6)
    data_adv  = safe_get(url_adv,  params=params)
    time.sleep(0.6)
    data_scor = safe_get(url_scor, params=params)
    time.sleep(0.6)
    data_misc = safe_get(url_misc, params=params)
    time.sleep(0.6)

    if not data_trad or not data_sum:
        return None, [], []

    try:
        # ── Summary (дата, команды, счёт по четвертям) ──────────────
        sum_headers = data_sum['resultSets'][0]['headers']
        sum_rows    = data_sum['resultSets'][0]['rowSet']
        line_headers= data_sum['resultSets'][5]['headers']  # LineScore
        line_rows   = data_sum['resultSets'][5]['rowSet']

        def hdr_idx(headers, key):
            try: return headers.index(key)
            except: return -1

        game_date = sum_rows[0][hdr_idx(sum_headers, 'GAME_DATE_EST')] if sum_rows else ''
        home_idx  = 1 if line_rows[0][hdr_idx(line_headers, 'TEAM_ABBREVIATION')] else 0
        # В lineScore: [0] = away, [1] = home обычно, проверяем
        away_row  = line_rows[0]
        home_row  = line_rows[1] if len(line_rows) > 1 else line_rows[0]

        def lval(row, col):
            idx = hdr_idx(line_headers, col)
            return row[idx] if idx >= 0 else 0

        home_team = lval(home_row, 'TEAM_ABBREVIATION')
        away_team = lval(away_row, 'TEAM_ABBREVIATION')
        home_pts  = lval(home_row, 'PTS') or 0
        away_pts  = lval(away_row, 'PTS') or 0
        home_q = [lval(home_row, f'PTS_QTR{i}') or 0 for i in range(1, 5)]
        away_q = [lval(away_row, f'PTS_QTR{i}') or 0 for i in range(1, 5)]
        home_ot= [lval(home_row, f'PTS_OT{i}') or 0 for i in range(1, 4)]
        away_ot= [lval(away_row, f'PTS_OT{i}') or 0 for i in range(1, 4)]

        mid = match_id(home_team, away_team, str(game_date)[:10])
        dt  = str(game_date)[:10] if game_date else '1970-01-01'

        # ── Traditional team stats ──────────────────────────────────
        t_headers = data_trad['resultSets'][1]['headers']
        t_rows    = data_trad['resultSets'][1]['rowSet']  # index 1 = team stats

        def tv(row, col):
            idx = hdr_idx(t_headers, col)
            v = row[idx] if idx >= 0 else 0
            return v if v is not None else 0

        # Find home/away team rows
        team_id_idx = hdr_idx(t_headers, 'TEAM_ABBREVIATION')
        home_t = next((r for r in t_rows if r[team_id_idx] == home_team), t_rows[0] if t_rows else [])
        away_t = next((r for r in t_rows if r[team_id_idx] == away_team), t_rows[1] if len(t_rows)>1 else [])

        # Advanced team stats
        adv_rows = data_adv['resultSets'][1]['rowSet'] if data_adv else []
        adv_headers = data_adv['resultSets'][1]['headers'] if data_adv else []
        def av(row, col):
            if not row or not adv_headers: return 0
            idx = hdr_idx(adv_headers, col)
            v = row[idx] if idx >= 0 else 0
            return v if v is not None else 0

        home_adv = next((r for r in adv_rows if r[hdr_idx(adv_headers,'TEAM_ABBREVIATION')] == home_team), []) if adv_rows and adv_headers else []
        away_adv = next((r for r in adv_rows if r[hdr_idx(adv_headers,'TEAM_ABBREVIATION')] == away_team), []) if adv_rows and adv_headers else []

        # Scoring / Misc
        scor_rows = data_scor['resultSets'][1]['rowSet'] if data_scor else []
        scor_hdrs = data_scor['resultSets'][1]['headers'] if data_scor else []
        def sv(row, col):
            if not row or not scor_hdrs: return 0
            idx = hdr_idx(scor_hdrs, col)
            v = row[idx] if idx >= 0 else 0
            return v if v is not None else 0
        home_scor = next((r for r in scor_rows if scor_hdrs and r[hdr_idx(scor_hdrs,'TEAM_ABBREVIATION')] == home_team), []) if scor_rows and scor_hdrs else []
        away_scor = next((r for r in scor_rows if scor_hdrs and r[hdr_idx(scor_hdrs,'TEAM_ABBREVIATION')] == away_team), []) if scor_rows and scor_hdrs else []

        misc_rows = data_misc['resultSets'][1]['rowSet'] if data_misc else []
        misc_hdrs = data_misc['resultSets'][1]['headers'] if data_misc else []
        def mv(row, col):
            if not row or not misc_hdrs: return 0
            idx = hdr_idx(misc_hdrs, col)
            v = row[idx] if idx >= 0 else 0
            return v if v is not None else 0
        home_misc = next((r for r in misc_rows if misc_hdrs and r[hdr_idx(misc_hdrs,'TEAM_ABBREVIATION')] == home_team), []) if misc_rows and misc_hdrs else []
        away_misc = next((r for r in misc_rows if misc_hdrs and r[hdr_idx(misc_hdrs,'TEAM_ABBREVIATION')] == away_team), []) if misc_rows and misc_hdrs else []

        # Assemble match row
        went_to_ot = 1 if any(x > 0 for x in home_ot + away_ot) else 0
        ot_periods = sum(1 for h, a in zip(home_ot, away_ot) if h > 0 or a > 0)

        match_row = {
            'match_id': mid, 'source': 'nba_api', 'date': dt,
            'season': season, 'season_type': 'Regular Season',
            'league': 'NBA', 'home_team': home_team, 'away_team': away_team,
            'home_pts': int(home_pts or 0), 'away_pts': int(away_pts or 0),
            'result': 'H' if int(home_pts or 0) > int(away_pts or 0) else 'A',
            'home_q1': int(home_q[0] or 0), 'away_q1': int(away_q[0] or 0),
            'home_q2': int(home_q[1] or 0), 'away_q2': int(away_q[1] or 0),
            'home_q3': int(home_q[2] or 0), 'away_q3': int(away_q[2] or 0),
            'home_q4': int(home_q[3] or 0), 'away_q4': int(away_q[3] or 0),
            'home_ot1': int(home_ot[0] or 0), 'away_ot1': int(away_ot[0] or 0),
            'home_ot2': int(home_ot[1] or 0), 'away_ot2': int(away_ot[1] or 0),
            'home_ot3': int(home_ot[2] or 0), 'away_ot3': int(away_ot[2] or 0),
            'went_to_ot': went_to_ot, 'ot_periods': ot_periods,
            'home_h1': int((home_q[0] or 0) + (home_q[1] or 0)),
            'away_h1': int((away_q[0] or 0) + (away_q[1] or 0)),
            'home_h2': int((home_q[2] or 0) + (home_q[3] or 0)),
            'away_h2': int((away_q[2] or 0) + (away_q[3] or 0)),
            # Traditional
            'home_fgm': int(tv(home_t,'FGM') or 0), 'away_fgm': int(tv(away_t,'FGM') or 0),
            'home_fga': int(tv(home_t,'FGA') or 0), 'away_fga': int(tv(away_t,'FGA') or 0),
            'home_fg_pct': float(tv(home_t,'FG_PCT') or 0), 'away_fg_pct': float(tv(away_t,'FG_PCT') or 0),
            'home_fg3m': int(tv(home_t,'FG3M') or 0), 'away_fg3m': int(tv(away_t,'FG3M') or 0),
            'home_fg3a': int(tv(home_t,'FG3A') or 0), 'away_fg3a': int(tv(away_t,'FG3A') or 0),
            'home_fg3_pct': float(tv(home_t,'FG3_PCT') or 0), 'away_fg3_pct': float(tv(away_t,'FG3_PCT') or 0),
            'home_ftm': int(tv(home_t,'FTM') or 0), 'away_ftm': int(tv(away_t,'FTM') or 0),
            'home_fta': int(tv(home_t,'FTA') or 0), 'away_fta': int(tv(away_t,'FTA') or 0),
            'home_ft_pct': float(tv(home_t,'FT_PCT') or 0), 'away_ft_pct': float(tv(away_t,'FT_PCT') or 0),
            'home_oreb': int(tv(home_t,'OREB') or 0), 'away_oreb': int(tv(away_t,'OREB') or 0),
            'home_dreb': int(tv(home_t,'DREB') or 0), 'away_dreb': int(tv(away_t,'DREB') or 0),
            'home_reb':  int(tv(home_t,'REB')  or 0), 'away_reb':  int(tv(away_t,'REB')  or 0),
            'home_ast':  int(tv(home_t,'AST')  or 0), 'away_ast':  int(tv(away_t,'AST')  or 0),
            'home_stl':  int(tv(home_t,'STL')  or 0), 'away_stl':  int(tv(away_t,'STL')  or 0),
            'home_blk':  int(tv(home_t,'BLK')  or 0), 'away_blk':  int(tv(away_t,'BLK')  or 0),
            'home_blka': int(tv(home_t,'BLKA') or 0), 'away_blka': int(tv(away_t,'BLKA') or 0),
            'home_tov':  int(tv(home_t,'TOV')  or 0), 'away_tov':  int(tv(away_t,'TOV')  or 0),
            'home_pf':   int(tv(home_t,'PF')   or 0), 'away_pf':   int(tv(away_t,'PF')   or 0),
            'home_pfd':  int(tv(home_t,'PFD')  or 0), 'away_pfd':  int(tv(away_t,'PFD')  or 0),
            # Scoring (points by type)
            'home_pts_paint': int(sv(home_scor,'PTS_PAINT') or 0), 'away_pts_paint': int(sv(away_scor,'PTS_PAINT') or 0),
            'home_pts_fb':  int(sv(home_scor,'PTS_FB') or 0),   'away_pts_fb':  int(sv(away_scor,'PTS_FB') or 0),
            'home_pts_2nd_chance': int(sv(home_scor,'PTS_2ND_CHANCE') or 0), 'away_pts_2nd_chance': int(sv(away_scor,'PTS_2ND_CHANCE') or 0),
            'home_pts_off_tov': int(sv(home_scor,'PTS_OFF_TOV') or 0), 'away_pts_off_tov': int(sv(away_scor,'PTS_OFF_TOV') or 0),
            # Advanced
            'home_ortg': float(av(home_adv,'OFF_RATING') or 0), 'away_ortg': float(av(away_adv,'OFF_RATING') or 0),
            'home_drtg': float(av(home_adv,'DEF_RATING') or 0), 'away_drtg': float(av(away_adv,'DEF_RATING') or 0),
            'home_nrtg': float(av(home_adv,'NET_RATING') or 0), 'away_nrtg': float(av(away_adv,'NET_RATING') or 0),
            'home_pace': float(av(home_adv,'PACE') or 0),       'away_pace': float(av(away_adv,'PACE') or 0),
            'home_efg_pct': float(av(home_adv,'EFG_PCT') or 0), 'away_efg_pct': float(av(away_adv,'EFG_PCT') or 0),
            'home_ts_pct': float(av(home_adv,'TS_PCT') or 0),   'away_ts_pct': float(av(away_adv,'TS_PCT') or 0),
            'home_ast_pct': float(av(home_adv,'AST_PCT') or 0), 'away_ast_pct': float(av(away_adv,'AST_PCT') or 0),
            'home_reb_pct': float(av(home_adv,'REB_PCT') or 0), 'away_reb_pct': float(av(away_adv,'REB_PCT') or 0),
            'home_tov_pct': float(av(home_adv,'TM_TOV_PCT') or 0), 'away_tov_pct': float(av(away_adv,'TM_TOV_PCT') or 0),
            'home_oreb_pct': float(av(home_adv,'OREB_PCT') or 0), 'away_oreb_pct': float(av(away_adv,'OREB_PCT') or 0),
            'home_pie': float(av(home_adv,'PIE') or 0), 'away_pie': float(av(away_adv,'PIE') or 0),
            # Misc
            'home_pts_bench': int(mv(home_misc,'PTS_OFF_TOV') or 0),
        }

        # ── Статистика игроков ─────────────────────────────────────
        p_headers = data_trad['resultSets'][0]['headers']  # player rows
        p_rows    = data_trad['resultSets'][0]['rowSet']

        adv_p_rows = data_adv['resultSets'][0]['rowSet'] if data_adv else []
        adv_p_hdrs = data_adv['resultSets'][0]['headers'] if data_adv else []

        scor_p_rows = data_scor['resultSets'][0]['rowSet'] if data_scor else []
        scor_p_hdrs = data_scor['resultSets'][0]['headers'] if data_scor else []

        player_rows = []
        for p in p_rows:
            def pv(col): idx = hdr_idx(p_headers, col); return p[idx] if idx >= 0 else 0
            pid   = str(pv('PLAYER_ID') or '')
            pname = str(pv('PLAYER_NAME') or '')
            pteam = str(pv('TEAM_ABBREVIATION') or '')

            # Match advanced row for same player
            p_adv = next((r for r in adv_p_rows if adv_p_hdrs and str(r[hdr_idx(adv_p_hdrs,'PLAYER_ID')]) == pid), [])
            def apv(col): idx = hdr_idx(adv_p_hdrs, col); return p_adv[idx] if p_adv and idx >= 0 else 0

            p_scor = next((r for r in scor_p_rows if scor_p_hdrs and str(r[hdr_idx(scor_p_hdrs,'PLAYER_ID')]) == pid), [])
            def spv(col): idx = hdr_idx(scor_p_hdrs, col); return p_scor[idx] if p_scor and idx >= 0 else 0

            mins_str = str(pv('MIN') or '0:0')
            try:
                parts = mins_str.split(':')
                mins_val = float(parts[0]) + float(parts[1])/60 if len(parts) >= 2 else float(parts[0])
            except: mins_val = 0.0

            player_rows.append({
                'match_id': mid, 'date': dt, 'season': season,
                'league': 'NBA', 'team': pteam,
                'opponent': away_team if pteam == home_team else home_team,
                'is_home': 1 if pteam == home_team else 0,
                'player_id': pid, 'player_name': pname,
                'position': str(pv('START_POSITION') or ''),
                'starter': 1 if pv('START_POSITION') else 0,
                'dnp': 1 if mins_val == 0 else 0,
                'min_played': round(mins_val, 2),
                'pts':  int(pv('PTS')  or 0),
                'fgm':  int(pv('FGM')  or 0), 'fga': int(pv('FGA') or 0),
                'fg_pct': float(pv('FG_PCT') or 0),
                'fg3m': int(pv('FG3M') or 0), 'fg3a': int(pv('FG3A') or 0),
                'fg3_pct': float(pv('FG3_PCT') or 0),
                'ftm':  int(pv('FTM')  or 0), 'fta': int(pv('FTA') or 0),
                'ft_pct': float(pv('FT_PCT') or 0),
                'oreb': int(pv('OREB') or 0), 'dreb': int(pv('DREB') or 0),
                'reb':  int(pv('REB')  or 0), 'ast': int(pv('AST') or 0),
                'stl':  int(pv('STL')  or 0), 'blk': int(pv('BLK') or 0),
                'tov':  int(pv('TOV')  or 0), 'pf':  int(pv('PF')  or 0),
                'pfd':  int(pv('PFD')  or 0),
                'plus_minus': int(pv('PLUS_MINUS') or 0),
                # Advanced
                'efg_pct':   float(apv('EFG_PCT') or 0),
                'ts_pct':    float(apv('TS_PCT') or 0),
                'usage_pct': float(apv('USG_PCT') or 0),
                'ortg':      float(apv('OFF_RATING') or 0),
                'drtg':      float(apv('DEF_RATING') or 0),
                'ast_pct':   float(apv('AST_PCT') or 0),
                'reb_pct':   float(apv('REB_PCT') or 0),
                'stl_pct':   float(apv('STL_PCT') or 0),
                'blk_pct':   float(apv('BLK_PCT') or 0),
                'tov_pct':   float(apv('TOV_PCT') or 0),
                # Scoring
                'pts_paint':      int(spv('PTS_PAINT') or 0),
                'pts_fb':         int(spv('PTS_FB') or 0),
                'pts_2nd_chance': int(spv('PTS_2ND_CHANCE') or 0),
                'pts_off_tov':    int(spv('PTS_OFF_TOV') or 0),
            })

        # ── Статистика по четвертям (командная) ───────────────────
        quarter_rows = []
        url_bq = f"{NBA_BASE}/boxscoreplayertrackv2"  # not what we want
        # Use period data from traditional boxscore
        # Get quarter-level from gamerotation or build from player splits
        # Для упрощения используем линию счёта
        for q_idx in range(1, 5 + ot_periods):
            if q_idx <= 4:
                h_pts = home_q[q_idx-1] if q_idx <= 4 else 0
                a_pts = away_q[q_idx-1] if q_idx <= 4 else 0
            else:
                ot_i = q_idx - 5
                h_pts = home_ot[ot_i] if ot_i < len(home_ot) else 0
                a_pts = away_ot[ot_i] if ot_i < len(away_ot) else 0
            # home score at end of quarter
            h_cum = sum(home_q[:min(q_idx,4)]) + sum(home_ot[:max(0,q_idx-4)])
            a_cum = sum(away_q[:min(q_idx,4)]) + sum(away_ot[:max(0,q_idx-4)])

            for team, opp, is_home, pts in [
                (home_team, away_team, 1, h_pts),
                (away_team, home_team, 0, a_pts),
            ]:
                quarter_rows.append({
                    'match_id': mid, 'date': dt, 'season': season,
                    'league': 'NBA', 'team': team, 'opponent': opp,
                    'is_home': is_home, 'quarter': q_idx,
                    'pts': pts,
                    'lead_at_end': int(h_cum - a_cum),
                })

        return match_row, quarter_rows, player_rows

    except Exception as e:
        log.error(f"Error parsing boxscore {game_id}: {e}", exc_info=True)
        return None, [], []


# ─────────────────────────────────────────────────────────────
# Главная функция
# ─────────────────────────────────────────────────────────────

def scrape_nba(ch_url: str, db: str, seasons_back: int = 3, skip_pbp: bool = True):
    """
    Загружает NBA box scores, player stats, quarter stats за N сезонов.
    PbP данные загружаются через отдельный loader (очень большой объём).
    """
    current_year = datetime.now().year
    # NBA сезон обычно начинается в октябре, заканчивается в июне
    # Если сейчас до июня — текущий сезон начался в прошлом году
    start_year = current_year - 1 if datetime.now().month < 7 else current_year
    seasons = list(range(start_year - seasons_back + 1, start_year + 1))

    log.info(f"=== NBA Scraper: seasons {seasons} ===")
    total_matches = 0

    for season_year in seasons:
        season_str = nba_season_str(season_year)
        log.info(f"Processing season {season_str}...")

        game_ids = get_nba_game_ids(season_year)
        if not game_ids:
            log.warning(f"No games found for {season_str}")
            continue

        match_rows, quarter_rows_all, player_rows_all = [], [], []
        batch_size = 50

        for i, gid in enumerate(game_ids):
            log.info(f"  Game {i+1}/{len(game_ids)}: {gid}")
            m_row, q_rows, p_rows = parse_boxscore(gid, season_str)
            if m_row:
                match_rows.append(m_row)
                quarter_rows_all.extend(q_rows)
                player_rows_all.extend(p_rows)

            if len(match_rows) >= batch_size:
                n = insert_batch(ch_url, db, 'basketball_matches_v2', match_rows)
                insert_batch(ch_url, db, 'basketball_quarter_stats', quarter_rows_all)
                insert_batch(ch_url, db, 'basketball_player_stats', player_rows_all)
                log.info(f"    Inserted batch: {n} matches, {len(quarter_rows_all)} quarters, {len(player_rows_all)} player rows")
                total_matches += n
                match_rows, quarter_rows_all, player_rows_all = [], [], []

            time.sleep(0.8)  # NBA API rate limit

        # Flush remaining
        if match_rows:
            insert_batch(ch_url, db, 'basketball_matches_v2', match_rows)
            insert_batch(ch_url, db, 'basketball_quarter_stats', quarter_rows_all)
            insert_batch(ch_url, db, 'basketball_player_stats', player_rows_all)
            total_matches += len(match_rows)

        log.info(f"Season {season_str}: total {total_matches} matches loaded")

    log.info(f"=== NBA DONE: {total_matches} matches total ===")
    return total_matches


# ─────────────────────────────────────────────────────────────
# Euroleague (via balldontlie backup)
# ─────────────────────────────────────────────────────────────

def scrape_euroleague_backup(ch_url: str, db: str, seasons_back: int = 2):
    """Загружает EuroLeague матчи через balldontlie API (бесплатный план)"""
    BDL_BASE = 'https://api.balldontlie.io/v1'
    # Используем их европейский endpoint если доступен
    # При отсутствии ключа доступен только NBA
    log.info("EuroLeague: attempting balldontlie scrape (requires API key for full access)")
    # Placeholder — реальная загрузка требует API key
    return 0


if __name__ == '__main__':
    import sys
    ch = sys.argv[1] if len(sys.argv) > 1 else 'http://localhost:8123'
    db = sys.argv[2] if len(sys.argv) > 2 else 'betquant'
    sb = int(sys.argv[3]) if len(sys.argv) > 3 else 2

    scrape_nba(ch, db, seasons_back=sb)
