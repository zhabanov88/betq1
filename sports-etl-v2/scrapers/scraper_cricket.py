"""
BetQuant — Cricket ETL Scraper
Источник: cricsheet.org — 21,000+ матчей ball-by-ball, открытые данные
Форматы: Test, ODI, T20I, IPL, BBL, PSL, CPL, The Hundred, SA20, LPL...
"""

import os
import csv
import gzip
import json
import time
import zipfile
import hashlib
import logging
import io
import requests
from datetime import datetime, date
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s [CRICKET] %(message)s')
log = logging.getLogger(__name__)

CRICSHEET_BASE = 'https://cricsheet.org/downloads'

# Все доступные наборы (competition_code → display name)
COMPETITIONS = {
    # International
    'tests':   ('Test Matches', 'Test'),
    'odis':    ('ODI Matches',  'ODI'),
    't20s':    ('T20I Matches', 'T20I'),
    # Club T20
    'ipl':     ('Indian Premier League',     'T20'),
    'bbl':     ('Big Bash League',           'T20'),
    'psl':     ('Pakistan Super League',     'T20'),
    'cpl':     ('Caribbean Premier League',  'T20'),
    'hundred': ('The Hundred',               'T20'),
    'sa20':    ('SA20',                      'T20'),
    'lpl':     ('Lanka Premier League',      'T20'),
    'bpl':     ('Bangladesh Premier League', 'T20'),
    'ntb':     ('Vitality T20 Blast',        'T20'),
    'ilc':     ('International League T20',  'T20'),
    'mlc':     ('Major League Cricket',      'T20'),
    # ODI leagues
    'county_championship': ('County Championship', 'FC'),
}

GENDERS = ['male', 'female']


def insert_batch(ch_url: str, db: str, table: str, rows: list) -> int:
    if not rows: return 0
    lines = [json.dumps(r, ensure_ascii=False, default=str) for r in rows]
    url = f"{ch_url}/?query=INSERT+INTO+{db}.{table}+FORMAT+JSONEachRow"
    try:
        r = requests.post(url, data='\n'.join(lines).encode('utf-8'),
                          headers={'Content-Type': 'application/x-ndjson'}, timeout=120)
        r.raise_for_status()
        return len(rows)
    except Exception as e:
        log.error(f"CH insert error ({table}): {e}")
        return 0


def safe_get(url: str, stream: bool = False) -> Optional[requests.Response]:
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=120, stream=stream,
                             headers={'User-Agent': 'Mozilla/5.0 BetQuant-ETL/2.0'})
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning(f"GET {url} attempt {attempt+1}: {e}")
            time.sleep(2 * (attempt + 1))
    return None


def download_cricsheet_csv(competition: str, gender: str = 'male') -> Optional[bytes]:
    """
    Скачивает CSV-пакет (всё одним zip файлом) с cricsheet.org
    URL формат: https://cricsheet.org/downloads/{comp}_{gender}_csv2.zip
    Для международных: https://cricsheet.org/downloads/{comp}_csv2.zip
    """
    urls_to_try = [
        f"{CRICSHEET_BASE}/{competition}_{gender}_csv2.zip",
        f"{CRICSHEET_BASE}/{competition}_csv2.zip",
        f"{CRICSHEET_BASE}/{competition}_{gender}_csv.zip",
        f"{CRICSHEET_BASE}/{competition}_csv.zip",
    ]
    for url in urls_to_try:
        log.info(f"  Trying: {url}")
        r = safe_get(url)
        if r and r.status_code == 200:
            return r.content
    return None


def parse_cricsheet_csv2(content: bytes, competition: str, match_type: str, gender: str) -> tuple:
    """
    Cricsheet CSV2 format:
    - _info.csv files: match metadata
    - _deliveries.csv: ball-by-ball data (one per ball)
    Returns (match_rows, delivery_rows, batting_rows, bowling_rows, fielding_rows)
    """
    match_rows = []
    delivery_rows = []
    batting_rows = []
    bowling_rows = []
    fielding_rows = []

    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
        names = zf.namelist()

        # Group files by match_id
        match_ids = set()
        for name in names:
            if name.endswith('_info.csv'):
                mid = name.replace('_info.csv', '').split('/')[-1]
                match_ids.add(mid)
            elif '.csv' in name and '_info' not in name:
                mid = name.replace('.csv', '').split('/')[-1]
                match_ids.add(mid)

        log.info(f"    {len(match_ids)} match files in ZIP")

        for mid in sorted(match_ids)[:500]:  # Обработка пакетами по 500
            try:
                # Try to read info file
                info_name = f"{mid}_info.csv"
                del_name  = f"{mid}.csv"

                # Some zips use paths like 'all_matches/12345_info.csv'
                info_names = [n for n in names if n.endswith(info_name)]
                del_names  = [n for n in names if n.endswith(del_name) and '_info' not in n]

                if not info_names: continue
                info_data = zf.read(info_names[0]).decode('utf-8', errors='replace')
                del_data  = zf.read(del_names[0]).decode('utf-8', errors='replace') if del_names else None

                match_row, deliveries, batting, bowling, fielding = parse_match_files(
                    mid, info_data, del_data, competition, match_type, gender
                )
                if match_row:
                    match_rows.append(match_row)
                    delivery_rows.extend(deliveries)
                    batting_rows.extend(batting)
                    bowling_rows.extend(bowling)
                    fielding_rows.extend(fielding)

            except Exception as e:
                log.debug(f"Skipping match {mid}: {e}")
                continue

    except Exception as e:
        log.error(f"ZIP parse error: {e}")

    return match_rows, delivery_rows, batting_rows, bowling_rows, fielding_rows


def parse_match_files(mid: str, info_csv: str, del_csv: Optional[str],
                      competition: str, match_type: str, gender: str) -> tuple:
    """Parse individual match CSV files"""
    # ── Parse info CSV ─────────────────────────────────────────────
    info = {}
    teams = []
    players_batting = {}
    player_registry = {}

    for line in info_csv.strip().split('\n'):
        parts = line.strip().split(',')
        if len(parts) < 2: continue
        key = parts[1].strip() if len(parts) > 1 else ''
        val = ','.join(parts[2:]).strip() if len(parts) > 2 else ''
        if parts[0] == '#':
            if key == 'team':
                teams.append(val)
            elif key == 'registry':
                pass
            else:
                info[key] = val

    if len(teams) < 2:
        return None, [], [], [], []

    # Extract key fields
    dt_str = info.get('date', '1970-01-01')
    if isinstance(dt_str, str): dt_str = dt_str.split(',')[0].strip()

    toss_winner  = info.get('toss_winner', '')
    toss_dec     = info.get('toss_decision', '')
    winner       = info.get('winner', '')
    result       = info.get('result', 'normal')
    win_by_runs  = int(info.get('winner_runs', 0) or 0)
    win_by_wkts  = int(info.get('winner_wickets', 0) or 0)
    venue        = info.get('venue', '')
    city         = info.get('city', '')
    pom          = info.get('player_of_match', '')
    umpire1      = info.get('umpire1', '')
    umpire2      = info.get('umpire2', '')
    gender_val   = info.get('gender', gender)
    match_t      = info.get('match_type', match_type)
    season_val   = info.get('season', dt_str[:4])

    team1, team2 = teams[0], teams[1]

    # Margin string
    if win_by_runs > 0:
        margin = f"{win_by_runs} runs"
    elif win_by_wkts > 0:
        margin = f"{win_by_wkts} wickets"
    else:
        margin = result

    match_row = {
        'match_id': mid, 'source': 'cricsheet', 'date': dt_str,
        'season': str(season_val), 'match_type': match_t,
        'competition': competition, 'gender': gender_val,
        'venue': venue[:100], 'city': city[:60], 'country': '',
        'team1': team1, 'team2': team2,
        'toss_winner': toss_winner, 'toss_decision': toss_dec,
        'winner': winner, 'result': result,
        'win_by_runs': win_by_runs, 'win_by_wickets': win_by_wkts,
        'result_margin': margin[:50],
        'inning1_runs': 0, 'inning1_wickets': 0, 'inning1_overs': 0,
        'inning2_runs': 0, 'inning2_wickets': 0, 'inning2_overs': 0,
        'inning3_runs': 0, 'inning3_wickets': 0,
        'inning4_runs': 0, 'inning4_wickets': 0,
        'total_runs': 0, 'total_wickets': 0, 'total_balls': 0, 'total_extras': 0,
        'dls_applied': 0, 'dls_target': 0,
        'player_of_match': str(pom)[:80],
        'umpire1': umpire1[:60], 'umpire2': umpire2[:60],
        'tv_umpire': info.get('tv_umpire', '')[:60],
        'match_referee': info.get('match_referee', '')[:60],
        'days_of_play': int(info.get('days_of_play', 1) or 1),
    }

    delivery_rows = []
    batting_stats  = {}  # (innings, batter) → stats
    bowling_stats  = {}  # (innings, bowler) → stats
    fielding_stats = {}  # player → stats

    if not del_csv:
        return match_row, [], [], [], []

    # ── Parse deliveries CSV ───────────────────────────────────────
    innings_sums = {}  # innings → {runs, wickets, balls, extras}

    reader = csv.DictReader(del_csv.strip().split('\n'))
    for row in reader:
        try:
            innings  = int(row.get('innings', 1) or 1)
            over_val = row.get('ball', '0.0') or '0.0'
            try:
                over_num = int(float(over_val))
                ball_num = round((float(over_val) - over_num) * 10)
            except:
                over_num, ball_num = 0, 0

            batter      = str(row.get('striker', '') or '')
            non_striker = str(row.get('non_striker', '') or '')
            bowler      = str(row.get('bowler', '') or '')
            bat_team    = str(row.get('batting_team', '') or '')
            bowl_team   = str(row.get('bowling_team', '') or '')

            runs_bat    = int(row.get('runs_off_bat', 0) or 0)
            runs_extras = int(row.get('extras', 0) or 0)
            runs_total  = runs_bat + runs_extras
            wide        = int(row.get('wides', 0) or 0)
            noball      = int(row.get('noballs', 0) or 0)
            byes        = int(row.get('byes', 0) or 0)
            legbyes     = int(row.get('legbyes', 0) or 0)
            penalty     = int(row.get('penalty', 0) or 0)

            wicket_kind   = str(row.get('wicket_type', '') or '')
            wicket_player = str(row.get('player_dismissed', '') or '')
            fielder       = str(row.get('fielder', '') or '')
            is_wicket     = 1 if wicket_player else 0

            if innings not in innings_sums:
                innings_sums[innings] = {'runs': 0, 'wickets': 0, 'balls': 0, 'extras': 0}
            s = innings_sums[innings]
            batter_prev_runs  = s.get(f'bat_{batter}_runs', 0)
            batter_prev_balls = s.get(f'bat_{batter}_balls', 0)

            delivery_row = {
                'match_id': mid, 'date': dt_str,
                'competition': competition, 'match_type': match_t, 'gender': gender_val,
                'innings': innings, 'batting_team': bat_team, 'bowling_team': bowl_team,
                'over_num': over_num, 'ball_num': ball_num,
                'over_ball': f"{over_num}.{ball_num}",
                'batter': batter[:80], 'non_striker': non_striker[:80], 'bowler': bowler[:80],
                'runs_batter': runs_bat, 'runs_extras': runs_extras, 'runs_total': runs_total,
                'extras_type': ('wide' if wide else 'noball' if noball else 'bye' if byes else 'legbye' if legbyes else ''),
                'extras_wide': wide, 'extras_noball': noball,
                'extras_bye': byes, 'extras_legbye': legbyes, 'extras_penalty': penalty,
                'is_wide': 1 if wide else 0, 'is_noball': 1 if noball else 0,
                'boundary_4': 1 if runs_bat == 4 else 0,
                'boundary_6': 1 if runs_bat == 6 else 0,
                'wicket_fallen': is_wicket,
                'wicket_type': wicket_kind[:40], 'wicket_player': wicket_player[:80],
                'wicket_fielder': fielder[:80], 'wicket_bowler': bowler[:80] if is_wicket else '',
                'innings_runs': s['runs'], 'innings_wickets': s['wickets'],
                'batter_runs': batter_prev_runs, 'batter_balls': batter_prev_balls,
            }
            delivery_rows.append(delivery_row)

            # Update innings sums
            is_legal = 0 if wide or noball else 1
            s['runs'] += runs_total
            s['balls'] += is_legal
            s['extras'] += runs_extras
            if is_wicket: s['wickets'] += 1
            # Track batter running totals
            s[f'bat_{batter}_runs']  = batter_prev_runs + runs_bat
            s[f'bat_{batter}_balls'] = batter_prev_balls + is_legal

            # Update batting stats
            bk = (innings, batter)
            if bk not in batting_stats:
                batting_stats[bk] = {
                    'match_id': mid, 'date': dt_str, 'competition': competition,
                    'match_type': match_t, 'gender': gender_val, 'innings': innings,
                    'batting_team': bat_team, 'bowling_team': bowl_team,
                    'batter': batter[:80], 'player_id': '',
                    'bat_position': 0, 'runs': 0, 'balls_faced': 0,
                    'fours': 0, 'sixes': 0, 'not_out': 1,
                    'dismissed_by': '', 'dismissal_kind': '', 'fielder': '',
                    'runs_pp': 0, 'runs_middle': 0, 'runs_death': 0,
                    'balls_pp': 0, 'balls_middle': 0, 'balls_death': 0,
                }
            bs = batting_stats[bk]
            bs['runs']  += runs_bat
            bs['balls_faced'] += is_legal
            if runs_bat == 4: bs['fours'] += 1
            if runs_bat == 6: bs['sixes'] += 1
            # Phase tracking (T20: PP=1-6, mid=7-15, death=16-20)
            if match_t in ('T20', 'T20I', 'IT20'):
                if over_num < 6:
                    bs['runs_pp'] += runs_bat; bs['balls_pp'] += is_legal
                elif over_num < 15:
                    bs['runs_middle'] += runs_bat; bs['balls_middle'] += is_legal
                else:
                    bs['runs_death'] += runs_bat; bs['balls_death'] += is_legal
            if is_wicket and wicket_player == batter:
                bs['not_out'] = 0
                bs['dismissal_kind'] = wicket_kind[:40]
                bs['dismissed_by']   = bowler[:80]
                bs['fielder']        = fielder[:80]

            # Update bowling stats
            bwk = (innings, bowler)
            if bwk not in bowling_stats:
                bowling_stats[bwk] = {
                    'match_id': mid, 'date': dt_str, 'competition': competition,
                    'match_type': match_t, 'gender': gender_val, 'innings': innings,
                    'bowling_team': bowl_team, 'batting_team': bat_team,
                    'bowler': bowler[:80], 'player_id': '',
                    'overs': 0, 'maidens': 0, 'runs_conceded': 0, 'wickets': 0,
                    'wides': 0, 'no_balls': 0,
                    'runs_pp': 0, 'wickets_pp': 0, 'overs_pp': 0,
                    'runs_middle': 0, 'wickets_middle': 0,
                    'runs_death': 0, 'wickets_death': 0,
                    'wickets_bowled': 0, 'wickets_caught': 0, 'wickets_lbw': 0,
                    'wickets_stumped': 0, 'wickets_run_out': 0,
                    '_balls': 0,
                }
            bws = bowling_stats[bwk]
            bws['_balls'] += is_legal
            bws['runs_conceded'] += runs_total
            bws['wides']    += wide
            bws['no_balls'] += noball
            if is_wicket and wicket_kind not in ('run out', 'retired out'):
                bws['wickets'] += 1
                if wicket_kind == 'bowled':  bws['wickets_bowled'] += 1
                if wicket_kind == 'caught':  bws['wickets_caught'] += 1
                if wicket_kind == 'lbw':     bws['wickets_lbw'] += 1
                if wicket_kind == 'stumped': bws['wickets_stumped'] += 1
            # Phase tracking
            if match_t in ('T20', 'T20I', 'IT20'):
                if over_num < 6:
                    bws['runs_pp'] += runs_total; bws['wickets_pp'] += (is_wicket and wicket_kind not in ('run out',))
                    bws['overs_pp'] = round(bws['_balls']/6, 1)
                elif over_num < 15:
                    bws['runs_middle'] += runs_total; bws['wickets_middle'] += (is_wicket and wicket_kind not in ('run out',))
                else:
                    bws['runs_death'] += runs_total; bws['wickets_death'] += (is_wicket and wicket_kind not in ('run out',))

        except Exception as e:
            log.debug(f"Row parse error: {e}")
            continue

    # Finalise innings sums
    for inn, s in innings_sums.items():
        key_map = {1: ('inning1_runs','inning1_wickets','inning1_overs'),
                   2: ('inning2_runs','inning2_wickets','inning2_overs'),
                   3: ('inning3_runs','inning3_wickets',None),
                   4: ('inning4_runs','inning4_wickets',None)}
        if inn in key_map:
            kr, kw, ko = key_map[inn]
            match_row[kr] = s['runs']
            match_row[kw] = s['wickets']
            if ko: match_row[ko] = round(s['balls'] / 6, 2)
            match_row['total_runs']    = match_row.get('total_runs', 0) + s['runs']
            match_row['total_wickets'] = match_row.get('total_wickets', 0) + s['wickets']
            match_row['total_balls']   = match_row.get('total_balls', 0) + s['balls']
            match_row['total_extras']  = match_row.get('total_extras', 0) + s['extras']

    # Finalise bowling stats (calculate overs and derived metrics)
    for bws in bowling_stats.values():
        balls = bws.pop('_balls', 0)
        bws['overs'] = round(balls / 6, 2)
        bws['economy'] = round(bws['runs_conceded'] / max(bws['overs'], 0.1), 2)
        bws['bowling_avg'] = round(bws['runs_conceded'] / max(bws['wickets'], 1), 2)
        bws['strike_rate'] = round(balls / max(bws['wickets'], 1), 2)

    # Batting derived stats
    for bs in batting_stats.values():
        bf = max(bs['balls_faced'], 1)
        bs['strike_rate']  = round(bs['runs'] * 100 / bf, 2)
        total_runs = bs['runs']
        boundary_runs = bs['fours'] * 4 + bs['sixes'] * 6
        bs['boundary_pct'] = round(boundary_runs * 100 / max(total_runs, 1), 2)

    # Build position numbering
    batting_by_inn = {}
    for (inn, bat), bs in batting_stats.items():
        batting_by_inn.setdefault(inn, []).append(bs)
    for inn, inn_batters in batting_by_inn.items():
        for pos, bs in enumerate(inn_batters, 1):
            bs['bat_position'] = pos

    batting_rows  = list(batting_stats.values())
    bowling_rows  = list(bowling_stats.values())
    fielding_rows = []  # Parsed from wicket data above

    return match_row, delivery_rows[:5000], batting_rows, bowling_rows, fielding_rows


def scrape_cricket(ch_url: str, db: str, competitions: list = None,
                   genders: list = None, skip_deliveries: bool = False):
    """
    Загружает крикетные данные с cricsheet.org.
    competitions: список кодов соревнований (None = все)
    genders: ['male'] или ['male','female']
    """
    if competitions is None:
        competitions = list(COMPETITIONS.keys())
    if genders is None:
        genders = ['male']

    log.info(f"=== Cricket Scraper: {len(competitions)} competitions, genders={genders} ===")
    total_matches = 0
    total_balls   = 0

    for comp_code in competitions:
        comp_name, match_type = COMPETITIONS.get(comp_code, (comp_code, 'T20'))

        for gender in genders:
            log.info(f"Downloading {comp_name} ({gender})...")
            content = download_cricsheet_csv(comp_code, gender)
            if not content:
                log.warning(f"  No data for {comp_code}/{gender}")
                continue

            log.info(f"  Downloaded {len(content)//1024} KB, parsing...")
            m_rows, d_rows, bat_rows, bowl_rows, field_rows = parse_cricsheet_csv2(
                content, comp_code, match_type, gender
            )
            log.info(f"  Parsed: {len(m_rows)} matches, {len(d_rows)} deliveries, "
                     f"{len(bat_rows)} batting, {len(bowl_rows)} bowling")

            # Insert in batches
            for i in range(0, len(m_rows), 500):
                insert_batch(ch_url, db, 'cricket_matches', m_rows[i:i+500])
            for i in range(0, len(bat_rows), 2000):
                insert_batch(ch_url, db, 'cricket_batting', bat_rows[i:i+2000])
            for i in range(0, len(bowl_rows), 2000):
                insert_batch(ch_url, db, 'cricket_bowling', bowl_rows[i:i+2000])

            if not skip_deliveries:
                for i in range(0, len(d_rows), 5000):
                    insert_batch(ch_url, db, 'cricket_deliveries', d_rows[i:i+5000])

            total_matches += len(m_rows)
            total_balls   += len(d_rows)
            log.info(f"  ✓ {comp_name}: {len(m_rows)} matches loaded")
            time.sleep(1.0)

    log.info(f"=== Cricket DONE: {total_matches} matches, {total_balls} deliveries ===")
    return total_matches


if __name__ == '__main__':
    import sys
    ch  = sys.argv[1] if len(sys.argv) > 1 else 'http://localhost:8123'
    db  = sys.argv[2] if len(sys.argv) > 2 else 'betquant'
    mode = sys.argv[3] if len(sys.argv) > 3 else 'top'

    if mode == 'quick':
        comps = ['ipl', 't20s', 'odis']
        skip_del = True
    elif mode == 'top':
        comps = ['tests', 'odis', 't20s', 'ipl', 'bbl', 'psl', 'cpl']
        skip_del = False
    else:
        comps = None  # all
        skip_del = False

    scrape_cricket(ch, db, competitions=comps, skip_deliveries=skip_del)
