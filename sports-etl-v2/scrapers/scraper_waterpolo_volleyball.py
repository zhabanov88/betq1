"""
BetQuant — Water Polo & Volleyball ETL Scrapers
Водное поло: FINA/LEN public scoreboards + Kaggle international dataset
Волейбол: FIVB / CEV public data + VNL API + openvolley datasets
"""

import time, json, hashlib, logging, requests, csv, io
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s [WP/VB] %(message)s')
log = logging.getLogger(__name__)

def insert_batch(ch_url, db, table, rows):
    if not rows: return 0
    lines = [json.dumps(r, ensure_ascii=False, default=str) for r in rows]
    url = f"{ch_url}/?query=INSERT+INTO+{db}.{table}+FORMAT+JSONEachRow"
    try:
        r = requests.post(url, data='\n'.join(lines).encode(), timeout=120)
        r.raise_for_status()
        return len(rows)
    except Exception as e:
        log.error(f"CH insert ({table}): {e}"); return 0

def safe_get(url, params=None):
    for i in range(3):
        try:
            r = requests.get(url, params=params, timeout=60,
                             headers={'User-Agent': 'Mozilla/5.0 BetQuant/2.0',
                                      'Accept': 'application/json, text/html'})
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning(f"GET attempt {i+1}: {e}")
            time.sleep(2*(i+1))
    return None

def mid(h, a, dt): return hashlib.md5(f"{dt}|{h}|{a}".encode()).hexdigest()[:16]
def flt(v):
    try: return float(v) if v not in (None,'','NA','nan') else 0.0
    except: return 0.0
def nt(v):
    try: return int(float(v)) if v not in (None,'','NA','nan') else 0
    except: return 0


# ═══════════════════════════════════════════════════════════════════
#  WATER POLO
# ═══════════════════════════════════════════════════════════════════

# ESPN Water Polo competition IDs
WP_ESPN_COMPS = {
    # Men's
    '3011': 'FINA World League',
    '3012': 'World Aquatics Championship',
    '3013': 'LEN Euro Cup',
    '3014': 'LEN Champions League Men',
    '3015': 'Olympic Water Polo Men',
    # Women's
    '3016': 'FINA World League Women',
    '3017': 'LEN Champions League Women',
    '3018': 'Olympic Water Polo Women',
}

# Sportdevs.com free API (rugby & waterpolo)
SPORTDEVS_BASE = 'https://api.sportdevs.com'

def fetch_wp_sportdevs(competition: str, season: str) -> list:
    """
    Sportdevs free API — water polo matches & stats
    Endpoint: /water-polo/matches?competition=...&season=...
    """
    url = f"{SPORTDEVS_BASE}/water-polo/matches"
    params = {'competition': competition, 'season': season}
    r = safe_get(url, params)
    if not r: return []
    try:
        data = r.json()
        matches = data if isinstance(data, list) else data.get('data', [])
        rows = []
        for m in matches:
            try:
                home = m.get('home_team', {})
                away = m.get('away_team', {})
                home_name = home.get('name', home.get('abbreviation', ''))
                away_name = away.get('name', away.get('abbreviation', ''))
                dt = str(m.get('date', ''))[:10] or '1970-01-01'
                score = m.get('score', {})
                h_score = nt(score.get('home', m.get('home_score', 0)))
                a_score = nt(score.get('away', m.get('away_score', 0)))

                periods = m.get('periods', [])
                h_q = [nt(periods[i].get('home_score', 0)) if len(periods) > i else 0 for i in range(4)]
                a_q = [nt(periods[i].get('away_score', 0)) if len(periods) > i else 0 for i in range(4)]

                stats_h = {s.get('name','').lower(): s.get('value',0)
                           for s in m.get('home_statistics', [])}
                stats_a = {s.get('name','').lower(): s.get('value',0)
                           for s in m.get('away_statistics', [])}

                row = {
                    'match_id': mid(home_name, away_name, dt),
                    'source': 'sportdevs', 'date': dt, 'season': season,
                    'competition': competition, 'competition_level': 'International',
                    'gender': 'male', 'round': m.get('round', ''),
                    'venue': m.get('venue', {}).get('name', '')[:80],
                    'home_team': home_name[:60], 'away_team': away_name[:60],
                    'home_score': h_score, 'away_score': a_score,
                    'result': 'H' if h_score > a_score else ('A' if a_score > h_score else 'D'),
                    'home_q1': h_q[0], 'away_q1': a_q[0],
                    'home_q2': h_q[1], 'away_q2': a_q[1],
                    'home_q3': h_q[2], 'away_q3': a_q[2],
                    'home_q4': h_q[3], 'away_q4': a_q[3],
                    'went_to_et': nt(m.get('extra_time', 0)),
                    # Shots & efficiency
                    'home_shots_total': nt(stats_h.get('shots', 0)),
                    'away_shots_total': nt(stats_a.get('shots', 0)),
                    'home_goals': h_score, 'away_goals': a_score,
                    'home_shot_pct': round(h_score/max(nt(stats_h.get('shots',1)),1)*100,1),
                    'away_shot_pct': round(a_score/max(nt(stats_a.get('shots',1)),1)*100,1),
                    # Power play
                    'home_powerplay_goals': nt(stats_h.get('powerplay_goals', stats_h.get('extra_man_goals', 0))),
                    'away_powerplay_goals': nt(stats_a.get('powerplay_goals', stats_a.get('extra_man_goals', 0))),
                    'home_powerplay_att': nt(stats_h.get('powerplay_att', stats_h.get('extra_man_att', 0))),
                    'away_powerplay_att': nt(stats_a.get('powerplay_att', stats_a.get('extra_man_att', 0))),
                    # Penalty
                    'home_penalty_goals': nt(stats_h.get('penalty_goals', 0)),
                    'away_penalty_goals': nt(stats_a.get('penalty_goals', 0)),
                    'home_penalty_att': nt(stats_h.get('penalty_att', 0)),
                    'away_penalty_att': nt(stats_a.get('penalty_att', 0)),
                    # Goalkeeper
                    'home_saves': nt(stats_h.get('saves', 0)),
                    'away_saves': nt(stats_a.get('saves', 0)),
                    'home_save_pct': flt(stats_h.get('save_pct', 0)),
                    'away_save_pct': flt(stats_a.get('save_pct', 0)),
                    # Exclusions
                    'home_exclusions': nt(stats_h.get('exclusions', stats_h.get('ejections', 0))),
                    'away_exclusions': nt(stats_a.get('exclusions', stats_a.get('ejections', 0))),
                    'home_exclusions_drawn': nt(stats_h.get('exclusions_drawn', 0)),
                    'away_exclusions_drawn': nt(stats_a.get('exclusions_drawn', 0)),
                    # Counterattack
                    'home_counterattack_goals': nt(stats_h.get('counterattack_goals', stats_h.get('counter_goals', 0))),
                    'away_counterattack_goals': nt(stats_a.get('counterattack_goals', stats_a.get('counter_goals', 0))),
                    # Other
                    'home_steals': nt(stats_h.get('steals', 0)),
                    'away_steals': nt(stats_a.get('steals', 0)),
                    'home_sprints_won': nt(stats_h.get('sprint_won', stats_h.get('swim_off_won', 0))),
                    'away_sprints_won': nt(stats_a.get('sprint_won', stats_a.get('swim_off_won', 0))),
                    'home_turnovers': nt(stats_h.get('turnovers', 0)),
                    'away_turnovers': nt(stats_a.get('turnovers', 0)),
                    'home_blocks': nt(stats_h.get('blocks', 0)),
                    'away_blocks': nt(stats_a.get('blocks', 0)),
                }
                rows.append(row)
            except Exception as e:
                log.debug(f"WP match parse: {e}")
        return rows
    except Exception as e:
        log.error(f"WP sportdevs parse: {e}")
        return []


def fetch_wp_kaggle_international() -> list:
    """
    Kaggle: International Water Polo Match Results dataset
    URL: https://www.kaggle.com/datasets/konakalab/international-water-polo-match-results
    Fallback: direct CSV from публичного зеркала
    """
    urls = [
        'https://raw.githubusercontent.com/konakalab/water-polo-data/main/matches.csv',
        'https://huggingface.co/datasets/sports/water-polo/resolve/main/international_matches.csv',
    ]
    for url in urls:
        r = safe_get(url)
        if r and r.status_code == 200:
            try:
                reader = csv.DictReader(io.StringIO(r.text))
                rows = []
                for rec in reader:
                    home = rec.get('home_team', rec.get('team1', ''))
                    away = rec.get('away_team', rec.get('team2', ''))
                    dt   = str(rec.get('date', ''))[:10] or '1970-01-01'
                    h_sc = nt(rec.get('home_score', rec.get('score1', 0)))
                    a_sc = nt(rec.get('away_score', rec.get('score2', 0)))
                    rows.append({
                        'match_id': mid(home, away, dt),
                        'source': 'kaggle_intl', 'date': dt,
                        'season': dt[:4],
                        'competition': rec.get('competition', rec.get('tournament', 'International')),
                        'competition_level': 'International',
                        'gender': rec.get('gender', 'male'),
                        'round': rec.get('round', rec.get('stage', '')),
                        'venue': rec.get('venue', rec.get('location', ''))[:80],
                        'home_team': home[:60], 'away_team': away[:60],
                        'home_score': h_sc, 'away_score': a_sc,
                        'result': 'H' if h_sc>a_sc else ('A' if a_sc>h_sc else 'D'),
                        'home_q1': nt(rec.get('q1_home', rec.get('home_q1', 0))),
                        'away_q1': nt(rec.get('q1_away', rec.get('away_q1', 0))),
                        'home_q2': nt(rec.get('q2_home', rec.get('home_q2', 0))),
                        'away_q2': nt(rec.get('q2_away', rec.get('away_q2', 0))),
                        'home_q3': nt(rec.get('q3_home', rec.get('home_q3', 0))),
                        'away_q3': nt(rec.get('q3_away', rec.get('away_q3', 0))),
                        'home_q4': nt(rec.get('q4_home', rec.get('home_q4', 0))),
                        'away_q4': nt(rec.get('q4_away', rec.get('away_q4', 0))),
                    })
                log.info(f"  WP Kaggle: {len(rows)} matches")
                return rows
            except Exception as e:
                log.debug(f"WP kaggle parse: {e}")
    return []


# ESPN Water Polo
def fetch_wp_espn(comp_id: str, season: str, gender: str = 'male') -> list:
    sport_path = 'water-polo' if gender == 'male' else 'water-polo-women'
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/{sport_path}/{comp_id}/scoreboard"
    params = {'limit': 300, 'dates': season}
    data_r = safe_get(url, params)
    if not data_r: return []
    try:
        data = data_r.json()
        events = data.get('events', [])
        rows = []
        for ev in events:
            try:
                comp_data = ev.get('competitions', [{}])[0]
                competitors = comp_data.get('competitors', [])
                if len(competitors) < 2: continue
                home = next((c for c in competitors if c.get('homeAway')=='home'), competitors[0])
                away = next((c for c in competitors if c.get('homeAway')=='away'), competitors[1])
                if ev.get('status',{}).get('type',{}).get('completed') != True: continue

                home_name = home.get('team',{}).get('abbreviation', '')
                away_name = away.get('team',{}).get('abbreviation', '')
                h_sc = nt(home.get('score', 0))
                a_sc = nt(away.get('score', 0))
                dt   = ev.get('date', '')[:10]

                ls_h = home.get('linescores', [])
                ls_a = away.get('linescores', [])
                h_q = [nt(ls_h[i].get('value',0)) if i < len(ls_h) else 0 for i in range(4)]
                a_q = [nt(ls_a[i].get('value',0)) if i < len(ls_a) else 0 for i in range(4)]

                stats_h = {s['name'].lower().replace(' ','_'): s.get('displayValue','0')
                           for s in home.get('statistics', [])}
                stats_a = {s['name'].lower().replace(' ','_'): s.get('displayValue','0')
                           for s in away.get('statistics', [])}

                def hs(k): return nt(stats_h.get(k, stats_h.get(k.replace('_',''),'0')))
                def as_(k): return nt(stats_a.get(k, stats_a.get(k.replace('_',''),'0')))

                rows.append({
                    'match_id': mid(home_name, away_name, dt),
                    'source': 'espn', 'date': dt, 'season': season,
                    'competition': WP_ESPN_COMPS.get(comp_id, comp_id),
                    'competition_level': 'International', 'gender': gender,
                    'round': str(ev.get('week', {}).get('number', '')),
                    'venue': comp_data.get('venue',{}).get('fullName','')[:80],
                    'home_team': home_name, 'away_team': away_name,
                    'home_score': h_sc, 'away_score': a_sc,
                    'result': 'H' if h_sc>a_sc else ('A' if a_sc>h_sc else 'D'),
                    'home_q1': h_q[0], 'away_q1': a_q[0],
                    'home_q2': h_q[1], 'away_q2': a_q[1],
                    'home_q3': h_q[2], 'away_q3': a_q[2],
                    'home_q4': h_q[3], 'away_q4': a_q[3],
                    'home_goals': h_sc, 'away_goals': a_sc,
                    'home_shots_total': hs('shots'), 'away_shots_total': as_('shots'),
                    'home_saves': hs('saves'), 'away_saves': as_('saves'),
                    'home_powerplay_goals': hs('extra_man_goals'), 'away_powerplay_goals': as_('extra_man_goals'),
                    'home_powerplay_att':   hs('extra_man_att'),   'away_powerplay_att':   as_('extra_man_att'),
                    'home_exclusions': hs('ejections'), 'away_exclusions': as_('ejections'),
                    'home_steals': hs('steals'), 'away_steals': as_('steals'),
                    'home_sprints_won': hs('swim_offs_won'), 'away_sprints_won': as_('swim_offs_won'),
                    'home_penalty_goals': hs('penalty_goals'), 'away_penalty_goals': as_('penalty_goals'),
                    'home_penalty_att': hs('penalty_att'), 'away_penalty_att': as_('penalty_att'),
                })
            except: pass
        return rows
    except Exception as e:
        log.error(f"WP ESPN parse: {e}")
        return []


def scrape_waterpolo(ch_url: str, db: str, seasons_back: int = 4):
    log.info("=== Water Polo Scraper ===")
    total = 0

    # 1. Kaggle international dataset (historical)
    rows = fetch_wp_kaggle_international()
    if rows:
        n = insert_batch(ch_url, db, 'waterpolo_matches', rows)
        total += n
        log.info(f"  Kaggle international: {n} matches")
    time.sleep(1)

    # 2. ESPN competitions
    curr_year = datetime.now().year
    for comp_id, comp_name in WP_ESPN_COMPS.items():
        gender = 'female' if 'Women' in comp_name else 'male'
        for yr in range(curr_year - seasons_back + 1, curr_year + 1):
            rows = fetch_wp_espn(comp_id, str(yr), gender)
            if rows:
                n = insert_batch(ch_url, db, 'waterpolo_matches', rows)
                total += n
                log.info(f"  ESPN {comp_name} {yr}: {n} matches")
            time.sleep(0.5)

    log.info(f"=== Water Polo DONE: {total} matches ===")
    return total


# ═══════════════════════════════════════════════════════════════════
#  VOLLEYBALL
# ═══════════════════════════════════════════════════════════════════

# VNL / FIVB API
VNL_BASE = 'https://www.volleyball.world/en/vnl'
FIVB_STATS_BASE = 'https://www.fivb.com/en/volleyball/competitions'

# ESPN Volleyball
VB_ESPN_COMPS = {
    '3301': ('VNL Men', 'male'),
    '3302': ('VNL Women', 'female'),
    '3303': ('World Championship Men', 'male'),
    '3304': ('World Championship Women', 'female'),
    '3305': ('Olympic Volleyball Men', 'male'),
    '3306': ('Olympic Volleyball Women', 'female'),
    '3307': ('CEV Champions League Men', 'male'),
    '3308': ('CEV Champions League Women', 'female'),
}

def fetch_vb_espn(comp_id: str, comp_name: str, season: str, gender: str) -> tuple:
    """ESPN volleyball matches with set scores"""
    sport = 'volleyball' if gender == 'male' else 'volleyball-women'
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/{sport}/{comp_id}/scoreboard"
    params = {'limit': 300, 'dates': season}
    r = safe_get(url, params)
    if not r: return [], []
    try:
        data = r.json()
        events = data.get('events', [])
        match_rows = []
        set_rows   = []

        for ev in events:
            try:
                if not ev.get('status',{}).get('type',{}).get('completed'): continue
                comp_data = ev.get('competitions', [{}])[0]
                competitors = comp_data.get('competitors', [])
                if len(competitors) < 2: continue

                home = next((c for c in competitors if c.get('homeAway')=='home'), competitors[0])
                away = next((c for c in competitors if c.get('homeAway')=='away'), competitors[1])
                home_name = home.get('team',{}).get('abbreviation', home.get('team',{}).get('shortDisplayName',''))
                away_name = away.get('team',{}).get('abbreviation', away.get('team',{}).get('shortDisplayName',''))
                dt   = ev.get('date','')[:10]

                ls_h = home.get('linescores', [])
                ls_a = away.get('linescores', [])
                # linescores = sets won; last entry = total sets
                sets_h = nt(home.get('score', 0))
                sets_a = nt(away.get('score', 0))
                total_sets = sets_h + sets_a

                # Per-set scores
                set_scores_h = [nt(ls_h[i].get('value',0)) if i < len(ls_h)-1 else 0 for i in range(5)]
                set_scores_a = [nt(ls_a[i].get('value',0)) if i < len(ls_a)-1 else 0 for i in range(5)]

                stats_h = {s['name'].lower().replace(' ','_'): s.get('displayValue','0')
                           for s in home.get('statistics', [])}
                stats_a = {s['name'].lower().replace(' ','_'): s.get('displayValue','0')
                           for s in away.get('statistics', [])}

                def hs(k): return nt(stats_h.get(k, '0'))
                def as_(k): return nt(stats_a.get(k, '0'))
                def hf(k): return flt(stats_h.get(k, '0').replace('%',''))
                def af(k): return flt(stats_a.get(k, '0').replace('%',''))

                total_h_pts = sum(set_scores_h)
                total_a_pts = sum(set_scores_a)

                match_row = {
                    'match_id': mid(home_name, away_name, dt),
                    'source': 'espn', 'date': dt, 'season': season,
                    'competition': comp_name, 'competition_level': 'International',
                    'gender': gender,
                    'round': str(ev.get('week',{}).get('number','')),
                    'venue': comp_data.get('venue',{}).get('fullName','')[:80],
                    'home_team': home_name, 'away_team': away_name,
                    'home_sets': sets_h, 'away_sets': sets_a,
                    'result': 'H' if sets_h > sets_a else 'A',
                    'total_sets': total_sets,
                    'home_s1': set_scores_h[0], 'away_s1': set_scores_a[0],
                    'home_s2': set_scores_h[1], 'away_s2': set_scores_a[1],
                    'home_s3': set_scores_h[2], 'away_s3': set_scores_a[2],
                    'home_s4': set_scores_h[3], 'away_s4': set_scores_a[3],
                    'home_s5': set_scores_h[4], 'away_s5': set_scores_a[4],
                    'total_points': total_h_pts + total_a_pts,
                    'home_total_pts': total_h_pts, 'away_total_pts': total_a_pts,
                    # Attack
                    'home_kills': hs('kills'), 'away_kills': as_('kills'),
                    'home_attack_err': hs('attack_errors'), 'away_attack_err': as_('attack_errors'),
                    'home_attack_att': hs('total_attacks'),  'away_attack_att': as_('total_attacks'),
                    'home_hit_pct': hf('hitting_percentage'), 'away_hit_pct': af('hitting_percentage'),
                    # Serves
                    'home_aces': hs('aces'), 'away_aces': as_('aces'),
                    'home_serve_err': hs('service_errors'), 'away_serve_err': as_('service_errors'),
                    'home_serve_att': hs('service_attempts'), 'away_serve_att': as_('service_attempts'),
                    # Blocks
                    'home_block_solos': hs('solo_blocks'), 'away_block_solos': as_('solo_blocks'),
                    'home_block_assists': hs('block_assists'), 'away_block_assists': as_('block_assists'),
                    'home_block_err': hs('blocking_errors'), 'away_block_err': as_('blocking_errors'),
                    'home_blocks_total': hs('total_blocks'), 'away_blocks_total': as_('total_blocks'),
                    # Setting & reception
                    'home_assists': hs('assists'), 'away_assists': as_('assists'),
                    'home_digs': hs('digs'), 'away_digs': as_('digs'),
                    'home_reception_err': hs('reception_errors'), 'away_reception_err': as_('reception_errors'),
                    'home_reception_att': hs('reception_attempts'), 'away_reception_att': as_('reception_attempts'),
                    # Points breakdown
                    'home_pts_from_kills':  hs('kills'),
                    'away_pts_from_kills':  as_('kills'),
                    'home_pts_from_aces':   hs('aces'),
                    'away_pts_from_aces':   as_('aces'),
                    'home_pts_from_blocks': hs('total_blocks'),
                    'away_pts_from_blocks': as_('total_blocks'),
                    'home_opponent_errors': hs('opponent_errors'),
                    'away_opponent_errors': as_('opponent_errors'),
                }
                match_rows.append(match_row)

                # Per-set rows
                for s_num in range(1, total_sets + 1):
                    if s_num > 5: break
                    h_pts = set_scores_h[s_num-1]
                    a_pts = set_scores_a[s_num-1]
                    if h_pts == 0 and a_pts == 0: continue
                    set_rows.append({
                        'match_id': match_row['match_id'],
                        'date': dt, 'competition': comp_name, 'gender': gender,
                        'home_team': home_name, 'away_team': away_name,
                        'set_num': s_num,
                        'home_pts': h_pts, 'away_pts': a_pts,
                        'duration_min': 0,
                    })

            except Exception as e:
                log.debug(f"VB ESPN event: {e}")

        return match_rows, set_rows
    except Exception as e:
        log.error(f"VB ESPN parse: {e}")
        return [], []


def fetch_vb_sportdevs(competition: str, season: str, gender: str = 'male') -> tuple:
    """Sportdevs API для клубного волейбола (SuperLega, PlusLiga, CEV)"""
    url = f"{SPORTDEVS_BASE}/volleyball/matches"
    params = {'competition': competition, 'season': season}
    r = safe_get(url, params)
    if not r: return [], []
    try:
        data = r.json()
        matches = data if isinstance(data, list) else data.get('data', [])
        match_rows, set_rows = [], []
        for m in matches:
            try:
                home = m.get('home_team', {})
                away = m.get('away_team', {})
                home_name = home.get('name', '')[:60]
                away_name = away.get('name', '')[:60]
                dt = str(m.get('date', ''))[:10] or '1970-01-01'
                sets_h = nt(m.get('home_score', 0))
                sets_a = nt(m.get('away_score', 0))
                sets_detail = m.get('set_scores', m.get('periods', []))

                s_h = [nt(sets_detail[i].get('home',0)) if len(sets_detail)>i else 0 for i in range(5)]
                s_a = [nt(sets_detail[i].get('away',0)) if len(sets_detail)>i else 0 for i in range(5)]

                stats_h = {s.get('name','').lower(): nt(s.get('value',0)) for s in m.get('home_statistics',[])}
                stats_a = {s.get('name','').lower(): nt(s.get('value',0)) for s in m.get('away_statistics',[])}

                match_rows.append({
                    'match_id': mid(home_name, away_name, dt),
                    'source': 'sportdevs', 'date': dt, 'season': season,
                    'competition': competition, 'competition_level': 'Club',
                    'gender': gender, 'round': m.get('round', ''),
                    'venue': m.get('venue', {}).get('name', '')[:80],
                    'home_team': home_name, 'away_team': away_name,
                    'home_sets': sets_h, 'away_sets': sets_a,
                    'result': 'H' if sets_h > sets_a else 'A',
                    'total_sets': sets_h + sets_a,
                    'home_s1': s_h[0], 'away_s1': s_a[0],
                    'home_s2': s_h[1], 'away_s2': s_a[1],
                    'home_s3': s_h[2], 'away_s3': s_a[2],
                    'home_s4': s_h[3], 'away_s4': s_a[3],
                    'home_s5': s_h[4], 'away_s5': s_a[4],
                    'home_total_pts': sum(s_h), 'away_total_pts': sum(s_a),
                    'total_points': sum(s_h)+sum(s_a),
                    'home_kills': stats_h.get('kills',0), 'away_kills': stats_a.get('kills',0),
                    'home_attack_err': stats_h.get('attack_errors',0), 'away_attack_err': stats_a.get('attack_errors',0),
                    'home_attack_att': stats_h.get('attacks',0), 'away_attack_att': stats_a.get('attacks',0),
                    'home_aces': stats_h.get('aces',0), 'away_aces': stats_a.get('aces',0),
                    'home_serve_err': stats_h.get('serve_errors',0), 'away_serve_err': stats_a.get('serve_errors',0),
                    'home_block_solos': stats_h.get('solo_blocks',0), 'away_block_solos': stats_a.get('solo_blocks',0),
                    'home_block_assists': stats_h.get('block_assists',0), 'away_block_assists': stats_a.get('block_assists',0),
                    'home_blocks_total': stats_h.get('blocks',0), 'away_blocks_total': stats_a.get('blocks',0),
                    'home_assists': stats_h.get('assists',0), 'away_assists': stats_a.get('assists',0),
                    'home_digs': stats_h.get('digs',0), 'away_digs': stats_a.get('digs',0),
                    'home_reception_err': stats_h.get('reception_errors',0), 'away_reception_err': stats_a.get('reception_errors',0),
                })

                for s_num in range(1, sets_h+sets_a+1):
                    if s_num > 5: break
                    if s_h[s_num-1]==0 and s_a[s_num-1]==0: continue
                    set_rows.append({
                        'match_id': mid(home_name, away_name, dt),
                        'date': dt, 'competition': competition, 'gender': gender,
                        'home_team': home_name, 'away_team': away_name,
                        'set_num': s_num,
                        'home_pts': s_h[s_num-1], 'away_pts': s_a[s_num-1],
                    })
            except: pass
        return match_rows, set_rows
    except Exception as e:
        log.error(f"VB sportdevs: {e}")
        return [], []


# Club competitions for sportdevs
VB_CLUB_COMPS = [
    ('superliga_men', 'SuperLega', 'male'),
    ('plusliga_men', 'PlusLiga', 'male'),
    ('bundesliga_vb_men', 'Bundesliga VB Men', 'male'),
    ('cev_champions_men', 'CEV Champions League Men', 'male'),
    ('superliga_women', 'SuperLega Women', 'female'),
    ('plusliga_women', 'PlusLiga Women', 'female'),
    ('cev_champions_women', 'CEV Champions League Women', 'female'),
    ('vnl_men', 'VNL Men', 'male'),
    ('vnl_women', 'VNL Women', 'female'),
]


def scrape_volleyball(ch_url: str, db: str, seasons_back: int = 3):
    log.info("=== Volleyball Scraper ===")
    total_matches = 0
    total_sets    = 0
    curr_year = datetime.now().year

    # 1. ESPN International
    for comp_id, (comp_name, gender) in VB_ESPN_COMPS.items():
        for yr in range(curr_year - seasons_back + 1, curr_year + 1):
            m_rows, s_rows = fetch_vb_espn(comp_id, comp_name, str(yr), gender)
            if m_rows:
                n = insert_batch(ch_url, db, 'volleyball_matches', m_rows)
                insert_batch(ch_url, db, 'volleyball_set_stats', s_rows)
                total_matches += n
                total_sets    += len(s_rows)
                log.info(f"  ESPN {comp_name} {yr}: {n} matches, {len(s_rows)} sets")
            time.sleep(0.5)

    # 2. Club competitions via sportdevs
    for comp_code, comp_name, gender in VB_CLUB_COMPS:
        for yr in range(curr_year - seasons_back + 1, curr_year + 1):
            m_rows, s_rows = fetch_vb_sportdevs(comp_code, str(yr), gender)
            if m_rows:
                n = insert_batch(ch_url, db, 'volleyball_matches', m_rows)
                insert_batch(ch_url, db, 'volleyball_set_stats', s_rows)
                total_matches += n
                total_sets    += len(s_rows)
                log.info(f"  Club {comp_name} {yr}: {n} matches, {len(s_rows)} sets")
            time.sleep(0.5)

    log.info(f"=== Volleyball DONE: {total_matches} matches, {total_sets} set rows ===")
    return total_matches


if __name__ == '__main__':
    import sys
    ch  = sys.argv[1] if len(sys.argv)>1 else 'http://localhost:8123'
    db  = sys.argv[2] if len(sys.argv)>2 else 'betquant'
    sb  = int(sys.argv[3]) if len(sys.argv)>3 else 3
    sport = sys.argv[4] if len(sys.argv)>4 else 'both'

    if sport in ('wp', 'waterpolo', 'both'):
        scrape_waterpolo(ch, db, sb)
    if sport in ('vb', 'volleyball', 'both'):
        scrape_volleyball(ch, db, sb)
