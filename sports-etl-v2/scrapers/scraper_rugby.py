"""
BetQuant — Rugby Union ETL Scraper
Источники:
  - rugbypy (Python package) — EPL/URC/Super Rugby 2022-2025
  - ESPN Scrum public data — исторические матчи
  - Open datasets (GitHub)
"""

import time, json, hashlib, logging, requests
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s [RUGBY] %(message)s')
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

def safe_get(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30,
                             headers={'User-Agent':'Mozilla/5.0 BetQuant/2.0'})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"GET {url} attempt {i+1}: {e}")
            time.sleep(2*(i+1))
    return None

def mid(h, a, dt): return hashlib.md5(f"{dt}|{h}|{a}".encode()).hexdigest()[:16]


# ── rugbypy API (open source, covers EPL/URC/Super Rugby 2022-2025) ──────────
RUGBYPY_BASE = 'https://api.rugbypass.com/v2'  # unofficial endpoint used by rugbypy

def get_rugbypy_matches():
    """
    Использует rugbypy Python package endpoints.
    Данные: 8000+ игроков, 250+ команд, 6000+ игр, 2022-2025
    """
    try:
        import subprocess
        result = subprocess.run(
            ['python3', '-c', '''
import json
try:
    from rugbypy.match import fetch_all_matches
    matches = fetch_all_matches()
    print(json.dumps(matches.to_dict("records") if hasattr(matches,"to_dict") else []))
except Exception as e:
    print(json.dumps({"error": str(e)}))
'''], capture_output=True, text=True, timeout=120)
        data = json.loads(result.stdout or '[]')
        return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"rugbypy not available: {e}")
        return []


def fetch_espnscrum_competition(comp_id: str, season: str) -> list:
    """
    ESPN Scrum public API — исторические результаты
    Endpoint: https://site.web.api.espn.com/apis/site/v2/sports/rugby/{comp_id}/scoreboard
    """
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/rugby/{comp_id}/scoreboard"
    params = {'limit': 200, 'dates': season}
    data = safe_get(url, params)
    if not data: return []
    events = data.get('events', [])
    results = []
    for ev in events:
        try:
            comp = ev.get('competitions', [{}])[0]
            competitors = comp.get('competitors', [{}]*2)
            if len(competitors) < 2: continue

            home = next((c for c in competitors if c.get('homeAway')=='home'), competitors[0])
            away = next((c for c in competitors if c.get('homeAway')=='away'), competitors[1])

            home_name = home.get('team',{}).get('abbreviation','')
            away_name = away.get('team',{}).get('abbreviation','')
            home_score = int(home.get('score',0) or 0)
            away_score = int(away.get('score',0) or 0)

            dt = (ev.get('date','') or '')[:10]
            status = ev.get('status',{}).get('type',{}).get('name','')
            if status not in ('STATUS_FINAL', 'Final'): continue

            # Stats from linescores
            linescores_h = home.get('linescores', [])
            linescores_a = away.get('linescores', [])
            h_h1 = int(linescores_h[0].get('value',0)) if linescores_h else 0
            a_h1 = int(linescores_a[0].get('value',0)) if linescores_a else 0

            statistics_h = {s['name']: s.get('displayValue','0')
                            for s in home.get('statistics', [])}
            statistics_a = {s['name']: s.get('displayValue','0')
                            for s in away.get('statistics', [])}

            def hs(k): return statistics_h.get(k, '0')
            def as_(k): return statistics_a.get(k, '0')
            def hi(k):
                try: return int(hs(k) or 0)
                except: return 0
            def ai(k):
                try: return int(as_(k) or 0)
                except: return 0
            def hf(k):
                try: return float(hs(k).replace('%','') or 0)
                except: return 0.0
            def af(k):
                try: return float(as_(k).replace('%','') or 0)
                except: return 0.0

            row = {
                'match_id': mid(home_name, away_name, dt),
                'source': 'espn_scrum', 'date': dt,
                'season': season, 'competition': str(comp_id),
                'round': ev.get('week',{}).get('number','') if ev.get('week') else '',
                'home_team': home_name, 'away_team': away_name,
                'venue': comp.get('venue',{}).get('fullName','')[:80],
                'attendance': int(comp.get('attendance',0) or 0),
                'home_score': home_score, 'away_score': away_score,
                'result': 'H' if home_score > away_score else ('A' if away_score > home_score else 'D'),
                'home_h1': h_h1, 'away_h1': a_h1,
                'home_h2': home_score - h_h1, 'away_h2': away_score - a_h1,
                'home_tries':       hi('tries'),           'away_tries':       ai('tries'),
                'home_conversions': hi('conversions'),     'away_conversions': ai('conversions'),
                'home_penalties_kick': hi('penaltyGoals'), 'away_penalties_kick': ai('penaltyGoals'),
                'home_drop_goals':  hi('dropGoals'),       'away_drop_goals':  ai('dropGoals'),
                'home_possession_pct': hf('possessionPct'), 'away_possession_pct': af('possessionPct'),
                'home_territory_pct':  hf('territoryPct'),  'away_territory_pct':  af('territoryPct'),
                'home_meters_carried': hi('metersCarried'), 'away_meters_carried': ai('metersCarried'),
                'home_carries':     hi('carries'),         'away_carries':     ai('carries'),
                'home_passes':      hi('passes'),          'away_passes':      ai('passes'),
                'home_tackles':     hi('tackles'),         'away_tackles':     ai('tackles'),
                'home_tackles_missed': hi('missedTackles'),'away_tackles_missed': ai('missedTackles'),
                'home_tackle_pct':  hf('tacklePct'),       'away_tackle_pct':  af('tacklePct'),
                'home_scrums_total': hi('scrums'),         'away_scrums_total': ai('scrums'),
                'home_scrums_won':   hi('scrumsWon'),      'away_scrums_won':   ai('scrumsWon'),
                'home_lineouts_total': hi('lineouts'),     'away_lineouts_total': ai('lineouts'),
                'home_lineouts_won': hi('lineoutsWon'),    'away_lineouts_won': ai('lineoutsWon'),
                'home_lineouts_stolen': hi('lineoutsStolen'), 'away_lineouts_stolen': ai('lineoutsStolen'),
                'home_clean_breaks': hi('cleanBreaks'),    'away_clean_breaks': ai('cleanBreaks'),
                'home_line_breaks':  hi('lineBreaks'),     'away_line_breaks':  ai('lineBreaks'),
                'home_offloads':    hi('offloads'),        'away_offloads':    ai('offloads'),
                'home_kicks_total': hi('kicks'),           'away_kicks_total': ai('kicks'),
                'home_22m_entries': hi('22mEntries'),      'away_22m_entries': ai('22mEntries'),
                'home_penalties_conceded': hi('penaltiesConceded'), 'away_penalties_conceded': ai('penaltiesConceded'),
                'home_yellow_cards': hi('yellowCards'),    'away_yellow_cards': ai('yellowCards'),
                'home_red_cards':   hi('redCards'),        'away_red_cards':   ai('redCards'),
                'home_turnovers_conceded': hi('turnoversConceded'), 'away_turnovers_conceded': ai('turnoversConceded'),
            }
            results.append(row)
        except Exception as e:
            log.debug(f"Event parse error: {e}")
    return results


# ESPN Rugby competition IDs
RUGBY_COMPETITIONS = {
    '180659': ('Six Nations', '2020,2021,2022,2023,2024'),
    '270557': ('Rugby Championship', '2020,2021,2022,2023,2024'),
    '289234': ('Rugby World Cup', '2023'),
    '270559': ('Premiership Rugby', '2021,2022,2023,2024'),
    '270560': ('URC', '2022,2023,2024'),
    '270556': ('Top 14', '2021,2022,2023,2024'),
    '270555': ('Super Rugby Pacific', '2022,2023,2024'),
    '164205': ('Heineken Champions Cup', '2021,2022,2023,2024'),
}


def scrape_rugby(ch_url: str, db: str, seasons_back: int = 3):
    log.info("=== Rugby Union Scraper ===")
    total = 0
    for comp_id, (comp_name, seasons_str) in RUGBY_COMPETITIONS.items():
        log.info(f"Competition: {comp_name}")
        seasons = seasons_str.split(',')[-seasons_back:]
        for season in seasons:
            rows = fetch_espnscrum_competition(comp_id, season)
            if rows:
                n = insert_batch(ch_url, db, 'rugby_matches', rows)
                total += n
                log.info(f"  {comp_name} {season}: {n} matches")
            time.sleep(1.0)
    log.info(f"=== Rugby DONE: {total} matches ===")
    return total


if __name__ == '__main__':
    import sys
    scrape_rugby(
        sys.argv[1] if len(sys.argv)>1 else 'http://localhost:8123',
        sys.argv[2] if len(sys.argv)>2 else 'betquant',
        int(sys.argv[3]) if len(sys.argv)>3 else 3
    )
