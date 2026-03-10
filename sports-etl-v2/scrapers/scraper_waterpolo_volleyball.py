"""
BetQuant — Volleyball ETL  (ESPN-only, final)
=============================================
ТОЛЬКО site.web.api.espn.com — единственный доступный домен.

БЫЛО (старый код):
  ✗ sport slug: 'volleyball' / 'volleyball-women'  → ESPN так не понимает
  ✗ comp_id: 3301–3308 → несуществующие
  ✗ dates=2024 → 400 Bad Request
  ✗ один запрос = max 200 матчей → теряли 600-800 NCAA матчей

СТАЛО:
  ✓ slug: 'womens-college-volleyball', 'mens-college-volleyball'
  ✓ dates=YYYYMMDD (одна дата = неделя)
  ✓ пагинация по неделям → все матчи сезона
  ✓ глобальная дедупликация по match_id
"""

import time, json, hashlib, logging, requests
from datetime import datetime, timedelta
from typing import Tuple, List

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [ETL-v2] %(levelname)s %(message)s')
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  Утилиты
# ─────────────────────────────────────────────────────────────────────────────

def insert_batch(ch_url: str, db: str, table: str, rows: list, batch: int = 2000) -> int:
    if not rows: return 0
    total = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        lines = [json.dumps(r, ensure_ascii=False, default=str) for r in chunk]
        url   = f"{ch_url}/?query=INSERT+INTO+{db}.{table}+FORMAT+JSONEachRow"
        try:
            r = requests.post(url, data='\n'.join(lines).encode(), timeout=120)
            r.raise_for_status()
            total += len(chunk)
        except Exception as e:
            log.error(f"  CH insert ({table}): {e}")
    return total


def safe_get(url: str, params: dict = None, timeout: int = 25):
    """
    ESPN-friendly GET.
    400/404/403 → None немедленно (slug не существует, не ретраим).
    Прочие ошибки → 2 ретрая.
    """
    headers = {
        'User-Agent':     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/122.0.0.0 Safari/537.36',
        'Accept':         'application/json, */*',
        'Accept-Language':'en-US,en;q=0.9',
        'Referer':        'https://www.espn.com/',
    }
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=headers,
                             timeout=timeout, allow_redirects=True)
            if r.status_code in (400, 403, 404, 410):
                return None
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError:
            return None
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                log.debug(f"  safe_get failed ({url}): {e}")
    return None


def mid(h: str, a: str, dt: str) -> str:
    return hashlib.md5(f"{dt}|{h}|{a}".encode()).hexdigest()[:16]

def flt(v) -> float:
    try: return float(v) if v not in (None, '', 'NA', 'nan', 'None') else 0.0
    except: return 0.0

def nt(v) -> int:
    try: return int(float(v)) if v not in (None, '', 'NA', 'nan', 'None') else 0
    except: return 0


def week_dates(year: int) -> List[str]:
    """
    Список дат с шагом 7 дней за весь год → YYYYMMDD.
    ESPN при dates=YYYYMMDD возвращает события в окне ~7 дней.
    """
    dates, d = [], datetime(year, 1, 1)
    while d.year == year:
        dates.append(d.strftime('%Y%m%d'))
        d += timedelta(days=7)
    return dates


# ─────────────────────────────────────────────────────────────────────────────
#  Парсер ESPN scoreboard
# ─────────────────────────────────────────────────────────────────────────────

def parse_scoreboard(data: dict, comp_name: str,
                     gender: str, year: int) -> Tuple[list, list]:
    match_rows, set_rows = [], []

    for ev in data.get('events', []):
        try:
            if not ev.get('status', {}).get('type', {}).get('completed'):
                continue

            comp_data   = ev.get('competitions', [{}])[0]
            competitors = comp_data.get('competitors', [])
            if len(competitors) < 2:
                continue

            home = next((c for c in competitors if c.get('homeAway') == 'home'),
                        competitors[0])
            away = next((c for c in competitors if c.get('homeAway') == 'away'),
                        competitors[1])

            def tname(c):
                t = c.get('team', {})
                return (t.get('displayName') or t.get('shortDisplayName')
                        or t.get('abbreviation') or '')[:60]

            home_name = tname(home)
            away_name = tname(away)
            dt        = ev.get('date', '')[:10]
            if not dt or not home_name or not away_name:
                continue

            h_sets     = nt(home.get('score', 0))
            a_sets     = nt(away.get('score', 0))
            total_sets = h_sets + a_sets
            if total_sets == 0:
                continue

            ls_h = home.get('linescores', [])
            ls_a = away.get('linescores', [])
            s_h  = [nt(ls_h[i].get('value', 0)) if i < len(ls_h) else 0 for i in range(5)]
            s_a  = [nt(ls_a[i].get('value', 0)) if i < len(ls_a) else 0 for i in range(5)]

            def stats(c):
                return {s['name'].lower().replace(' ', '_'): s.get('displayValue', '0')
                        for s in c.get('statistics', []) if 'name' in s}

            sh, sa = stats(home), stats(away)
            ih = lambda k: nt(sh.get(k, 0))
            ia = lambda k: nt(sa.get(k, 0))

            match_id = mid(home_name, away_name, dt)

            match_rows.append({
                'match_id':    match_id,
                'source':      'espn',
                'date':        dt,
                'season':      str(year),
                'competition': comp_name,
                'competition_level': comp_data.get('type', {}).get('abbreviation', 'NCAA'),
                'gender':      gender,
                'round':       str(comp_data.get('type', {}).get('text', '')),
                'venue':       comp_data.get('venue', {}).get('fullName', '')[:80],
                'home_team':   home_name,
                'away_team':   away_name,
                'home_sets':   h_sets,  'away_sets':   a_sets,
                'result':      'H' if h_sets > a_sets else 'A',
                'total_sets':  total_sets,
                'home_s1': s_h[0], 'away_s1': s_a[0],
                'home_s2': s_h[1], 'away_s2': s_a[1],
                'home_s3': s_h[2], 'away_s3': s_a[2],
                'home_s4': s_h[3], 'away_s4': s_a[3],
                'home_s5': s_h[4], 'away_s5': s_a[4],
                'home_total_pts': sum(s_h), 'away_total_pts': sum(s_a),
                'total_points':   sum(s_h) + sum(s_a),
                'duration_min':   0,
                'home_kills':         ih('kills'),
                'away_kills':         ia('kills'),
                'home_attack_err':    ih('attack_errors'),
                'away_attack_err':    ia('attack_errors'),
                'home_attack_att':    ih('total_attacks'),
                'away_attack_att':    ia('total_attacks'),
                'home_hit_pct':       flt(sh.get('hitting_percentage', 0)),
                'away_hit_pct':       flt(sa.get('hitting_percentage', 0)),
                'home_aces':          ih('service_aces'),
                'away_aces':          ia('service_aces'),
                'home_serve_err':     ih('service_errors'),
                'away_serve_err':     ia('service_errors'),
                'home_blocks_total':  ih('team_blocks'),
                'away_blocks_total':  ia('team_blocks'),
                'home_block_solos':   ih('solo_blocks'),
                'away_block_solos':   ia('solo_blocks'),
                'home_block_assists': ih('block_assists'),
                'away_block_assists': ia('block_assists'),
                'home_digs':          ih('digs'),
                'away_digs':          ia('digs'),
                'home_reception_err': ih('reception_errors'),
                'away_reception_err': ia('reception_errors'),
                'home_assists':       ih('assists'),
                'away_assists':       ia('assists'),
                'home_pts_from_kills':   ih('kills'),
                'away_pts_from_kills':   ia('kills'),
                'home_pts_from_aces':    ih('service_aces'),
                'away_pts_from_aces':    ia('service_aces'),
                'home_pts_from_blocks':  ih('team_blocks'),
                'away_pts_from_blocks':  ia('team_blocks'),
                'home_opponent_errors':  ih('opponent_errors'),
                'away_opponent_errors':  ia('opponent_errors'),
            })

            for i in range(min(total_sets, 5)):
                if s_h[i] == 0 and s_a[i] == 0:
                    continue
                set_rows.append({
                    'match_id':    match_id,
                    'date':        dt,
                    'competition': comp_name,
                    'gender':      gender,
                    'home_team':   home_name,
                    'away_team':   away_name,
                    'set_num':     i + 1,
                    'home_pts':    s_h[i],
                    'away_pts':    s_a[i],
                    'duration_min': 0,
                })

        except Exception as e:
            log.debug(f"  event parse: {e}")

    return match_rows, set_rows


# ─────────────────────────────────────────────────────────────────────────────
#  ESPN volleyball slugs
# ─────────────────────────────────────────────────────────────────────────────

ESPN_VB_SLUGS = [
    ('womens-college-volleyball', 'NCAA Women Volleyball', 'female'),  # ✅ confirmed
    ('mens-college-volleyball',   'NCAA Men Volleyball',   'male'),    # ✅ confirmed
    ('beach-volleyball',          'Beach Volleyball',      'mixed'),   # try
    ('mens-volleyball',           'Men Volleyball Intl',   'male'),    # try
    ('womens-volleyball',         'Women Volleyball Intl', 'female'),  # try
]

ESPN_VB_BASE = 'https://site.web.api.espn.com/apis/site/v2/sports/volleyball'


def _fetch_slug_year(slug: str, comp_name: str, gender: str,
                     year: int, seen_ids: set) -> Tuple[list, list]:
    """Полная загрузка одного slug/year через еженедельную пагинацию."""
    url       = f"{ESPN_VB_BASE}/{slug}/scoreboard"
    all_m     = []
    all_s     = []
    slug_live = False  # True если хоть один запрос вернул 200

    for date_str in week_dates(year):
        r = safe_get(url, params={'dates': date_str, 'limit': 100,
                                  'lang': 'en', 'region': 'us'})
        if r is None:
            if not slug_live:
                return [], []   # slug вообще не доступен
            break               # данные закончились

        slug_live = True
        try:
            m_rows, s_rows = parse_scoreboard(r.json(), comp_name, gender, year)
            new_m = [m for m in m_rows if m['match_id'] not in seen_ids]
            for m in new_m:
                seen_ids.add(m['match_id'])
            all_m.extend(new_m)
            all_s.extend(s_rows)
        except Exception as e:
            log.debug(f"  parse {slug} {date_str}: {e}")

        time.sleep(0.15)

    return all_m, all_s


# ─────────────────────────────────────────────────────────────────────────────
#  Главная функция
# ─────────────────────────────────────────────────────────────────────────────

def scrape_volleyball(ch_url: str, db: str, seasons_back: int = 3) -> int:
    log.info("=== Volleyball Scraper (ESPN-only, weekly pagination) ===")

    curr_year     = datetime.now().year
    total_matches = 0
    total_sets    = 0
    seen_ids      = set()

    for slug, comp_name, gender in ESPN_VB_SLUGS:
        slug_total = 0

        for yr in range(curr_year - seasons_back + 1, curr_year + 1):
            m_rows, s_rows = _fetch_slug_year(slug, comp_name, gender, yr, seen_ids)

            if not m_rows:
                if slug_total == 0 and yr == curr_year - seasons_back + 1:
                    log.info(f"  Slug '{slug}' недоступен, пропускаем")
                    break
                continue

            n = insert_batch(ch_url, db, 'volleyball_matches', m_rows)
            insert_batch(ch_url, db, 'volleyball_set_stats', s_rows)
            slug_total    += n
            total_matches += n
            total_sets    += len(s_rows)
            log.info(f"  {comp_name} {yr}: {n} матчей, {len(s_rows)} партий")

        if slug_total:
            log.info(f"  └─ Итого {comp_name}: {slug_total} матчей")

    log.info(f"=== Volleyball DONE: {total_matches} матчей, {total_sets} партий ===")
    return total_matches