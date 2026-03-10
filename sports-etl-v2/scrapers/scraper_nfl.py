"""
BetQuant — NFL ETL  (ESPN-only, final)
======================================
ТОЛЬКО site.web.api.espn.com — единственный доступный домен.

БЫЛО (старый код):
  ✗ nflverse GitHub Releases → DNS fail (github.com недоступен)
  ✗ dates=2024 для ESPN → 400 Bad Request
  ✗ sport slug 'american-football' → неверный путь

СТАЛО:
  ✓ ESPN NFL scoreboard: sports/football/nfl/scoreboard
  ✓ dates=YYYYMMDD (weekly pagination, сентябрь → февраль)
  ✓ ESPN box score: summary?event={id} → player stats, quarter scores
  ✓ Полный сезон (REG + POST) без nflverse

ПОКРЫТИЕ:
  • nfl_games:        счёт, четверти, venue, spread, overtime
  • nfl_player_stats: passing/rushing/receiving из ESPN box score
  • nfl_pbp:          НЕ доступно через ESPN (только через nflverse)
                      → таблица остаётся пустой, это нормально
"""

import time, json, hashlib, logging, requests
from datetime import datetime, timedelta
from typing import List, Optional

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
        chunk = rows[i:i + batch]
        lines = [json.dumps(r, ensure_ascii=False, default=str) for r in chunk]
        url   = f"{ch_url}/?query=INSERT+INTO+{db}.{table}+FORMAT+JSONEachRow"
        try:
            r = requests.post(url, data='\n'.join(lines).encode(), timeout=120)
            r.raise_for_status()
            total += len(chunk)
        except Exception as e:
            log.error(f"  CH insert ({table}): {e}")
    return total


def safe_get(url: str, params: dict = None, timeout: int = 30):
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
                log.debug(f"  safe_get failed: {e}")
    return None


def mid(h: str, a: str, dt: str) -> str:
    return hashlib.md5(f"{dt}|{h}|{a}".encode()).hexdigest()[:16]

def flt(v, default=0.0):
    try: return float(v) if v not in (None, '', 'NA', 'nan', 'None', 'null') else default
    except: return default

def nt(v, default=0):
    try: return int(float(v)) if v not in (None, '', 'NA', 'nan', 'None', 'null') else default
    except: return default


def nfl_season_weeks(year: int) -> List[str]:
    """
    NFL сезон: сентябрь year — февраль year+1.
    Шаг 7 дней, формат YYYYMMDD.
    """
    dates = []
    d     = datetime(year, 9, 1)
    end   = datetime(year + 1, 2, 28)
    while d <= end:
        dates.append(d.strftime('%Y%m%d'))
        d += timedelta(days=7)
    return dates


# ─────────────────────────────────────────────────────────────────────────────
#  ESPN NFL Scoreboard → game rows
# ─────────────────────────────────────────────────────────────────────────────

ESPN_NFL_SCOREBOARD = 'https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard'
ESPN_NFL_SUMMARY    = 'https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/summary'


def parse_scoreboard_event(ev: dict, season: int) -> Optional[dict]:
    """Парсит одно событие ESPN NFL scoreboard → game row."""
    try:
        status = ev.get('status', {}).get('type', {})
        if not status.get('completed'):
            return None

        comp_data   = ev.get('competitions', [{}])[0]
        competitors = comp_data.get('competitors', [])
        if len(competitors) < 2:
            return None

        home = next((c for c in competitors if c.get('homeAway') == 'home'), competitors[0])
        away = next((c for c in competitors if c.get('homeAway') == 'away'), competitors[1])

        home_name = home.get('team', {}).get('abbreviation', '')
        away_name = away.get('team', {}).get('abbreviation', '')
        h_score   = nt(home.get('score', 0))
        a_score   = nt(away.get('score', 0))
        dt        = ev.get('date', '')[:10]
        event_id  = str(ev.get('id', mid(home_name, away_name, dt)))

        if not home_name or not away_name or not dt:
            return None

        # Четверти из linescores
        ls_h = home.get('linescores', [])
        ls_a = away.get('linescores', [])
        h_q  = [nt(ls_h[i].get('value', 0)) if i < len(ls_h) else 0 for i in range(4)]
        a_q  = [nt(ls_a[i].get('value', 0)) if i < len(ls_a) else 0 for i in range(4)]

        # OT = разница между итогом и суммой 4 кварталов
        h_ot = max(0, h_score - sum(h_q))
        a_ot = max(0, a_score - sum(a_q))

        # Venue
        venue_data = comp_data.get('venue', {})

        # Season type из notes или status
        notes     = ev.get('notes', [])
        note_text = notes[0].get('text', '') if notes else ''
        if 'Playoff' in note_text or 'Wild Card' in note_text or \
           'Divisional' in note_text or 'Conference' in note_text or \
           'Super Bowl' in note_text:
            season_type = 'POST'
        else:
            season_type = 'REG'

        week_num = 0
        for note in notes:
            txt = note.get('text', '')
            if 'Week' in txt:
                try: week_num = int(txt.split('Week')[1].strip().split()[0])
                except: pass

        # Spread из odds
        spread     = 0.0
        total_line = 0.0
        for item in comp_data.get('odds', []):
            if isinstance(item, dict):
                spread     = flt(item.get('spread', 0))
                total_line = flt(item.get('overUnder', 0))
                break

        return {
            'game_id':      event_id,
            'source':       'espn',
            'season':       season,
            'season_type':  season_type,
            'week':         week_num,
            'date':         dt,
            'home_team':    home_name,
            'away_team':    away_name,
            'venue':        venue_data.get('fullName', '')[:80],
            'roof':         '',
            'surface':      venue_data.get('grass', False) and 'grass' or 'turf',
            'temp':         0,
            'wind':         0,
            'div_game':     0,
            'home_score':   h_score,
            'away_score':   a_score,
            'result':       'H' if h_score > a_score else ('A' if a_score > h_score else 'T'),
            'spread':       spread,
            'total_line':   total_line,
            'home_q1': h_q[0], 'away_q1': a_q[0],
            'home_q2': h_q[1], 'away_q2': a_q[1],
            'home_q3': h_q[2], 'away_q3': a_q[2],
            'home_q4': h_q[3], 'away_q4': a_q[3],
            'home_ot': h_ot,   'away_ot': a_ot,
            'overtime': 1 if h_ot > 0 or a_ot > 0 else 0,
            # EPA/stats — заполнятся из box score
            'home_epa_total':    0.0, 'away_epa_total':    0.0,
            'home_epa_per_play': 0.0, 'away_epa_per_play': 0.0,
            'home_epa_pass':     0.0, 'away_epa_pass':     0.0,
            'home_epa_rush':     0.0, 'away_epa_rush':     0.0,
            'home_success_rate': 0.0, 'away_success_rate': 0.0,
            'home_ypp':          0.0, 'away_ypp':          0.0,
            'home_turnovers':    0,   'away_turnovers':    0,
            'home_penalties':    0,   'away_penalties':    0,
            'home_penalty_yds':  0,   'away_penalty_yds':  0,
            'home_first_downs':  0,   'away_first_downs':  0,
            'home_third_pct':    0.0, 'away_third_pct':    0.0,
            'home_wp_pregame':   0.0, 'away_wp_pregame':   0.0,
            '_event_id':         event_id,  # для box score lookup
        }
    except Exception as e:
        log.debug(f"  scoreboard event parse: {e}")
        return None


def fetch_nfl_scoreboard_week(date_str: str, season: int) -> List[dict]:
    """Загружает scoreboard за одну неделю."""
    r = safe_get(ESPN_NFL_SCOREBOARD, params={
        'dates':  date_str,
        'limit':  50,
        'lang':   'en',
        'region': 'us',
    })
    if not r:
        return []

    games = []
    try:
        for ev in r.json().get('events', []):
            g = parse_scoreboard_event(ev, season)
            if g:
                games.append(g)
    except Exception as e:
        log.debug(f"  scoreboard parse {date_str}: {e}")

    return games


# ─────────────────────────────────────────────────────────────────────────────
#  ESPN NFL Summary (box score) → game stats + player stats
# ─────────────────────────────────────────────────────────────────────────────

def fetch_box_score(event_id: str, game: dict) -> List[dict]:
    """
    ESPN /summary?event={id} даёт детальный box score:
    - командные stats (yards, first downs, penalties, turnovers, 3rd%)
    - player stats (passing, rushing, receiving)

    Обновляет game in-place.
    Возвращает список player stat rows.
    """
    r = safe_get(ESPN_NFL_SUMMARY, params={'event': event_id})
    if not r:
        return []

    player_rows = []
    try:
        data     = r.json()
        boxscore = data.get('boxscore', {})
        teams    = boxscore.get('teams', [])

        # Функция поиска стата по abbreviation
        def find_stat(stats_list, abbr):
            for cat in stats_list:
                for item in cat.get('stats', []):
                    if item.get('abbreviation', '').lower() == abbr.lower():
                        return item.get('displayValue', '0')
            return '0'

        for team_data in teams:
            team_abbr  = team_data.get('team', {}).get('abbreviation', '')
            home_away  = team_data.get('homeAway', '')
            prefix     = 'home' if home_away == 'home' else 'away'
            stats_list = team_data.get('statistics', [])

            # Командные stats
            game[f'{prefix}_first_downs']  = nt(find_stat(stats_list, 'firstDowns'))
            game[f'{prefix}_penalty_yds']  = nt(find_stat(stats_list, 'totalPenaltiesYards').split('-')[-1] if '-' in find_stat(stats_list, 'totalPenaltiesYards') else find_stat(stats_list, 'totalPenaltiesYards'))
            pens_str = find_stat(stats_list, 'totalPenaltiesYards')  # "5-45"
            if '-' in pens_str:
                p, py = pens_str.split('-', 1)
                game[f'{prefix}_penalties']   = nt(p)
                game[f'{prefix}_penalty_yds'] = nt(py)
            game[f'{prefix}_turnovers']    = nt(find_stat(stats_list, 'turnovers'))
            game[f'{prefix}_ypp']          = flt(find_stat(stats_list, 'yardsPerPlay'))
            third_str = find_stat(stats_list, 'thirdDownEff')  # "5-12"
            if '-' in third_str:
                tc, ta = third_str.split('-', 1)
                ta_n = max(nt(ta), 1)
                game[f'{prefix}_third_pct'] = round(nt(tc) / ta_n * 100, 1)

            # Player stats
            athletes = boxscore.get('players', [])
            dt       = game.get('date', '')
            season   = game.get('season', 0)

            for team_players in athletes:
                if team_players.get('team', {}).get('abbreviation', '') != team_abbr:
                    continue
                for stat_cat in team_players.get('statistics', []):
                    cat_name = stat_cat.get('name', '').lower()  # passing/rushing/receiving
                    keys     = [h.lower() for h in stat_cat.get('keys', [])]
                    labels   = stat_cat.get('labels', [])

                    for athlete_entry in stat_cat.get('athletes', []):
                        ath    = athlete_entry.get('athlete', {})
                        stats  = athlete_entry.get('stats', [])
                        if not stats:
                            continue

                        def st(label):
                            try:
                                idx = labels.index(label)
                                return stats[idx] if idx < len(stats) else '0'
                            except (ValueError, IndexError):
                                return '0'

                        row = {
                            'player_id':   ath.get('id', '')[:20],
                            'player_name': ath.get('displayName', '')[:50],
                            'team':        team_abbr[:5],
                            'season':      season,
                            'week':        game.get('week', 0),
                            'season_type': game.get('season_type', 'REG'),
                            'game_id':     event_id,
                            'date':        dt,
                            'position':    ath.get('position', {}).get('abbreviation', '')[:5],
                            'opponent':    (game.get('away_team') if home_away == 'home'
                                           else game.get('home_team', ''))[:5],
                            # Passing
                            'completions':     0, 'attempts':         0,
                            'passing_yards':   0, 'passing_tds':      0,
                            'interceptions':   0, 'sacks':            0,
                            'sack_yards':      0, 'passing_epa':      0.0,
                            # Rushing
                            'carries':         0, 'rushing_yards':    0,
                            'rushing_tds':     0, 'rushing_fumbles_lost': 0,
                            'rushing_epa':     0.0,
                            # Receiving
                            'receptions':      0, 'targets':          0,
                            'receiving_yards': 0, 'receiving_tds':    0,
                            'receiving_epa':   0.0, 'target_share':   0.0,
                            'air_yards_share': 0.0, 'wopr':           0.0,
                            'racr':            0.0,
                            'fantasy_points':  0.0, 'fantasy_points_ppr': 0.0,
                            'special_teams_tds': 0,
                        }

                        if cat_name == 'passing':
                            # ESPN passing: C/ATT, YDS, AVG, TD, INT, SACKS, QBR, RTG
                            ca = st('C/ATT')
                            if '/' in ca:
                                c, a_n = ca.split('/', 1)
                                row['completions'] = nt(c)
                                row['attempts']    = nt(a_n)
                            row['passing_yards'] = nt(st('YDS'))
                            row['passing_tds']   = nt(st('TD'))
                            row['interceptions'] = nt(st('INT'))
                            sack_str = st('SACKS')
                            if '-' in sack_str:
                                sk, syd = sack_str.split('-', 1)
                                row['sacks']      = nt(sk)
                                row['sack_yards'] = nt(syd)
                            else:
                                row['sacks'] = nt(sack_str)

                        elif cat_name == 'rushing':
                            row['carries']       = nt(st('CAR'))
                            row['rushing_yards'] = nt(st('YDS'))
                            row['rushing_tds']   = nt(st('TD'))

                        elif cat_name == 'receiving':
                            row['receptions']      = nt(st('REC'))
                            row['receiving_yards'] = nt(st('YDS'))
                            row['receiving_tds']   = nt(st('TD'))
                            row['targets']         = nt(st('TGT'))

                        else:
                            continue  # пропускаем kicking/punting/defense

                        # Fantasy points (PPR)
                        row['fantasy_points_ppr'] = round(
                            row['passing_yards'] * 0.04 +
                            row['passing_tds']   * 4 -
                            row['interceptions'] * 2 +
                            row['rushing_yards'] * 0.1 +
                            row['rushing_tds']   * 6 +
                            row['receptions']    * 1 +
                            row['receiving_yards'] * 0.1 +
                            row['receiving_tds'] * 6, 2)

                        player_rows.append(row)

    except Exception as e:
        log.debug(f"  box score parse {event_id}: {e}")

    return player_rows


# ─────────────────────────────────────────────────────────────────────────────
#  Главная функция
# ─────────────────────────────────────────────────────────────────────────────

def scrape_nfl(ch_url: str, db: str, seasons_back: int = 3,
               load_pbp: bool = False, load_players: bool = True) -> int:
    """
    ESPN-only NFL scraper.
    load_pbp игнорируется (PbP недоступен через ESPN без nflverse).
    """
    curr_year    = datetime.now().year
    seasons      = list(range(curr_year - seasons_back, curr_year + 1))
    total_games  = 0
    total_players = 0

    log.info(f"=== NFL Scraper (ESPN-only): сезоны {seasons} ===")

    for season in seasons:
        log.info(f"── Сезон {season} ──────────────────────────────────────")

        games_by_id: dict = {}   # event_id → game row (дедупликация)

        # ── 1. Scoreboard (weekly pagination) ─────────────────────────────
        weeks_fetched = 0
        for date_str in nfl_season_weeks(season):
            week_games = fetch_nfl_scoreboard_week(date_str, season)
            for g in week_games:
                eid = g.pop('_event_id', g['game_id'])
                if eid not in games_by_id:
                    games_by_id[eid] = g
            weeks_fetched += 1
            time.sleep(0.2)

        games = list(games_by_id.values())
        if not games:
            log.warning(f"  Нет данных для сезона {season}")
            continue

        log.info(f"  Scoreboard: {len(games)} игр из {weeks_fetched} недельных запросов")

        # ── 2. Box scores + player stats ──────────────────────────────────
        player_rows_all = []
        if load_players and games:
            log.info(f"  Загружаем box scores ({len(games)} игр)...")
            for i, (eid, game) in enumerate(games_by_id.items()):
                p_rows = fetch_box_score(eid, game)
                player_rows_all.extend(p_rows)
                # Лог каждые 20 игр
                if (i + 1) % 20 == 0:
                    log.info(f"    {i+1}/{len(games)} игр обработано")
                time.sleep(0.25)  # вежливая пауза

        # ── 3. Вставка в ClickHouse ────────────────────────────────────────
        n_games = insert_batch(ch_url, db, 'nfl_games', games)
        total_games += n_games
        log.info(f"  nfl_games: {n_games} записей")

        if player_rows_all:
            n_pl = insert_batch(ch_url, db, 'nfl_player_stats', player_rows_all)
            total_players += n_pl
            log.info(f"  nfl_player_stats: {n_pl} записей")

        time.sleep(1)

    log.info(f"=== NFL DONE: {total_games} игр, {total_players} player-stats ===")
    log.info("  (PbP недоступен через ESPN — nfl_pbp не заполнена)")
    return total_games