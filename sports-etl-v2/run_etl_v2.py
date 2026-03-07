#!/usr/bin/env python3
"""
BetQuant Sports ETL v2 — Master Orchestrator
Виды спорта: Basketball (NBA), Cricket, Rugby Union, NFL, Water Polo, Volleyball
Использование:
  python3 run_etl_v2.py [--ch-url URL] [--db DB] [--seasons N] [--sport SPORT] [--quick]

Флаги спорта:
  --basketball   NBA (box score + player stats + quarter stats)
  --cricket      Cricsheet ball-by-ball (21k+ матчей)
  --rugby        Rugby Union (6 Nations, WC, Premiership, URC, Super Rugby, Top14)
  --nfl          American Football (nflverse PbP + player stats)
  --waterpolo    Water Polo (FINA, LEN, Olympic)
  --volleyball   Volleyball (VNL, CEV, SuperLega, PlusLiga)
  --all          Все виды (по умолчанию)

Примеры:
  python3 run_etl_v2.py --quick --sport cricket
  python3 run_etl_v2.py --seasons 2 --sport basketball,nfl
  python3 run_etl_v2.py --all --seasons 3
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

import requests, os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [ETL-v2] %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_CH_URL = os.getenv('CLICKHOUSE_URL', 'http://clickhouse:8123')
DEFAULT_DB     = os.getenv('CLICKHOUSE_DB',  'betquant')


# ── Utility ───────────────────────────────────────────────────────────────────

def ch_query(url: str, query: str) -> bool:
    try:
        r = requests.post(f"{url}/", data=query.encode(), timeout=60)
        r.raise_for_status()
        return True
    except Exception as e:
        log.error(f"CH query error: {e}")
        return False


def apply_schema(ch_url: str, db: str) -> bool:
    log.info("Applying schema v2...")
    schema_path = os.path.join(os.path.dirname(__file__), 'schema', 'schema_sports_v2.sql')
    if not os.path.exists(schema_path):
        log.error(f"Schema file not found: {schema_path}")
        return False
    with open(schema_path, 'r') as f:
        sql = f.read()
    # Split by semicolons and execute each statement
    statements = [s.strip() for s in sql.split(';') if s.strip() and not s.strip().startswith('--')]
    ok = 0
    for stmt in statements:
        if ch_query(ch_url, stmt):
            ok += 1
        else:
            log.warning(f"Failed statement: {stmt[:80]}...")
    log.info(f"Schema applied: {ok}/{len(statements)} statements OK")
    return ok > 0


def log_etl(ch_url: str, db: str, sport: str, source: str, table: str,
            rows: int, duration: float, status: str, msg: str = ''):
    row = {
        'ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'sport': sport, 'source': source, 'competition': '',
        'season': '', 'table_name': table,
        'rows_loaded': rows, 'duration_sec': round(duration, 2),
        'status': status, 'message': msg[:500],
    }
    data = json.dumps(row)
    try:
        requests.post(
            f"{ch_url}/?query=INSERT+INTO+{db}.etl_log_v2+FORMAT+JSONEachRow",
            data=data.encode(), timeout=10
        )
    except: pass


# ── Sport runners ─────────────────────────────────────────────────────────────

def run_basketball(ch_url, db, seasons, quick):
    log.info("━━━ BASKETBALL (NBA) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    t0 = time.time()
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from scrapers.scraper_basketball import scrape_nba
        n = scrape_nba(ch_url, db, seasons_back=seasons)
        log_etl(ch_url, db, 'basketball', 'nba_api', 'basketball_matches_v2', n,
                time.time()-t0, 'ok', f"{n} matches loaded")
        return n
    except Exception as e:
        log.error(f"Basketball ETL failed: {e}", exc_info=True)
        log_etl(ch_url, db, 'basketball', 'nba_api', 'basketball_matches_v2', 0,
                time.time()-t0, 'error', str(e))
        return 0


def run_cricket(ch_url, db, seasons, quick):
    log.info("━━━ CRICKET (cricsheet.org) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    t0 = time.time()
    try:
        from scrapers.scraper_cricket import scrape_cricket, COMPETITIONS
        if quick:
            comps = ['ipl', 't20s', 'odis']
            skip_del = True
        else:
            comps = list(COMPETITIONS.keys())
            skip_del = False
        n = scrape_cricket(ch_url, db, competitions=comps, skip_deliveries=skip_del)
        log_etl(ch_url, db, 'cricket', 'cricsheet', 'cricket_matches', n,
                time.time()-t0, 'ok', f"{n} matches loaded")
        return n
    except Exception as e:
        log.error(f"Cricket ETL failed: {e}", exc_info=True)
        log_etl(ch_url, db, 'cricket', 'cricsheet', 'cricket_matches', 0,
                time.time()-t0, 'error', str(e))
        return 0


def run_rugby(ch_url, db, seasons, quick):
    log.info("━━━ RUGBY UNION ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    t0 = time.time()
    try:
        from scrapers.scraper_rugby import scrape_rugby
        n = scrape_rugby(ch_url, db, seasons_back=seasons)
        log_etl(ch_url, db, 'rugby', 'espn_scrum', 'rugby_matches', n,
                time.time()-t0, 'ok', f"{n} matches loaded")
        return n
    except Exception as e:
        log.error(f"Rugby ETL failed: {e}", exc_info=True)
        log_etl(ch_url, db, 'rugby', 'espn_scrum', 'rugby_matches', 0,
                time.time()-t0, 'error', str(e))
        return 0


def run_nfl(ch_url, db, seasons, quick):
    log.info("━━━ NFL (nflverse) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    t0 = time.time()
    try:
        from scrapers.scraper_nfl import scrape_nfl
        n = scrape_nfl(ch_url, db, seasons_back=seasons,
                       load_pbp=not quick, load_players=True)
        log_etl(ch_url, db, 'nfl', 'nflverse', 'nfl_games', n,
                time.time()-t0, 'ok', f"{n} games loaded")
        return n
    except Exception as e:
        log.error(f"NFL ETL failed: {e}", exc_info=True)
        log_etl(ch_url, db, 'nfl', 'nflverse', 'nfl_games', 0,
                time.time()-t0, 'error', str(e))
        return 0


def run_waterpolo(ch_url, db, seasons, quick):
    log.info("━━━ WATER POLO ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    t0 = time.time()
    try:
        from scrapers.scraper_waterpolo_volleyball import scrape_waterpolo
        n = scrape_waterpolo(ch_url, db, seasons_back=seasons)
        log_etl(ch_url, db, 'waterpolo', 'multi', 'waterpolo_matches', n,
                time.time()-t0, 'ok', f"{n} matches loaded")
        return n
    except Exception as e:
        log.error(f"WaterPolo ETL failed: {e}", exc_info=True)
        log_etl(ch_url, db, 'waterpolo', 'multi', 'waterpolo_matches', 0,
                time.time()-t0, 'error', str(e))
        return 0


def run_volleyball(ch_url, db, seasons, quick):
    log.info("━━━ VOLLEYBALL ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    t0 = time.time()
    try:
        from scrapers.scraper_waterpolo_volleyball import scrape_volleyball
        n = scrape_volleyball(ch_url, db, seasons_back=seasons)
        log_etl(ch_url, db, 'volleyball', 'multi', 'volleyball_matches', n,
                time.time()-t0, 'ok', f"{n} matches loaded")
        return n
    except Exception as e:
        log.error(f"Volleyball ETL failed: {e}", exc_info=True)
        log_etl(ch_url, db, 'volleyball', 'multi', 'volleyball_matches', 0,
                time.time()-t0, 'error', str(e))
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='BetQuant Sports ETL v2')
    parser.add_argument('--ch-url',   default=DEFAULT_CH_URL)
    parser.add_argument('--db',       default=DEFAULT_DB)
    parser.add_argument('--seasons',  type=int, default=3)
    parser.add_argument('--sport',    default='all',
                        help='Comma-separated: basketball,cricket,rugby,nfl,waterpolo,volleyball,all')
    parser.add_argument('--quick',    action='store_true', help='Quick mode (limited data)')
    parser.add_argument('--schema-only', action='store_true')
    parser.add_argument('--skip-schema', action='store_true')

    args = parser.parse_args()

    ch_url  = args.ch_url
    db      = args.db
    seasons = args.seasons
    quick   = args.quick
    sports  = [s.strip() for s in args.sport.split(',')]
    run_all = 'all' in sports

    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║       BetQuant Sports ETL v2 — Advanced Statistics      ║")
    log.info("║  Basketball | Cricket | Rugby | NFL | WaterPolo | VBall  ║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info(f"ClickHouse: {ch_url}, DB: {db}")
    log.info(f"Seasons back: {seasons}, Quick: {quick}, Sports: {sports}")

    # Apply schema
    if not args.skip_schema:
        if not apply_schema(ch_url, db):
            log.error("Schema application failed. Use --skip-schema to bypass.")
            if not args.schema_only:
                log.warning("Continuing anyway with existing schema...")

    if args.schema_only:
        log.info("Schema-only mode. Done.")
        return

    # Run scrapers
    results = {}
    t_total = time.time()

    if run_all or 'basketball' in sports:
        results['basketball'] = run_basketball(ch_url, db, seasons, quick)

    if run_all or 'cricket' in sports:
        results['cricket'] = run_cricket(ch_url, db, seasons, quick)

    if run_all or 'rugby' in sports:
        results['rugby'] = run_rugby(ch_url, db, seasons, quick)

    if run_all or 'nfl' in sports:
        results['nfl'] = run_nfl(ch_url, db, seasons, quick)

    if run_all or 'waterpolo' in sports:
        results['waterpolo'] = run_waterpolo(ch_url, db, seasons, quick)

    if run_all or 'volleyball' in sports:
        results['volleyball'] = run_volleyball(ch_url, db, seasons, quick)

    # Summary
    total_time = time.time() - t_total
    log.info("")
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║                    ETL v2 COMPLETE                       ║")
    log.info("╠══════════════════════════════════════════════════════════╣")
    for sport, n in results.items():
        log.info(f"║  {sport:<20} {n:>8} records loaded           ║")
    log.info(f"╠══════════════════════════════════════════════════════════╣")
    log.info(f"║  Total time: {total_time/60:.1f} minutes                              ║")
    log.info("╚══════════════════════════════════════════════════════════╝")


    def trigger_retrain(table: str, host: str = 'http://localhost:3000'):
        """Триггер переобучения нейросети после загрузки данных"""
        try:
            r = requests.post(
                f'{host}/api/neural/auto-retrain',
                json={'table': table},
                timeout=120  # обучение может занять до 2 мин
            )
            data = r.json()
            if data.get('ok'):
                print(f'✅ Neural retrained for {table}: accuracy {data.get("accuracy")}%')
            else:
                print(f'⚠️  Neural retrain skipped: {data.get("message")}')
        except Exception as e:
            print(f'⚠️  Neural retrain failed: {e}')

    # Вызов в конце ETL:
    trigger_retrain('football_matches')

if __name__ == '__main__':
    main()
