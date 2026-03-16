#!/usr/bin/env python3
"""
BetQuant ETL — Master Orchestrator
Запускает все скраперы в правильном порядке:
  1. Применяет расширенную схему ClickHouse
  2. Футбол (football-data.co.uk + understat)
  3. Хоккей (NHL API + KHL)
  4. Теннис (Sackmann ATP/WTA)
  5. Баскетбол (balldontlie NBA)
  6. Бейсбол (Chadwick)
  7. Финальный отчёт

Usage:
  python3 run_etl.py --ch-host http://localhost:8123 --seasons 3
  python3 run_etl.py --ch-host http://localhost:8123 --quick   # только топ-5 лиг, 1 сезон
"""

import subprocess
import sys
import json
import urllib.request
import argparse
import time
import os
from datetime import datetime

# ── ClickHouse client ─────────────────────────────────────────────────

class CH:
    def __init__(self, host, db, user='default', pw=''):
        self.host = host.rstrip('/')
        self.db   = db
        self.user = user
        self.pw   = pw

    def q(self, sql):
        url = f"{self.host}/?database={self.db}&user={self.user}"
        if self.pw: url += f"&password={self.pw}"
        req = urllib.request.Request(url, data=sql.encode(),
                                     headers={'Content-Type': 'application/octet-stream'})
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read().decode('utf-8').strip()

    def count(self, table):
        try:
            return int(self.q(f"SELECT count() FROM {self.db}.{table}"))
        except Exception:
            return -1

    def schema(self, sql_file):
        with open(sql_file) as f:
            statements = f.read().split(';')
        ok = 0
        errors = []
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt or stmt.startswith('--'):
                continue
            try:
                self.q(stmt)
                ok += 1
            except Exception as e:
                errors.append(str(e)[:100])
        return ok, errors


def run_script(script_path, extra_args, label):
    """Запускает Python скрипт как subprocess, возвращает (success, output)"""
    cmd = [sys.executable, script_path] + extra_args
    print(f"\n{'='*60}")
    print(f"▶ {label}")
    print(f"  cmd: {' '.join(cmd)}")
    print(f"{'='*60}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=False,  # показываем вывод в реальном времени
            timeout=7200,          # 2 часа максимум
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"  ⚠️ Timeout (2 часа) для {label}")
        return False
    except Exception as e:
        print(f"  ✗ {e}")
        return False


def apply_schema(ch, schema_file):
    """Применяет SQL схему"""
    print(f"\n{'='*60}")
    print(f"▶ СХЕМА ClickHouse: {schema_file}")
    print(f"{'='*60}")
    ok, errors = ch.schema(schema_file)
    if errors:
        print(f"  ⚠️ {len(errors)} ошибок при создании схемы:")
        for e in errors[:5]:
            print(f"    - {e}")
    else:
        print(f"  ✓ {ok} объектов создано/обновлено")
    return len(errors) == 0


def print_report(ch):
    """Итоговый отчёт по всем таблицам"""
    tables = {
        # Футбол
        'football_matches':    '⚽ Футбол — матчи',
        'football_events':     '⚽ Футбол — события (голы/карточки поминутно)',
        'football_team_form':  '⚽ Футбол — форма команд (rolling stats)',
        # Хоккей
        'hockey_matches':      '🏒 Хоккей — матчи',
        'hockey_events':       '🏒 Хоккей — события (голы/штрафы поминутно)',
        'hockey_team_form':    '🏒 Хоккей — форма команд (rolling stats)',
        # Другие виды
        'tennis_matches':      '🎾 Теннис ATP/WTA',
        'basketball_matches':  '🏀 Баскетбол NBA',
        'baseball_matches':    '⚾ Бейсбол MLB',
        # Служебные
        'etl_log':             '📋 ETL лог',
    }

    print(f"\n{'='*70}")
    print(f"  📊 ФИНАЛЬНЫЙ ОТЧЁТ — BetQuant ClickHouse")
    print(f"  {'Дата':<10}: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}")

    total_rows = 0
    for table, label in tables.items():
        n = ch.count(table)
        if n >= 0:
            print(f"  {label:<46} {n:>10,} строк")
            total_rows += max(n, 0)

    print(f"{'─'*70}")
    print(f"  {'ИТОГО':<46} {total_rows:>10,} строк")
    print(f"{'='*70}")

    # Топ лиги по количеству матчей
    try:
        sql = """
        SELECT league_name, count() as cnt
        FROM betquant.football_matches
        GROUP BY league_name
        ORDER BY cnt DESC LIMIT 10
        FORMAT TabSeparated
        """
        result = ch.q(sql)
        if result:
            print(f"\n  🏆 Топ футбольных лиг по кол-ву матчей:")
            for line in result.split('\n')[:10]:
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) == 2:
                        print(f"    {parts[0]:<35} {int(parts[1]):>6,}")
    except Exception:
        pass

    # Диапазон дат
    try:
        for table, sport in [('football_matches', '⚽'), ('hockey_matches', '🏒'), ('tennis_matches', '🎾')]:
            sql = f"SELECT min(date), max(date), count() FROM betquant.{table} FORMAT TabSeparated"
            r = ch.q(sql)
            if r.strip():
                parts = r.strip().split('\t')
                if len(parts) == 3:
                    print(f"  {sport} {table}: {parts[0]} — {parts[1]}  ({int(parts[2]):,} матчей)")
    except Exception:
        pass

    # ETL лог
    try:
        sql = """
        SELECT sport, source, status, sum(rows_loaded) as rows
        FROM betquant.etl_log
        GROUP BY sport, source, status
        ORDER BY sport, rows DESC
        FORMAT TabSeparated
        """
        result = ch.q(sql)
        if result:
            print(f"\n  📋 ETL Log summary:")
            for line in result.split('\n'):
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) == 4:
                        icon = '✅' if parts[2] == 'ok' else ('⚠️' if parts[2] == 'skip' else '❌')
                        print(f"    {icon} {parts[0]:<12} {parts[1]:<25} {parts[2]:<8} {int(parts[3]):>8,}")
    except Exception:
        pass

    print(f"\n{'='*70}")
    print(f"  ✅ База готова для бэктестинга!")
    print(f"  Подключение: {ch.host}")
    print(f"{'='*70}\n")


# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='BetQuant Master ETL')
    parser.add_argument('--ch-host',  default='http://localhost:8123')
    parser.add_argument('--ch-db',    default='betquant')
    parser.add_argument('--ch-user',  default='default')
    parser.add_argument('--ch-pass',  default='')
    parser.add_argument('--seasons',  type=int, default=3,
                        help='Сколько сезонов назад (default: 3)')
    parser.add_argument('--quick',    action='store_true',
                        help='Быстрый режим: топ-5 лиг, 1 сезон, без событий')
    parser.add_argument('--football-only',    action='store_true')
    parser.add_argument('--hockey-only',      action='store_true')
    parser.add_argument('--other-only',       action='store_true')
    parser.add_argument('--schema-only',      action='store_true',
                        help='Только применить схему, без загрузки данных')
    parser.add_argument('--report-only',      action='store_true')
    parser.add_argument('--skip-football',    action='store_true')
    parser.add_argument('--skip-hockey',      action='store_true')
    parser.add_argument('--skip-other',       action='store_true')
    parser.add_argument('--xg-source',        default='',
                        help='Источник xG: fbref|statsbomb|both|none (default: из ENV ETL_XG_SOURCE)')
    parser.add_argument('--fdo-token',        default='',
                        help='Токен football-data.org (или ENV FOOTBALLDATA_ORG_TOKEN)')
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))

    ch_args = [
        '--ch-host', args.ch_host,
        '--ch-db',   args.ch_db,
        '--ch-user', args.ch_user,
        '--ch-pass', args.ch_pass,
    ]

    ch = CH(args.ch_host, args.ch_db, args.ch_user, args.ch_pass)

    # ── Проверка подключения ─────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  BetQuant ETL — Master Orchestrator")
    print(f"  ClickHouse: {args.ch_host}")
    print(f"{'='*60}")

    try:
        ch.q("SELECT 1")
        print("✅ ClickHouse доступен")
    except Exception as e:
        print(f"❌ ClickHouse недоступен: {e}")
        print(f"\nПроверь что ClickHouse запущен:")
        print(f"  docker compose up -d clickhouse")
        print(f"  curl {args.ch_host}/ping")
        sys.exit(1)

    if args.report_only:
        print_report(ch)
        return

    # ── Схема ────────────────────────────────────────────────────────
    schema_file = os.path.join(script_dir, 'schema', 'clickhouse_extended.sql')
    schema_file = os.path.normpath(schema_file)
    if os.path.exists(schema_file):
        apply_schema(ch, schema_file)
    else:
        print(f"⚠️ Файл схемы не найден: {schema_file}")

    if args.schema_only:
        print_report(ch)
        return

    t_start = time.time()
    results = {}

    # ── Футбол ──────────────────────────────────────────────────────
    # ── Футбол ──────────────────────────────────────────────────────
    if not args.skip_football and not args.hockey_only and not args.other_only:
        xg_source = args.xg_source or os.environ.get('ETL_XG_SOURCE', 'fbref')
        fdo_token = args.fdo_token or os.environ.get('FOOTBALLDATA_ORG_TOKEN', '')

        football_args = ch_args + ['--seasons', str(args.seasons)]
        if args.quick:
            football_args += ['--leagues', 'E0,SP1,D1,I1,F1', '--skip-form']
        else:
            football_args += ['--leagues', 'top']

        # Источник xG вместо сломанного Understat
        football_args += ['--xg-source', xg_source]

        # Токен football-data.org если задан
        if fdo_token:
            football_args += ['--fdo-token', fdo_token]

        results['football'] = run_script(
            os.path.join(script_dir, 'scrapers', 'scraper_football.py'),
            football_args,
            f'ФУТБОЛ — football-data.co.uk + OpenLigaDB + football-data.org + xG:{xg_source}'
        )

    # ── Хоккей ──────────────────────────────────────────────────────
    if not args.skip_hockey and not args.football_only and not args.other_only:
        hockey_args = ch_args + ['--seasons', str(args.seasons)]
        if args.quick:
            hockey_args += ['--skip-pbp', '--max-games', '200']

        results['hockey'] = run_script(
            os.path.join(script_dir, 'scrapers', 'scraper_hockey.py'),
            hockey_args,
            'ХОККЕЙ — NHL API + KHL'
        )

    # ── Другие виды ──────────────────────────────────────────────────
    if not args.skip_other and not args.football_only and not args.hockey_only:
        other_args = ch_args + ['--years', str(args.seasons)]
        if args.quick:
            other_args += ['--skip-basketball', '--skip-baseball']

        results['other'] = run_script(
            os.path.join(script_dir, 'scrapers', 'scraper_other_sports.py'),
            other_args,
            'ТЕННИС / БАСКЕТБОЛ / БЕЙСБОЛ'
        )

    elapsed = time.time() - t_start
    print(f"\n⏱ Время выполнения: {elapsed/60:.1f} минут")

    # Результаты по скриптам
    print(f"\n{'─'*40}")
    for sport, ok in results.items():
        icon = '✅' if ok else '❌'
        print(f"  {icon} {sport}")

    # ── Финальный отчёт ──────────────────────────────────────────────
    print_report(ch)

# ── Neural auto-retrain trigger ──────────────────────────────────────────────
def trigger_retrain(tables: list, host: str = 'http://localhost:3000'):
    import urllib.request as _req, json as _json
    for table in tables:
        try:
            body = _json.dumps({'table': table}).encode()
            r = _req.urlopen(
                _req.Request(
                    f'{host}/api/neural/auto-retrain',
                    data=body,
                    headers={'Content-Type': 'application/json'},
                    method='POST'
                ), timeout=180
            )
            data = _json.loads(r.read())
            if data.get('ok'):
                print(f'✅ Neural retrained [{table}]: accuracy {data.get("accuracy")}%')
            else:
                print(f'⚠️  Neural retrain skipped [{table}]: {data.get("message")}')
        except Exception as e:
            print(f'⚠️  Neural retrain failed [{table}]: {e}')


if __name__ == '__main__':
    main()
    betquant_host = os.environ.get('BETQUANT_HOST', 'http://localhost:3000')
    trigger_retrain([
        'football_matches',
        'hockey_matches',
        'tennis_matches',
    ], host=betquant_host)
