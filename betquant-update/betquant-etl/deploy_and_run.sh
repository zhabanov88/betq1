#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════
#  BetQuant ETL — Deploy & Run Script
#  Копирует скрипты на сервер и запускает загрузку данных
#
#  Usage:
#    chmod +x deploy_and_run.sh
#    ./deploy_and_run.sh
#
#  Кастомизация:
#    CH_HOST=http://localhost:8123  # ClickHouse хост (внутри Docker: http://clickhouse:8123)
#    SEASONS=3                       # Сколько сезонов назад
#    MODE=quick|full                 # quick = топ-5 лиг быстро, full = всё
# ═══════════════════════════════════════════════════════════════════════

set -e

# ── Настройки ────────────────────────────────────────────────────────
CH_HOST="${CH_HOST:-http://localhost:8123}"
CH_DB="${CH_DB:-betquant}"
CH_USER="${CH_USER:-default}"
CH_PASS="${CH_PASS:-}"
SEASONS="${SEASONS:-3}"
MODE="${MODE:-full}"
ETL_DIR="/opt/betquant-etl"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║       BetQuant ETL — Data Population Script         ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  CH Host:  ${CH_HOST}"
echo "║  Seasons:  ${SEASONS}"
echo "║  Mode:     ${MODE}"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── Проверка зависимостей ─────────────────────────────────────────────
echo "📦 Проверяем Python..."
python3 --version || { echo "❌ Python3 не найден"; exit 1; }

echo "📦 Устанавливаем зависимости..."
pip3 install requests beautifulsoup4 lxml --quiet 2>/dev/null || \
pip3 install requests beautifulsoup4 lxml --break-system-packages --quiet 2>/dev/null || \
echo "  (pip недоступен, используем stdlib)"

# ── Копируем скрипты ─────────────────────────────────────────────────
echo ""
echo "📁 Копируем ETL скрипты в ${ETL_DIR}..."
mkdir -p "${ETL_DIR}/scrapers" "${ETL_DIR}/schema"

# Определяем путь к нашим скриптам (рядом с этим файлом)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cp "${SCRIPT_DIR}/schema/clickhouse_extended.sql" "${ETL_DIR}/schema/"
cp "${SCRIPT_DIR}/scrapers/scraper_football.py"   "${ETL_DIR}/scrapers/"
cp "${SCRIPT_DIR}/scrapers/scraper_hockey.py"     "${ETL_DIR}/scrapers/"
cp "${SCRIPT_DIR}/scrapers/scraper_other_sports.py" "${ETL_DIR}/scrapers/"
cp "${SCRIPT_DIR}/run_etl.py"                     "${ETL_DIR}/"

echo "  ✓ Скрипты скопированы"

# ── Проверяем ClickHouse ──────────────────────────────────────────────
echo ""
echo "🔌 Проверяем ClickHouse ${CH_HOST}..."
if curl -sf "${CH_HOST}/ping" > /dev/null 2>&1; then
    echo "  ✓ ClickHouse доступен"
else
    echo ""
    echo "  ⚠️ ClickHouse недоступен на ${CH_HOST}"
    echo "  Попробуем внутренний адрес контейнера..."
    CH_HOST="http://clickhouse:8123"
    if curl -sf "${CH_HOST}/ping" > /dev/null 2>&1; then
        echo "  ✓ ClickHouse доступен на ${CH_HOST}"
    else
        echo ""
        echo "  ❌ ClickHouse не найден. Запусти:"
        echo "     docker compose up -d clickhouse"
        echo "     docker compose up -d  (весь стек)"
        exit 1
    fi
fi

# ── Применяем расширенную схему ───────────────────────────────────────
echo ""
echo "🗃️ Применяем расширенную схему ClickHouse..."
curl -sf \
  "${CH_HOST}/?user=${CH_USER}&database=${CH_DB}" \
  --data-binary @"${ETL_DIR}/schema/clickhouse_extended.sql" \
  -o /dev/null && echo "  ✓ Схема применена" || echo "  ⚠️ Применяем через Python..."

# ── Запускаем ETL ─────────────────────────────────────────────────────
echo ""
echo "🚀 Запускаем ETL (режим: ${MODE})..."
echo ""

EXTRA_ARGS=""
if [ "${MODE}" = "quick" ]; then
    EXTRA_ARGS="--quick"
fi

cd "${ETL_DIR}"
python3 run_etl.py \
  --ch-host  "${CH_HOST}" \
  --ch-db    "${CH_DB}" \
  --ch-user  "${CH_USER}" \
  --ch-pass  "${CH_PASS}" \
  --seasons  "${SEASONS}" \
  ${EXTRA_ARGS}

echo ""
echo "✅ ETL завершён!"
echo "   Проверь базу: curl '${CH_HOST}/?database=${CH_DB}&query=SELECT+count()+FROM+football_matches'"
