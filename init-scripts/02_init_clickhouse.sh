#!/bin/bash
# BetQuant — ClickHouse Schema Initializer
# Запускается один раз при первом старте стека через сервис clickhouse-init
set -e

echo "╔══════════════════════════════════════════════╗"
echo "║     BetQuant ClickHouse Schema Init          ║"
echo "╚══════════════════════════════════════════════╝"

echo "Waiting for ClickHouse to be ready..."
until clickhouse-client --host clickhouse --query "SELECT 1" 2>/dev/null; do
  echo "  ... waiting"
  sleep 2
done
echo "✅ ClickHouse is ready"

echo ""
echo "📄 Applying legacy schema (matches, odds, team_stats, tennis)..."
clickhouse-client --host clickhouse --multiline --multiquery < /init_legacy.sql 2>/dev/null || true
echo "  ✓ Legacy schema applied"

echo ""
echo "📄 Applying extended schema (football_matches, hockey, basketball, etl_log)..."
clickhouse-client --host clickhouse --multiline --multiquery < /init_extended.sql 2>/dev/null || true
echo "  ✓ Extended schema applied"

echo ""
echo "✅ ClickHouse schema init complete"