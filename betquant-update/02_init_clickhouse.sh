#!/bin/bash
set -e

echo "Waiting for ClickHouse to be ready..."
until clickhouse-client --host clickhouse --query "SELECT 1" 2>/dev/null; do
  sleep 2
done

echo "Running ClickHouse legacy schema..."
clickhouse-client --host clickhouse --multiline --multiquery < /init_legacy.sql 2>/dev/null || true

echo "Running ClickHouse extended schema..."
clickhouse-client --host clickhouse --multiline --multiquery < /init_extended.sql 2>/dev/null || true

echo "ClickHouse schema init complete"
