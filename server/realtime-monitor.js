'use strict';
/**
 * BetQuant — server/realtime-monitor.js v4
 * Исправления:
 * 1. TABLE_DEFS → реальные ETL-таблицы (football_matches и др.)
 * 2. Проверка существования таблицы перед COUNT
 * 3. docker start с проверкой существования контейнера
 * 4. Новые endpoints: /ch-tables, /summary, /etl-log
 */
const express      = require('express');
const { execFile } = require('child_process');
const http         = require('http');
const router       = express.Router();

const CONTAINER = process.env.REALTIME_CONTAINER || 'betquant-realtime';
const CH_HOST   = process.env.CH_HOST            || 'http://clickhouse:8123';
const CH_DB     = process.env.CH_DATABASE        || 'betquant';

// ── ClickHouse HTTP query (только stdlib) ─────────────────────────────────────
function chQuery(sql) {
  return new Promise((resolve) => {
    let parsed;
    try { parsed = new URL(CH_HOST); } catch { return resolve(null); }
    const path = `/?database=${encodeURIComponent(CH_DB)}&query=${encodeURIComponent(sql)}`;
    const req  = http.request(
      { hostname: parsed.hostname, port: parseInt(parsed.port) || 8123,
        path, method: 'GET', timeout: 8000 },
      (res) => { let buf = ''; res.on('data', d => buf += d); res.on('end', () => resolve(buf.trim())); }
    );
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.end();
  });
}

// ── Docker CLI helper ─────────────────────────────────────────────────────────
function docker(args, ms = 15000) {
  return new Promise(resolve => {
    execFile('docker', args, { timeout: ms }, (err, stdout, stderr) =>
      resolve({ ok: !err, stdout: (stdout||'').trim(), stderr: (stderr||'').trim() })
    );
  });
}
async function dockerAvailable() {
  const r = await docker(['info', '--format', '{{.ServerVersion}}'], 5000);
  return r.ok && r.stdout.length > 0;
}

// ── Реальные ETL-таблицы ──────────────────────────────────────────────────────
const ETL_TABLES = [
  { name: 'football_matches',   col: 'date',       label: '⚽ Футбол — матчи',       maxDays: 7  },
  { name: 'football_team_form', col: 'date',       label: '⚽ Футбол — форма команд', maxDays: 7  },
  { name: 'football_events',    col: 'date',       label: '⚽ Футбол — события',      maxDays: 7  },
  { name: 'hockey_matches',     col: 'date',       label: '🏒 Хоккей — матчи',        maxDays: 7  },
  { name: 'tennis_matches',     col: 'date',       label: '🎾 Теннис — матчи',        maxDays: 14 },
  { name: 'basketball_matches', col: 'date',       label: '🏀 Баскетбол — матчи',     maxDays: 14 },
  { name: 'baseball_matches',   col: 'date',       label: '⚾ Бейсбол — матчи',       maxDays: 14 },
  { name: 'cricket_matches',    col: 'date',       label: '🏏 Крикет',                maxDays: 30 },
  { name: 'rugby_matches',      col: 'date',       label: '🏉 Регби',                 maxDays: 30 },
  { name: 'nfl_games',          col: 'date',       label: '🏈 NFL',                   maxDays: 30 },
  { name: 'etl_log',            col: 'created_at', label: '📋 ETL лог',               maxDays: 30 },
];

// ── GET /api/realtime/ping ────────────────────────────────────────────────────
router.get('/ping', (req, res) => res.json({
  ok: true, container: CONTAINER, ch: CH_HOST, db: CH_DB, ts: new Date().toISOString()
}));

// ── GET /api/realtime/status ──────────────────────────────────────────────────
router.get('/status', async (req, res) => {
  const now = Date.now();

  // 1. Docker контейнер
  let container = { status: 'unknown', startedAt: null, restarts: 0, dockerAvailable: false, error: null };
  const hasDocker = await dockerAvailable();
  container.dockerAvailable = hasDocker;

  if (hasDocker) {
    const r = await docker(['inspect', '--format',
      '{{.State.Status}}|{{.State.StartedAt}}|{{.RestartCount}}', CONTAINER]);
    if (r.ok && r.stdout && !r.stdout.startsWith('Error')) {
      const [status, startedAt, restarts] = r.stdout.split('|');
      container = { status, startedAt, restarts: parseInt(restarts)||0, dockerAvailable: true, error: null };
    } else {
      container.status = 'not_found';
      container.error  = `Контейнер ${CONTAINER} не найден. Запустите: docker compose up -d realtime`;
    }
  } else {
    container.error = 'Docker socket недоступен. Добавьте в docker-compose.yml (сервис app):\n  volumes:\n    - /var/run/docker.sock:/var/run/docker.sock:ro\nЗатем: docker compose up -d app';
  }

  // 2. ClickHouse ping
  const chPing    = await chQuery('SELECT 1 FORMAT TabSeparated');
  const chConnected = chPing === '1';

  // 3. Состояние каждой ETL-таблицы
  const tables = await Promise.all(ETL_TABLES.map(async ({ name, col, label, maxDays }) => {
    // Сначала проверяем что таблица вообще есть
    const existsRaw = await chQuery(
      `SELECT count() FROM system.tables WHERE database='${CH_DB}' AND name='${name}' FORMAT TabSeparated`
    );
    if (!existsRaw || parseInt(existsRaw) === 0) {
      return { name, label, count: 0, last: null, ageMin: null, healthy: false,
               exists: false, error: 'Таблица не существует — запустите ETL' };
    }

    const raw = await chQuery(
      `SELECT count() AS n, toString(max(${col})) AS last FROM ${CH_DB}.${name} FORMAT TabSeparated`
    );
    if (!raw) {
      return { name, label, count: 0, last: null, ageMin: null, healthy: false,
               exists: true, error: 'ClickHouse timeout' };
    }
    const [c, l]  = raw.split('\t');
    const count   = parseInt(c) || 0;
    const last    = l && l !== '\\N' && !l.trim().startsWith('1970') ? l.trim() : null;
    const lastMs  = last ? new Date(last.replace(' ', 'T')+'Z').getTime() : 0;
    const ageMin  = (lastMs && lastMs > 0) ? Math.round((now - lastMs) / 60000) : null;
    const healthy = count > 0 && ageMin !== null && ageMin < maxDays * 24 * 60;
    return { name, label, count, last, ageMin, healthy, exists: true, error: null };
  }));

  // 4. ETL источники из лога
  let etlSources = [];
  const logExists = tables.find(t => t.name === 'etl_log')?.exists;
  if (logExists && chConnected) {
    const raw = await chQuery(
      `SELECT source, sum(rows_loaded) AS rows, toString(max(created_at)) AS last_run
       FROM ${CH_DB}.etl_log WHERE status='ok'
       GROUP BY source ORDER BY rows DESC
       FORMAT TabSeparated`
    );
    if (raw) {
      etlSources = raw.split('\n').filter(l => l.trim()).map(l => {
        const [source, rows, last_run] = l.split('\t');
        return { source: source?.trim(), rows: parseInt(rows)||0, last_run: last_run?.trim() };
      });
    }
  }

  // 5. Odds API конфиг
  const oddsApiConfigured = (process.env.ODDS_API_KEY || '').length > 5;

  res.json({
    container, tables, chConnected, etlSources,
    oddsApi: { configured: oddsApiConfigured, key: oddsApiConfigured ? '✅ задан' : '❌ не задан' },
    timestamp: new Date().toISOString(),
  });
});

// ── GET /api/realtime/ch-tables ───────────────────────────────────────────────
router.get('/ch-tables', async (req, res) => {
  const raw = await chQuery(
    `SELECT name, total_rows, formatReadableSize(total_bytes) AS size, engine
     FROM system.tables WHERE database='${CH_DB}' ORDER BY total_rows DESC FORMAT JSON`
  );
  if (!raw) return res.json({ tables: [] });
  try { res.json({ tables: JSON.parse(raw).data || [] }); }
  catch(e) { res.json({ tables: [], error: e.message }); }
});

// ── GET /api/realtime/summary ─────────────────────────────────────────────────
router.get('/summary', async (req, res) => {
  const names = ['football_matches','hockey_matches','tennis_matches','basketball_matches','football_events'];
  const result = {};
  await Promise.all(names.map(async t => {
    const raw = await chQuery(
      `SELECT count(), min(date), max(date) FROM ${CH_DB}.${t} FORMAT TabSeparated`
    );
    if (raw) {
      const [count, minDate, maxDate] = raw.split('\t');
      result[t] = { count: parseInt(count)||0, minDate: minDate?.trim(), maxDate: maxDate?.trim() };
    } else {
      result[t] = { count: 0 };
    }
  }));
  res.json(result);
});

// ── GET /api/realtime/etl-log ─────────────────────────────────────────────────
router.get('/etl-log', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit)||50, 200);
  const raw   = await chQuery(
    `SELECT created_at, sport, source, league, season, rows_loaded, status, message
     FROM ${CH_DB}.etl_log ORDER BY created_at DESC LIMIT ${limit} FORMAT JSON`
  );
  if (!raw) return res.json({ rows: [] });
  try { res.json({ rows: JSON.parse(raw).data || [] }); }
  catch(e) { res.json({ rows: [], error: e.message }); }
});

// ── GET /api/realtime/logs ────────────────────────────────────────────────────
router.get('/logs', async (req, res) => {
  const lines     = Math.min(parseInt(req.query.lines)||100, 500);
  const hasDocker = await dockerAvailable();
  if (!hasDocker) {
    return res.json({ lines: [
      '❌ Docker socket недоступен.',
      'Добавьте в docker-compose.yml (сервис app):',
      '  volumes:',
      '    - /var/run/docker.sock:/var/run/docker.sock:ro',
      'Затем: docker compose up -d app',
    ], container: CONTAINER });
  }
  const result   = await docker(['logs','--tail',String(lines),'--timestamps',CONTAINER], 20000);
  const combined = [result.stderr, result.stdout].filter(Boolean).join('\n');
  const logLines = combined.split('\n').filter(Boolean).reverse();
  res.json({ lines: logLines.length ? logLines : ['Контейнер не запущен или логов нет'], container: CONTAINER });
});

// ── POST /api/realtime/restart|stop|start ─────────────────────────────────────
async function dockerAction(action, res) {
  const hasDocker = await dockerAvailable();
  if (!hasDocker) {
    return res.json({ ok: false,
      message: 'Docker socket недоступен. Добавьте /var/run/docker.sock в volumes сервиса app и пересоберите.' });
  }
  // Проверяем существование контейнера
  const inspect = await docker(['inspect', CONTAINER], 5000);
  if (!inspect.ok) {
    return res.json({ ok: false,
      message: `Контейнер ${CONTAINER} не найден.\nЗапустите: docker compose up -d realtime` });
  }
  const result = await docker([action, CONTAINER], 30000);
  res.json({
    ok:      result.ok,
    message: result.ok
      ? `✅ ${CONTAINER}: ${action==='restart'?'перезапущен':action==='stop'?'остановлен':'запущен'}`
      : `❌ ${result.stderr || 'Ошибка: docker ' + action}`,
  });
}
router.post('/restart', (req, res) => dockerAction('restart', res));
router.post('/stop',    (req, res) => dockerAction('stop',    res));
router.post('/start',   (req, res) => dockerAction('start',   res));

module.exports = router;