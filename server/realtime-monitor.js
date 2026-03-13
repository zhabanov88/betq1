'use strict';
/**
 * BetQuant — server/realtime-monitor.js  (v3 — финальный)
 *
 * Ошибки предыдущих версий:
 * 1. v1 использовал global fetch — недоступен в Node < 18
 * 2. v2 имел мёртвую переменную opts и опечатку в path: '/?' + ${qs}
 * 3. В index.js роут подключался ДО объявления requireAuth → ReferenceError
 *    (исправляется в index.js, не здесь)
 */

const express    = require('express');
const { execFile } = require('child_process');
const http       = require('http');
const router     = express.Router();

const CONTAINER = process.env.REALTIME_CONTAINER || 'betquant-realtime';
const CH_HOST   = process.env.CH_HOST            || 'http://clickhouse:8123';
const CH_DB     = process.env.CH_DATABASE        || 'betquant';

// ── ClickHouse HTTP query — только встроенный http модуль ────────────────────
function chQuery(sql) {
  return new Promise((resolve) => {
    let parsed;
    try { parsed = new URL(CH_HOST); } catch { return resolve(null); }

    const path = `/?database=${encodeURIComponent(CH_DB)}&query=${encodeURIComponent(sql)}`;

    const req = http.request(
      {
        hostname: parsed.hostname,
        port:     parseInt(parsed.port) || 8123,
        path,
        method:   'GET',
        timeout:  8000,
      },
      (res) => {
        let buf = '';
        res.on('data', d => buf += d);
        res.on('end', () => resolve(buf.trim()));
      }
    );
    req.on('error',   () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.end();
  });
}

// ── Docker helper — через execFile (docker CLI) ───────────────────────────────
function docker(args, timeoutMs = 15000) {
  return new Promise((resolve) => {
    execFile('docker', args, { timeout: timeoutMs }, (err, stdout, stderr) => {
      resolve({
        ok:     !err,
        stdout: (stdout || '').trim(),
        stderr: (stderr || '').trim(),
      });
    });
  });
}

// ── Проверка доступности Docker socket ───────────────────────────────────────
async function dockerAvailable() {
  const r = await docker(['info', '--format', '{{.ServerVersion}}'], 5000);
  return r.ok && r.stdout.length > 0;
}

// ════════════════════════════════════════════════════════════════════════════
// GET /api/realtime/ping — быстрая самопроверка (без Docker, без CH)
// ════════════════════════════════════════════════════════════════════════════
router.get('/ping', (req, res) => {
  res.json({
    ok:        true,
    container: CONTAINER,
    ch:        CH_HOST,
    db:        CH_DB,
    ts:        new Date().toISOString(),
  });
});

// ════════════════════════════════════════════════════════════════════════════
// GET /api/realtime/status
// ════════════════════════════════════════════════════════════════════════════
router.get('/status', async (req, res) => {
  const now = Date.now();

  // 1. Docker контейнер
  let container = { status: 'unknown', startedAt: null, restarts: 0, dockerAvailable: false, error: null };
  const hasDocker = await dockerAvailable();
  container.dockerAvailable = hasDocker;

  if (hasDocker) {
    const r = await docker([
      'inspect', '--format',
      '{{.State.Status}}|{{.State.StartedAt}}|{{.RestartCount}}',
      CONTAINER,
    ]);
    if (r.ok && r.stdout && !r.stdout.startsWith('Error')) {
      const [status, startedAt, restarts] = r.stdout.split('|');
      container = { status, startedAt, restarts: parseInt(restarts) || 0, dockerAvailable: true, error: null };
    } else {
      container.status = 'not_found';
      container.error  = r.stderr || 'Container not found. Run: docker compose up -d realtime';
    }
  } else {
    container.error = 'Docker socket недоступен. Добавьте в docker-compose.yml сервис app:\n  volumes:\n    - /var/run/docker.sock:/var/run/docker.sock:ro';
  }

  // 2. Таблицы ClickHouse — каждая таблица отдельным запросом (UNION ALL ломается если таблица не существует)
  const TABLE_DEFS = [
    { name: 'live_stats',        col: 'recorded_at', maxAgeMin: 10   },
    { name: 'odds',              col: 'recorded_at', maxAgeMin: 1500 },
    { name: 'odds_timeseries',   col: 'timestamp',   maxAgeMin: 1500 },
    { name: 'predictions',       col: 'created_at',  maxAgeMin: 130  },
    { name: 'strategy_signals',  col: 'created_at',  maxAgeMin: 130  },
    { name: 'player_stats',      col: 'date',        maxAgeMin: 1500 },
  ];

  const FREQ_LABELS = {
    live_stats: 'каждую минуту', odds: 'раз в сутки',
    odds_timeseries: 'раз в сутки', predictions: 'каждые 2ч',
    strategy_signals: 'каждые 2ч', player_stats: 'раз в сутки',
  };

  const tables = await Promise.all(TABLE_DEFS.map(async ({ name, col, maxAgeMin }) => {
    const sql = `SELECT count() AS n, toString(max(${col})) AS last FROM ${CH_DB}.${name} FORMAT TabSeparated`;
    const raw = await chQuery(sql);

    if (!raw) {
      return { name, count: 0, last: null, ageMin: null, healthy: false, error: 'CH недоступен', freq: FREQ_LABELS[name] };
    }

    const parts = raw.split('\t');
    const count = parseInt(parts[0]) || 0;
    const last  = parts[1] && parts[1] !== '\\N' && parts[1] !== '1970-01-01 00:00:00' ? parts[1] : null;
    const lastMs = last ? new Date(last.replace(' ', 'T') + 'Z').getTime() : 0;
    const ageMin = lastMs ? Math.round((now - lastMs) / 60000) : null;

    return {
      name,
      count,
      last,
      ageMin,
      healthy: ageMin !== null && ageMin <= maxAgeMin,
      freq:    FREQ_LABELS[name],
    };
  }));

  const chConnected = tables.some(t => t.error !== 'CH недоступен');

  // 3. Расход The Odds API
  const oddsRaw = await chQuery(
    `SELECT count() FROM ${CH_DB}.odds WHERE source='the-odds-api' AND recorded_at >= toStartOfMonth(today()) FORMAT TabSeparated`
  );
  const oddsRows     = parseInt(oddsRaw) || 0;
  const oddsRequests = Math.round(oddsRows / 30);

  res.json({
    container,
    tables,
    chConnected,
    oddsApi: {
      requestsThisMonth: oddsRequests,
      rowsThisMonth:     oddsRows,
      limit:             400,
      pct:               Math.min(100, Math.round(oddsRequests / 4)),
    },
    timestamp: new Date().toISOString(),
  });
});

// ════════════════════════════════════════════════════════════════════════════
// GET /api/realtime/logs
// ════════════════════════════════════════════════════════════════════════════
router.get('/logs', async (req, res) => {
  const lines = Math.min(parseInt(req.query.lines) || 100, 500);
  const hasDocker = await dockerAvailable();

  if (!hasDocker) {
    return res.json({
      lines: ['❌ Docker socket недоступен внутри контейнера app.', 'Добавьте в docker-compose.yml (сервис app):', '  volumes:', '    - /var/run/docker.sock:/var/run/docker.sock:ro', 'Затем: docker compose up -d app'],
      container: CONTAINER,
    });
  }

  // docker logs пишет в stderr (это нормально для docker)
  const result = await docker(['logs', '--tail', String(lines), '--timestamps', CONTAINER], 20000);
  const combined = [result.stderr, result.stdout].filter(Boolean).join('\n');
  const logLines = combined.split('\n').filter(Boolean).reverse();

  res.json({
    lines: logLines.length ? logLines : ['Контейнер не запущен или логов нет'],
    container: CONTAINER,
  });
});

// ════════════════════════════════════════════════════════════════════════════
// POST /api/realtime/restart|stop|start
// ════════════════════════════════════════════════════════════════════════════
async function dockerAction(action, res) {
  const hasDocker = await dockerAvailable();
  if (!hasDocker) {
    return res.json({
      ok: false,
      message: 'Docker socket недоступен. Добавьте /var/run/docker.sock в volumes сервиса app и пересоберите: docker compose up -d app',
    });
  }
  const result = await docker([action, CONTAINER], 30000);
  res.json({
    ok:      result.ok,
    message: result.ok
      ? `✅ Контейнер ${CONTAINER}: ${action === 'restart' ? 'перезапущен' : action === 'stop' ? 'остановлен' : 'запущен'}`
      : (result.stderr || `Ошибка команды docker ${action}`),
  });
}

router.post('/restart', (req, res) => dockerAction('restart', res));
router.post('/stop',    (req, res) => dockerAction('stop',    res));
router.post('/start',   (req, res) => dockerAction('start',   res));

module.exports = router;