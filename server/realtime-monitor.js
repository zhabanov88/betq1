'use strict';
/**
 * BetQuant — server/realtime-monitor.js
 * ======================================
 * API для панели мониторинга realtime collector.
 *
 * Подключить в server/index.js:
 *   const realtimeMonitor = require('./realtime-monitor');
 *   app.use('/api/realtime', requireAuth, realtimeMonitor);
 *
 * Маршруты:
 *   GET  /api/realtime/status     — состояние всех таблиц + контейнера
 *   GET  /api/realtime/logs       — последние N строк лога контейнера
 *   POST /api/realtime/restart    — перезапустить контейнер
 *   POST /api/realtime/stop       — остановить контейнер
 *   POST /api/realtime/start      — запустить контейнер
 *   GET  /api/realtime/odds-usage — расход запросов The Odds API
 */

const express = require('express');
const { execFile } = require('child_process');
const router = express.Router();

const CONTAINER = process.env.REALTIME_CONTAINER || 'betquant-realtime';
const CH_HOST   = process.env.CH_HOST            || 'http://localhost:8123';
const CH_DB     = process.env.CH_DATABASE        || 'betquant';

// ── ClickHouse query ─────────────────────────────────────────────────────────
async function chQuery(sql) {
  const url = `${CH_HOST}/?database=${CH_DB}&query=${encodeURIComponent(sql)}`;
  try {
    const resp = await fetch(url, { signal: AbortSignal.timeout(8000) });
    if (!resp.ok) throw new Error(`CH ${resp.status}`);
    return (await resp.text()).trim();
  } catch (e) {
    return null;
  }
}

// ── Docker exec helper ────────────────────────────────────────────────────────
function docker(args, timeoutMs = 10000) {
  return new Promise((resolve) => {
    execFile('docker', args, { timeout: timeoutMs }, (err, stdout, stderr) => {
      resolve({
        ok:     !err,
        stdout: (stdout || '').trim(),
        stderr: (stderr || '').trim(),
        code:   err?.code || 0,
      });
    });
  });
}

// ── GET /api/realtime/status ─────────────────────────────────────────────────
router.get('/status', async (req, res) => {
  const now = Date.now();

  // 1. Состояние Docker-контейнера
  const dockerStatus = await docker([
    'inspect', '--format',
    '{{.State.Status}}|{{.State.StartedAt}}|{{.RestartCount}}',
    CONTAINER,
  ]);

  let container = { status: 'unknown', startedAt: null, restarts: 0 };
  if (dockerStatus.ok && dockerStatus.stdout) {
    const [status, startedAt, restarts] = dockerStatus.stdout.split('|');
    container = { status, startedAt, restarts: parseInt(restarts) || 0 };
  }

  // 2. Состояние таблиц ClickHouse
  const tablesSql = `
    SELECT
      'live_stats'         AS t, count() AS n, toString(max(recorded_at)) AS last
      FROM ${CH_DB}.live_stats
    UNION ALL SELECT 'odds',             count(), toString(max(recorded_at)) FROM ${CH_DB}.odds
    UNION ALL SELECT 'odds_timeseries',  count(), toString(max(timestamp))   FROM ${CH_DB}.odds_timeseries
    UNION ALL SELECT 'predictions',      count(), toString(max(created_at))  FROM ${CH_DB}.predictions
    UNION ALL SELECT 'strategy_signals', count(), toString(max(created_at))  FROM ${CH_DB}.strategy_signals
    UNION ALL SELECT 'player_stats',     count(), toString(max(date))        FROM ${CH_DB}.player_stats
    FORMAT TabSeparated
  `;

  const tablesRaw = await chQuery(tablesSql);
  const tables = [];
  if (tablesRaw) {
    for (const line of tablesRaw.split('\n').filter(Boolean)) {
      const [name, count, last] = line.split('\t');
      const lastMs   = last && last !== '1970-01-01 00:00:00' ? new Date(last.replace(' ', 'T') + 'Z').getTime() : 0;
      const ageMin   = lastMs ? Math.round((now - lastMs) / 60000) : null;

      // Определяем "здоровье" по ожидаемой частоте обновления
      const maxAgeMin = {
        live_stats:        10,
        odds:              1500,   // раз в сутки
        odds_timeseries:   1500,
        predictions:       130,    // каждые 2 часа
        strategy_signals:  130,
        player_stats:      1500,
      }[name] || 1440;

      tables.push({
        name,
        count:   parseInt(count) || 0,
        last:    last || null,
        ageMin,
        healthy: ageMin === null ? false : ageMin <= maxAgeMin,
      });
    }
  }

  // 3. Расход The Odds API в этом месяце
  const oddsUsageSql = `
    SELECT count() FROM ${CH_DB}.odds
    WHERE source = 'the-odds-api'
      AND recorded_at >= toStartOfMonth(today())
    FORMAT TabSeparated
  `;
  const oddsUsageRaw = await chQuery(oddsUsageSql);
  const oddsUsageRows = parseInt(oddsUsageRaw) || 0;
  // ~30 строк на один API-запрос (8 видов спорта × 3-4 букмекера)
  const oddsApiRequests = Math.round(oddsUsageRows / 30);

  res.json({
    container,
    tables,
    oddsApi: {
      requestsThisMonth: oddsApiRequests,
      rowsThisMonth:     oddsUsageRows,
      limit:             400,
      pct:               Math.round(oddsApiRequests / 400 * 100),
    },
    timestamp: new Date().toISOString(),
  });
});

// ── GET /api/realtime/logs ────────────────────────────────────────────────────
router.get('/logs', async (req, res) => {
  const lines = Math.min(parseInt(req.query.lines) || 100, 500);
  const result = await docker(['logs', '--tail', String(lines), '--timestamps', CONTAINER], 15000);

  // docker logs пишет и в stdout и в stderr
  const raw    = (result.stdout + '\n' + result.stderr).trim();
  const logLines = raw.split('\n').filter(Boolean).reverse(); // новые сверху

  res.json({ lines: logLines, container: CONTAINER });
});

// ── POST /api/realtime/restart ────────────────────────────────────────────────
router.post('/restart', async (req, res) => {
  const result = await docker(['restart', CONTAINER], 20000);
  res.json({
    ok:      result.ok,
    message: result.ok ? `Контейнер ${CONTAINER} перезапущен` : result.stderr,
  });
});

// ── POST /api/realtime/stop ───────────────────────────────────────────────────
router.post('/stop', async (req, res) => {
  const result = await docker(['stop', CONTAINER], 20000);
  res.json({
    ok:      result.ok,
    message: result.ok ? `Контейнер ${CONTAINER} остановлен` : result.stderr,
  });
});

// ── POST /api/realtime/start ──────────────────────────────────────────────────
router.post('/start', async (req, res) => {
  const result = await docker(['start', CONTAINER], 20000);
  res.json({
    ok:      result.ok,
    message: result.ok ? `Контейнер ${CONTAINER} запущен` : result.stderr,
  });
});

// ── GET /api/realtime/odds-usage ──────────────────────────────────────────────
router.get('/odds-usage', async (req, res) => {
  const sql = `
    SELECT
      toDate(recorded_at)  AS day,
      count()              AS rows,
      round(count() / 30)  AS est_requests
    FROM ${CH_DB}.odds
    WHERE source = 'the-odds-api'
      AND recorded_at >= today() - 30
    GROUP BY day
    ORDER BY day DESC
    FORMAT TabSeparated
  `;
  const raw = await chQuery(sql);
  const days = [];
  if (raw) {
    for (const line of raw.split('\n').filter(Boolean)) {
      const [day, rows, estReq] = line.split('\t');
      days.push({ day, rows: parseInt(rows) || 0, estRequests: parseInt(estReq) || 0 });
    }
  }
  res.json({ days });
});

module.exports = router;