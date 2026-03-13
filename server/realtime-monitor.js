'use strict';
/**
 * BetQuant — server/realtime-monitor.js  (v2 — исправленный)
 * ===========================================================
 * Подключить в server/index.js ОДНОЙ СТРОКОЙ в конце блока роутов:
 *
 *   try {
 *     app.use('/api/realtime', requireAuth, require('./realtime-monitor'));
 *   } catch(e) { console.warn('realtime-monitor:', e.message); }
 */

const express  = require('express');
const { execFile } = require('child_process');
const http     = require('http');
const router   = express.Router();

const CONTAINER = process.env.REALTIME_CONTAINER || 'betquant-realtime';
const CH_HOST   = process.env.CH_HOST            || 'http://localhost:8123';
const CH_DB     = process.env.CH_DATABASE        || 'betquant';

// ── ClickHouse HTTP query (без fetch, через http модуль) ─────────────────────
function chQuery(sql) {
  return new Promise((resolve) => {
    const url   = new URL(CH_HOST);
    const qs    = `database=${encodeURIComponent(CH_DB)}&query=${encodeURIComponent(sql)}`;
    const opts  = {
      hostname: url.hostname,
      port:     url.port || 8123,
      path:     `/?' + ${qs}`,
      method:   'GET',
    };
    // Используем простой GET с query-string
    const fullPath = `/?database=${encodeURIComponent(CH_DB)}&query=${encodeURIComponent(sql)}`;
    const req = http.request(
      { hostname: url.hostname, port: parseInt(url.port) || 8123, path: fullPath, method: 'GET', timeout: 8000 },
      (res) => {
        let data = '';
        res.on('data', d => data += d);
        res.on('end', () => resolve(data.trim()));
      }
    );
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.end();
  });
}

// ── Docker exec helper ────────────────────────────────────────────────────────
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

// ── Проверка доступности Docker ───────────────────────────────────────────────
async function dockerAvailable() {
  const r = await docker(['info', '--format', '{{.ServerVersion}}'], 5000);
  return r.ok && r.stdout.length > 0;
}

// ── GET /api/realtime/status ─────────────────────────────────────────────────
router.get('/status', async (req, res) => {
  const now = Date.now();

  // 1. Docker контейнер
  let container = { status: 'unknown', startedAt: null, restarts: 0, dockerAvailable: false };

  const hasDocker = await dockerAvailable();
  container.dockerAvailable = hasDocker;

  if (hasDocker) {
    const r = await docker([
      'inspect', '--format',
      '{{.State.Status}}|{{.State.StartedAt}}|{{.RestartCount}}',
      CONTAINER,
    ]);
    if (r.ok && r.stdout && !r.stdout.includes('Error')) {
      const [status, startedAt, restarts] = r.stdout.split('|');
      container = { status, startedAt, restarts: parseInt(restarts) || 0, dockerAvailable: true };
    } else {
      // Контейнер не найден — значит не создан
      container.status    = 'not_found';
      container.error     = r.stderr || 'Container not found';
      container.dockerAvailable = true;
    }
  }

  // 2. Таблицы ClickHouse
  const tablesSql = [
    `SELECT 'live_stats' AS t, count() AS n, toString(max(recorded_at)) AS last FROM ${CH_DB}.live_stats`,
    `SELECT 'odds', count(), toString(max(recorded_at)) FROM ${CH_DB}.odds`,
    `SELECT 'odds_timeseries', count(), toString(max(timestamp)) FROM ${CH_DB}.odds_timeseries`,
    `SELECT 'predictions', count(), toString(max(created_at)) FROM ${CH_DB}.predictions`,
    `SELECT 'strategy_signals', count(), toString(max(created_at)) FROM ${CH_DB}.strategy_signals`,
    `SELECT 'player_stats', count(), toString(max(date)) FROM ${CH_DB}.player_stats`,
  ].join(' UNION ALL ') + ' FORMAT TabSeparated';

  const tablesRaw = await chQuery(tablesSql);
  const tables = [];

  if (tablesRaw) {
    const maxAgeMin = {
      live_stats: 10, odds: 1500, odds_timeseries: 1500,
      predictions: 130, strategy_signals: 130, player_stats: 1500,
    };
    for (const line of tablesRaw.split('\n').filter(Boolean)) {
      const [name, count, last] = line.split('\t');
      if (!name) continue;
      const lastMs  = last && last !== '1970-01-01 00:00:00' && last !== '\\N'
        ? new Date(last.replace(' ', 'T') + 'Z').getTime() : 0;
      const ageMin  = lastMs ? Math.round((now - lastMs) / 60000) : null;
      const maxAge  = maxAgeMin[name] || 1440;
      tables.push({
        name,
        count:   parseInt(count) || 0,
        last:    last && last !== '\\N' ? last : null,
        ageMin,
        healthy: ageMin !== null && ageMin <= maxAge,
      });
    }
  }

  // 3. Расход The Odds API
  const oddsRaw = await chQuery(
    `SELECT count() FROM ${CH_DB}.odds WHERE source='the-odds-api' AND recorded_at >= toStartOfMonth(today()) FORMAT TabSeparated`
  );
  const oddsRows     = parseInt(oddsRaw) || 0;
  const oddsRequests = Math.round(oddsRows / 30);

  res.json({
    container,
    tables,
    oddsApi: {
      requestsThisMonth: oddsRequests,
      rowsThisMonth:     oddsRows,
      limit:             400,
      pct:               Math.min(100, Math.round(oddsRequests / 400 * 100)),
    },
    chConnected: tablesRaw !== null,
    timestamp:   new Date().toISOString(),
  });
});

// ── GET /api/realtime/logs ────────────────────────────────────────────────────
router.get('/logs', async (req, res) => {
  const lines = Math.min(parseInt(req.query.lines) || 100, 500);

  const hasDocker = await dockerAvailable();
  if (!hasDocker) {
    return res.json({ lines: ['Docker недоступен из контейнера. Добавьте /var/run/docker.sock в volumes.'], container: CONTAINER });
  }

  const result = await docker(['logs', '--tail', String(lines), '--timestamps', CONTAINER], 20000);
  const raw    = (result.stderr + '\n' + result.stdout).trim(); // docker logs → stderr
  const logLines = raw.split('\n').filter(Boolean).reverse();

  res.json({ lines: logLines.length ? logLines : ['Логов нет или контейнер не запущен'], container: CONTAINER });
});

// ── POST /api/realtime/restart ────────────────────────────────────────────────
router.post('/restart', async (req, res) => {
  const hasDocker = await dockerAvailable();
  if (!hasDocker) return res.json({ ok: false, message: 'Docker сокет недоступен. Добавьте /var/run/docker.sock в volumes сервиса app.' });

  const result = await docker(['restart', CONTAINER], 30000);
  res.json({ ok: result.ok, message: result.ok ? `${CONTAINER} перезапущен` : (result.stderr || 'Ошибка') });
});

// ── POST /api/realtime/stop ───────────────────────────────────────────────────
router.post('/stop', async (req, res) => {
  const hasDocker = await dockerAvailable();
  if (!hasDocker) return res.json({ ok: false, message: 'Docker сокет недоступен. Добавьте /var/run/docker.sock в volumes сервиса app.' });

  const result = await docker(['stop', CONTAINER], 20000);
  res.json({ ok: result.ok, message: result.ok ? `${CONTAINER} остановлен` : (result.stderr || 'Ошибка') });
});

// ── POST /api/realtime/start ──────────────────────────────────────────────────
router.post('/start', async (req, res) => {
  const hasDocker = await dockerAvailable();
  if (!hasDocker) return res.json({ ok: false, message: 'Docker сокет недоступен. Добавьте /var/run/docker.sock в volumes сервиса app.' });

  const result = await docker(['start', CONTAINER], 20000);
  res.json({ ok: result.ok, message: result.ok ? `${CONTAINER} запущен` : (result.stderr || 'Ошибка') });
});

// ── GET /api/realtime/ping ────────────────────────────────────────────────────
// Быстрая проверка что роут работает
router.get('/ping', (req, res) => {
  res.json({ ok: true, ch: CH_HOST, container: CONTAINER, ts: new Date().toISOString() });
});

module.exports = router;