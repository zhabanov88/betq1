'use strict';
// Локальная разработка: читаем .env из корня проекта
// В Docker переменные приходят из docker-compose environment: — dotenv просто ничего не сделает
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const express = require('express');
const path    = require('path');
const session = require('express-session');
const vm      = require('vm');

const app = express();

// ── Middleware ──────────────────────────────────────────────────────────────
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ── Static (public/) ────────────────────────────────────────────────────────
const PUBLIC = path.join(__dirname, '../public');
app.use(express.static(PUBLIC));

// ── Session ─────────────────────────────────────────────────────────────────
app.set('trust proxy', 1);
app.use(session({
  secret: process.env.SESSION_SECRET || 'betquant-dev-secret',
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: false,
    httpOnly: true,
    sameSite: 'lax',
    maxAge: 7 * 24 * 60 * 60 * 1000
  }
}));

// ── DB — подключение с retry для Docker ──────────────────────────────────────
let pgPool = null, clickhouse = null;

try {
  const { Pool } = require('pg');
  pgPool = new Pool({
    host:                    process.env.PG_HOST     || 'localhost',
    port:                    parseInt(process.env.PG_PORT) || 5432,
    database:                process.env.PG_DATABASE || 'betquant',
    user:                    process.env.PG_USER     || 'betquant',
    password:                process.env.PG_PASSWORD || 'betquant123',
    max:                     5,
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis:       30000,
  });
  const testPg = () => pgPool.query('SELECT 1')
    .then(async () => {
      console.log('✅ PostgreSQL connected');
      try {
        const bcrypt = require('bcrypt');
        const hash   = await bcrypt.hash('admin123', 10);
        await pgPool.query(`
          INSERT INTO users (username, password_hash, email, role)
          VALUES ('admin', $1, 'admin@betquant.pro', 'admin')
          ON CONFLICT (username) DO UPDATE SET password_hash = $1
        `, [hash]);
        console.log('✅ Admin user ready (admin / admin123)');
      } catch (e) {
        console.warn('⚠️  Admin seed skipped:', e.message);
      }
    })
    .catch(e => {
      console.warn('⚠️  PostgreSQL retry in 3s:', e.message);
      setTimeout(testPg, 3000);
    });
  testPg();
} catch (e) { console.warn('⚠️  pg module issue:', e.message); }

try {
  const { createClient } = require('@clickhouse/client');
  clickhouse = createClient({
    host:            process.env.CH_HOST     || 'http://localhost:8123',
    username:        process.env.CH_USER     || 'default',
    password:        process.env.CH_PASSWORD || '',
    database:        process.env.CH_DATABASE || 'betquant',
    request_timeout: 10000,
  });
  const testCh = () => clickhouse.ping()
    .then(r => { if (r.success) console.log('✅ ClickHouse connected'); else throw new Error('ping failed'); })
    .catch(e => {
      console.warn('⚠️  ClickHouse retry in 3s:', e.message);
      setTimeout(testCh, 3000);
    });
  testCh();
} catch (e) { console.warn('⚠️  @clickhouse/client issue:', e.message); }
// ── Neural Networks ────────────────────────────────────────────────────────
app.locals.clickhouse = clickhouse;
const neuralRoutes = require('./neural');
app.use('/api/neural', neuralRoutes);

// ── Приоритет 1: Live Monitor, Value Finder, CLV ──────────────────────────
app.locals.pgPool = pgPool;   // если ещё не прописано

const liveRoutes  = require('./live');
const valueRoutes = require('./value');
const clvRoutes   = require('./clv');

app.use('/api/live',  liveRoutes);
app.use('/api/value', valueRoutes);
app.use('/api/clv',   clvRoutes);

// ── Stats routes (для "Графики и коэффициенты" + "Статистика") ───────────
const statsRoutes = (() => {
  try { return require('./stats_routes'); }
  catch(e) { console.warn('⚠️  stats_routes missing:', e.message); return null; }
})();
if (statsRoutes) {
  // Прокидываем clickhouse в req.app.locals
  app.use('/api/stats', requireAuth, statsRoutes);
}


// ── Priority 2: Telegram, Odds Compare ───────────────────────────────────
// ВАЖНО: telegram лежит в server/telegram.js (НЕ routes/telegram.js!)
// ВАЖНО: odds-compare лежит в server/routes/odds-compare.js
const telegramModule = (() => {
  try {
    return require('./telegram');                   // server/telegram.js  ✅
  } catch(e) {
    console.warn('⚠️  telegram module error:', e.message);
    return null;
  }
})();

if (telegramModule) {
  app.use('/api/telegram', telegramModule.router);
  global.__betquant_tg = telegramModule.tgAPI;
}

try {
  const oddsCompareRoutes = require('./odds-compare');  // server/routes/odds-compare.js  ✅
  app.use('/api/odds-compare', oddsCompareRoutes);
} catch(e) { console.warn('⚠️  odds-compare route error:', e.message); }


// ── Auth middleware ─────────────────────────────────────────────────────────
function requireAuth(req, res, next) {
  if (req.session?.userId || req.session?.demo) return next();
  const token = req.headers['x-auth-token'] || req.headers['authorization']?.replace('Bearer ', '');
  if (token && token !== 'null' && token !== 'undefined' && token.length > 3) {
    req.session.demo = true;
    return next();
  }
  res.status(401).json({ error: 'Unauthorized' });
}

// ══════════════════════════════════════════════════════════
//  AUTH ROUTES
// ══════════════════════════════════════════════════════════
app.post('/api/auth/register', async (req, res) => {
  const { username, password, email } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Missing fields' });
  try {
    if (pgPool) {
      const bcrypt = require('bcrypt');
      const hash = await bcrypt.hash(password, 10);
      const r = await pgPool.query(
        'INSERT INTO users (username, password_hash, email) VALUES ($1,$2,$3) RETURNING id',
        [username, hash, email]
      );
      req.session.userId = r.rows[0].id;
    } else {
      req.session.userId = 1;
    }
    req.session.username = username;
    req.session.demo = !pgPool;
    res.json({ success: true, username });
  } catch (e) {
    if (e.code === '23505') return res.status(409).json({ error: 'Username already taken' });
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Missing fields' });
  try {
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM users WHERE username=$1', [username]);
      if (!r.rows[0]) return res.status(401).json({ error: 'Invalid credentials' });
      const bcrypt = require('bcrypt');
      const ok = await bcrypt.compare(password, r.rows[0].password_hash);
      if (!ok) return res.status(401).json({ error: 'Invalid credentials' });
      req.session.userId   = r.rows[0].id;
      req.session.username = r.rows[0].username;
      req.session.role     = r.rows[0].role;
      req.session.demo     = false;
    } else {
      req.session.userId = 1; req.session.username = username; req.session.demo = true;
    }
    res.json({ success: true, username: req.session.username, demo: req.session.demo });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/auth/demo', (req, res) => {
  req.session.userId = 0; req.session.username = 'Demo'; req.session.demo = true;
  res.json({ success: true, username: 'Demo', demo: true });
});

app.post('/api/auth/logout', (req, res) => {
  req.session.destroy(() => res.json({ success: true }));
});

app.get('/api/auth/me', requireAuth, (req, res) => {
  res.json({ userId: req.session.userId, username: req.session.username, demo: req.session.demo });
});

// ══════════════════════════════════════════════════════════
//  DB / STATS ROUTES
// ══════════════════════════════════════════════════════════
app.post('/api/db/query', requireAuth, async (req, res) => {
  const { sql } = req.body;
  if (!sql) return res.status(400).json({ error: 'No SQL' });
  const forbidden = /\b(drop|truncate|delete|insert|update|alter|create|grant|revoke)\b/i;
  if (forbidden.test(sql)) return res.status(403).json({ error: 'Only SELECT queries allowed' });
  try {
    if (!clickhouse) return res.json({ columns: [], rows: [], note: 'ClickHouse not connected' });
    const r = await clickhouse.query({ query: sql, format: 'JSON' });
    const d = await r.json();
    const rows = d.data || [];
    const columns = rows.length ? Object.keys(rows[0]) : [];
    res.json({ columns, rows, rowCount: rows.length });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.get('/api/stats/summary', requireAuth, async (req, res) => {
  try {
    if (!clickhouse) return res.json({ total: 0, leagues: 0, demo: true });
    const r = await clickhouse.query({
      query: 'SELECT count() as total, uniq(league) as leagues FROM betquant.matches',
      format: 'JSON'
    });
    const d = await r.json();
    res.json(d.data?.[0] || { total: 0, leagues: 0 });
  } catch (e) { res.json({ total: 0, leagues: 0, demo: true }); }
});

// ETL Status — количество строк в каждой таблице
app.get('/api/stats/etl-status', requireAuth, async (req, res) => {
  if (!clickhouse) return res.json({});
  const tables = [
    'football_matches', 'football_events', 'football_team_form',
    'hockey_matches', 'hockey_events',
    'tennis_extended', 'basketball_matches', 'baseball_matches',
    // sports-etl-v2 tables
    'basketball_matches_v2', 'cricket_matches', 'rugby_matches',
    'nfl_games', 'waterpolo_matches', 'volleyball_matches',
  ];
  const result = {};
  await Promise.all(tables.map(async t => {
    try {
      const r = await clickhouse.query({ query: `SELECT count() as n FROM betquant.${t}`, format: 'JSON' });
      const d = await r.json();
      result[t] = parseInt(d.data?.[0]?.n || 0);
    } catch { result[t] = 0; }
  }));
  // Диапазон дат футбол
  try {
    const r = await clickhouse.query({
      query: `SELECT min(date) as from_d, max(date) as to_d FROM betquant.football_matches`,
      format: 'JSON'
    });
    const d = await r.json();
    result._football_range = d.data?.[0] || {};
  } catch { result._football_range = {}; }
  res.json(result);
});

app.get('/api/stats/goals-by-minute', requireAuth, async (req, res) => {
  const { league, season } = req.query;
  try {
    if (!clickhouse) return res.json([]);
    const lf = league ? `AND league_code = '${league}'` : '';
    const q = `
      SELECT minute,
        countIf(event_type = 'goal') as goals,
        count() as total_shots
      FROM betquant.football_events
      WHERE minute <= 95 ${lf}
      GROUP BY minute ORDER BY minute
    `;
    const r = await clickhouse.query({ query: q, format: 'JSON' });
    const d = await r.json();
    res.json(d.data || []);
  } catch (e) { res.json([]); }
});

// ══════════════════════════════════════════════════════════
//  BACKTEST
// ══════════════════════════════════════════════════════════
app.post('/api/backtest', requireAuth, async (req, res) => {
  const { strategyCode, config = {} } = req.body;
  if (!strategyCode) return res.status(400).json({ error: 'No strategy code' });
  try {
    let matches = [];
    if (clickhouse) {
      const { league = '', season = '', dateFrom = '', dateTo = '' } = config;
      let where = 'WHERE 1=1';
      if (league)   where += ` AND league_code = '${league.replace(/'/g,"''")}'`;
      if (season)   where += ` AND season = '${season.replace(/'/g,"''")}'`;
      if (dateFrom) where += ` AND date >= '${dateFrom}'`;
      if (dateTo)   where += ` AND date <= '${dateTo}'`;
      const r = await clickhouse.query({
        query: `SELECT * FROM betquant.football_matches ${where} ORDER BY date LIMIT 50000`,
        format: 'JSON'
      });
      const d = await r.json();
      matches = d.data || [];
    }
    if (!matches.length) {
      if (config.demoMode === true) {
        matches = generateDemoMatches(config);
      } else {
        return res.status(422).json({
          error: 'Нет данных для бэктеста. Подключите ClickHouse и загрузите исторические данные, либо включите тестовый режим (кнопка «Тестовые данные» в параметрах бэктеста).',
          hint: 'enable_demo',
        });
      }
    }
    const result = runBacktest(strategyCode, matches, config);
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

function generateDemoMatches(config = {}) {
  const leagues = ['АПЛ', 'Бундеслига', 'Ла Лига', 'Серия А', 'Лига 1'];
  const matches = [];
  const now = new Date();
  for (let i = 0; i < 2000; i++) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    matches.push({
      date:         d.toISOString().slice(0, 10),
      league:       leagues[i % 5],
      home_team:    `Команда_Д${i % 20}`,
      away_team:    `Команда_Г${i % 20}`,
      home_goals:   Math.floor(Math.random() * 4),
      away_goals:   Math.floor(Math.random() * 4),
      b365_home:    +(1.5 + Math.random() * 2).toFixed(2),
      b365_draw:    +(2.8 + Math.random() * 1).toFixed(2),
      b365_away:    +(2.0 + Math.random() * 3).toFixed(2),
      b365_over25:  +(1.7 + Math.random() * 0.6).toFixed(2),
      b365_under25: +(1.9 + Math.random() * 0.8).toFixed(2),
    });
  }
  return matches;
}

function runBacktest(code, matches, config = {}) {
  const { stake = 1, bankroll = 1000 } = config;
  let bets = [], bank = bankroll;
  const sandbox = {
    evaluate: null,
    console: { log: () => {}, warn: () => {}, error: () => {} }
  };
  try {
    vm.createContext(sandbox);
    vm.runInContext(code, sandbox, { timeout: 5000 });
    if (typeof sandbox.evaluate !== 'function') throw new Error('evaluate() function not found');
  } catch (e) { return { error: 'Strategy compile error: ' + e.message }; }

  for (const match of matches) {
    try {
      const signal = sandbox.evaluate(match, {}, {}, {
        implied: o => 1/o,
        value: (o, p) => p * o - 1,
        kelly: (o, p) => (p * o - 1) / (o - 1),
      });
      if (!signal) continue;
      const { market, stake: s = stake, prob = 0.5 } = signal;
      let odds = 0;
      if (market === 'home') odds = match.b365_home || match.odds_home || 0;
      else if (market === 'draw') odds = match.b365_draw || match.odds_draw || 0;
      else if (market === 'away') odds = match.b365_away || match.odds_away || 0;
      else if (market === 'over') odds = match.b365_over25 || match.odds_over || 0;
      else if (market === 'under') odds = match.b365_under25 || match.odds_under || 0;
      if (!odds || odds < 1.01) continue;
      const betSize = Math.min(s * stake, bank * 0.25);
      let win = false;
      if (market === 'home')  win = match.home_goals > match.away_goals;
      if (market === 'draw')  win = match.home_goals === match.away_goals;
      if (market === 'away')  win = match.away_goals > match.home_goals;
      if (market === 'over')  win = (match.home_goals + match.away_goals) > 2.5;
      if (market === 'under') win = (match.home_goals + match.away_goals) < 2.5;
      if (market === 'btts')  win = match.home_goals > 0 && match.away_goals > 0;
      const pnl = win ? betSize * (odds - 1) : -betSize;
      bank += pnl;
      bets.push({ date: match.date, match: `${match.home_team} vs ${match.away_team}`, market, odds: +odds.toFixed(2), stake: +betSize.toFixed(2), win, pnl: +pnl.toFixed(2), bank: +bank.toFixed(2) });
    } catch {}
  }
  if (!bets.length) return { bets: [], stats: { total: 0 }, note: 'No signals generated' };
  const wins = bets.filter(b => b.win).length;
  const totalPnl = bets.reduce((s, b) => s + b.pnl, 0);
  const totalStaked = bets.reduce((s, b) => s + b.stake, 0);
  let maxBank = bankroll, minBank = bankroll, peak = bankroll, maxDD = 0;
  for (const b of bets) {
    if (b.bank > peak) peak = b.bank;
    const dd = (peak - b.bank) / peak;
    if (dd > maxDD) maxDD = dd;
  }
  return {
    bets: bets.slice(-500),
    stats: {
      total: bets.length,
      wins,
      losses: bets.length - wins,
      winRate: +(wins / bets.length * 100).toFixed(1),
      roi: +(totalPnl / totalStaked * 100).toFixed(2),
      pnl: +totalPnl.toFixed(2),
      finalBank: +bank.toFixed(2),
      maxDrawdown: +(maxDD * 100).toFixed(1),
    }
  };
}

// ══════════════════════════════════════════════════════════
//  ETL ROUTES — запуск Python скраперов
// ══════════════════════════════════════════════════════════
const tasks = new Map();

// Симуляция прогресса (fallback если Python недоступен)
async function simulateProgress(taskId) {
  const steps = [
    [10, 'Подготовка окружения...'],
    [20, 'Применяем схему БД...'],
    [40, 'Загружаем данные...'],
    [70, 'Обрабатываем матчи...'],
    [90, 'Финализация...'],
    [100, '✅ Готово (симуляция — Python не найден)'],
  ];
  const t = tasks.get(taskId);
  for (const [pct, message] of steps) {
    await new Promise(r => setTimeout(r, 400 + Math.random() * 600));
    t.pct = pct; t.message = message; t.type = pct === 100 ? 'success' : 'info';
    if (pct === 100) t.status = 'done';
  }
}

// Запуск ETL v1 (betquant-etl — футбол, хоккей, теннис, NBA, MLB)
app.post('/api/etl/run', requireAuth, async (req, res) => {
  const { sport = 'football', seasons = 3, quick = false, version = 'v1' } = req.body;
  const taskId = 'etl_' + Date.now();
  tasks.set(taskId, { status: 'running', pct: 0, message: 'Starting ETL...', type: 'info', log: [] });
  res.json({ taskId, message: 'ETL started' });

  const { spawn } = require('child_process');
  // ETL v1 — betquant-etl/run_etl.py (футбол/хоккей/теннис/NBA/MLB)
  // ETL v2 — sports-etl-v2/run_etl_v2.py (баскетбол/крикет/регби/NFL/водное поло/волейбол)
  let etlScript, extraArgs = [];
  if (version === 'v2') {
    etlScript = require('path').join(__dirname, '../sports-etl-v2/run_etl_v2.py');
    extraArgs = [
      '--ch-url', process.env.CH_HOST || 'http://clickhouse:8123',
      '--db',     process.env.CH_DATABASE || 'betquant',
      '--seasons', String(seasons),
    ];
    if (quick) extraArgs.push('--quick');
    if (sport !== 'all') extraArgs.push('--sport', sport);
  } else {
    etlScript = require('path').join(__dirname, '../betquant-etl/run_etl.py');
    extraArgs = [
      '--ch-host', process.env.CH_HOST || 'http://clickhouse:8123',
      '--ch-db',   process.env.CH_DATABASE || 'betquant',
      '--seasons', String(seasons),
    ];
    if (quick) extraArgs.push('--quick');
    if (sport === 'football') extraArgs.push('--football-only');
    else if (sport === 'hockey') extraArgs.push('--hockey-only');
    else if (sport === 'other') extraArgs.push('--other-only');
  }

  const t = tasks.get(taskId);

  // Проверяем наличие Python и скрипта
  const fs = require('fs');
  if (!fs.existsSync(etlScript)) {
    console.warn(`⚠️  ETL script not found: ${etlScript} — using simulation`);
    simulateProgress(taskId);
    return;
  }

  const proc = spawn('python3', [etlScript, ...extraArgs], { env: { ...process.env } });
  let pct = 5;

  proc.stdout.on('data', (d) => {
    const line = d.toString().trim();
    if (!line) return;
    t.log = t.log || [];
    t.log.push(line);
    if (t.log.length > 200) t.log = t.log.slice(-200);
    if (line.includes('матчей загружено') || line.includes('✓') || line.includes('loaded')) pct = Math.min(pct + 5, 90);
    if (line.includes('ИТОГ') || line.includes('ФИНАЛ') || line.includes('complete')) pct = 95;
    t.pct = pct;
    t.message = line.slice(0, 150);
    t.type = 'info';
  });
  proc.stderr.on('data', (d) => {
    const line = d.toString().trim();
    if (line) { t.log = t.log || []; t.log.push('ERR: ' + line); }
  });
  proc.on('close', (code) => {
    t.status  = code === 0 ? 'done' : 'error';
    t.pct     = 100;
    t.message = code === 0 ? '✅ ETL завершён успешно!' : `❌ ETL завершился с кодом ${code}`;
    t.type    = code === 0 ? 'success' : 'error';

    // ── Автообучение нейросетей после успешного ETL ──
    if (code === 0) {
      const tableMap = {
        football:   ['football_matches'],
        hockey:     ['hockey_matches'],
        tennis:     ['tennis_matches'],
        other:      ['tennis_matches'],
        basketball: ['basketball_matches_v2'],
        all:        ['football_matches','hockey_matches','tennis_matches','basketball_matches_v2'],
      };
      const tables = tableMap[sport] || tableMap['all'];
      const port = process.env.PORT || 3000;
      tables.forEach(tbl => {
        fetch(`http://localhost:${port}/api/neural/auto-retrain`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ table: tbl }),
        }).then(r => r.json())
          .then(d => console.log(`[Neural] Auto-retrain ${tbl}: accuracy ${d.accuracy || '?'}%`))
          .catch(e => console.warn(`[Neural] Auto-retrain failed ${tbl}:`, e.message));
      });
    }
  });
  proc.on('error', (e) => {
    // Python не установлен — запускаем симуляцию
    console.warn('⚠️  python3 not found, using simulation');
    simulateProgress(taskId);
  });
});

app.get('/api/etl/progress/:taskId', requireAuth, (req, res) => {
  const t = tasks.get(req.params.taskId);
  if (!t) return res.status(404).json({ error: 'Not found' });
  res.json(t);
});

app.get('/api/etl/log', requireAuth, async (req, res) => {
  try {
    if (!clickhouse) return res.json([]);
    const r = await clickhouse.query({
      query: `SELECT * FROM betquant.etl_log ORDER BY ts DESC LIMIT 100`,
      format: 'JSON'
    });
    const d = await r.json();
    res.json(d.data || []);
  } catch (e) { res.json([]); }
});

// ══════════════════════════════════════════════════════════
//  STRATEGIES
// ══════════════════════════════════════════════════════════
app.get('/api/strategies', requireAuth, async (req, res) => {
  try {
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM strategies WHERE user_id=$1 OR is_public=true ORDER BY created_at DESC', [req.session.userId]);
      return res.json(r.rows);
    }
    res.json([]);
  } catch (e) { res.json([]); }
});

app.post('/api/strategies', requireAuth, async (req, res) => {
  const { name, code, description, sport, tags } = req.body;
  try {
    if (pgPool) {
      const r = await pgPool.query(
        'INSERT INTO strategies (user_id, name, code, description, sport, tags) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *',
        [req.session.userId, name, code, description, sport, tags]
      );
      return res.json(r.rows[0]);
    }
    res.json({ id: Date.now(), name, code });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/strategies/:id', requireAuth, async (req, res) => {
  const { name, code, description, sport, tags } = req.body;
  try {
    if (pgPool) {
      const r = await pgPool.query(
        'UPDATE strategies SET name=$1,code=$2,description=$3,sport=$4,tags=$5 WHERE id=$6 AND user_id=$7 RETURNING *',
        [name, code, description, sport, tags, req.params.id, req.session.userId]
      );
      return res.json(r.rows[0]);
    }
    res.json({ id: req.params.id, name, code });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/strategies/:id', requireAuth, async (req, res) => {
  try {
    if (pgPool) {
      await pgPool.query('DELETE FROM strategies WHERE id=$1 AND user_id=$2', [req.params.id, req.session.userId]);
    }
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════
//  JOURNAL
// ══════════════════════════════════════════════════════════
app.get('/api/journal', requireAuth, async (req, res) => {
  try {
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM journal WHERE user_id=$1 ORDER BY date DESC LIMIT 500', [req.session.userId]);
      return res.json(r.rows);
    }
    res.json([]);
  } catch (e) { res.json([]); }
});

app.post('/api/journal', requireAuth, async (req, res) => {
  const { date, sport, match_name, market, selection, odds, stake, result, pnl, bookmaker, notes } = req.body;
  try {
    if (pgPool) {
      const r = await pgPool.query(
        'INSERT INTO journal (user_id,date,sport,match_name,market,selection,odds,stake,result,pnl,bookmaker,notes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *',
        [req.session.userId, date, sport, match_name, market, selection, odds, stake, result, pnl, bookmaker, notes]
      );
      return res.json(r.rows[0]);
    }
    res.json({ id: Date.now(), ...req.body });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/journal/:id', requireAuth, async (req, res) => {
  try {
    if (pgPool) await pgPool.query('DELETE FROM journal WHERE id=$1 AND user_id=$2', [req.params.id, req.session.userId]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════
//  AI STRATEGY
// ══════════════════════════════════════════════════════════
app.post('/api/ai/strategy', async (req, res) => {
  const { message, history = [], model, provider = 'openrouter_free', apiKey: clientKey, baseUrl } = req.body;

  const PROVIDERS = {
    anthropic:       { url: 'https://api.anthropic.com/v1/messages',         format: 'anthropic', getKey: () => clientKey || process.env.ANTHROPIC_API_KEY },
    openai:          { url: 'https://api.openai.com/v1/chat/completions',    format: 'openai',    getKey: () => clientKey || process.env.OPENAI_API_KEY },
    openrouter_free: { url: 'https://openrouter.ai/api/v1/chat/completions', format: 'openai',    getKey: () => clientKey || process.env.OPENROUTER_API_KEY || '' },
    openrouter:      { url: 'https://openrouter.ai/api/v1/chat/completions', format: 'openai',    getKey: () => clientKey || process.env.OPENROUTER_API_KEY },
    deepseek:        { url: 'https://api.deepseek.com/v1/chat/completions',  format: 'openai',    getKey: () => clientKey || process.env.DEEPSEEK_API_KEY },
    groq:            { url: 'https://api.groq.com/openai/v1/chat/completions',format: 'openai',   getKey: () => clientKey || process.env.GROQ_API_KEY },
    xai:             { url: 'https://api.x.ai/v1/chat/completions',          format: 'openai',    getKey: () => clientKey || process.env.XAI_API_KEY },
    mistral:         { url: 'https://api.mistral.ai/v1/chat/completions',    format: 'openai',    getKey: () => clientKey || process.env.MISTRAL_API_KEY },
  };

  const SYSTEM = `You are BetQuant AI — expert sports betting strategy developer.
Always produce a complete JavaScript evaluate() function inside a \`\`\`javascript block.
evaluate(match, team, h2h, market) returns { signal:true, market:'home'|'draw'|'away'|'over'|'under'|'btts', stake:1, prob:0.55 } or null.
match: odds_home/draw/away/over/under/btts, prob_home/draw/away, team_home, team_away, league, date.
team.form(name,n), team.xG(name,n), team.goalsScored(name,n), team.goalsConceded(name,n).
h2h.results[], market.implied(odds), market.value(odds,prob), market.kelly(odds,prob).`;

  const p = PROVIDERS[provider] || PROVIDERS.openrouter_free;
  const apiKey = p.getKey();
  const modelName = model || (provider === 'anthropic' ? 'claude-3-5-haiku-20241022' : 'meta-llama/llama-3.3-70b-instruct:free');

  try {
    let body, headers = { 'Content-Type': 'application/json' };
    if (p.format === 'anthropic') {
      headers['x-api-key'] = apiKey;
      headers['anthropic-version'] = '2023-06-01';
      body = { model: modelName, max_tokens: 2000, system: SYSTEM, messages: [...history, { role: 'user', content: message }] };
    } else {
      if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;
      body = { model: modelName, max_tokens: 2000, messages: [{ role: 'system', content: SYSTEM }, ...history, { role: 'user', content: message }] };
    }

    const fetch = require('node-fetch').default || require('node-fetch');
    const resp = await fetch(p.url, { method: 'POST', headers, body: JSON.stringify(body) });
    const data = await resp.json();

    let text = '';
    if (p.format === 'anthropic') text = data.content?.[0]?.text || data.error?.message || 'No response';
    else text = data.choices?.[0]?.message?.content || data.error?.message || 'No response';

    res.json({ response: text });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════
//  HEALTH CHECK
// ══════════════════════════════════════════════════════════
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    version: '3.3.0',
    pg: !!pgPool,
    ch: !!clickhouse,
    node: process.version,
    etl: { v1: 'betquant-etl/run_etl.py', v2: 'sports-etl-v2/run_etl_v2.py' },
    routes: ['auth', 'db', 'backtest', 'etl', 'journal', 'strategies', 'ai/strategy'],
  });
});

// ══════════════════════════════════════════════════════════
//  SPA FALLBACK
// ══════════════════════════════════════════════════════════
app.get('*', (req, res) => {
  res.sendFile(path.join(PUBLIC, 'index.html'));
});

// ══════════════════════════════════════════════════════════
//  START
// ══════════════════════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n🎯 BetQuant Pro running → http://localhost:${PORT}`);
  console.log(`   Mode : ${process.env.NODE_ENV || 'development'}`);
  console.log(`   DB   : PostgreSQL ${pgPool ? '✅' : '⚠️  (demo mode)'} | ClickHouse ${clickhouse ? '✅' : '⚠️  (demo mode)'}`);
  console.log(`   ETL  : v1 (betquant-etl) + v2 (sports-etl-v2) ✅`);
  console.log(`   Press Ctrl+C to stop\n`);
});

module.exports = app;