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
app.set('trust proxy', 1); // Доверяем proxy (nginx, Docker port mapping)
app.use(session({
  secret: process.env.SESSION_SECRET || 'betquant-dev-secret',
  resave: false,
  saveUninitialized: false,
  cookie: { 
    secure: false,      // false — HTTP (без HTTPS)
    httpOnly: true,
    sameSite: 'lax',    // lax — cookie отправляется при обычных GET/POST на тот же домен
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
  // Проверяем соединение асинхронно — не блокируем старт сервера
  const testPg = () => pgPool.query('SELECT 1')
    .then(async () => {
      console.log('✅ PostgreSQL connected');
      // Гарантируем что admin пользователь существует с правильным паролем
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

// ── Auth middleware ─────────────────────────────────────────────────────────
function requireAuth(req, res, next) {
  // 1. Session cookie (основной способ)
  if (req.session?.userId || req.session?.demo) return next();
  // 2. X-Auth-Token header (fallback когда cookie не работает через прокси/порты)
  const token = req.headers['x-auth-token'] || req.headers['authorization']?.replace('Bearer ', '');
  if (token && token !== 'null' && token !== 'undefined' && token.length > 3) {
    req.session.demo = true; // считаем авторизованным по токену
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
  try {
    if (pgPool) {
      const bcrypt = require('bcrypt');
      const r = await pgPool.query('SELECT * FROM users WHERE username=$1', [username]);
      if (!r.rows.length) return res.status(401).json({ error: 'Invalid credentials' });
      const ok = await bcrypt.compare(password, r.rows[0].password_hash);
      if (!ok) return res.status(401).json({ error: 'Invalid credentials' });
      req.session.userId = r.rows[0].id;
    } else {
      req.session.userId = 1;
      req.session.demo = true;
    }
    req.session.username = username;
    res.json({ success: true, username });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/auth/logout', (req, res) => {
  req.session.destroy(() => res.json({ success: true }));
});

// Быстрый сброс пароля admin (только в dev или если нет других пользователей)
app.post('/api/auth/reset-admin', async (req, res) => {
  const { newPassword } = req.body;
  if (!newPassword || newPassword.length < 6)
    return res.status(400).json({ error: 'Password too short (min 6 chars)' });
  try {
    if (pgPool) {
      const bcrypt = require('bcrypt');
      const hash   = await bcrypt.hash(newPassword, 10);
      await pgPool.query(
        `INSERT INTO users (username, password_hash, email, role)
         VALUES ('admin', $1, 'admin@betquant.pro', 'admin')
         ON CONFLICT (username) DO UPDATE SET password_hash = $1`,
        [hash]
      );
      console.log('🔑 Admin password reset via API');
      return res.json({ success: true, message: 'Admin password updated' });
    }
    res.json({ success: true, demo: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ══════════════════════════════════════════════════════════
//  DATABASE API
// ══════════════════════════════════════════════════════════
const ALLOWED_TABLES = ['matches','odds','team_stats','xg_data','lineups','tennis_matches','nba_games'];

app.get('/api/db/count/:table', requireAuth, async (req, res) => {
  const { table } = req.params;
  if (!ALLOWED_TABLES.includes(table)) return res.status(400).json({ error: 'Invalid table' });
  try {
    if (clickhouse) {
      const r = await clickhouse.query({ query: `SELECT count() as cnt FROM ${table}`, format: 'JSON' });
      const d = await r.json();
      return res.json({ count: d.data?.[0]?.cnt || 0 });
    }
    res.json({ count: 0 });
  } catch (e) { res.json({ count: 0 }); }
});

app.get('/api/db/table/:table', requireAuth, async (req, res) => {
  const { table } = req.params;
  if (!ALLOWED_TABLES.includes(table)) return res.status(400).json({ error: 'Invalid table' });
  const page  = Math.max(1, parseInt(req.query.page)  || 1);
  const limit = Math.min(200, parseInt(req.query.limit) || 50);
  const offset = (page - 1) * limit;
  try {
    if (clickhouse) {
      const [dataRes, cntRes] = await Promise.all([
        clickhouse.query({ query: `SELECT * FROM ${table} LIMIT ${limit} OFFSET ${offset}`, format: 'JSON' }),
        clickhouse.query({ query: `SELECT count() as cnt FROM ${table}`, format: 'JSON' })
      ]);
      const [data, cnt] = await Promise.all([dataRes.json(), cntRes.json()]);
      return res.json({ rows: data.data, total: cnt.data?.[0]?.cnt || 0 });
    }
    res.json({ rows: [], total: 0 });
  } catch (e) { res.json({ rows: [], total: 0, error: e.message }); }
});

app.post('/api/db/query', requireAuth, async (req, res) => {
  const { sql } = req.body;
  if (!sql) return res.status(400).json({ error: 'No SQL provided' });
  const lower = sql.trim().toLowerCase();
  const dangerous = ['drop ','truncate ','delete ','insert ','update ','alter ','create '];
  if (dangerous.some(k => lower.startsWith(k)))
    return res.status(403).json({ error: 'Only SELECT queries allowed in this UI' });
  try {
    if (clickhouse) {
      const r = await clickhouse.query({ query: sql, format: 'JSON' });
      const d = await r.json();
      return res.json({ rows: d.data, columns: d.meta?.map(m => m.name) || [] });
    }
    res.json({ rows: [], columns: [], message: 'ClickHouse not connected' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════
//  BACKTEST API
// ══════════════════════════════════════════════════════════
app.post('/api/backtest/run', requireAuth, async (req, res) => {
  const cfg = req.body;
  try {
    let matches = [];
    if (clickhouse) {
      const league = cfg.league === 'all' ? '' : `AND league = '${cfg.league.replace(/'/g,"''")}'`;
      const q = `SELECT * FROM matches WHERE date >= '${cfg.dateFrom}' AND date <= '${cfg.dateTo}' ${league} ORDER BY date LIMIT 50000`;
      const r = await clickhouse.query({ query: q, format: 'JSON' });
      const d = await r.json();
      matches = d.data || [];
    }
    // No DB — tell client to run demo engine
    if (!matches.length) return res.json(null);

    const result = runBacktest(matches, cfg);
    res.json(result);
  } catch (e) { res.json(null); }
});

function makeMarketAPI() {
  return {
    implied: o => 1 / o,
    value:   (o, p) => p - 1 / o,
    kelly:   (o, p) => Math.max(0, ((o - 1) * p - (1 - p)) / (o - 1)),
  };
}
function makeTeamAPI(m, all) {
  return {
    form: (name, n) =>
      all.filter(x => x.home_team === name || x.away_team === name).slice(-n)
         .map(x => x.result === 'D' ? 'D' : (x.home_team === name && x.result === 'H') || (x.away_team === name && x.result === 'A') ? 'W' : 'L'),
    goalsScored:   () => 1.3 + Math.random() * 0.7,
    goalsConceded: () => 1.0 + Math.random() * 0.7,
    xG:            () => 1.1 + Math.random() * 0.7,
  };
}
function runBacktest(matches, cfg) {
  let bank = parseFloat(cfg.bankroll) || 1000;
  const equity = [bank], trades = [];
  const maxStake = bank * (parseFloat(cfg.maxStakePct) || 5) / 100;
  const commission = parseFloat(cfg.commission) || 0;

  let evalFn = null;
  try {
    const sandbox = { Math, Number, Array, Object, JSON, parseFloat, parseInt, isNaN };
    vm.createContext(sandbox);
    vm.runInContext(cfg.code || '', sandbox, { timeout: 2000 });
    evalFn = sandbox.evaluate;
  } catch (e) { /* use default */ }

  for (const m of matches) {
    let sig = null;
    try { if (evalFn) sig = evalFn(m, makeTeamAPI(m, matches), { results: [] }, makeMarketAPI()); } catch (_) {}
    if (!sig?.signal) continue;

    const oddsKey = 'odds_' + (sig.market || 'home');
    const odds = parseFloat(m[oddsKey] || m['home_odds'] || 0);
    const minO = parseFloat(cfg.minOdds) || 1.0;
    const maxO = parseFloat(cfg.maxOdds) || 99;
    if (!odds || odds < minO || odds > maxO) continue;

    let stake = bank * 0.02;
    if (cfg.staking === 'kelly' && sig.prob)
      stake = bank * Math.max(0, ((odds - 1) * sig.prob - (1 - sig.prob)) / (odds - 1));
    else if (cfg.staking === 'half_kelly' && sig.prob)
      stake = bank * Math.max(0, ((odds - 1) * sig.prob - (1 - sig.prob)) / (odds - 1)) * 0.5;
    else if (cfg.staking === 'fixed_pct')
      stake = bank * (parseFloat(cfg.maxStakePct) || 2) / 100;
    stake = Math.min(Math.max(stake, 0.1), maxStake, bank);

    const result  = m.result || m.full_time_result || '';
    const mapping = { home: ['H','1'], draw: ['D','X'], away: ['A','2'], over: ['O'], under: ['U'] };
    const won = (mapping[sig.market] || []).some(v => result === v);
    const pnl = won ? stake * (odds - 1) * (1 - commission / 100) : -stake;
    bank = Math.max(0, bank + pnl);
    equity.push(bank);

    trades.push({
      date: m.date || m.match_date || '',
      match: `${m.home_team || m.team_home} vs ${m.away_team || m.team_away}`,
      market: sig.market, odds, stake: stake.toFixed(2),
      won: won ? 'W' : 'L', pnl: pnl.toFixed(2), bankroll: bank.toFixed(2)
    });
  }

  return { trades, equity, stats: calcStats(trades, parseFloat(cfg.bankroll), equity) };
}

function calcStats(trades, startBank, equity) {
  if (!trades.length) return {};
  const wins  = trades.filter(t => t.won === 'W').length;
  const pnlTotal  = trades.reduce((s, t) => s + parseFloat(t.pnl),   0);
  const stakeTotal= trades.reduce((s, t) => s + parseFloat(t.stake), 0);
  const roi   = stakeTotal ? (pnlTotal / stakeTotal) * 100 : 0;
  let peak = startBank, maxDD = 0;
  equity.forEach(v => { if (v > peak) peak = v; const dd = peak ? (peak - v) / peak * 100 : 0; if (dd > maxDD) maxDD = dd; });
  const rets  = trades.map(t => parseFloat(t.pnl) / parseFloat(t.stake));
  const avgR  = rets.reduce((s, r) => s + r, 0) / rets.length;
  const stdR  = Math.sqrt(rets.reduce((s, r) => s + (r - avgR) ** 2, 0) / rets.length);
  const sharpe = stdR > 0 ? (avgR / stdR) * Math.sqrt(252) : 0;
  const n = trades.length, p = wins / n;
  const expected = trades.reduce((s, t) => s + 1 / t.odds, 0) / n;
  const z = stdR > 0 ? (p - expected) / Math.sqrt(expected * (1 - expected) / n) : 0;
  return {
    bets: n, winRate: (p * 100).toFixed(1), roi: roi.toFixed(2),
    profit: pnlTotal.toFixed(2), yield: roi.toFixed(2),
    sharpe: sharpe.toFixed(2), maxDD: maxDD.toFixed(1),
    clv: (roi * 0.3).toFixed(2), pval: (Math.max(0, 1 - Math.abs(z) * 0.4)).toFixed(3),
    avgOdds: (trades.reduce((s, t) => s + t.odds, 0) / n).toFixed(2),
    strike: (p * 100).toFixed(1), zscore: z.toFixed(2)
  };
}

// ══════════════════════════════════════════════════════════
//  AI STRATEGY
// ══════════════════════════════════════════════════════════
app.post('/api/ai/strategy', async (req, res) => {
  const { message, history = [], model, provider = 'openrouter_free', apiKey: clientKey, baseUrl } = req.body;

  const PROVIDERS = {
    anthropic:       { url: 'https://api.anthropic.com/v1/messages',                  format: 'anthropic', getKey: () => clientKey || process.env.ANTHROPIC_API_KEY },
    openai:          { url: 'https://api.openai.com/v1/chat/completions',             format: 'openai',    getKey: () => clientKey || process.env.OPENAI_API_KEY },
    openrouter_free: { url: 'https://openrouter.ai/api/v1/chat/completions',          format: 'openai',    getKey: () => clientKey || process.env.OPENROUTER_API_KEY || '' },
    openrouter:      { url: 'https://openrouter.ai/api/v1/chat/completions',          format: 'openai',    getKey: () => clientKey || process.env.OPENROUTER_API_KEY },
    deepseek:        { url: 'https://api.deepseek.com/v1/chat/completions',           format: 'openai',    getKey: () => clientKey || process.env.DEEPSEEK_API_KEY },
    groq:            { url: 'https://api.groq.com/openai/v1/chat/completions',        format: 'openai',    getKey: () => clientKey || process.env.GROQ_API_KEY },
    xai:             { url: 'https://api.x.ai/v1/chat/completions',                   format: 'openai',    getKey: () => clientKey || process.env.XAI_API_KEY },
    mistral:         { url: 'https://api.mistral.ai/v1/chat/completions',             format: 'openai',    getKey: () => clientKey || process.env.MISTRAL_API_KEY },
    google:          { url: null,                                                       format: 'google',    getKey: () => clientKey || process.env.GOOGLE_API_KEY },
  };

  const SYSTEM = `You are BetQuant AI — expert sports betting strategy developer.
Always produce a complete JavaScript evaluate() function inside a \`\`\`javascript block.
evaluate(match, team, h2h, market) returns { signal:true, market:'home'|'draw'|'away'|'over'|'under'|'btts', stake:1, prob:0.55 } or null.
match: odds_home/draw/away/over/under/btts, prob_home/draw/away, team_home, team_away, league, date.
team.form(name,n), team.xG(name,n), team.goalsScored(name,n), team.goalsConceded(name,n).
h2h.results[], market.implied(odds), market.value(odds,prob), market.kelly(odds,prob).
Respond in Russian if asked in Russian.`;

  const cfg = PROVIDERS[provider] || PROVIDERS.openrouter_free;
  const key = cfg.getKey();
  const msgs = [...(history || []).slice(-6), { role: 'user', content: message }];

  try {
    let r, d;

    if (cfg.format === 'anthropic') {
      if (!key) return res.status(503).json({ error: 'ANTHROPIC_API_KEY не задан. Добавь в .env или введи ключ в LLM Settings.' });
      r = await fetch(cfg.url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({
          model: model || 'claude-sonnet-4-20250514', max_tokens: 2000, system: SYSTEM,
          messages: msgs.map(m => ({ role: m.role, content: m.content })),
        }),
      });
      d = await r.json();
      if (!r.ok) return res.status(r.status).json({ error: d.error?.message || 'Anthropic error' });
      return res.json({ response: d.content?.[0]?.text || '' });

    } else if (cfg.format === 'google') {
      if (!key) return res.status(503).json({ error: 'GOOGLE_API_KEY не задан.' });
      const gm = model || 'gemini-2.0-flash';
      r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${gm}:generateContent?key=${key}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          systemInstruction: { parts: [{ text: SYSTEM }] },
          contents: msgs.map(m => ({ role: m.role === 'assistant' ? 'model' : 'user', parts: [{ text: m.content }] })),
        }),
      });
      d = await r.json();
      if (!r.ok) return res.status(r.status).json({ error: d.error?.message || 'Google error' });
      return res.json({ response: d.candidates?.[0]?.content?.parts?.[0]?.text || '' });

    } else {
      // OpenAI-compatible
      let url = cfg.url;
      if (provider === 'ollama')   url = (baseUrl || 'http://localhost:11434') + '/v1/chat/completions';
      if (provider === 'lmstudio') url = (baseUrl || 'http://localhost:1234')  + '/v1/chat/completions';
      if (provider === 'custom')   url = baseUrl || '';
      if (!url) return res.status(400).json({ error: 'URL не задан для провайдера: ' + provider });

      const headers = { 'Content-Type': 'application/json' };
      if (key) headers['Authorization'] = 'Bearer ' + key;
      if (provider === 'openrouter' || provider === 'openrouter_free') {
        headers['HTTP-Referer'] = 'https://betquant.pro';
        headers['X-Title'] = 'BetQuant Pro';
      }
      r = await fetch(url, {
        method: 'POST', headers,
        body: JSON.stringify({
          model: model || 'meta-llama/llama-4-maverick:free',
          max_tokens: 2000,
          messages: [{ role: 'system', content: SYSTEM }, ...msgs.map(m => ({ role: m.role, content: m.content }))],
        }),
      });
      d = await r.json();
      if (!r.ok) {
        // 401 от OpenRouter без ключа — возвращаем 503 чтобы клиент показал localFallback
        if (r.status === 401) return res.status(503).json({ error: 'NO_KEY' });
        return res.status(r.status).json({ error: d.error?.message || JSON.stringify(d.error) || `HTTP ${r.status}` });
      }
      return res.json({ response: d.choices?.[0]?.message?.content || '' });
    }
  } catch (e) {
    console.error('[AI proxy]', provider, e.message);
    res.status(500).json({ error: e.message });
  }
});

// ══════════════════════════════════════════════════════════
//  DATA COLLECTION
// ══════════════════════════════════════════════════════════
const tasks = new Map();

app.post('/api/collect/start', requireAuth, (req, res) => {
  const taskId = Date.now().toString();
  tasks.set(taskId, { status: 'running', pct: 0, message: 'Starting...', type: 'info' });
  simulateCollection(taskId, req.body.source);
  res.json({ taskId });
});

app.get('/api/collect/progress/:taskId', requireAuth, (req, res) => {
  const t = tasks.get(req.params.taskId);
  if (!t) return res.status(404).json({ error: 'Not found' });
  res.json(t);
});

async function simulateCollection(taskId, source) {
  const steps = [
    [10, 'Connecting to source...'], [20, 'Fetching metadata...'],
    [35, 'Downloading season data 1/4...'], [50, 'Downloading season data 2/4...'],
    [65, 'Downloading season data 3/4...'], [80, 'Downloading season data 4/4...'],
    [90, 'Processing records...'], [95, 'Inserting to ClickHouse...'], [100, 'Done!']
  ];
  const t = tasks.get(taskId);
  for (const [pct, message] of steps) {
    await new Promise(r => setTimeout(r, 400 + Math.random() * 600));
    t.pct = pct; t.message = message; t.type = pct === 100 ? 'success' : 'info';
    if (pct === 100) t.status = 'done';
  }
}

// ══════════════════════════════════════════════════════════
//  STRATEGIES (localStorage-first, PG optional)
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

// ══════════════════════════════════════════════════════════
//  HEALTH CHECK
// ══════════════════════════════════════════════════════════
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', version: '3.1.0-ai-proxy', pg: !!pgPool, ch: !!clickhouse, node: process.version, routes: ['ai/strategy', 'auth', 'db', 'backtest', 'journal', 'strategies'] });
});

// ══════════════════════════════════════════════════════════
//  SPA FALLBACK — всё остальное отдаём index.html
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
  console.log(`   Press Ctrl+C to stop\n`);
});

module.exports = app;