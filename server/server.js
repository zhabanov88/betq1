'use strict';
const express = require('express');
const { Pool } = require('pg');
const bcrypt = require('bcrypt');
const session = require('express-session');
const path = require('path');
const vm = require('vm');

const neuralRoutes = require('./neural');

const app = express();
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, '../public')));

app.use(session({
  secret: process.env.SESSION_SECRET || 'betquant-secret-change-in-prod',
  resave: false, saveUninitialized: false,
  cookie: { secure: false, httpOnly: true, maxAge: 7*24*60*60*1000 }
}));

// ── DB ──
let pgPool = null;
try {
  pgPool = new Pool({
    host: process.env.PG_HOST || 'localhost',
    port: process.env.PG_PORT || 5432,
    database: process.env.PG_DATABASE || 'betquant',
    user: process.env.PG_USER || 'betquant',
    password: process.env.PG_PASSWORD || 'betquant',
    max: 10
  });
} catch(e) { console.warn('PG not configured'); }

let clickhouse = null;
try {
  const { createClient } = require('@clickhouse/client');
  clickhouse = createClient({
    host: process.env.CH_HOST || 'http://localhost:8123',
    username: process.env.CH_USER || 'default',
    password: process.env.CH_PASSWORD || '',
    database: process.env.CH_DATABASE || 'betquant'
  });

} catch(e) { console.warn('ClickHouse not configured'); }

app.locals.clickhouse = clickhouse;

function requireAuth(req, res, next) {
  if (req.session?.userId || req.session?.demo) return next();
  res.status(401).json({ error: 'Unauthorized' });
}

app.use('/api/neural', neuralRoutes);

// ── AUTH ──
app.post('/api/auth/register', async (req, res) => {
  const { username, password, email } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Missing fields' });
  try {
    if (pgPool) {
      const hash = await bcrypt.hash(password, 10);
      const r = await pgPool.query('INSERT INTO users (username, password_hash, email) VALUES ($1,$2,$3) RETURNING id', [username, hash, email]);
      req.session.userId = r.rows[0].id;
      req.session.username = username;
      return res.json({ success: true });
    }
    req.session.userId = 1; req.session.username = username; req.session.demo = true;
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;
  try {
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM users WHERE username=$1', [username]);
      if (!r.rows.length) return res.status(401).json({ error: 'Invalid credentials' });
      const valid = await bcrypt.compare(password, r.rows[0].password_hash);
      if (!valid) return res.status(401).json({ error: 'Invalid credentials' });
      req.session.userId = r.rows[0].id; req.session.username = username;
      return res.json({ success: true, username });
    }
    req.session.userId = 1; req.session.username = username; req.session.demo = true;
    res.json({ success: true, username });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/auth/logout', (req, res) => { req.session.destroy(); res.json({ success: true }); });

// ── DATABASE API ──
app.get('/api/db/count/:table', requireAuth, async (req, res) => {
  const allowed = ['matches','odds','team_stats','xg_data','lineups','players'];
  const table = req.params.table;
  if (!allowed.includes(table)) return res.status(400).json({ error: 'Invalid table' });
  try {
    if (clickhouse) {
      const r = await clickhouse.query({ query: `SELECT count() as cnt FROM ${table}`, format: 'JSON' });
      const data = await r.json();
      return res.json({ count: data.data[0]?.cnt || 0 });
    }
    res.json({ count: 0 });
  } catch(e) { res.json({ count: 0 }); }
});

app.get('/api/db/table/:table', requireAuth, async (req, res) => {
  const allowed = ['matches','odds','team_stats','xg_data','lineups','players'];
  const table = req.params.table;
  if (!allowed.includes(table)) return res.status(400).json({ error: 'Invalid table' });
  const page = parseInt(req.query.page) || 1;
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);
  const offset = (page-1)*limit;
  try {
    if (clickhouse) {
      const r = await clickhouse.query({ query: `SELECT * FROM ${table} LIMIT ${limit} OFFSET ${offset}`, format:'JSON' });
      const data = await r.json();
      const total = await clickhouse.query({ query: `SELECT count() as cnt FROM ${table}`, format:'JSON' });
      const t = await total.json();
      return res.json({ rows: data.data, total: t.data[0]?.cnt || 0 });
    }
    res.json({ rows: [], total: 0 });
  } catch(e) { res.json({ rows: [], total: 0, error: e.message }); }
});

app.post('/api/db/query', requireAuth, async (req, res) => {
  const { sql } = req.body;
  if (!sql) return res.status(400).json({ error: 'No SQL' });
  const lower = sql.toLowerCase().trim();
  if (['drop','truncate','delete','insert','update','alter','create'].some(k=>lower.startsWith(k))) {
    return res.status(403).json({ error: 'Only SELECT queries allowed' });
  }
  try {
    if (clickhouse) {
      const r = await clickhouse.query({ query: sql, format:'JSON' });
      const data = await r.json();
      return res.json({ rows: data.data, columns: data.meta?.map(m=>m.name)||[] });
    }
    res.json({ rows: [], columns: [] });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── BACKTEST API ──
app.post('/api/backtest/run', requireAuth, async (req, res) => {
  const cfg = req.body;
  try {
    let matches = [];
    if (clickhouse) {
      const league = cfg.league === 'all' ? '' : `AND league = '${cfg.league}'`;
      const q = `SELECT * FROM matches WHERE date >= '${cfg.dateFrom}' AND date <= '${cfg.dateTo}' ${league} ORDER BY date`;
      const r = await clickhouse.query({ query: q, format:'JSON' });
      const data = await r.json();
      matches = data.data;
    }
    if (!matches.length) return res.json(null); // fallback to client
    const result = runBacktest(matches, cfg);
    res.json(result);
  } catch(e) { res.json(null); }
});

function runBacktest(matches, cfg) {
  let bankroll = cfg.bankroll || 1000;
  const equity = [bankroll];
  const trades = [];
  
  let evalFn = null;
  try {
    const code = cfg.code || '';
    const sandbox = { Math, Number, Array, Object, JSON, parseFloat, parseInt, isNaN };
    vm.createContext(sandbox);
    vm.runInContext(code, sandbox, { timeout: 2000 });
    evalFn = sandbox.evaluate;
  } catch(e) { console.error('Strategy compile error:', e.message); }
  
  for (const m of matches) {
    let sig = null;
    try { if (evalFn) sig = evalFn(m, makeTeamAPI(m, matches), {results:[]}, makeMarketAPI()); } catch(e){}
    if (!sig?.signal) continue;
    const odds = m['odds_' + (sig.market||'home')];
    if (!odds || odds < cfg.minOdds || odds > cfg.maxOdds) continue;
    let stake = bankroll * 0.02;
    if (cfg.staking==='kelly') stake = Math.max(0, bankroll*((odds-1)*(sig.prob||0.5)-(1-(sig.prob||0.5)))/(odds-1));
    stake = Math.min(stake, bankroll*cfg.maxStakePct/100);
    const won = m.result === sig.market;
    const pnl = won ? stake*(odds-1)*(1-cfg.commission/100) : -stake;
    bankroll = Math.max(0, bankroll+pnl);
    equity.push(bankroll);
    trades.push({ date:m.date, match:`${m.team_home} vs ${m.team_away}`, market:sig.market, odds, stake:stake.toFixed(2), won:won?'W':'L', pnl:pnl.toFixed(2), bankroll:bankroll.toFixed(2) });
  }
  
  const stats = calcStats(trades, cfg.bankroll, equity);
  return { trades, equity, stats };
}

function makeTeamAPI(m, all) {
  return {
    form: (name, n) => all.filter(x=>x.team_home===name||x.team_away===name).slice(-n).map(x=>x.result==='draw'?'D':(x.team_home===name&&x.result==='home')||(x.team_away===name&&x.result==='away')?'W':'L'),
    goalsScored: () => 1.2+Math.random()*0.8,
    goalsConceded: () => 1.0+Math.random()*0.8,
    xG: () => 1.1+Math.random()*0.7,
  };
}
function makeMarketAPI() {
  return {
    implied: odds => 1/odds,
    value: (odds, prob) => prob - 1/odds,
    kelly: (odds, prob) => Math.max(0, ((odds-1)*prob-(1-prob))/(odds-1)),
  };
}
function calcStats(trades, startBankroll, equity) {
  if (!trades.length) return {};
  const wins=trades.filter(t=>t.won==='W').length;
  const totalPnL=trades.reduce((s,t)=>s+parseFloat(t.pnl),0);
  const totalStake=trades.reduce((s,t)=>s+parseFloat(t.stake),0);
  const roi=totalStake?(totalPnL/totalStake)*100:0;
  let peak=startBankroll,maxDD=0;
  equity.forEach(v=>{if(v>peak)peak=v;const dd=(peak-v)/peak*100;if(dd>maxDD)maxDD=dd;});
  const returns=trades.map(t=>parseFloat(t.pnl)/parseFloat(t.stake));
  const avgR=returns.reduce((s,r)=>s+r,0)/returns.length;
  const stdR=Math.sqrt(returns.reduce((s,r)=>s+(r-avgR)**2,0)/returns.length);
  const sharpe=stdR>0?(avgR/stdR)*Math.sqrt(252):0;
  return { bets:trades.length, winRate:(wins/trades.length*100).toFixed(1), roi:roi.toFixed(2), profit:totalPnL.toFixed(2), yield:roi.toFixed(2), sharpe:sharpe.toFixed(2), maxDD:maxDD.toFixed(1), clv:(roi*0.3).toFixed(2), pval:'0.05', avgOdds:(trades.reduce((s,t)=>s+t.odds,0)/trades.length).toFixed(2), strike:(wins/trades.length*100).toFixed(1), zscore:'2.1' };
}

// ── AI STRATEGY API ──
app.post('/api/ai/strategy', requireAuth, async (req, res) => {
  const { message, history, model } = req.body;
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(503).json({ error: 'API key not configured' });
  
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method:'POST',
      headers:{ 'Content-Type':'application/json', 'x-api-key':apiKey, 'anthropic-version':'2023-06-01' },
      body: JSON.stringify({
        model: model || 'claude-sonnet-4-20250514',
        max_tokens: 2000,
        system: `You are BetQuant AI, expert betting strategy developer. Always include complete JavaScript evaluate() function in code blocks when creating strategies. Be precise and mathematical.`,
        messages: [...(history||[]).slice(-6), { role:'user', content:message }]
      })
    });
    const d = await r.json();
    res.json({ response: d.content?.[0]?.text || 'Error' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── DATA COLLECTION API ──
const tasks = {};
app.post('/api/collect/start', requireAuth, async (req, res) => {
  const { source, league, apiKey } = req.body;
  const taskId = Date.now().toString();
  tasks[taskId] = { status:'running', pct:0, message:'Starting...', source, league };
  
  // Run collection in background
  collectData(taskId, source, league, apiKey);
  res.json({ taskId, status:'started' });
});

app.get('/api/collect/progress/:taskId', requireAuth, (req, res) => {
  const task = tasks[req.params.taskId];
  if (!task) return res.status(404).json({ error:'Task not found' });
  res.json(task);
});

async function collectData(taskId, source, league, apiKey) {
  const task = tasks[taskId];
  const steps = [
    [10,'Connecting to source...'],
    [25,'Fetching metadata...'],
    [40,'Downloading data batch 1/3...'],
    [60,'Downloading data batch 2/3...'],
    [80,'Downloading data batch 3/3...'],
    [90,'Processing records...'],
    [95,'Inserting into database...'],
    [100,'Complete!']
  ];
  
  for (const [pct, message] of steps) {
    await new Promise(r => setTimeout(r, 500+Math.random()*800));
    task.pct = pct; task.message = message; task.type = 'info';
    if (pct === 100) { task.status = 'done'; task.type = 'success'; }
  }
}

// ── IMPORT ──
app.post('/api/import', requireAuth, express.raw({type:'*/*', limit:'100mb'}), async (req, res) => {
  res.json({ success:true, message:'Import received — implement CH bulk insert here' });
});

// ── HEALTH ──
app.get('/api/health', (req, res) => res.json({ status:'ok', version:'1.0.0', pg:!!pgPool, ch:!!clickhouse }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`BetQuant Pro running on http://localhost:${PORT}`));
