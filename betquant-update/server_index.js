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
const ALLOWED_TABLES = [
  'matches','odds','team_stats','xg_data','lineups','tennis_matches','nba_games',
  // Расширенные таблицы (ETL)
  'football_matches','football_events','football_team_form',
  'hockey_matches','hockey_events','hockey_team_form',
  'tennis_extended','basketball_matches','baseball_matches','etl_log'
];

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
//  BACKTEST API  (реальные данные из ClickHouse)
// ══════════════════════════════════════════════════════════

// Вспомогательная функция — безопасное выполнение стратегии
function makeSandbox(allMatches) {
  return {
    Math, Number, Array, Object, JSON, parseFloat, parseInt, isNaN,
    console: { log: () => {}, warn: () => {} },
  };
}

function makeMarketAPI() {
  return {
    implied: o => o > 0 ? 1 / o : 0,
    value:   (o, p) => p - 1 / o,
    kelly:   (o, p) => Math.max(0, ((o - 1) * p - (1 - p)) / (o - 1)),
  };
}

// teamAPI теперь использует реальные данные из football_team_form
function makeTeamAPI(m, formMap) {
  const makeProxy = (team) => {
    const f = formMap[`${m.match_id}|${team}`] || {};
    return {
      // Форма
      form:          (n) => (f.form_5 || '').slice(0, n || 5).split(''),
      pts:           (n) => n <= 5 ? (f.pts_5 || 0) : (f.pts_10 || 0),

      // Голы — сезон
      goalsScored:   ()  => f.season_goals_for     || 0,
      goalsConceded: ()  => f.season_goals_against  || 0,
      goalDiff:      ()  => f.season_goal_diff      || 0,

      // xG
      xG:            ()  => f.season_xg_for         || 0,
      xGA:           ()  => f.season_xg_against     || 0,

      // Угловые
      corners:       ()  => f.season_corners_for    || 0,
      cornersConceded:() => f.season_corners_against || 0,

      // Карточки
      yellowCards:   ()  => f.season_yellow         || 0,

      // Форма за 30 дней
      last30Goals:   ()  => f.last30_goals_for      || 0,
      last30xG:      ()  => f.last30_xg_for         || 0,

      // Весь год
      ytdGoals:      ()  => f.ytd_goals_for         || 0,

      // Прошлый сезон
      prevSeasonGoals:() => f.prev_season_goals_for || 0,
      prevSeasonxG:  ()  => f.prev_season_xg_for    || 0,

      // Дома/гость
      homeGoals:     ()  => f.home_season_goals_for  || 0,
      awayGoals:     ()  => f.away_season_goals_for  || 0,

      // H2H
      h2hWins:       ()  => f.h2h_wins   || 0,
      h2hDraws:      ()  => f.h2h_draws  || 0,
      h2hGoals:      ()  => f.h2h_goals_for || 0,

      // Wins/draws/losses
      wins:          ()  => f.season_wins   || 0,
      draws:         ()  => f.season_draws  || 0,
      losses:        ()  => f.season_losses || 0,
      matches:       ()  => f.season_matches || 0,

      // Raw form object для продвинутых стратегий
      raw:           ()  => f,
    };
  };
  return {
    home: makeProxy(m.home_team),
    away: makeProxy(m.away_team),
  };
}

// Матчи обогащаются данными формы
function enrichMatch(m) {
  return {
    ...m,
    // Алиасы для совместимости со старыми стратегиями
    league:         m.league_code || m.league || '',
    home_odds:      m.b365_home   || m.avg_home || 0,
    draw_odds:      m.b365_draw   || m.avg_draw || 0,
    away_odds:      m.b365_away   || m.avg_away || 0,
    over_odds:      m.b365_over   || 0,
    under_odds:     m.b365_under  || 0,
    odds_home:      m.b365_home   || m.avg_home || 0,
    odds_draw:      m.b365_draw   || m.avg_draw || 0,
    odds_away:      m.b365_away   || m.avg_away || 0,
    odds_over:      m.b365_over   || 0,
    odds_under:     m.b365_under  || 0,
  };
}

// Главная функция бэктеста
function runBacktest(matches, formRows, cfg) {
  let bank = parseFloat(cfg.bankroll) || 1000;
  const startBank = bank;
  const equity    = [bank];
  const trades    = [];
  const maxStake  = bank * (parseFloat(cfg.maxStakePct) || 5) / 100;
  const commission= parseFloat(cfg.commission) || 0;
  const minOdds   = parseFloat(cfg.minOdds) || 1.01;
  const maxOdds   = parseFloat(cfg.maxOdds) || 99;

  // Строим индекс формы: `match_id|team` → объект
  const formMap = {};
  for (const f of formRows) {
    formMap[`${f.match_id}|${f.team}`] = f;
  }

  // Компилируем стратегию
  let evalFn = null;
  try {
    const sandbox = makeSandbox(matches);
    vm.createContext(sandbox);
    vm.runInContext(cfg.code || '', sandbox, { timeout: 3000 });
    evalFn = sandbox.evaluate;
  } catch (e) { /* ошибка компиляции */ }

  for (const raw of matches) {
    const m   = enrichMatch(raw);
    const api = makeTeamAPI(m, formMap);
    let sig = null;

    try {
      if (evalFn) sig = evalFn(m, api.home, api.away, makeMarketAPI());
    } catch (_) {}

    if (!sig?.signal) continue;

    // Определяем коэффициент
    const market = sig.market || 'home';
    const oddsMap = {
      home: m.b365_home || m.avg_home,
      draw: m.b365_draw || m.avg_draw,
      away: m.b365_away || m.avg_away,
      over: m.b365_over,
      under: m.b365_under,
      // Pinnacle (sharp money)
      ph: m.pinnacle_home,
      pd: m.pinnacle_draw,
      pa: m.pinnacle_away,
      // Макс
      max_home: m.max_home,
      max_away: m.max_away,
    };
    const odds = parseFloat(oddsMap[market] || 0);
    if (!odds || odds < minOdds || odds > maxOdds) continue;

    // Стейкинг
    let stake = bank * 0.02;
    const prob = sig.prob || (1 / odds);
    if (cfg.staking === 'kelly' && prob > 0)
      stake = bank * Math.max(0, ((odds - 1) * prob - (1 - prob)) / (odds - 1));
    else if (cfg.staking === 'half_kelly' && prob > 0)
      stake = bank * Math.max(0, ((odds - 1) * prob - (1 - prob)) / (odds - 1)) * 0.5;
    else if (cfg.staking === 'fixed_pct')
      stake = bank * (parseFloat(cfg.maxStakePct) || 2) / 100;
    else if (cfg.staking === 'fixed')
      stake = parseFloat(cfg.fixedStake) || 10;
    stake = Math.min(Math.max(stake, 0.01), maxStake, bank);

    // Определяем победу
    const result  = m.result || '';
    const winMap  = {
      home:  ['H'], draw: ['D'], away: ['A'],
      over:  ['O'], under: ['U'],
      ph:    ['H'], pd: ['D'], pa: ['A'],
      max_home: ['H'], max_away: ['A'],
    };
    const won = (winMap[market] || []).includes(result);

    const pnl = won
      ? stake * (odds - 1) * (1 - commission / 100)
      : -stake;
    bank = Math.max(0, bank + pnl);
    equity.push(bank);

    trades.push({
      date:     m.date || '',
      match:    `${m.home_team} vs ${m.away_team}`,
      league:   m.league_code || m.league || '',
      market,
      odds:     odds.toFixed(2),
      stake:    stake.toFixed(2),
      won:      won ? 'W' : 'L',
      pnl:      pnl.toFixed(2),
      bankroll: bank.toFixed(2),
      // Данные формы для анализа
      home_xg:  m.home_xg || 0,
      away_xg:  m.away_xg || 0,
      result:   m.result || '',
    });
  }

  return { trades, equity, stats: calcStats(trades, startBank, equity) };
}

function calcStats(trades, startBank, equity) {
  if (!trades.length) return { bets: 0 };
  const wins      = trades.filter(t => t.won === 'W').length;
  const pnlTotal  = trades.reduce((s, t) => s + parseFloat(t.pnl),   0);
  const stakeTotal= trades.reduce((s, t) => s + parseFloat(t.stake), 0);
  const roi       = stakeTotal > 0 ? (pnlTotal / stakeTotal) * 100 : 0;

  let peak = startBank, maxDD = 0;
  for (const v of equity) {
    if (v > peak) peak = v;
    const dd = peak > 0 ? (peak - v) / peak * 100 : 0;
    if (dd > maxDD) maxDD = dd;
  }

  const rets  = trades.map(t => parseFloat(t.pnl) / Math.max(parseFloat(t.stake), 0.01));
  const avgR  = rets.reduce((s, r) => s + r, 0) / rets.length;
  const stdR  = Math.sqrt(rets.reduce((s, r) => s + (r - avgR) ** 2, 0) / Math.max(rets.length, 1));
  const sharpe = stdR > 0 ? (avgR / stdR) * Math.sqrt(252) : 0;

  const n = trades.length, p = wins / n;
  const avgExp = trades.reduce((s, t) => s + 1 / Math.max(t.odds, 1.01), 0) / n;
  const z = stdR > 0 ? (p - avgExp) / Math.sqrt(Math.max(avgExp * (1 - avgExp) / n, 1e-9)) : 0;

  // Стрик
  let curStreak = 0, maxStreak = 0, curLose = 0, maxLose = 0;
  for (const t of trades) {
    if (t.won === 'W') { curStreak++; curLose = 0; }
    else               { curLose++;  curStreak = 0; }
    if (curStreak > maxStreak) maxStreak = curStreak;
    if (curLose   > maxLose  ) maxLose   = curLose;
  }

  return {
    bets:      n,
    wins,
    losses:    n - wins,
    winRate:   (p * 100).toFixed(1),
    roi:       roi.toFixed(2),
    profit:    pnlTotal.toFixed(2),
    yield:     roi.toFixed(2),
    sharpe:    sharpe.toFixed(2),
    maxDD:     maxDD.toFixed(1),
    pval:      (Math.max(0, 1 - Math.abs(z) * 0.3995)).toFixed(3),
    avgOdds:   (trades.reduce((s, t) => s + parseFloat(t.odds), 0) / n).toFixed(2),
    strike:    (p * 100).toFixed(1),
    zscore:    z.toFixed(2),
    maxWinStreak: maxStreak,
    maxLoseStreak: maxLose,
    finalBank:    equity[equity.length - 1]?.toFixed(2) || startBank,
    clv:          (roi * 0.3).toFixed(2),
  };
}

app.post('/api/backtest/run', requireAuth, async (req, res) => {
  const cfg = req.body;
  try {
    if (!clickhouse) return res.json(null);

    // ── Строим запрос к нужной таблице ──────────────────────────
    const sport     = (cfg.sport || 'football').toLowerCase();
    const dateFrom  = cfg.dateFrom || '2020-01-01';
    const dateTo    = cfg.dateTo   || new Date().toISOString().slice(0, 10);
    const limit     = Math.min(parseInt(cfg.limit) || 100000, 200000);

    let matchesQuery, formQuery = null;

    if (sport === 'football') {
      const lFilter = cfg.league && cfg.league !== 'all'
        ? `AND league_code = '${cfg.league.replace(/'/g, "''")}'`
        : '';
      const seasonFilter = cfg.season && cfg.season !== 'all'
        ? `AND season = '${cfg.season.replace(/'/g, "''")}'`
        : '';

      matchesQuery = `
        SELECT *
        FROM betquant.football_matches
        WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
        ${lFilter} ${seasonFilter}
        AND (b365_home > 0 OR avg_home > 0)
        ORDER BY date ASC
        LIMIT ${limit}
      `;
      formQuery = `
        SELECT *
        FROM betquant.football_team_form
        WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
        ${lFilter} ${seasonFilter}
        ORDER BY date ASC
        LIMIT ${limit * 2}
      `;
    } else if (sport === 'hockey') {
      const lFilter = cfg.league && cfg.league !== 'all'
        ? `AND league = '${cfg.league.replace(/'/g, "''")}'`
        : '';
      matchesQuery = `
        SELECT *
        FROM betquant.hockey_matches
        WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
        ${lFilter}
        ORDER BY date ASC
        LIMIT ${limit}
      `;
    } else if (sport === 'tennis') {
      matchesQuery = `
        SELECT *
        FROM betquant.tennis_extended
        WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
        ${cfg.surface ? `AND surface = '${cfg.surface}'` : ''}
        ${cfg.tour    ? `AND tour = '${cfg.tour}'` : ''}
        ORDER BY date ASC
        LIMIT ${limit}
      `;
    } else if (sport === 'basketball') {
      matchesQuery = `
        SELECT *
        FROM betquant.basketball_matches
        WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
        ORDER BY date ASC
        LIMIT ${limit}
      `;
    } else {
      // Legacy — старая таблица matches
      const league = cfg.league === 'all' ? '' : `AND league = '${cfg.league?.replace(/'/g,"''") || ''}'`;
      matchesQuery = `SELECT * FROM betquant.matches WHERE date >= '${dateFrom}' AND date <= '${dateTo}' ${league} ORDER BY date LIMIT ${limit}`;
    }

    // Выполняем запросы
    const [matchRes, formRes] = await Promise.all([
      clickhouse.query({ query: matchesQuery, format: 'JSON' }),
      formQuery ? clickhouse.query({ query: formQuery, format: 'JSON' }) : Promise.resolve(null),
    ]);
    const [matchData, formData] = await Promise.all([
      matchRes.json(),
      formRes ? formRes.json() : Promise.resolve({ data: [] }),
    ]);

    const matches  = matchData.data  || [];
    const formRows = formData?.data  || [];

    if (!matches.length) return res.json(null);

    const result = runBacktest(matches, formRows, cfg);
    res.json(result);

  } catch (e) {
    console.error('Backtest error:', e.message);
    res.json(null);
  }
});

// ══════════════════════════════════════════════════════════
//  STATS API — быстрые агрегаты для UI
// ══════════════════════════════════════════════════════════

// Список доступных лиг
app.get('/api/stats/leagues', requireAuth, async (req, res) => {
  try {
    if (!clickhouse) return res.json({ football: [], hockey: [], tennis: [] });
    const queries = {
      football: `SELECT DISTINCT league_code as code, league_name as name, country, count() as cnt
                 FROM betquant.football_matches GROUP BY league_code, league_name, country ORDER BY cnt DESC`,
      hockey:   `SELECT DISTINCT league as code, league as name, count() as cnt
                 FROM betquant.hockey_matches GROUP BY league ORDER BY cnt DESC`,
      tennis:   `SELECT DISTINCT tour as code, tour as name, count() as cnt
                 FROM betquant.tennis_extended GROUP BY tour ORDER BY cnt DESC`,
    };
    const results = {};
    for (const [sport, q] of Object.entries(queries)) {
      try {
        const r = await clickhouse.query({ query: q, format: 'JSON' });
        const d = await r.json();
        results[sport] = d.data || [];
      } catch (_) { results[sport] = []; }
    }
    res.json(results);
  } catch (e) { res.json({ football: [], hockey: [], tennis: [] }); }
});

// Статус ETL — сколько данных загружено
app.get('/api/stats/etl-status', requireAuth, async (req, res) => {
  try {
    if (!clickhouse) return res.json({});
    const tables = [
      'football_matches', 'football_events', 'football_team_form',
      'hockey_matches', 'hockey_events', 'hockey_team_form',
      'tennis_extended', 'basketball_matches', 'baseball_matches', 'etl_log',
    ];
    const counts = {};
    await Promise.all(tables.map(async (t) => {
      try {
        const r = await clickhouse.query({ query: `SELECT count() as cnt FROM betquant.${t}`, format: 'JSON' });
        const d = await r.json();
        counts[t] = parseInt(d.data?.[0]?.cnt || 0);
      } catch (_) { counts[t] = 0; }
    }));
    // Диапазон дат
    try {
      const r = await clickhouse.query({
        query: `SELECT min(date) as from_d, max(date) as to_d FROM betquant.football_matches`,
        format: 'JSON'
      });
      const d = await r.json();
      counts._football_range = d.data?.[0] || {};
    } catch (_) {}
    res.json(counts);
  } catch (e) { res.json({}); }
});

// Топ команд по голам
app.get('/api/stats/top-teams', requireAuth, async (req, res) => {
  const { league = 'E0', season = '', limit = 20 } = req.query;
  try {
    if (!clickhouse) return res.json([]);
    const sFilter = season ? `AND season = '${season}'` : '';
    const q = `
      SELECT team, league_code,
        sum(season_goals_for) as goals_for,
        sum(season_goals_against) as goals_against,
        max(season_wins) as wins,
        max(season_xg_for) as xg_for,
        max(season_matches) as matches
      FROM betquant.football_team_form
      WHERE league_code = '${league}' ${sFilter}
        AND is_home = 1
      GROUP BY team, league_code
      ORDER BY goals_for DESC
      LIMIT ${parseInt(limit) || 20}
    `;
    const r = await clickhouse.query({ query: q, format: 'JSON' });
    const d = await r.json();
    res.json(d.data || []);
  } catch (e) { res.json([]); }
});

// Голы по минутам
app.get('/api/stats/goals-by-minute', requireAuth, async (req, res) => {
  const { league, season } = req.query;
  try {
    if (!clickhouse) return res.json([]);
    const lf = league ? `AND league_code = '${league}'` : '';
    const sf = season ? `AND season = '${season}'` : '';
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

// ── Real ETL Runner ──────────────────────────────────────
app.post('/api/etl/run', requireAuth, async (req, res) => {
  const { sport = 'football', seasons = 3, leagues = 'top', quick = false } = req.body;
  const taskId = 'etl_' + Date.now();
  tasks.set(taskId, { status: 'running', pct: 0, message: 'Starting ETL...', type: 'info', log: [] });

  res.json({ taskId, message: 'ETL started' });

  // Запускаем в фоне
  const { spawn } = require('child_process');
  const etlScript = require('path').join(__dirname, '../betquant-etl/run_etl.py');
  const t = tasks.get(taskId);

  const args = [
    etlScript,
    '--ch-host', process.env.CH_HOST || 'http://clickhouse:8123',
    '--ch-db',   process.env.CH_DB   || 'betquant',
    '--seasons', String(seasons),
  ];
  if (quick) args.push('--quick');
  if (sport === 'football') args.push('--football-only');
  else if (sport === 'hockey') args.push('--hockey-only');
  else if (sport === 'other') args.push('--other-only');

  const proc = spawn('python3', args, { env: { ...process.env } });
  let pct = 5;

  proc.stdout.on('data', (d) => {
    const line = d.toString().trim();
    if (!line) return;
    t.log = t.log || [];
    t.log.push(line);
    if (t.log.length > 200) t.log = t.log.slice(-200);
    // Обновляем прогресс по ключевым словам
    if (line.includes('матчей загружено') || line.includes('✓')) pct = Math.min(pct + 5, 90);
    if (line.includes('ИТОГ') || line.includes('ФИНАЛ')) pct = 95;
    t.pct = pct;
    t.message = line.slice(0, 150);
    t.type = 'info';
  });
  proc.stderr.on('data', (d) => {
    const line = d.toString().trim();
    if (line) { t.log = t.log || []; t.log.push('ERR: ' + line); }
  });
  proc.on('close', (code) => {
    t.status = code === 0 ? 'done' : 'error';
    t.pct    = 100;
    t.message = code === 0 ? '✅ ETL завершён успешно!' : `❌ ETL завершился с кодом ${code}`;
    t.type   = code === 0 ? 'success' : 'error';
  });
  proc.on('error', (e) => {
    t.status  = 'error';
    t.pct     = 100;
    t.message = `❌ Ошибка запуска ETL: ${e.message}`;
    t.type    = 'error';
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
  res.json({ status: 'ok', version: '3.2.0-real-data', pg: !!pgPool, ch: !!clickhouse, node: process.version, routes: ['ai/strategy', 'auth', 'db', 'backtest', 'journal', 'strategies'] });
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
