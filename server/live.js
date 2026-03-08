'use strict';
/**
 * BetQuant Pro — Live Monitor  /api/live/*
 *
 * Цепочка источников данных (без блокировок по VPN):
 *  1. football-data.org  (FOOTBALL_DATA_KEY) — бесплатно, без VPN ограничений
 *     Регистрация: https://www.football-data.org/client/register
 *     Даёт: матчи сегодня + результаты, 10 req/min бесплатно
 *
 *  2. TheSportsDB        (THESPORTSDB_KEY или без ключа) — полностью бесплатный
 *     Регистрация не нужна (используем публичный ключ "3")
 *     Даёт: матчи текущего дня по всем лигам
 *
 *  3. apifootball.com    (APIFOOTBALL_KEY) — бесплатный тариф
 *     Регистрация: https://apifootball.com (email, нет VPN блокировки)
 *     Даёт: live scores, события, статистику
 *
 *  4. api-football.com   (API_FOOTBALL_KEY) — оригинальный, но блокирует VPN
 *
 *  Demo-данные — только при ?demo=true или bq_demo_mode=true на клиенте.
 */

const express = require('express');
const router  = express.Router();

const CACHE   = { matches: [], matchExtra: {}, ts: 0, busy: false, source: 'none' };
const POLL_MS = 60_000; // 1 мин (football-data.org: 10 req/min)

// ── Ключи (берём из env) ──────────────────────────────────────────────────
const FDORG_KEY     = process.env.FOOTBALL_DATA_KEY    || '';  // football-data.org
const TSDB_KEY      = process.env.THESPORTSDB_KEY      || '3'; // '3' = публичный бесплатный ключ
const APIFB2_KEY    = process.env.APIFOOTBALL_KEY      || '';  // apifootball.com
const APIFB1_KEY    = process.env.API_FOOTBALL_KEY     || '';  // api-football.com (старый)

// ── football-data.org competition IDs ────────────────────────────────────
// PL=2021 BL1=2002 SA=2019 PD=2014 FL1=2015 CL=2001
const FDORG_COMPETITIONS = [2021, 2002, 2019, 2014, 2015, 2001];

// ─── fetch helper ────────────────────────────────────────────────────────
async function fetchJSON(url, headers = {}) {
  const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const t    = ctrl ? setTimeout(() => ctrl.abort(), 10000) : null;
  try {
    const r = await fetch(url, { headers, signal: ctrl?.signal });
    if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
    return await r.json();
  } catch(e) {
    if (e.name === 'AbortError') throw new Error('Timeout: ' + url);
    throw e;
  } finally {
    if (t) clearTimeout(t);
  }
}

// ════════════════════════════════════════════════════════════════════════
//  ИСТОЧНИК 1: football-data.org  (рекомендуется — нет VPN блокировки)
// ════════════════════════════════════════════════════════════════════════
async function fetchFootballDataOrg() {
  if (!FDORG_KEY) return null;
  try {
    const today = new Date().toISOString().slice(0, 10);
    // Получаем матчи сегодняшнего дня по всем компетициям
    const d = await fetchJSON(
      `https://api.football-data.org/v4/matches?dateFrom=${today}&dateTo=${today}`,
      { 'X-Auth-Token': FDORG_KEY }
    );
    if (!d?.matches?.length) return null;

    const leagueNames = {
      2021: 'Premier League', 2002: 'Bundesliga', 2019: 'Serie A',
      2014: 'La Liga', 2015: 'Ligue 1', 2001: 'Champions League',
    };

    return d.matches.slice(0, 15).map(m => {
      const isLive = ['IN_PLAY','PAUSED'].includes(m.status);
      const isDone = m.status === 'FINISHED';
      return {
        id:         `fdorg_${m.id}`,
        sport:      'football',
        status:     isLive ? 'live' : isDone ? 'finished' : 'scheduled',
        minute:     m.minute || 0,
        league:     leagueNames[m.competition?.id] || m.competition?.name || 'Football',
        country:    m.area?.name || '',
        home:       m.homeTeam?.shortName || m.homeTeam?.name || 'Хозяева',
        homeScore:  m.score?.fullTime?.home ?? m.score?.halfTime?.home ?? 0,
        homeLogo:   '⚽',
        away:       m.awayTeam?.shortName || m.awayTeam?.name || 'Гости',
        awayScore:  m.score?.fullTime?.away ?? m.score?.halfTime?.away ?? 0,
        awayLogo:   '⚽',
        startTime:  m.utcDate,
        odds:       {},
        openOdds:   {},
        stats:      {},
        events:     [],
        _raw:       m,
      };
    });
  } catch(e) {
    console.warn('[live] football-data.org error:', e.message);
    return null;
  }
}

// ════════════════════════════════════════════════════════════════════════
//  ИСТОЧНИК 2: TheSportsDB  (полностью бесплатный, без ключа)
// ════════════════════════════════════════════════════════════════════════
async function fetchTheSportsDB() {
  try {
    const today = new Date().toISOString().slice(0, 10);
    const d = await fetchJSON(
      `https://www.thesportsdb.com/api/v1/json/${TSDB_KEY}/eventsday.php?d=${today}&s=Soccer`
    );
    if (!d?.events?.length) return null;

    return d.events.slice(0, 15).map(ev => {
      const hasScore = ev.intHomeScore !== null && ev.intHomeScore !== '';
      const isLive   = ev.strStatus === 'Match Finished' ? false
                     : (ev.strStatus && ev.strStatus !== '' && ev.strStatus !== 'NS') ? true : false;
      return {
        id:        `tsdb_${ev.idEvent}`,
        sport:     'football',
        status:    ev.strStatus === 'Match Finished' ? 'finished' : hasScore ? 'live' : 'scheduled',
        minute:    0,
        league:    ev.strLeague || 'Football',
        country:   ev.strCountry || '',
        home:      ev.strHomeTeam || 'Хозяева',
        homeScore: +(ev.intHomeScore || 0),
        homeLogo:  '⚽',
        away:      ev.strAwayTeam || 'Гости',
        awayScore: +(ev.intAwayScore || 0),
        awayLogo:  '⚽',
        startTime: ev.strTimestamp || `${ev.dateEvent}T${ev.strTime || '19:00:00'}Z`,
        odds:      {},
        openOdds:  {},
        stats:     {},
        events:    [],
      };
    });
  } catch(e) {
    console.warn('[live] TheSportsDB error:', e.message);
    return null;
  }
}

// ════════════════════════════════════════════════════════════════════════
//  ИСТОЧНИК 3: apifootball.com  (бесплатный тариф, регистрация по email)
// ════════════════════════════════════════════════════════════════════════
async function fetchApiFootball2() {
  if (!APIFB2_KEY) return null;
  try {
    const today = new Date().toISOString().slice(0, 10);
    const d = await fetchJSON(
      `https://apiv3.apifootball.com/?action=get_events&from=${today}&to=${today}&APIkey=${APIFB2_KEY}`
    );
    if (!Array.isArray(d) || !d.length) return null;
    return d.slice(0, 15).map(m => {
      const isLive = m.match_status === '1st Half' || m.match_status === '2nd Half' ||
                     m.match_status === 'Half Time' || (+m.match_status > 0 && +m.match_status <= 90);
      return {
        id:        `apifb2_${m.match_id}`,
        sport:     'football',
        status:    isLive ? 'live' : m.match_status === 'Finished' ? 'finished' : 'scheduled',
        minute:    +m.match_status || 0,
        league:    m.league_name || 'Football',
        country:   m.country_name || '',
        home:      m.match_hometeam_name || 'Хозяева',
        homeScore: +(m.match_hometeam_score || 0),
        homeLogo:  m.team_home_badge || '⚽',
        away:      m.match_awayteam_name || 'Гости',
        awayScore: +(m.match_awayteam_score || 0),
        awayLogo:  m.team_away_badge || '⚽',
        startTime: `${m.match_date}T${m.match_time || '19:00:00'}`,
        odds:      { home: +m.odd_1||0, draw: +m.odd_x||0, away: +m.odd_2||0 },
        openOdds:  {},
        stats:     {
          home_shots:   +(m.statistics?.[0]?.home || 0),
          away_shots:   +(m.statistics?.[0]?.away || 0),
          home_poss:    +(m.statistics?.[2]?.home?.replace('%','') || 50),
          away_poss:    +(m.statistics?.[2]?.away?.replace('%','') || 50),
          home_corners: +(m.statistics?.[6]?.home || 0),
          away_corners: +(m.statistics?.[6]?.away || 0),
        },
        events: (m.goalscorer || []).map(g => ({
          minute: +g.time, type: 'goal',
          team:   g.home_scorer ? 'home' : 'away',
          player: g.home_scorer || g.away_scorer || '',
          score:  g.score,
        })),
      };
    });
  } catch(e) {
    console.warn('[live] apifootball.com error:', e.message);
    return null;
  }
}

// ════════════════════════════════════════════════════════════════════════
//  ИСТОЧНИК 4: api-football.com  (оригинальный, блокирует VPN)
// ════════════════════════════════════════════════════════════════════════
async function fetchApiFootball1() {
  if (!APIFB1_KEY) return null;
  try {
    const d = await fetchJSON(
      'https://v3.football.api-sports.io/fixtures?live=all',
      { 'x-apisports-key': APIFB1_KEY }
    );
    if (!d?.response?.length) return null;
    return d.response.slice(0, 10).map(f => ({
      id:        `apifb1_${f.fixture.id}`,
      sport:     'football',
      status:    f.fixture.status.short === 'NS' ? 'scheduled' : 'live',
      minute:    f.fixture.status.elapsed || 0,
      league:    f.league.name,
      country:   f.league.country,
      home:      f.teams.home.name,
      homeScore: f.goals.home,
      homeLogo:  '⚽',
      away:      f.teams.away.name,
      awayScore: f.goals.away,
      awayLogo:  '⚽',
      startTime: f.fixture.date,
      odds:      {}, openOdds: {}, stats: {}, events: [],
    }));
  } catch(e) {
    console.warn('[live] api-football.com error:', e.message);
    return null;
  }
}

// ════════════════════════════════════════════════════════════════════════
//  Demo data
// ════════════════════════════════════════════════════════════════════════
function demoMatches() {
  const now = Date.now();
  const ago = n => new Date(now - n * 60000).toISOString();
  const fwd = n => new Date(now + n * 3600000).toISOString();
  return [
    {
      id:'lv1', sport:'football', status:'live', minute:67,
      league:'Premier League', country:'England',
      home:'Арсенал', homeScore:2, homeLogo:'🔴',
      away:'Челси', awayScore:1, awayLogo:'🔵',
      startTime:ago(67),
      odds:{ home:1.45, draw:4.20, away:7.50, over25:1.30 },
      openOdds:{ home:1.80, draw:3.60, away:4.20 },
      stats:{ home_shots:14, away_shots:8, home_sot:6, away_sot:3,
              home_poss:58, away_poss:42, home_corners:7, away_corners:3,
              home_xg:2.34, away_xg:0.87, home_da:34, away_da:18 },
      events:[
        { minute:12, type:'goal', team:'home', player:'Сака',   score:'1:0' },
        { minute:38, type:'yellow', team:'away', player:'Мудрик' },
        { minute:54, type:'goal', team:'home', player:'Одегор', score:'2:0' },
        { minute:61, type:'goal', team:'away', player:'Джексон',score:'2:1' },
      ],
    },
    {
      id:'lv2', sport:'football', status:'live', minute:34,
      league:'Bundesliga', country:'Germany',
      home:'Бавария', homeScore:1, homeLogo:'🔴',
      away:'Дортмунд', awayScore:1, awayLogo:'🟡',
      startTime:ago(34),
      odds:{ home:1.90, draw:3.60, away:4.20 },
      openOdds:{ home:1.75, draw:3.80, away:4.80 },
      stats:{ home_shots:9, away_shots:7, home_sot:4, away_sot:3,
              home_poss:55, away_poss:45, home_corners:4, away_corners:2,
              home_xg:1.12, away_xg:0.98, home_da:22, away_da:18 },
      events:[
        { minute:18, type:'goal', team:'home', player:'Мюллер', score:'1:0' },
        { minute:29, type:'goal', team:'away', player:'Санчо',  score:'1:1' },
      ],
    },
    {
      id:'sc1', sport:'football', status:'scheduled', minute:0,
      league:'La Liga', country:'Spain',
      home:'Реал Мадрид', homeScore:0, homeLogo:'⚪',
      away:'Барселона',   awayScore:0, awayLogo:'🔵',
      startTime:fwd(2), odds:{ home:2.10, draw:3.40, away:3.20 }, openOdds:{}, stats:{}, events:[],
    },
  ];
}

// ════════════════════════════════════════════════════════════════════════
//  Odds history builder
// ════════════════════════════════════════════════════════════════════════
function buildOddsHistory(m) {
  if (!m.odds?.home && !m.openOdds?.home) return [];
  const pts = 10, now = Date.now();
  return Array.from({ length: pts }, (_, i) => {
    const pct = i / (pts - 1);
    const lerp = (a, b) => b ? +(a + (b - a) * pct).toFixed(2) : null;
    const baseH = m.openOdds?.home || (m.odds?.home ? m.odds.home * 1.2 : null);
    const baseD = m.openOdds?.draw || (m.odds?.draw ? m.odds.draw * 1.05 : null);
    const baseA = m.openOdds?.away || (m.odds?.away ? m.odds.away * 0.85 : null);
    return {
      t: new Date(now - (pts - 1 - i) * 600000).toISOString(),
      minute: m.status === 'live' ? Math.round(m.minute * pct) : null,
      phase: m.status === 'live' && i >= pts * 0.6 ? 'live' : 'pre',
      home: lerp(baseH, m.odds?.home),
      draw: lerp(baseD, m.odds?.draw),
      away: lerp(baseA, m.odds?.away),
    };
  }).filter(h => h.home || h.draw);
}

// ════════════════════════════════════════════════════════════════════════
//  In-play signal
// ════════════════════════════════════════════════════════════════════════
function inPlaySignal(m) {
  if (m.status !== 'live') return null;
  const s = m.stats || {}, min = m.minute || 0;
  const hG = m.homeScore || 0, aG = m.awayScore || 0;
  const diff = hG - aG, rem = Math.max(0, 90 - min);
  const pH = (s.home_shots||0)*0.35 + (s.home_da||0)*0.15 + (s.home_poss||50)*0.01;
  const pA = (s.away_shots||0)*0.35 + (s.away_da||0)*0.15 + (s.away_poss||50)*0.01;
  const projGoals = ((s.home_xg||0) + (s.away_xg||0)) * (rem / 90) * 1.1;
  const signals = [];
  if (projGoals > 0.6 && rem > 15)
    signals.push({ type:'value', market:'over25', confidence:Math.min(85,55+projGoals*12),
      label:`Ждём голов: ${projGoals.toFixed(1)} прогноз`, rationale:`xG: ${((s.home_xg||0)+(s.away_xg||0)).toFixed(2)}`, odds:m.odds?.over25 });
  if (diff >= 2 && pH > pA)
    signals.push({ type:'hold', market:'home_win', confidence:Math.min(90,68+diff*8),
      label:`${m.home} контролирует — результат надёжен`, rationale:`Разрыв ${diff} гол(а).`, odds:m.odds?.home });
  signals.sort((a,b)=>b.confidence-a.confidence);
  return { matchId:m.id, minute:min, score:`${hG}:${aG}`, signals,
    topSignal:signals[0]||null, pressureIndex:{home:+pH.toFixed(1),away:+pA.toFixed(1)},
    projectedGoals:+projGoals.toFixed(1), remaining:rem,
    riskLevel:rem<15?'high':rem<30?'medium':'low' };
}

// ════════════════════════════════════════════════════════════════════════
//  Cache refresh — пробуем источники по цепочке
// ════════════════════════════════════════════════════════════════════════
async function refresh(useDemo = false) {
  if (CACHE.busy) return;
  CACHE.busy = true;
  try {
    let matches = null, source = 'none';

    // Цепочка: football-data.org → apifootball.com → TheSportsDB → api-football.com
    if (!matches && FDORG_KEY) {
      matches = await fetchFootballDataOrg();
      if (matches) source = 'football-data.org';
    }
    if (!matches && APIFB2_KEY) {
      matches = await fetchApiFootball2();
      if (matches) source = 'apifootball.com';
    }
    if (!matches) {
      // TheSportsDB — публичный, всегда доступен
      matches = await fetchTheSportsDB();
      if (matches?.length) source = 'thesportsdb';
    }
    if (!matches && APIFB1_KEY) {
      matches = await fetchApiFootball1();
      if (matches) source = 'api-football.com';
    }
    if (!matches && useDemo) {
      matches = demoMatches();
      source  = 'demo';
    }

    CACHE.matches = matches || [];
    CACHE.source  = source;
    for (const m of CACHE.matches) {
      CACHE.matchExtra[m.id] = { oddsHistory: buildOddsHistory(m), signal: inPlaySignal(m) };
      const tg = global.__betquant_tg;
      if (tg?.isEnabled() && CACHE.matchExtra[m.id].signal?.topSignal?.confidence >= 70) {
        tg.sendLiveSignal(CACHE.matchExtra[m.id].signal, `${m.home} vs ${m.away}`).catch(()=>{});
      }
    }
    CACHE.ts = Date.now();
    console.log(`[live] refreshed: ${CACHE.matches.length} matches from ${source}`);
  } catch(e) {
    console.warn('[live] refresh error:', e.message);
  } finally {
    CACHE.busy = false;
  }
}

refresh(false);
setInterval(() => refresh(false), POLL_MS);

// ════════════════════════════════════════════════════════════════════════
//  Routes
// ════════════════════════════════════════════════════════════════════════
router.get('/matches', async (req, res) => {
  const wantDemo = req.query.demo === 'true';
  if (Date.now() - CACHE.ts > POLL_MS) await refresh(wantDemo);
  let list = (wantDemo && !CACHE.matches.length) ? demoMatches() : CACHE.matches;
  if (req.query.sport)  list = list.filter(m => m.sport  === req.query.sport);
  if (req.query.status) list = list.filter(m => m.status === req.query.status);

  // Подсказка о регистрации
  let hint = null;
  if (!CACHE.matches.length && !wantDemo) {
    hint = !FDORG_KEY
      ? 'Добавьте FOOTBALL_DATA_KEY в .env. Бесплатная регистрация: football-data.org/client/register'
      : 'Сегодня нет матчей в расписании или API не вернул данные.';
  }

  res.json({
    matches:    list,
    liveCount:  list.filter(m => m.status === 'live').length,
    total:      list.length,
    lastUpdate: CACHE.ts,
    source:     CACHE.source || (wantDemo ? 'demo' : 'none'),
    hint,
  });
});

router.get('/match/:id', async (req, res) => {
  if (Date.now() - CACHE.ts > POLL_MS) await refresh(false);
  const m = CACHE.matches.find(x => x.id === req.params.id);
  if (!m) return res.status(404).json({ error: 'Match not found' });
  const ex = CACHE.matchExtra[m.id] || {};
  res.json({ ...m, oddsHistory: ex.oddsHistory || [], signal: ex.signal || null });
});

router.get('/odds/:id', async (req, res) => {
  if (Date.now() - CACHE.ts > POLL_MS) await refresh(false);
  const m = CACHE.matches.find(x => x.id === req.params.id);
  if (!m) return res.status(404).json({ error: 'Match not found' });
  const ex = CACHE.matchExtra[m.id] || {};
  res.json({ matchId: m.id, history: ex.oddsHistory || buildOddsHistory(m), current: m.odds });
});

router.post('/refresh', async (req, res) => {
  CACHE.ts = 0;
  await refresh(req.body?.demo === true);
  res.json({ ok: true, matches: CACHE.matches.length, source: CACHE.source });
});

// Статус источников данных
router.get('/sources', (req, res) => {
  res.json({
    configured: {
      'football-data.org':  !!FDORG_KEY,
      'apifootball.com':    !!APIFB2_KEY,
      'thesportsdb':        true, // всегда доступен
      'api-football.com':   !!APIFB1_KEY,
    },
    active:    CACHE.source,
    lastUpdate: CACHE.ts,
    hint: !FDORG_KEY && !APIFB2_KEY && !APIFB1_KEY
      ? 'Рекомендуем добавить FOOTBALL_DATA_KEY — бесплатная регистрация на football-data.org без VPN ограничений'
      : null,
  });
});

module.exports = router;