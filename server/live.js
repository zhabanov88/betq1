'use strict';
/**
 * BetQuant Pro — Live Monitor  /api/live/*
 * ИСПРАВЛЕНИЯ:
 *  - tgAPI.sendLiveSignal вызывается с корректным форматом (signal, matchName, strategyId)
 *  - fetch с обработкой таймаута
 *  - demoMatches возвращает стабильные данные без рандома при каждом вызове
 */

const express = require('express');
const router  = express.Router();

// ─── Cache ────────────────────────────────────────────────────────────────
const CACHE   = { matches: [], matchExtra: {}, ts: 0, busy: false };
const POLL_MS = 30_000;
const APIFB_KEY = process.env.API_FOOTBALL_KEY || '';

// ─── fetch helper ────────────────────────────────────────────────────────
async function fetchJSON(url, headers = {}) {
  const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const t    = ctrl ? setTimeout(() => ctrl.abort(), 8000) : null;
  try {
    const r = await fetch(url, { headers, signal: ctrl?.signal });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch(e) {
    if (e.name === 'AbortError') throw new Error('API request timeout');
    throw e;
  } finally {
    if (t) clearTimeout(t);
  }
}

// ─── Demo data ────────────────────────────────────────────────────────────
function demoMatches() {
  const now = Date.now();
  const ago = n => new Date(now - n * 60000).toISOString();
  const fwd = n => new Date(now + n * 3600000).toISOString();

  return [
    {
      id: 'lv1', sport: 'football', status: 'live', minute: 67,
      league: 'Premier League', country: 'England',
      home: 'Arsenal',  homeScore: 2, homeLogo: '🔴',
      away: 'Chelsea',  awayScore: 1, awayLogo: '🔵',
      startTime: ago(67),
      odds: { home: 1.45, draw: 4.20, away: 7.50, over25: 1.30, under25: 3.40 },
      openOdds: { home: 1.80, draw: 3.60, away: 4.20 },
      stats: {
        home_shots: 14, away_shots: 8,
        home_sot: 6,    away_sot: 3,
        home_poss: 58,  away_poss: 42,
        home_corners: 7, away_corners: 3,
        home_xg: 2.34,  away_xg: 0.87,
        home_da: 34,    away_da: 18,
      },
      events: [
        { minute: 12, type: 'goal',   team: 'home', player: 'Saka',    score: '1:0' },
        { minute: 38, type: 'yellow', team: 'away', player: 'Enzo',    score: '1:0' },
        { minute: 54, type: 'goal',   team: 'home', player: 'Ødegaard',score: '2:0' },
        { minute: 61, type: 'goal',   team: 'away', player: 'Jackson', score: '2:1' },
      ],
    },
    {
      id: 'lv2', sport: 'football', status: 'live', minute: 34,
      league: 'La Liga', country: 'Spain',
      home: 'Real Madrid', homeScore: 1, homeLogo: '⚪',
      away: 'Barcelona',   awayScore: 1, awayLogo: '🔴',
      startTime: ago(34),
      odds: { home: 2.30, draw: 3.10, away: 3.20, over25: 1.55, under25: 2.30 },
      openOdds: { home: 2.10, draw: 3.40, away: 3.50 },
      stats: {
        home_shots: 7, away_shots: 9,
        home_sot: 3,   away_sot: 4,
        home_poss: 46, away_poss: 54,
        home_corners: 3, away_corners: 5,
        home_xg: 0.98, away_xg: 1.34,
        home_da: 21,   away_da: 28,
      },
      events: [
        { minute: 15, type: 'goal', team: 'home', player: 'Vinicius', score: '1:0' },
        { minute: 28, type: 'goal', team: 'away', player: 'Yamal',    score: '1:1' },
      ],
    },
    {
      id: 'lv3', sport: 'basketball', status: 'live', minute: 48,
      league: 'NBA', country: 'USA',
      home: 'Lakers',   homeScore: 112, homeLogo: '💜',
      away: 'Warriors', awayScore: 108, awayLogo: '💛',
      startTime: ago(48),
      odds: { home: 1.60, away: 2.40 },
      openOdds: { home: 1.90, away: 1.95 },
      stats: { home_poss: 52, away_poss: 48 },
      events: [],
    },
    {
      id: 'sc1', sport: 'football', status: 'scheduled', minute: 0,
      league: 'Bundesliga', country: 'Germany',
      home: 'Bayern Munich', homeScore: null, homeLogo: '🔴',
      away: 'Dortmund',      awayScore: null, awayLogo: '🟡',
      startTime: fwd(2),
      odds: { home: 1.70, draw: 3.80, away: 4.50, over25: 1.55, under25: 2.35 },
      openOdds: { home: 1.70, draw: 3.80, away: 4.50 },
      stats: {}, events: [],
    },
    {
      id: 'sc2', sport: 'football', status: 'scheduled', minute: 0,
      league: 'Serie A', country: 'Italy',
      home: 'Inter Milan', homeScore: null, homeLogo: '⚫',
      away: 'Juventus',    awayScore: null, awayLogo: '⚪',
      startTime: fwd(4),
      odds: { home: 2.10, draw: 3.20, away: 3.40, over25: 1.80, under25: 2.00 },
      openOdds: { home: 2.10, draw: 3.20, away: 3.40 },
      stats: {}, events: [],
    },
  ];
}

// ─── Odds history ─────────────────────────────────────────────────────────
function buildOddsHistory(m) {
  const pts   = [];
  const base  = m.openOdds;
  const start = new Date(m.startTime).getTime();

  // Pre-match: 24h до старта (10 точек)
  for (let h = 24; h >= 0; h -= 3) {
    const n = () => (Math.random() - .5) * .08;
    pts.push({
      t: new Date(start - h * 3600000).toISOString(), phase: 'pre',
      home: Math.max(1.01, +(base.home + n()).toFixed(2)),
      draw: base.draw ? Math.max(1.01, +(base.draw + n()).toFixed(2)) : null,
      away: Math.max(1.01, +(base.away + n()).toFixed(2)),
    });
  }

  if (m.status !== 'live') return pts;

  // Live: каждые 5 минут
  for (let min = 0; min <= m.minute; min += 5) {
    const goalImpact = (m.events || [])
      .filter(e => e.type === 'goal' && e.minute <= min)
      .reduce((acc, e) => acc + (e.team === 'home' ? -.18 : .18), 0);
    const n = () => (Math.random() - .5) * .04;
    pts.push({
      t: new Date(start + min * 60000).toISOString(), phase: 'live', minute: min,
      home: Math.max(1.01, +(base.home + goalImpact + n()).toFixed(2)),
      draw: base.draw ? Math.max(1.01, +(base.draw - Math.abs(goalImpact) * .4 + n()).toFixed(2)) : null,
      away: Math.max(1.01, +(base.away - goalImpact + n()).toFixed(2)),
    });
  }
  return pts;
}

// ─── In-play signal ───────────────────────────────────────────────────────
function inPlaySignal(m) {
  if (m.status !== 'live') return null;
  const s   = m.stats || {};
  const min = m.minute || 1;
  const hG  = m.homeScore ?? 0;
  const aG  = m.awayScore ?? 0;
  const diff = hG - aG;
  const rem  = 90 - min;

  const pH = ((s.home_sot || 0) * 3 + (s.home_da || 0) * .5 + (s.home_xg || 0) * 10) / (min / 10 || 1);
  const pA = ((s.away_sot || 0) * 3 + (s.away_da || 0) * .5 + (s.away_xg || 0) * 10) / (min / 10 || 1);
  const projGoals = ((hG + aG) / min) * 90;

  const signals = [];

  if (diff < 0 && pH > pA * 1.4 && rem > 20)
    signals.push({
      type: 'comeback', market: 'draw',
      confidence: Math.min(82, 55 + pH * 2),
      label: `${m.home} под давлением — возможный камбэк`,
      rationale: `xG хозяев: ${(s.home_xg||0).toFixed(2)} — атаки превышают гостей в ${(pH/Math.max(pA,0.1)).toFixed(1)}x`,
      odds: m.odds?.draw,
    });

  if (diff === 0 && pH > pA * 1.3 && min > 60)
    signals.push({
      type: 'home_late', market: 'home',
      confidence: Math.min(76, 50 + pH * 2),
      label: `Давление ${m.home} — поздний гол`,
      rationale: `Превосходство в атаке в концовке матча.`,
      odds: m.odds?.home,
    });

  if (projGoals > 2.8 && (s.home_xg||0) + (s.away_xg||0) > 1.8 && (m.odds?.over25 || 99) < 2.8)
    signals.push({
      type: 'over', market: 'over_2.5',
      confidence: Math.min(78, 50 + projGoals * 5),
      label: 'Темп матча — Тотал Больше 2.5',
      rationale: `Проекция: ${projGoals.toFixed(1)} гола. xG суммарный: ${((s.home_xg||0)+(s.away_xg||0)).toFixed(2)}`,
      odds: m.odds?.over25,
    });

  if (diff >= 2 && pH > pA)
    signals.push({
      type: 'hold', market: 'home_win',
      confidence: Math.min(90, 68 + diff * 8),
      label: `${m.home} контролирует — результат надёжен`,
      rationale: `Разрыв ${diff} гол(а) + доминирование в атаке.`,
      odds: m.odds?.home,
    });

  signals.sort((a, b) => b.confidence - a.confidence);

  return {
    matchId: m.id, minute: min,
    score: `${hG}:${aG}`,
    signals,
    topSignal: signals[0] || null,
    pressureIndex: { home: +pH.toFixed(1), away: +pA.toFixed(1) },
    projectedGoals: +projGoals.toFixed(1),
    remaining: rem,
    riskLevel: rem < 15 ? 'high' : rem < 30 ? 'medium' : 'low',
    note: signals.length ? null : 'Явных in-play сигналов нет',
  };
}

// ─── API-Football ─────────────────────────────────────────────────────────
async function fetchAPIFootball() {
  if (!APIFB_KEY) return null;
  try {
    const d = await fetchJSON(
      'https://v3.football.api-sports.io/fixtures?live=all',
      { 'x-apisports-key': APIFB_KEY }
    );
    if (!d?.response?.length) return null;
    return d.response.slice(0, 8).map(f => {
      const hStats = f.statistics?.[0]?.statistics || [];
      const aStats = f.statistics?.[1]?.statistics || [];
      const gs     = (arr, type) => +(arr.find(s => s.type === type)?.value || 0);
      return {
        id:        `api_${f.fixture.id}`,
        sport:     'football',
        status:    f.fixture.status.short === 'NS' ? 'scheduled' : 'live',
        minute:    f.fixture.status.elapsed || 0,
        league:    f.league.name, country: f.league.country,
        home:      f.teams.home.name, homeScore: f.goals.home, homeLogo: '⚽',
        away:      f.teams.away.name, awayScore: f.goals.away, awayLogo: '⚽',
        startTime: f.fixture.date,
        odds: {}, openOdds: {}, events: [],
        stats: {
          home_shots: gs(hStats,'Total Shots'),   away_shots: gs(aStats,'Total Shots'),
          home_sot:   gs(hStats,'Shots on Goal'), away_sot:   gs(aStats,'Shots on Goal'),
          home_poss:  +(gs(hStats,'Ball Possession').toString().replace('%','')),
          away_poss:  +(gs(aStats,'Ball Possession').toString().replace('%','')),
          home_corners: gs(hStats,'Corner Kicks'), away_corners: gs(aStats,'Corner Kicks'),
          home_da:    gs(hStats,'Dangerous Attacks'), away_da: gs(aStats,'Dangerous Attacks'),
          home_xg:    gs(hStats,'Expected_Goals') || gs(hStats,'Shots on Goal') * .35,
          away_xg:    gs(aStats,'Expected_Goals') || gs(aStats,'Shots on Goal') * .35,
        },
      };
    });
  } catch(e) {
    console.warn('[live] api-football error:', e.message);
    return null;
  }
}

// ─── Cache refresh ────────────────────────────────────────────────────────
async function refresh() {
  if (CACHE.busy) return;
  CACHE.busy = true;
  try {
    const api = await fetchAPIFootball();
    CACHE.matches = api || demoMatches();
    for (const m of CACHE.matches) {
      const sig = inPlaySignal(m);
      CACHE.matchExtra[m.id] = {
        oddsHistory: buildOddsHistory(m),
        signal:      sig,
      };

      // ── Telegram алерты для live сигналов ──────────────────────────────
      const tg = global.__betquant_tg;
      if (tg && tg.isEnabled() && sig?.topSignal?.confidence >= 70) {
        const matchName = `${m.home} vs ${m.away}`;
        tg.sendLiveSignal(sig, matchName).catch(() => {});
      }
    }
    CACHE.ts = Date.now();
  } catch(e) {
    console.warn('[live] refresh error:', e.message);
  } finally {
    CACHE.busy = false;
  }
}

// Первоначальная загрузка + polling
refresh();
setInterval(refresh, POLL_MS);

// ─── Routes ───────────────────────────────────────────────────────────────

router.get('/matches', async (req, res) => {
  if (Date.now() - CACHE.ts > POLL_MS) await refresh();
  let list = CACHE.matches;
  if (req.query.sport)  list = list.filter(m => m.sport  === req.query.sport);
  if (req.query.status) list = list.filter(m => m.status === req.query.status);
  res.json({
    matches:     list,
    liveCount:   list.filter(m => m.status === 'live').length,
    total:       list.length,
    lastUpdate:  CACHE.ts,
    source:      APIFB_KEY ? 'api-football' : 'demo',
  });
});

router.get('/match/:id', async (req, res) => {
  if (Date.now() - CACHE.ts > POLL_MS) await refresh();
  const m = CACHE.matches.find(x => x.id === req.params.id);
  if (!m) return res.status(404).json({ error: 'Match not found' });
  const ex = CACHE.matchExtra[m.id] || {};
  res.json({ ...m, oddsHistory: ex.oddsHistory || [], signal: ex.signal || null });
});

router.get('/odds/:id', async (req, res) => {
  if (Date.now() - CACHE.ts > POLL_MS) await refresh();
  const m = CACHE.matches.find(x => x.id === req.params.id);
  if (!m) return res.status(404).json({ error: 'Match not found' });
  const ex = CACHE.matchExtra[m.id] || {};
  res.json({ matchId: m.id, history: ex.oddsHistory || buildOddsHistory(m), current: m.odds });
});

router.post('/refresh', async (req, res) => {
  CACHE.ts = 0;
  await refresh();
  res.json({ ok: true, matches: CACHE.matches.length });
});

module.exports = router;