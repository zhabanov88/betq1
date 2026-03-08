'use strict';
/**
 * BetQuant Pro — Stats API  /api/stats/*
 * Новые эндпоинты для модуля "Графики и коэффициенты" + "Статистика".
 * Данные — из ClickHouse. При отсутствии ClickHouse возвращает пустые массивы.
 */
const express = require('express');
const router  = express.Router();

function getCH(req) { return req.app.locals.clickhouse || null; }

/**
 * GET /api/stats/teams
 * ?league=epl&season=2024&limit=20
 * Возвращает таблицу команд с голами, xG, очками
 */
router.get('/teams', async (req, res) => {
  const ch     = getCH(req);
  const league = req.query.league || 'epl';
  const season = req.query.season || '';
  const limit  = Math.min(50, parseInt(req.query.limit || 20));

  if (!ch) return res.json({ teams: [], source: 'none', hint: 'ClickHouse не подключён' });

  try {
    const seasonFilter = season ? `AND season='${season}'` : '';
    const r = await ch.query({
      query: `
        SELECT
          home_team AS team,
          count() AS matches,
          countIf(home_goals > away_goals) AS wins,
          countIf(home_goals = away_goals) AS draws,
          countIf(home_goals < away_goals) AS losses,
          sum(home_goals) AS goals_for,
          sum(away_goals) AS goals_against,
          sum(home_goals) - sum(away_goals) AS gd,
          countIf(home_goals > away_goals)*3 + countIf(home_goals = away_goals) AS points,
          round(avg(ifNull(home_xg, home_goals * 0.85)), 2) AS xg,
          round(avg(ifNull(away_xg, away_goals * 0.85)), 2) AS xga,
          countIf(home_goals > away_goals) AS home_wins
        FROM betquant.football_matches
        WHERE league_code = '${league}' ${seasonFilter}
        GROUP BY home_team
        ORDER BY points DESC
        LIMIT ${limit}
      `,
      format: 'JSON',
    });
    const d = await r.json();
    res.json({ teams: d.data || [], source: 'clickhouse', league, season });
  } catch(e) {
    console.warn('[stats/teams]', e.message);
    res.json({ teams: [], source: 'error', error: e.message });
  }
});

/**
 * GET /api/stats/goals-by-minute
 * ?league=epl
 * Распределение голов по минутам (0-90)
 */
router.get('/goals-by-minute', async (req, res) => {
  const ch     = getCH(req);
  const league = req.query.league || 'epl';
  if (!ch) return res.json([]);
  try {
    const r = await ch.query({
      query: `
        SELECT
          intDiv(goal_minute, 10)*10 AS minute,
          count() AS goals
        FROM betquant.match_events
        WHERE event_type = 'goal' AND league_code = '${league}'
        GROUP BY minute
        ORDER BY minute
      `,
      format: 'JSON',
    });
    const d = await r.json();
    res.json(d.data || []);
  } catch(e) {
    // Пробуем альтернативный запрос через matches таблицу
    res.json([]);
  }
});

/**
 * GET /api/stats/leagues
 * Список доступных лиг в ClickHouse
 */
router.get('/leagues', async (req, res) => {
  const ch = getCH(req);
  if (!ch) return res.json({ leagues: [], hint: 'ClickHouse не подключён' });
  try {
    const r = await ch.query({
      query: `SELECT DISTINCT league_code, league_name FROM betquant.football_matches ORDER BY league_code LIMIT 50`,
      format: 'JSON',
    });
    const d = await r.json();
    res.json({ leagues: d.data || [] });
  } catch(e) {
    res.json({ leagues: [], error: e.message });
  }
});

/**
 * GET /api/stats/seasons
 * ?league=epl — список сезонов
 */
router.get('/seasons', async (req, res) => {
  const ch     = getCH(req);
  const league = req.query.league || 'epl';
  if (!ch) return res.json({ seasons: [] });
  try {
    const r = await ch.query({
      query: `SELECT DISTINCT season FROM betquant.football_matches WHERE league_code='${league}' ORDER BY season DESC LIMIT 20`,
      format: 'JSON',
    });
    const d = await r.json();
    res.json({ seasons: (d.data||[]).map(x=>x.season) });
  } catch(e) {
    res.json({ seasons: [] });
  }
});

module.exports = router;