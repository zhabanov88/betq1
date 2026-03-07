/**
 * BETTING ADVANCED - Football Data Collector
 * 
 * DATA SOURCES FOR BACKTESTING (free and commercial):
 * 
 * FREE SOURCES:
 * 1. football-data.org - Top 5 leagues, free tier 10req/min, 2014-present
 *    GET https://api.football-data.org/v4/competitions/PL/matches?season=2023
 * 
 * 2. OpenLigaDB - German football only, unlimited, historical from 1960s
 *    GET https://api.openligadb.de/getmatchdata/bl1/2020
 * 
 * 3. Transfermarkt (scraping) - Player values, squad info, 20+ years data
 * 
 * 4. FBref.com (scraping) - Advanced stats, xG from 2017-present
 *    Powered by StatsBomb data
 * 
 * 5. Understat.com (scraping) - xG data for top 6 leagues from 2014
 * 
 * 6. The Odds API - Historical odds snapshots, 500 req/month free
 *    GET https://api.the-odds-api.com/v4/historical/sports/.../odds
 * 
 * 7. Football-Data.co.uk CSV files - FREE historical odds from 1993!
 *    URL pattern: https://www.football-data.co.uk/mmz4281/2324/E0.csv
 *    Leagues: E0=EPL, D1=Bundesliga, I1=Serie A, SP1=La Liga, F1=Ligue1
 *    Contains: Match results + odds from 20+ bookmakers
 * 
 * 8. BetsAPI free tier - Live scores, some historical
 * 
 * COMMERCIAL (best for serious backtesting):
 * 9. Betfair Exchange API - Historical BSP prices, exchange odds
 * 10. Pinnacle API - Sharpest market, CLV analysis
 * 11. StatsBomb - Professional xG, 360 data
 * 12. Opta / Wyscout - Full event data
 * 13. Infogol - xG focused betting data
 */

require('dotenv').config();
const axios = require('axios');
const { pgPool, clickhouse } = require('../server/db/connections');
const logger = require('../server/services/logger');

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ─── football-data.org collector ─────────────────────────────────────────────
class FootballDataCollector {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.baseUrl = 'https://api.football-data.org/v4';
    this.rateLimit = 6000; // 10 req/min = 1 per 6s
  }

  async fetch(endpoint) {
    await sleep(this.rateLimit);
    const resp = await axios.get(`${this.baseUrl}${endpoint}`, {
      headers: { 'X-Auth-Token': this.apiKey }
    });
    return resp.data;
  }

  async collectSeason(competitionCode, season) {
    logger.info(`Collecting ${competitionCode} season ${season}...`);
    
    try {
      const data = await this.fetch(`/competitions/${competitionCode}/matches?season=${season}`);
      const matches = data.matches || [];
      
      let inserted = 0;
      for (const m of matches) {
        try {
          // Upsert teams
          if (m.homeTeam?.id) {
            await pgPool.query(
              `INSERT INTO teams (id, name, short_name) VALUES ($1, $2, $3)
               ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
              [`fd_${m.homeTeam.id}`, m.homeTeam.name, m.homeTeam.shortName]
            );
          }
          if (m.awayTeam?.id) {
            await pgPool.query(
              `INSERT INTO teams (id, name, short_name) VALUES ($1, $2, $3)
               ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
              [`fd_${m.awayTeam.id}`, m.awayTeam.name, m.awayTeam.shortName]
            );
          }

          // Upsert match
          const scoreHome = m.score?.fullTime?.home;
          const scoreAway = m.score?.fullTime?.away;
          const htHome = m.score?.halfTime?.home;
          const htAway = m.score?.halfTime?.away;

          await pgPool.query(
            `INSERT INTO matches 
               (id, competition_id, season, matchday, home_team_id, away_team_id, 
                scheduled_at, status, score_home, score_away, score_ht_home, score_ht_away, venue, referee)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
             ON CONFLICT (id) DO UPDATE SET
               status = EXCLUDED.status,
               score_home = EXCLUDED.score_home,
               score_away = EXCLUDED.score_away,
               score_ht_home = EXCLUDED.score_ht_home,
               score_ht_away = EXCLUDED.score_ht_away`,
            [
              `fd_${m.id}`, competitionCode, season.toString(), m.matchday,
              `fd_${m.homeTeam?.id}`, `fd_${m.awayTeam?.id}`,
              m.utcDate, m.status,
              scoreHome, scoreAway, htHome, htAway,
              m.venue, m.referees?.[0]?.name
            ]
          );
          inserted++;
        } catch (e) {
          logger.warn(`Match insert error ${m.id}: ${e.message}`);
        }
      }
      
      logger.info(`${competitionCode}/${season}: ${inserted}/${matches.length} matches stored`);
      return inserted;
    } catch (e) {
      logger.error(`Error collecting ${competitionCode}/${season}: ${e.message}`);
      return 0;
    }
  }

  async collectAllSeasons(competitions = ['PL', 'BL1', 'SA', 'PD', 'FL1'], fromYear = 2014) {
    const currentYear = new Date().getFullYear();
    let total = 0;
    
    for (const comp of competitions) {
      for (let year = fromYear; year < currentYear; year++) {
        total += await this.collectSeason(comp, year);
      }
    }
    
    logger.info(`Total collected: ${total} matches`);
    return total;
  }
}

// ─── Football-Data.co.uk CSV collector (FREE historical odds!) ────────────────
class FootballDataCsvCollector {
  constructor() {
    this.baseUrl = 'https://www.football-data.co.uk/mmz4281';
    this.leagues = {
      'PL':  { code: 'E0', name: 'Premier League', country: 'England' },
      'ELC': { code: 'E1', name: 'Championship', country: 'England' },
      'BL1': { code: 'D1', name: 'Bundesliga', country: 'Germany' },
      'BL2': { code: 'D2', name: '2. Bundesliga', country: 'Germany' },
      'SA':  { code: 'I1', name: 'Serie A', country: 'Italy' },
      'PD':  { code: 'SP1', name: 'La Liga', country: 'Spain' },
      'FL1': { code: 'F1', name: 'Ligue 1', country: 'France' },
      'PPL': { code: 'P1', name: 'Primeira Liga', country: 'Portugal' },
      'DED': { code: 'N1', name: 'Eredivisie', country: 'Netherlands' },
      'SB':  { code: 'SC0', name: 'Premiership', country: 'Scotland' },
    };
    
    // Standard bookmaker columns in the CSV
    this.bookmakers = ['B365', 'BW', 'IW', 'PS', 'WH', 'VC', 'Pinn'];
  }

  formatSeason(year) {
    // Football-data.co.uk uses format: 9394, 0001, 2324
    const start = year % 100;
    const end = (year + 1) % 100;
    return `${String(start).padStart(2, '0')}${String(end).padStart(2, '0')}`;
  }

  async downloadCsv(leagueCode, season) {
    const seasonStr = this.formatSeason(season);
    const url = `${this.baseUrl}/${seasonStr}/${leagueCode}.csv`;
    
    try {
      const resp = await axios.get(url, { 
        responseType: 'text',
        timeout: 30000,
        headers: { 'User-Agent': 'BettingAdvanced/1.0 (Research Tool)' }
      });
      return resp.data;
    } catch (e) {
      if (e.response?.status === 404) return null;
      throw e;
    }
  }

  parseCsv(csvText) {
    const lines = csvText.trim().split('\n');
    if (lines.length < 2) return [];
    
    const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
    const rows = [];
    
    for (let i = 1; i < lines.length; i++) {
      const values = lines[i].split(',').map(v => v.trim().replace(/"/g, ''));
      if (values.length < 10) continue;
      
      const row = {};
      headers.forEach((h, idx) => { row[h] = values[idx] || null; });
      rows.push(row);
    }
    
    return rows;
  }

  parseDate(dateStr) {
    if (!dateStr) return null;
    // Format: DD/MM/YY or DD/MM/YYYY
    const parts = dateStr.split('/');
    if (parts.length !== 3) return null;
    const day = parseInt(parts[0]);
    const month = parseInt(parts[1]) - 1;
    let year = parseInt(parts[2]);
    if (year < 100) year += year > 50 ? 1900 : 2000;
    return new Date(year, month, day, 15, 0, 0); // Default 15:00 kickoff
  }

  async processAndStore(competitionId, season, rows) {
    let matchCount = 0;
    let oddsCount = 0;
    const oddsRows = [];

    for (const row of rows) {
      const matchDate = this.parseDate(row.Date);
      if (!matchDate || !row.HomeTeam || !row.AwayTeam) continue;

      const homeScore = parseInt(row.FTHG || row.HG);
      const awayScore = parseInt(row.FTAG || row.AG);
      const htHomeScore = parseInt(row.HTHG);
      const htAwayScore = parseInt(row.HTAG);

      // Generate stable IDs from team names + date
      const homeId = `csv_${row.HomeTeam.toLowerCase().replace(/\s+/g, '_')}`;
      const awayId = `csv_${row.AwayTeam.toLowerCase().replace(/\s+/g, '_')}`;
      const matchId = `csv_${competitionId}_${season}_${homeId}_${awayId}_${matchDate.toISOString().split('T')[0]}`;

      // Upsert teams
      await pgPool.query(
        `INSERT INTO teams (id, name, sport_id) VALUES ($1, $2, 1) ON CONFLICT (id) DO NOTHING`,
        [homeId, row.HomeTeam]
      );
      await pgPool.query(
        `INSERT INTO teams (id, name, sport_id) VALUES ($1, $2, 1) ON CONFLICT (id) DO NOTHING`,
        [awayId, row.AwayTeam]
      );

      // Determine status
      const isFinished = !isNaN(homeScore) && !isNaN(awayScore);
      const status = isFinished ? 'FINISHED' : 'SCHEDULED';

      // Upsert match
      await pgPool.query(
        `INSERT INTO matches (id, competition_id, season, home_team_id, away_team_id, scheduled_at, status, score_home, score_away, score_ht_home, score_ht_away)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, score_home = EXCLUDED.score_home, score_away = EXCLUDED.score_away`,
        [matchId, competitionId, season.toString(), homeId, awayId, matchDate, status,
         isNaN(homeScore) ? null : homeScore, isNaN(awayScore) ? null : awayScore,
         isNaN(htHomeScore) ? null : htHomeScore, isNaN(htAwayScore) ? null : htAwayScore]
      );
      matchCount++;

      // Match stats
      if (row.HS !== undefined) {
        await pgPool.query(
          `INSERT INTO match_stats (match_id, home_shots, away_shots, home_shots_on_target, away_shots_on_target, home_corners, away_corners, home_yellow_cards, away_yellow_cards, home_red_cards, away_red_cards, home_fouls, away_fouls)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
           ON CONFLICT (match_id) DO NOTHING`,
          [matchId, 
           parseInt(row.HS) || null, parseInt(row.AS) || null,
           parseInt(row.HST) || null, parseInt(row.AST) || null,
           parseInt(row.HC) || null, parseInt(row.AC) || null,
           parseInt(row.HY) || null, parseInt(row.AY) || null,
           parseInt(row.HR) || null, parseInt(row.AR) || null,
           parseInt(row.HF) || null, parseInt(row.AF) || null]
        );
      }

      // Collect odds
      const bookmakerMap = {
        'B365H': { bm: 'bet365', market: '1x2', sel: 'home' },
        'B365D': { bm: 'bet365', market: '1x2', sel: 'draw' },
        'B365A': { bm: 'bet365', market: '1x2', sel: 'away' },
        'BWH':  { bm: 'bwin', market: '1x2', sel: 'home' },
        'BWD':  { bm: 'bwin', market: '1x2', sel: 'draw' },
        'BWA':  { bm: 'bwin', market: '1x2', sel: 'away' },
        'PSH':  { bm: 'pinnacle', market: '1x2', sel: 'home' },
        'PSD':  { bm: 'pinnacle', market: '1x2', sel: 'draw' },
        'PSA':  { bm: 'pinnacle', market: '1x2', sel: 'away' },
        'WHH':  { bm: 'williamhill', market: '1x2', sel: 'home' },
        'WHD':  { bm: 'williamhill', market: '1x2', sel: 'draw' },
        'WHA':  { bm: 'williamhill', market: '1x2', sel: 'away' },
        'VCH':  { bm: 'vcbet', market: '1x2', sel: 'home' },
        'VCD':  { bm: 'vcbet', market: '1x2', sel: 'draw' },
        'VCA':  { bm: 'vcbet', market: '1x2', sel: 'away' },
        'B365>2.5': { bm: 'bet365', market: 'over_under', sel: 'over' },
        'B365<2.5': { bm: 'bet365', market: 'over_under', sel: 'under' },
        'B365AH':  { bm: 'bet365', market: 'asian_handicap', sel: 'home' },
        'B365AHA': { bm: 'bet365', market: 'asian_handicap', sel: 'away' },
      };

      for (const [col, info] of Object.entries(bookmakerMap)) {
        const oddsVal = parseFloat(row[col]);
        if (!isNaN(oddsVal) && oddsVal > 1.0 && oddsVal < 1000) {
          oddsRows.push({
            match_id: matchId,
            bookmaker: info.bm,
            market: info.market,
            selection: info.sel,
            odds: oddsVal,
            recorded_at: matchDate.toISOString(),
            source: 'football-data.co.uk',
            is_closing: true  // CSV data is typically closing/opening odds
          });
          oddsCount++;
        }
      }
    }

    // Batch insert odds to ClickHouse
    if (oddsRows.length > 0) {
      await clickhouse.insert({
        table: 'odds_history',
        values: oddsRows,
        format: 'JSONEachRow'
      });
    }

    return { matchCount, oddsCount };
  }

  async collectLeagueSeason(competitionId, leagueCode, season) {
    logger.info(`Downloading ${competitionId} ${season}...`);
    
    const csvText = await this.downloadCsv(leagueCode, season);
    if (!csvText) {
      logger.warn(`No data for ${competitionId} ${season}`);
      return null;
    }

    const rows = this.parseCsv(csvText);
    if (!rows.length) {
      logger.warn(`Empty CSV for ${competitionId} ${season}`);
      return null;
    }

    const result = await this.processAndStore(competitionId, season, rows);
    logger.info(`${competitionId}/${season}: ${result.matchCount} matches, ${result.oddsCount} odds records`);
    return result;
  }

  async collectAll(fromYear = 2005) {
    const currentYear = new Date().getFullYear();
    const results = {};
    
    for (const [compId, league] of Object.entries(this.leagues)) {
      results[compId] = [];
      for (let year = fromYear; year < currentYear; year++) {
        try {
          const result = await this.collectLeagueSeason(compId, league.code, year);
          if (result) results[compId].push({ season: year, ...result });
          await sleep(1000); // Be respectful
        } catch (e) {
          logger.error(`Error ${compId}/${year}: ${e.message}`);
        }
      }
    }
    
    return results;
  }
}

// ─── OpenLigaDB - German football (FREE, no key, historical from 1963) ────────
class OpenLigaDBCollector {
  constructor() {
    this.baseUrl = 'https://api.openligadb.de';
    this.leagues = {
      'bl1': { competitionId: 'BL1', name: 'Bundesliga' },
      'bl2': { competitionId: 'BL2', name: '2. Bundesliga' },
      'bl3': { competitionId: 'BL3', name: '3. Liga' },
    };
  }

  async fetchSeason(leagueSlug, season) {
    const resp = await axios.get(`${this.baseUrl}/getmatchdata/${leagueSlug}/${season}`);
    return resp.data;
  }

  async collectSeason(leagueSlug, season) {
    const matches = await this.fetchSeason(leagueSlug, season);
    const { competitionId } = this.leagues[leagueSlug];
    let count = 0;

    for (const m of matches) {
      if (!m.team1 || !m.team2) continue;

      const homeId = `ol_${m.team1.teamId}`;
      const awayId = `ol_${m.team2.teamId}`;

      await pgPool.query(
        `INSERT INTO teams (id, name, sport_id) VALUES ($1, $2, 1) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
        [homeId, m.team1.teamName]
      );
      await pgPool.query(
        `INSERT INTO teams (id, name, sport_id) VALUES ($1, $2, 1) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
        [awayId, m.team2.teamName]
      );

      const finalScore = m.matchResults?.find(r => r.resultTypeID === 2);
      const htScore = m.matchResults?.find(r => r.resultTypeID === 1);

      await pgPool.query(
        `INSERT INTO matches (id, competition_id, season, matchday, home_team_id, away_team_id, scheduled_at, status, score_home, score_away, score_ht_home, score_ht_away)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, score_home = EXCLUDED.score_home, score_away = EXCLUDED.score_away`,
        [
          `ol_${m.matchID}`, competitionId, season.toString(), m.group?.groupOrderID,
          homeId, awayId,
          m.matchDateTimeUTC, m.matchIsFinished ? 'FINISHED' : 'SCHEDULED',
          finalScore?.pointsTeam1, finalScore?.pointsTeam2,
          htScore?.pointsTeam1, htScore?.pointsTeam2
        ]
      );
      count++;
    }

    logger.info(`OpenLigaDB ${leagueSlug}/${season}: ${count} matches`);
    return count;
  }

  async collectAll(fromYear = 2000) {
    const currentYear = new Date().getFullYear();
    for (const leagueSlug of Object.keys(this.leagues)) {
      for (let year = fromYear; year < currentYear; year++) {
        try {
          await this.collectSeason(leagueSlug, year);
          await sleep(500);
        } catch (e) {
          logger.warn(`OpenLigaDB error ${leagueSlug}/${year}: ${e.message}`);
        }
      }
    }
  }
}

// ─── Understat.com - xG data scraper (top 6 leagues from 2014) ───────────────
class UnderstatCollector {
  constructor() {
    this.baseUrl = 'https://understat.com';
    this.leagues = ['EPL', 'La_liga', 'Bundesliga', 'Serie_A', 'Ligue_1', 'RFPL'];
    this.competitionMap = { 'EPL': 'PL', 'La_liga': 'PD', 'Bundesliga': 'BL1', 'Serie_A': 'SA', 'Ligue_1': 'FL1', 'RFPL': 'RPL' };
  }

  async getLeagueSeason(league, season) {
    const url = `${this.baseUrl}/league/${league}/${season}`;
    const resp = await axios.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; research bot)' }
    });

    // Extract embedded JSON
    const html = resp.data;
    const match = html.match(/var datesData\s*=\s*JSON\.parse\('(.+?)'\)/);
    if (!match) return [];

    const jsonStr = match[1].replace(/\\'/g, "'").replace(/\\\\/g, '\\');
    return JSON.parse(jsonStr);
  }

  async updateXGForSeason(league, season) {
    try {
      const matches = await this.getLeagueSeason(league, season);
      const competitionId = this.competitionMap[league];
      let updated = 0;

      for (const m of matches) {
        if (!m.isResult) continue;

        // Find matching match in DB by teams and date
        const date = new Date(m.datetime);
        const result = await pgPool.query(
          `SELECT m.id FROM matches m
           JOIN teams th ON m.home_team_id = th.id
           JOIN teams ta ON m.away_team_id = ta.id
           WHERE m.competition_id = $1
             AND m.season = $2
             AND DATE(m.scheduled_at) = $3
             AND (th.name ILIKE $4 OR th.name ILIKE $5)
           LIMIT 1`,
          [competitionId, season.toString(), date.toISOString().split('T')[0], `%${m.h?.title}%`, `%${m.h?.short_title}%`]
        );

        if (result.rows.length) {
          await pgPool.query(
            `INSERT INTO match_stats (match_id, home_xg, away_xg)
             VALUES ($1, $2, $3)
             ON CONFLICT (match_id) DO UPDATE SET home_xg = EXCLUDED.home_xg, away_xg = EXCLUDED.away_xg`,
            [result.rows[0].id, parseFloat(m.xG?.h) || 0, parseFloat(m.xG?.a) || 0]
          );
          updated++;
        }
      }

      logger.info(`Understat ${league}/${season}: ${updated} xG records updated`);
    } catch (e) {
      logger.error(`Understat error ${league}/${season}: ${e.message}`);
    }
  }
}

// ─── The Odds API - Historical odds ──────────────────────────────────────────
class TheOddsAPICollector {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.baseUrl = 'https://api.the-odds-api.com/v4';
    this.sports = [
      'soccer_epl', 'soccer_germany_bundesliga', 'soccer_italy_serie_a',
      'soccer_spain_la_liga', 'soccer_france_ligue_one',
      'basketball_nba', 'icehockey_nhl', 'baseball_mlb', 'americanfootball_nfl'
    ];
  }

  async getHistoricalOdds(sport, date, markets = 'h2h,totals') {
    const resp = await axios.get(`${this.baseUrl}/historical/sports/${sport}/odds`, {
      params: {
        apiKey: this.apiKey,
        regions: 'eu',
        markets,
        date: date.toISOString(),
        oddsFormat: 'decimal'
      }
    });
    return resp.data;
  }

  async collectDateRange(sport, startDate, endDate) {
    const current = new Date(startDate);
    const rows = [];

    while (current <= endDate) {
      try {
        const data = await this.getHistoricalOdds(sport, current);
        
        for (const event of data.data || []) {
          for (const bookmaker of event.bookmakers || []) {
            for (const market of bookmaker.markets || []) {
              for (const outcome of market.outcomes || []) {
                rows.push({
                  match_id: event.id,
                  bookmaker: bookmaker.key,
                  market: market.key,
                  selection: outcome.name.toLowerCase(),
                  odds: outcome.price,
                  recorded_at: current.toISOString(),
                  source: 'the-odds-api'
                });
              }
            }
          }
        }

        current.setDate(current.getDate() + 1);
        await sleep(1200); // Rate limiting
      } catch (e) {
        logger.error(`TheOddsAPI error: ${e.message}`);
        current.setDate(current.getDate() + 1);
      }
    }

    if (rows.length > 0) {
      await clickhouse.insert({ table: 'odds_history', values: rows, format: 'JSONEachRow' });
    }
    
    return rows.length;
  }
}

module.exports = {
  FootballDataCollector,
  FootballDataCsvCollector,
  OpenLigaDBCollector,
  UnderstatCollector,
  TheOddsAPICollector
};
