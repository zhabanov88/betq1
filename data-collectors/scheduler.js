/**
 * Data Collection Scheduler
 * Runs continuously, collecting data on schedule
 */

require('dotenv').config();
const cron = require('node-cron');
const {
  FootballDataCollector,
  FootballDataCsvCollector,
  OpenLigaDBCollector,
  TheOddsAPICollector
} = require('./football-collector');
const logger = require('../server/services/logger');

const fd = new FootballDataCollector(process.env.FOOTBALL_DATA_API_KEY);
const csvCollector = new FootballDataCsvCollector();
const openLiga = new OpenLigaDBCollector();
const oddsApi = process.env.THE_ODDS_API_KEY ? new TheOddsAPICollector(process.env.THE_ODDS_API_KEY) : null;

// Every day at 6am: collect yesterday's matches
cron.schedule('0 6 * * *', async () => {
  logger.info('Daily match collection started...');
  const today = new Date();
  const currentYear = today.getFullYear();
  const currentSeason = today.getMonth() >= 6 ? currentYear : currentYear - 1;

  const competitions = ['PL', 'BL1', 'SA', 'PD', 'FL1'];
  for (const comp of competitions) {
    await fd.collectSeason(comp, currentSeason);
  }
  await openLiga.collectAll(currentSeason);
});

// Every hour: collect live odds
cron.schedule('0 * * * *', async () => {
  if (!oddsApi) return;
  logger.info('Hourly odds collection...');
  const tomorrow = new Date();
  tomorrow.setDate(tomorrow.getDate() + 2);
  // Collect odds for upcoming matches
});

// Weekly: collect CSV historical data (new season data)
cron.schedule('0 3 * * 0', async () => {
  logger.info('Weekly CSV update...');
  const currentYear = new Date().getFullYear();
  await csvCollector.collectAll(currentYear - 1);
});

logger.info('Data collector scheduler started');
logger.info('Schedules: Daily matches 6am, Hourly odds, Weekly CSV Sunday 3am');

// Initial collection on startup
setTimeout(async () => {
  logger.info('Initial data check...');
  // Only collect if DB seems empty
}, 10000);
