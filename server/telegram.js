'use strict';
/**
 * BetQuant Pro — Telegram Multi-Bot Notification System
 * ИСПРАВЛЕНИЯ v2:
 *  - loadFromPG / saveToPG — полная персистентность в PostgreSQL
 *  - fetch с fallback для Node < 18
 *  - CLV reminder правильно подключён к этому модулю (не к odds-compare)
 *  - Корректная обработка ошибок tgCall
 *  - pgPool передаётся через initWithPG() из index.js после подключения БД
 */

const express = require('express');
const router  = express.Router();

// ─── fetch polyfill (Node < 18 не имеет глобального fetch) ───────────────
let nodeFetch;
try {
  nodeFetch = typeof fetch !== 'undefined' ? fetch.bind(globalThis) : require('node-fetch');
} catch(e) {
  // node-fetch не установлен — используем http/https
  const https = require('https');
  const http  = require('http');
  nodeFetch = (url, opts = {}) => new Promise((resolve, reject) => {
    const lib  = url.startsWith('https') ? https : http;
    const body = opts.body || null;
    const req  = lib.request(url, {
      method:  opts.method || 'GET',
      headers: opts.headers || {},
      timeout: 10000,
    }, (res) => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => resolve({
        ok: res.statusCode >= 200 && res.statusCode < 300,
        status: res.statusCode,
        json: () => Promise.resolve(JSON.parse(data)),
        text: () => Promise.resolve(data),
      }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });
    if (body) req.write(body);
    req.end();
  });
}

// ─── In-memory store ──────────────────────────────────────────────────────
const store = {
  bots: process.env.TELEGRAM_BOT_TOKEN ? [{
    id: 'default', name: 'Default Bot',
    token: process.env.TELEGRAM_BOT_TOKEN,
    botName: null, isDefault: true,
    createdAt: new Date().toISOString(),
  }] : [],

  recipients: process.env.TELEGRAM_CHAT_ID ? [{
    id: 'default', label: 'Default Channel',
    chatId: process.env.TELEGRAM_CHAT_ID,
    tags: ['all'], isActive: true,
  }] : [],

  distributions: (process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) ? [{
    id: 'default', name: 'Default Distribution',
    botId: 'default', recipientIds: ['default'],
    description: 'Дефолтная рассылка',
  }] : [],

  strategyConfigs: {},  // { strategyId: { botId, distributionId, formatId, enabled } }

  formats: [
    {
      id: 'default_value', name: 'Value Bet (стандарт)', isDefault: true, type: 'value',
      template: `💎 <b>VALUE BET</b>\n⚽ {match}\n🏆 {league}\n📊 {label} @ <b>{odds}</b>\n✅ Edge: <b>+{edge}%</b>\n📐 Kelly: {kelly}%`,
      fields: ['match','league','label','odds','edge','kelly'],
    },
    {
      id: 'default_value_full', name: 'Value Bet (расширенный)', isDefault: false, type: 'value',
      template: `💎 <b>VALUE BET SIGNAL</b>\n\n⚽ <b>{match}</b>\n🏆 {league}\n📊 Рынок: <b>{label}</b>\n📈 Коэффициент: <b>{odds}</b>\n\n🔵 Implied: {impliedProb}%\n🟢 Model: <b>{modelProb}%</b>\n✅ Edge: <b>+{edge}%</b>\n📐 Kelly: {kelly}%`,
      fields: ['match','league','label','odds','impliedProb','modelProb','edge','kelly'],
    },
    {
      id: 'default_live', name: 'Live Signal (стандарт)', isDefault: true, type: 'live',
      template: `🔴 <b>IN-PLAY</b>\n⚽ {match}\n⏱ {minute}' | {score}\n📣 {signalLabel}\n💪 {confidence}%`,
      fields: ['match','minute','score','signalLabel','confidence'],
    },
    {
      id: 'default_neural', name: 'Neural Retrain', isDefault: true, type: 'neural',
      template: `🧠 <b>NEURAL RETRAIN COMPLETE</b>\n🏆 Спорт: <b>{sport}</b>\n📊 Точность: <b>{accuracy}%</b>\n📋 Строк: {rowsUsed}`,
      fields: ['sport','accuracy','rowsUsed'],
    },
  ],
};

// ─── PG pool (устанавливается из index.js через initWithPG) ──────────────
let _pgPool = null;

/**
 * Вызывается из server/index.js ПОСЛЕ того как pgPool готов:
 *   const { router, tgAPI, initWithPG } = require('./telegram');
 *   ...
 *   initWithPG(pgPool);
 */
async function initWithPG(pgPool) {
  if (!pgPool) return;
  _pgPool = pgPool;
  await ensureTables(pgPool);
  await loadFromPG(pgPool);
  console.log('[Telegram] PG storage ready');
}

// ─── Создаём таблицы если их нет ─────────────────────────────────────────
async function ensureTables(pg) {
  try {
    await pg.query(`
      CREATE TABLE IF NOT EXISTS tg_bots (
        id VARCHAR(64) PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        token TEXT NOT NULL,
        bot_name VARCHAR(100),
        is_default BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS tg_recipients (
        id VARCHAR(64) PRIMARY KEY,
        label VARCHAR(200) NOT NULL,
        chat_id VARCHAR(100) NOT NULL,
        tags TEXT[] DEFAULT '{}',
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS tg_distributions (
        id VARCHAR(64) PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        bot_id VARCHAR(64),
        recipient_ids TEXT[] DEFAULT '{}',
        description TEXT DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS tg_strategy_configs (
        strategy_id VARCHAR(100) PRIMARY KEY,
        distribution_id VARCHAR(64),
        bot_id VARCHAR(64),
        format_id VARCHAR(64),
        enabled BOOLEAN DEFAULT TRUE,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS tg_formats (
        id VARCHAR(64) PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        template TEXT NOT NULL,
        type VARCHAR(50) DEFAULT 'any',
        is_default BOOLEAN DEFAULT FALSE,
        fields TEXT[] DEFAULT '{}',
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
  } catch(e) {
    console.warn('[Telegram] ensureTables error:', e.message);
  }
}

// ─── Загрузка из PG (при старте) ─────────────────────────────────────────
async function loadFromPG(pg) {
  try {
    const [bots, recs, dists, cfgRows, fmts] = await Promise.all([
      pg.query('SELECT * FROM tg_bots ORDER BY created_at'),
      pg.query('SELECT * FROM tg_recipients ORDER BY created_at'),
      pg.query('SELECT * FROM tg_distributions ORDER BY created_at'),
      pg.query('SELECT * FROM tg_strategy_configs'),
      pg.query('SELECT * FROM tg_formats ORDER BY created_at'),
    ]);

    // Боты
    if (bots.rows.length) {
      // Мержим: если есть дефолтный из .env — сохраняем токен, но берём остальные из PG
      const pgBots = bots.rows.map(r => ({
        id: r.id, name: r.name, token: r.token,
        botName: r.bot_name, isDefault: r.is_default,
        createdAt: r.created_at,
      }));
      // Если в .env задан токен и его нет в PG — оставляем env-бот
      const envBot = store.bots.find(b => b.id === 'default');
      const hasDefault = pgBots.some(b => b.isDefault);
      store.bots = pgBots;
      if (envBot && !pgBots.find(b => b.id === 'default')) {
        if (!hasDefault) envBot.isDefault = true;
        store.bots.unshift(envBot);
      }
    }

    // Получатели
    if (recs.rows.length) {
      const pgRecs = recs.rows.map(r => ({
        id: r.id, label: r.label, chatId: r.chat_id,
        tags: r.tags || [], isActive: r.is_active,
      }));
      const envRec = store.recipients.find(r => r.id === 'default');
      store.recipients = pgRecs;
      if (envRec && !pgRecs.find(r => r.id === 'default')) {
        store.recipients.unshift(envRec);
      }
    }

    // Рассылки
    if (dists.rows.length) {
      const pgDists = dists.rows.map(r => ({
        id: r.id, name: r.name, botId: r.bot_id,
        recipientIds: r.recipient_ids || [], description: r.description || '',
      }));
      const envDist = store.distributions.find(d => d.id === 'default');
      store.distributions = pgDists;
      if (envDist && !pgDists.find(d => d.id === 'default')) {
        store.distributions.unshift(envDist);
      }
    }

    // Strategy configs
    if (cfgRows.rows.length) {
      store.strategyConfigs = {};
      for (const r of cfgRows.rows) {
        store.strategyConfigs[r.strategy_id] = {
          distributionId: r.distribution_id || undefined,
          botId:          r.bot_id          || undefined,
          formatId:       r.format_id       || undefined,
          enabled:        r.enabled,
        };
      }
    }

    // Форматы: мержим дефолтные (из кода) с кастомными (из PG)
    if (fmts.rows.length) {
      const pgFmts = fmts.rows.map(r => ({
        id: r.id, name: r.name, template: r.template,
        type: r.type, isDefault: r.is_default,
        fields: r.fields || [],
      }));
      // Добавляем только кастомные (не встроенные) из PG
      const builtinIds = new Set(store.formats.map(f => f.id));
      for (const f of pgFmts) {
        if (!builtinIds.has(f.id)) store.formats.push(f);
        else {
          // Обновляем встроенный если isDefault изменился
          const idx = store.formats.findIndex(x => x.id === f.id);
          if (idx >= 0) store.formats[idx].isDefault = f.isDefault;
        }
      }
    }

    console.log(`[Telegram] Loaded from PG: ${store.bots.length} bots, ${store.recipients.length} recipients, ${store.distributions.length} distributions`);
  } catch(e) {
    console.warn('[Telegram] loadFromPG error (using memory):', e.message);
  }
}

// ─── Сохранение в PG ─────────────────────────────────────────────────────
async function saveBotToPG(bot) {
  if (!_pgPool) return;
  try {
    await _pgPool.query(`
      INSERT INTO tg_bots(id, name, token, bot_name, is_default)
      VALUES($1,$2,$3,$4,$5)
      ON CONFLICT(id) DO UPDATE SET name=$2, token=$3, bot_name=$4, is_default=$5
    `, [bot.id, bot.name, bot.token, bot.botName || null, bot.isDefault]);
  } catch(e) { console.warn('[Telegram] saveBotToPG:', e.message); }
}

async function saveRecipientToPG(rec) {
  if (!_pgPool) return;
  try {
    await _pgPool.query(`
      INSERT INTO tg_recipients(id, label, chat_id, tags, is_active)
      VALUES($1,$2,$3,$4,$5)
      ON CONFLICT(id) DO UPDATE SET label=$2, chat_id=$3, tags=$4, is_active=$5
    `, [rec.id, rec.label, rec.chatId, rec.tags || [], rec.isActive]);
  } catch(e) { console.warn('[Telegram] saveRecipientToPG:', e.message); }
}

async function saveDistToPG(dist) {
  if (!_pgPool) return;
  try {
    await _pgPool.query(`
      INSERT INTO tg_distributions(id, name, bot_id, recipient_ids, description)
      VALUES($1,$2,$3,$4,$5)
      ON CONFLICT(id) DO UPDATE SET name=$2, bot_id=$3, recipient_ids=$4, description=$5
    `, [dist.id, dist.name, dist.botId, dist.recipientIds || [], dist.description || '']);
  } catch(e) { console.warn('[Telegram] saveDistToPG:', e.message); }
}

async function saveStratCfgToPG(strategyId, cfg) {
  if (!_pgPool) return;
  try {
    await _pgPool.query(`
      INSERT INTO tg_strategy_configs(strategy_id, distribution_id, bot_id, format_id, enabled, updated_at)
      VALUES($1,$2,$3,$4,$5, NOW())
      ON CONFLICT(strategy_id) DO UPDATE
        SET distribution_id=$2, bot_id=$3, format_id=$4, enabled=$5, updated_at=NOW()
    `, [strategyId, cfg.distributionId || null, cfg.botId || null, cfg.formatId || null, cfg.enabled !== false]);
  } catch(e) { console.warn('[Telegram] saveStratCfgToPG:', e.message); }
}

async function saveFmtToPG(fmt) {
  if (!_pgPool) return;
  try {
    await _pgPool.query(`
      INSERT INTO tg_formats(id, name, template, type, is_default, fields)
      VALUES($1,$2,$3,$4,$5,$6)
      ON CONFLICT(id) DO UPDATE SET name=$2, template=$3, type=$4, is_default=$5, fields=$6
    `, [fmt.id, fmt.name, fmt.template, fmt.type, fmt.isDefault, fmt.fields || []]);
  } catch(e) { console.warn('[Telegram] saveFmtToPG:', e.message); }
}

async function deleteFromPG(table, id) {
  if (!_pgPool) return;
  try {
    await _pgPool.query(`DELETE FROM ${table} WHERE id=$1`, [id]);
  } catch(e) { console.warn(`[Telegram] deleteFromPG ${table}:`, e.message); }
}

// ─── Telegram API ─────────────────────────────────────────────────────────
async function tgCall(token, method, body = {}) {
  const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = ctrl ? setTimeout(() => ctrl.abort(), 10000) : null;
  try {
    const r = await nodeFetch(`https://api.telegram.org/bot${token}/${method}`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(body),
      signal:  ctrl?.signal,
    });
    const d = await r.json();
    if (!d.ok) throw new Error(d.description || `Telegram error: ${method}`);
    return d.result;
  } catch(e) {
    if (e.name === 'AbortError') throw new Error('Telegram request timeout');
    throw e;
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

async function sendTo(token, chatId, text) {
  return tgCall(token, 'sendMessage', {
    chat_id: chatId, text, parse_mode: 'HTML',
    disable_web_page_preview: true,
  });
}

function resolveBot(botId) {
  const id = botId || store.bots.find(b => b.isDefault)?.id;
  return store.bots.find(b => b.id === id);
}

function renderTemplate(template, data) {
  return template.replace(/\{(\w+)\}/g, (_, key) => {
    const val = data[key];
    return (val !== undefined && val !== null) ? String(val) : `{${key}}`;
  });
}

async function sendViaDistribution(distributionId, text, botIdOverride) {
  const dist = store.distributions.find(d => d.id === distributionId)
    || store.distributions.find(d => d.id === 'default');
  if (!dist) throw new Error('No distribution configured. Add at least one distribution in Telegram settings.');

  const bot = resolveBot(botIdOverride || dist.botId);
  if (!bot?.token) throw new Error(`Bot not found or has no token (id: ${botIdOverride || dist.botId})`);

  const recipients = store.recipients.filter(r =>
    dist.recipientIds.includes(r.id) && r.isActive
  );
  if (!recipients.length) throw new Error('No active recipients in this distribution');

  const results = [];
  for (const rec of recipients) {
    try {
      await sendTo(bot.token, rec.chatId, text);
      results.push({ recipientId: rec.id, label: rec.label, chatId: rec.chatId, ok: true });
    } catch(e) {
      results.push({ recipientId: rec.id, label: rec.label, chatId: rec.chatId, ok: false, error: e.message });
    }
  }
  return results;
}

function buildAlertText(type, data, formatId) {
  const fmt = store.formats.find(f => f.id === formatId)
    || store.formats.find(f => f.type === type && f.isDefault)
    || store.formats.find(f => f.type === 'any' && f.isDefault)
    || store.formats[0];

  if (!fmt) return `[${type.toUpperCase()}] ${data.match || JSON.stringify(data)}`;

  const typeIcons = { value: '💎', live: '🔴', odds: '📉', neural: '🧠', clv: '📐' };
  const tplData = {
    type_icon:   typeIcons[type] || '🔔',
    // Прямые поля
    match:       data.match       || data.matchName || '',
    league:      data.league      || '',
    label:       data.label       || '',
    odds:        data.odds        || '',
    edge:        data.edge        || '',
    kelly:       data.kelly       || '',
    impliedProb: data.impliedProb || '',
    modelProb:   data.modelProb   || '',
    lH:          data.lH          || '',
    lA:          data.lA          || '',
    sport:       data.sport       || '',
    accuracy:    data.accuracy    || '',
    rowsUsed:    data.rowsUsed    || '',
    // Live-специфичные
    minute:      data.minute      || data.signal?.minute      || '',
    score:       data.score       || data.signal?.score       || '',
    signalLabel: data.signalLabel || data.signal?.topSignal?.label      || '',
    confidence:  data.confidence  || data.signal?.topSignal?.confidence?.toFixed(0) || '',
    ...data, // override with any extra fields in data
  };
  return renderTemplate(fmt.template, tplData);
}

// ─── Dedup cache ──────────────────────────────────────────────────────────
const sentCache = new Map();
const DEDUP_TTL = 60 * 60 * 1000; // 1 час

function clearExpiredDedup() {
  const now = Date.now();
  for (const [k, t] of sentCache) {
    if (now - t > DEDUP_TTL) sentCache.delete(k);
  }
}
setInterval(clearExpiredDedup, 10 * 60 * 1000);

// ─── Public tgAPI ────────────────────────────────────────────────────────
const tgAPI = {
  isEnabled() {
    return store.bots.some(b => b.token) &&
           store.distributions.some(d => d.recipientIds.length > 0);
  },

  async sendAlert(type, data, strategyId) {
    if (!this.isEnabled()) return;

    const cfg = strategyId ? store.strategyConfigs[strategyId] : null;
    if (cfg?.enabled === false) return;

    const distributionId = cfg?.distributionId || 'default';
    const botId          = cfg?.botId          || null;
    const formatId       = cfg?.formatId       || null;

    // Dedup
    const dedupKey = `${type}_${data.match || ''}_${data.market || data.label || ''}_${Math.floor(Date.now() / DEDUP_TTL)}`;
    if (sentCache.has(dedupKey)) return;
    sentCache.set(dedupKey, Date.now());

    const text = buildAlertText(type, data, formatId);
    try {
      const results = await sendViaDistribution(distributionId, text, botId);
      const failed = results.filter(r => !r.ok);
      if (failed.length) {
        console.warn(`[Telegram] ${failed.length} recipients failed:`, failed.map(r => r.error));
      }
    } catch(e) {
      console.warn(`[Telegram] sendAlert(${type}) error:`, e.message);
    }
  },

  async sendValueAlert(bet, strategyId) {
    return this.sendAlert('value', bet, strategyId);
  },

  async sendLiveSignal(signal, matchName, strategyId) {
    return this.sendAlert('live', {
      match:       matchName || signal.matchId || '',
      minute:      signal.minute      || '',
      score:       signal.score       || '',
      signalLabel: signal.topSignal?.label      || '',
      confidence:  signal.topSignal?.confidence || '',
      signal,
    }, strategyId);
  },

  async sendOddsSpike(data, strategyId) {
    return this.sendAlert('odds', data, strategyId);
  },

  async sendNeuralRetrain(data, strategyId) {
    return this.sendAlert('neural', data, strategyId);
  },

  async sendCLVReminder(bets) {
    if (!this.isEnabled()) return;
    const list = (bets || []).slice(0, 5)
      .map(b => `• ${b.match_name} @ <b>${(+b.bet_odds).toFixed(2)}</b>`)
      .join('\n');
    const text = `⏰ <b>CLV REMINDER</b>\n\n${bets.length} ставок без closing odds:\n\n${list}${bets.length > 5 ? `\n...ещё ${bets.length - 5}` : ''}\n\nОткрой CLV Tracker!`;
    try {
      await sendViaDistribution('default', text);
    } catch(e) {
      console.warn('[Telegram] CLV reminder error:', e.message);
    }
  },
};

// ─── Scheduled CLV reminder ───────────────────────────────────────────────
let _clvReminderTimer = null;
function startCLVReminder() {
  if (_clvReminderTimer) return;
  _clvReminderTimer = setInterval(async () => {
    if (!tgAPI.isEnabled() || !_pgPool) return;
    try {
      const r = await _pgPool.query(
        `SELECT * FROM clv_bets WHERE closing_odds IS NULL AND settled=FALSE
         AND bet_date < NOW() - INTERVAL '12 hours' LIMIT 20`
      );
      if (r.rows.length) await tgAPI.sendCLVReminder(r.rows);
    } catch(e) { /* таблица может не существовать */ }
  }, 12 * 60 * 60 * 1000);
}

// ══════════════════════════════════════════════════════════════════════════
//  ROUTES — БОТЫ
// ══════════════════════════════════════════════════════════════════════════

router.get('/bots', (_req, res) => {
  res.json(store.bots.map(b => ({
    ...b,
    token: b.token ? b.token.slice(0, 10) + '••••••' : null,
  })));
});

router.post('/bots', async (req, res) => {
  const { name, token, isDefault } = req.body;
  if (!name || !token) return res.status(400).json({ error: 'name and token required' });
  try {
    const me = await tgCall(token, 'getMe');
    const id = 'bot_' + Date.now();
    if (isDefault) {
      store.bots.forEach(b => { b.isDefault = false; });
      // Обновим в PG тоже
      if (_pgPool) await _pgPool.query('UPDATE tg_bots SET is_default=FALSE').catch(()=>{});
    }
    const bot = { id, name, token, botName: me.username, isDefault: !!isDefault, createdAt: new Date().toISOString() };
    store.bots.push(bot);
    await saveBotToPG(bot);
    res.json({ id, botName: me.username });
  } catch(e) {
    res.status(400).json({ error: 'Telegram error: ' + e.message });
  }
});

router.put('/bots/:id', async (req, res) => {
  const bot = store.bots.find(b => b.id === req.params.id);
  if (!bot) return res.status(404).json({ error: 'Bot not found' });
  const { name, token, isDefault } = req.body;
  if (name) bot.name = name;
  if (token && !token.includes('••••••')) {
    try {
      const me = await tgCall(token, 'getMe');
      bot.token   = token;
      bot.botName = me.username;
    } catch(e) { return res.status(400).json({ error: 'Invalid token: ' + e.message }); }
  }
  if (isDefault) {
    store.bots.forEach(b => { b.isDefault = false; });
    bot.isDefault = true;
    if (_pgPool) await _pgPool.query('UPDATE tg_bots SET is_default=FALSE').catch(()=>{});
  }
  await saveBotToPG(bot);
  res.json({ ok: true });
});

router.delete('/bots/:id', async (req, res) => {
  const idx = store.bots.findIndex(b => b.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Bot not found' });
  if (store.bots[idx].isDefault && store.bots.length > 1) {
    return res.status(400).json({ error: 'Cannot delete default bot — set another bot as default first' });
  }
  store.bots.splice(idx, 1);
  await deleteFromPG('tg_bots', req.params.id);
  res.json({ ok: true });
});

router.post('/bots/:id/test', async (req, res) => {
  const bot = store.bots.find(b => b.id === req.params.id);
  if (!bot) return res.status(404).json({ error: 'Bot not found' });
  if (!bot.token) return res.status(400).json({ error: 'Bot has no token' });
  const { chatId } = req.body;
  const testChatId = chatId || store.recipients.find(r => r.isActive)?.chatId;
  if (!testChatId) return res.status(400).json({ error: 'Provide chatId or add at least one recipient first' });
  try {
    await sendTo(bot.token, testChatId, `✅ <b>BetQuant Pro</b>\nТест бота <b>${bot.name}</b>\n⏱ ${new Date().toLocaleString('ru')}`);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  ROUTES — ПОЛУЧАТЕЛИ
// ══════════════════════════════════════════════════════════════════════════

router.get('/recipients', (_req, res) => res.json(store.recipients));

router.post('/recipients', async (req, res) => {
  const { label, chatId, tags } = req.body;
  if (!label || !chatId) return res.status(400).json({ error: 'label and chatId required' });
  const id  = 'rec_' + Date.now();
  const rec = { id, label, chatId: String(chatId), tags: tags || [], isActive: true };
  store.recipients.push(rec);
  await saveRecipientToPG(rec);
  res.json({ id });
});

router.put('/recipients/:id', async (req, res) => {
  const rec = store.recipients.find(r => r.id === req.params.id);
  if (!rec) return res.status(404).json({ error: 'Recipient not found' });
  Object.assign(rec, req.body);
  await saveRecipientToPG(rec);
  res.json({ ok: true });
});

router.delete('/recipients/:id', async (req, res) => {
  const idx = store.recipients.findIndex(r => r.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Recipient not found' });
  store.recipients.splice(idx, 1);
  store.distributions.forEach(d => {
    d.recipientIds = d.recipientIds.filter(id => id !== req.params.id);
  });
  // Обновим рассылки в PG
  for (const d of store.distributions) await saveDistToPG(d);
  await deleteFromPG('tg_recipients', req.params.id);
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════════════════════
//  ROUTES — РАССЫЛКИ
// ══════════════════════════════════════════════════════════════════════════

router.get('/distributions', (_req, res) => res.json(store.distributions));

router.post('/distributions', async (req, res) => {
  const { name, botId, recipientIds, description } = req.body;
  if (!name || !botId) return res.status(400).json({ error: 'name and botId required' });
  const id   = 'dist_' + Date.now();
  const dist = { id, name, botId, recipientIds: recipientIds || [], description: description || '' };
  store.distributions.push(dist);
  await saveDistToPG(dist);
  res.json({ id });
});

router.put('/distributions/:id', async (req, res) => {
  const dist = store.distributions.find(d => d.id === req.params.id);
  if (!dist) return res.status(404).json({ error: 'Distribution not found' });
  Object.assign(dist, req.body);
  await saveDistToPG(dist);
  res.json({ ok: true });
});

router.delete('/distributions/:id', async (req, res) => {
  const idx = store.distributions.findIndex(d => d.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Distribution not found' });
  if (store.distributions.length === 1) {
    return res.status(400).json({ error: 'Cannot delete the only distribution' });
  }
  await deleteFromPG('tg_distributions', req.params.id);
  store.distributions.splice(idx, 1);
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════════════════════
//  ROUTES — КОНФИГИ СТРАТЕГИЙ
// ══════════════════════════════════════════════════════════════════════════

router.get('/strategy-configs', (_req, res) => res.json(store.strategyConfigs));

router.put('/strategy-configs/:strategyId', async (req, res) => {
  const { strategyId } = req.params;
  const prev = store.strategyConfigs[strategyId] || {};
  const next = { ...prev, ...req.body };
  // Очищаем undefined/пустые строки
  for (const k of Object.keys(next)) {
    if (next[k] === undefined || next[k] === '') delete next[k];
  }
  store.strategyConfigs[strategyId] = next;
  await saveStratCfgToPG(strategyId, next);
  res.json({ ok: true, config: next });
});

router.delete('/strategy-configs/:strategyId', async (req, res) => {
  delete store.strategyConfigs[req.params.strategyId];
  if (_pgPool) {
    await _pgPool.query('DELETE FROM tg_strategy_configs WHERE strategy_id=$1', [req.params.strategyId]).catch(()=>{});
  }
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════════════════════
//  ROUTES — ФОРМАТЫ
// ══════════════════════════════════════════════════════════════════════════

router.get('/formats', (_req, res) => res.json(store.formats));

router.post('/formats', async (req, res) => {
  const { name, template, type } = req.body;
  if (!name || !template) return res.status(400).json({ error: 'name and template required' });
  const id     = 'fmt_' + Date.now();
  const fields = (template.match(/\{(\w+)\}/g) || []).map(f => f.slice(1, -1));
  const fmt    = { id, name, template, type: type || 'any', isDefault: false, fields };
  store.formats.push(fmt);
  await saveFmtToPG(fmt);
  res.json({ id });
});

router.put('/formats/:id', async (req, res) => {
  const fmt = store.formats.find(f => f.id === req.params.id);
  if (!fmt) return res.status(404).json({ error: 'Format not found' });
  if (req.body.template) {
    req.body.fields = (req.body.template.match(/\{(\w+)\}/g) || []).map(f => f.slice(1, -1));
  }
  if (req.body.isDefault) {
    const targetType = req.body.type || fmt.type;
    store.formats.filter(f => f.type === targetType || f.type === 'any').forEach(f => { f.isDefault = false; });
  }
  Object.assign(fmt, req.body);
  await saveFmtToPG(fmt);
  res.json({ ok: true });
});

router.delete('/formats/:id', async (req, res) => {
  const f = store.formats.find(x => x.id === req.params.id);
  if (!f) return res.status(404).json({ error: 'Format not found' });
  if (f.isDefault) return res.status(400).json({ error: 'Cannot delete a default format. Change the default first.' });
  store.formats.splice(store.formats.indexOf(f), 1);
  await deleteFromPG('tg_formats', req.params.id);
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════════════════════
//  ROUTES — ОТПРАВКА
// ══════════════════════════════════════════════════════════════════════════

router.post('/send', async (req, res) => {
  const { text, distributionId, botId, chatId } = req.body;
  if (!text) return res.status(400).json({ error: 'text required' });

  if (chatId) {
    const bot = resolveBot(botId);
    if (!bot?.token) return res.status(400).json({ error: 'No bot configured' });
    try {
      await sendTo(bot.token, chatId, text);
      return res.json({ ok: true });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  try {
    const results = await sendViaDistribution(distributionId || 'default', text, botId);
    const failed = results.filter(r => !r.ok);
    res.json({ ok: failed.length === 0, results, sent: results.filter(r => r.ok).length, failed: failed.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

router.post('/alert', async (req, res) => {
  const { type, data, strategyId } = req.body;
  if (!type || !data) return res.status(400).json({ error: 'type and data required' });
  if (!tgAPI.isEnabled()) return res.status(400).json({ error: 'Telegram not configured. Add a bot and distribution first.' });
  try {
    await tgAPI.sendAlert(type, data, strategyId);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

router.post('/test', async (req, res) => {
  const { distributionId, botId } = req.body;
  if (!tgAPI.isEnabled()) return res.status(400).json({ error: 'Telegram not configured. Add a bot and distribution first.' });
  try {
    const text    = `✅ <b>BetQuant Pro</b> — тест рассылки\n⏱ ${new Date().toLocaleString('ru')}`;
    const results = await sendViaDistribution(distributionId || 'default', text, botId);
    res.json({ ok: true, results });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

router.get('/status', (_req, res) => {
  res.json({
    bots:            store.bots.length,
    recipients:      store.recipients.filter(r => r.isActive).length,
    distributions:   store.distributions.length,
    formats:         store.formats.length,
    strategyConfigs: Object.keys(store.strategyConfigs).length,
    defaultBot:      store.bots.find(b => b.isDefault)?.botName || null,
    enabled:         tgAPI.isEnabled(),
    pgConnected:     !!_pgPool,
  });
});

router.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  const msg = req.body?.message;
  if (!msg?.text) return;
  const bot = store.bots.find(b => b.isDefault);
  if (!bot?.token) return;
  const chatId = String(msg.chat.id);
  const text   = msg.text.trim().toLowerCase();
  const reply  = async t => { try { await sendTo(bot.token, chatId, t); } catch(e){} };
  if      (text === '/start' || text === '/help') await reply(`🎯 <b>BetQuant Pro</b>\n\n/status — статус\n/help — справка`);
  else if (text === '/status') await reply(`✅ BetQuant Pro работает\n⏱ ${new Date().toLocaleString('ru')}`);
});

module.exports = { router, tgAPI, store, initWithPG, startCLVReminder };