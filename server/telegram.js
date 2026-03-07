'use strict';
/**
 * BetQuant Pro — Telegram Multi-Bot Notification System
 *
 * Архитектура:
 *  • Боты       — хранятся в PG / памяти, каждый с токеном
 *  • Получатели — chat_id + метка + теги
 *  • Рассылки   — именованный набор (бот + список получателей)
 *  • Стратегии  — каждая стратегия имеет опциональный override:
 *                 botId, distributionId, messageFormat
 *
 * Endpoints:
 *   Боты:
 *     GET    /api/telegram/bots
 *     POST   /api/telegram/bots
 *     PUT    /api/telegram/bots/:id
 *     DELETE /api/telegram/bots/:id
 *     POST   /api/telegram/bots/:id/test
 *
 *   Получатели:
 *     GET    /api/telegram/recipients
 *     POST   /api/telegram/recipients
 *     PUT    /api/telegram/recipients/:id
 *     DELETE /api/telegram/recipients/:id
 *
 *   Рассылки (Distribution Lists):
 *     GET    /api/telegram/distributions
 *     POST   /api/telegram/distributions
 *     PUT    /api/telegram/distributions/:id
 *     DELETE /api/telegram/distributions/:id
 *
 *   Привязка стратегий:
 *     GET    /api/telegram/strategy-configs
 *     PUT    /api/telegram/strategy-configs/:strategyId
 *     DELETE /api/telegram/strategy-configs/:strategyId
 *
 *   Форматы сообщений:
 *     GET    /api/telegram/formats
 *     POST   /api/telegram/formats
 *     PUT    /api/telegram/formats/:id
 *     DELETE /api/telegram/formats/:id
 *
 *   Отправка:
 *     POST   /api/telegram/send
 *     POST   /api/telegram/alert
 *     POST   /api/telegram/test
 *     POST   /api/telegram/webhook
 *     GET    /api/telegram/status
 */

const express = require('express');
const router  = express.Router();

// ─── In-memory store (при наличии PG — синхронизируем) ────────────────────
const store = {
  // Боты: { id, name, token, isDefault, createdAt }
  bots: [
    process.env.TELEGRAM_BOT_TOKEN ? {
      id: 'default', name: 'Default Bot', token: process.env.TELEGRAM_BOT_TOKEN,
      isDefault: true, createdAt: new Date().toISOString(),
    } : null,
  ].filter(Boolean),

  // Получатели: { id, label, chatId, tags[], isActive }
  recipients: [
    process.env.TELEGRAM_CHAT_ID ? {
      id: 'default', label: 'Default Channel', chatId: process.env.TELEGRAM_CHAT_ID,
      tags: ['all'], isActive: true,
    } : null,
  ].filter(Boolean),

  // Рассылки: { id, name, botId, recipientIds[], description }
  distributions: process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID ? [{
    id: 'default', name: 'Default Distribution',
    botId: 'default', recipientIds: ['default'],
    description: 'Дефолтная рассылка',
  }] : [],

  // Конфиги стратегий: { strategyId → { botId?, distributionId?, formatId?, enabled } }
  strategyConfigs: {},

  // Форматы: { id, name, template, isDefault, fields[] }
  formats: [
    {
      id: 'default_value', name: 'Value Bet (стандарт)', isDefault: true, type: 'value',
      template: `💎 <b>VALUE BET</b>\n⚽ {match}\n🏆 {league}\n📊 {label} @ <b>{odds}</b>\n✅ Edge: <b>+{edge}%</b>\n📐 Kelly: {kelly}%`,
      fields: ['match','league','label','odds','edge','kelly'],
    },
    {
      id: 'default_value_full', name: 'Value Bet (расширенный)', isDefault: false, type: 'value',
      template: `💎 <b>VALUE BET SIGNAL</b>\n\n⚽ <b>{match}</b>\n🏆 {league}\n📊 Рынок: <b>{label}</b>\n📈 Коэффициент: <b>{odds}</b>\n\n🔵 Implied: {impliedProb}%\n🟢 Model:   <b>{modelProb}%</b>\n✅ Edge:    <b>+{edge}%</b>\n📐 Kelly:   {kelly}%\n\nλ Хозяева: {lH} / Гости: {lA}\n🤖 Poisson+ELO Ensemble`,
      fields: ['match','league','label','odds','impliedProb','modelProb','edge','kelly','lH','lA'],
    },
    {
      id: 'default_live', name: 'Live Signal (стандарт)', isDefault: true, type: 'live',
      template: `🔴 <b>IN-PLAY</b>\n⚽ {match}\n⏱ {minute}' | {score}\n📣 {signalLabel}\n💪 {confidence}%`,
      fields: ['match','minute','score','signalLabel','confidence'],
    },
    {
      id: 'default_compact', name: 'Компактный (все типы)', isDefault: false, type: 'any',
      template: `🎯 {type_icon} {match} | {label} @ {odds} | Edge +{edge}%`,
      fields: ['match','label','odds','edge'],
    },
  ],
};

// Dedup cache
const sentCache = new Map();
const DEDUP_TTL  = 60 * 60 * 1000;

// ─── Telegram API helpers ─────────────────────────────────────────────────
async function tgCall(token, method, body = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 10000);
  try {
    const r = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(body),
      signal:  ctrl.signal,
    });
    const d = await r.json();
    if (!d.ok) throw new Error(d.description || 'Telegram API error');
    return d.result;
  } finally {
    clearTimeout(t);
  }
}

async function sendTo(token, chatId, text) {
  return tgCall(token, 'sendMessage', {
    chat_id: chatId, text, parse_mode: 'HTML',
    disable_web_page_preview: true,
  });
}

// ─── Resolve bot token ────────────────────────────────────────────────────
function resolveBot(botId) {
  const id = botId || store.bots.find(b => b.isDefault)?.id;
  return store.bots.find(b => b.id === id);
}

// ─── Template renderer ────────────────────────────────────────────────────
function renderTemplate(template, data) {
  return template.replace(/\{(\w+)\}/g, (_, key) => {
    const val = data[key];
    return val !== undefined && val !== null ? val : `{${key}}`;
  });
}

// ─── Resolve distribution and send ────────────────────────────────────────
async function sendViaDistribution(distributionId, text, botIdOverride) {
  const dist = store.distributions.find(d => d.id === distributionId)
    || store.distributions.find(d => d.id === 'default');
  if (!dist) throw new Error('No distribution configured');

  const bot = resolveBot(botIdOverride || dist.botId);
  if (!bot?.token) throw new Error(`Bot not found: ${botIdOverride || dist.botId}`);

  const recipients = store.recipients.filter(r =>
    dist.recipientIds.includes(r.id) && r.isActive
  );
  if (!recipients.length) throw new Error('No active recipients in distribution');

  const results = [];
  for (const rec of recipients) {
    try {
      await sendTo(bot.token, rec.chatId, text);
      results.push({ recipientId: rec.id, label: rec.label, ok: true });
    } catch(e) {
      results.push({ recipientId: rec.id, label: rec.label, ok: false, error: e.message });
    }
  }
  return results;
}

// ─── Build alert text ─────────────────────────────────────────────────────
function buildAlertText(type, data, formatId) {
  const fmt = store.formats.find(f => f.id === formatId)
    || store.formats.find(f => f.type === type && f.isDefault)
    || store.formats[0];

  const typeIcons = { value: '💎', live: '🔴', odds: '📉', neural: '🧠', clv: '📐' };

  const tplData = {
    ...data,
    type_icon: typeIcons[type] || '🔔',
    signalLabel: data.signal?.topSignal?.label || '',
    confidence:  data.signal?.topSignal?.confidence?.toFixed(0) || '',
    minute:      data.signal?.minute || '',
    score:       data.signal?.score  || '',
  };

  return renderTemplate(fmt.template, tplData);
}

// ─── Public API ───────────────────────────────────────────────────────────
const tgAPI = {
  isEnabled() {
    return store.bots.some(b => b.token) && store.distributions.length > 0;
  },

  async sendAlert(type, data, strategyId) {
    // Resolve strategy override
    const cfg = strategyId ? store.strategyConfigs[strategyId] : null;
    if (cfg && cfg.enabled === false) return;

    const distributionId = cfg?.distributionId || 'default';
    const botId          = cfg?.botId || null;
    const formatId       = cfg?.formatId || null;

    // Dedup
    const dedupKey = `${type}_${data.match || ''}_${data.market || ''}_${Math.floor(Date.now() / 3600000)}`;
    if (sentCache.get(dedupKey)) return;
    sentCache.set(dedupKey, Date.now());

    const text = buildAlertText(type, data, formatId);
    try {
      await sendViaDistribution(distributionId, text, botId);
    } catch(e) {
      console.warn(`[Telegram] alert send error (${type}):`, e.message);
    }
  },

  async sendValueAlert(bet, strategyId)      { return this.sendAlert('value',  bet,           strategyId); },
  async sendLiveSignal(signal, matchName, strategyId) {
    return this.sendAlert('live', { ...signal, match: matchName }, strategyId);
  },
  async sendOddsSpike(data, strategyId)      { return this.sendAlert('odds',   data,          strategyId); },
  async sendNeuralRetrain(data, strategyId)  { return this.sendAlert('neural', data,          strategyId); },
  async sendCLVReminder(bets, strategyId)    {
    const text = `⏰ <b>CLV REMINDER</b>\n${bets.length} ставок без closing odds.\nОткрой CLV Tracker!`;
    const cfg  = strategyId ? store.strategyConfigs[strategyId] : null;
    const distId = cfg?.distributionId || 'default';
    return sendViaDistribution(distId, text, cfg?.botId).catch(e => console.warn('[Telegram] clv reminder:', e.message));
  },
};

// ═════════════════════════════════════════════════════════════════════════════
//  ROUTES — БОТЫ
// ═════════════════════════════════════════════════════════════════════════════

router.get('/bots', (req, res) => {
  // Маскируем токены
  res.json(store.bots.map(b => ({
    ...b, token: b.token ? b.token.slice(0, 10) + '••••••' : null,
  })));
});

router.post('/bots', async (req, res) => {
  const { name, token, isDefault } = req.body;
  if (!name || !token) return res.status(400).json({ error: 'name and token required' });
  try {
    const me = await tgCall(token, 'getMe');
    const id = 'bot_' + Date.now();
    if (isDefault) store.bots.forEach(b => { b.isDefault = false; });
    store.bots.push({ id, name, token, botName: me.username, isDefault: !!isDefault, createdAt: new Date().toISOString() });
    res.json({ id, botName: me.username });
  } catch(e) {
    res.status(400).json({ error: 'Telegram error: ' + e.message });
  }
});

router.put('/bots/:id', async (req, res) => {
  const bot = store.bots.find(b => b.id === req.params.id);
  if (!bot) return res.status(404).json({ error: 'Not found' });
  const { name, token, isDefault } = req.body;
  if (name) bot.name = name;
  if (token && token !== bot.token?.slice(0,10)+'••••••') {
    try { const me = await tgCall(token, 'getMe'); bot.token = token; bot.botName = me.username; }
    catch(e) { return res.status(400).json({ error: e.message }); }
  }
  if (isDefault) { store.bots.forEach(b => { b.isDefault = false; }); bot.isDefault = true; }
  res.json({ ok: true });
});

router.delete('/bots/:id', (req, res) => {
  const idx = store.bots.findIndex(b => b.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  if (store.bots[idx].isDefault && store.bots.length > 1) {
    return res.status(400).json({ error: 'Cannot delete default bot. Set another as default first.' });
  }
  store.bots.splice(idx, 1);
  res.json({ ok: true });
});

router.post('/bots/:id/test', async (req, res) => {
  const bot = store.bots.find(b => b.id === req.params.id);
  if (!bot) return res.status(404).json({ error: 'Not found' });
  const { chatId } = req.body;
  const testChatId = chatId || store.recipients.find(r => r.isActive)?.chatId;
  if (!testChatId) return res.status(400).json({ error: 'No chat ID. Provide chatId or add a recipient.' });
  try {
    await sendTo(bot.token, testChatId, `✅ <b>BetQuant Pro</b> — тест бота <b>${bot.name}</b>\n⏱ ${new Date().toLocaleString('ru')}`);
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ═════════════════════════════════════════════════════════════════════════════
//  ROUTES — ПОЛУЧАТЕЛИ
// ═════════════════════════════════════════════════════════════════════════════

router.get('/recipients', (req, res) => res.json(store.recipients));

router.post('/recipients', (req, res) => {
  const { label, chatId, tags } = req.body;
  if (!label || !chatId) return res.status(400).json({ error: 'label and chatId required' });
  const id = 'rec_' + Date.now();
  store.recipients.push({ id, label, chatId, tags: tags || [], isActive: true });
  res.json({ id });
});

router.put('/recipients/:id', (req, res) => {
  const rec = store.recipients.find(r => r.id === req.params.id);
  if (!rec) return res.status(404).json({ error: 'Not found' });
  Object.assign(rec, req.body);
  res.json({ ok: true });
});

router.delete('/recipients/:id', (req, res) => {
  const idx = store.recipients.findIndex(r => r.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  store.recipients.splice(idx, 1);
  // Удалим из рассылок
  store.distributions.forEach(d => {
    d.recipientIds = d.recipientIds.filter(id => id !== req.params.id);
  });
  res.json({ ok: true });
});

// ═════════════════════════════════════════════════════════════════════════════
//  ROUTES — РАССЫЛКИ
// ═════════════════════════════════════════════════════════════════════════════

router.get('/distributions', (req, res) => res.json(store.distributions));

router.post('/distributions', (req, res) => {
  const { name, botId, recipientIds, description } = req.body;
  if (!name || !botId) return res.status(400).json({ error: 'name and botId required' });
  const id = 'dist_' + Date.now();
  store.distributions.push({ id, name, botId, recipientIds: recipientIds || [], description: description || '' });
  res.json({ id });
});

router.put('/distributions/:id', (req, res) => {
  const dist = store.distributions.find(d => d.id === req.params.id);
  if (!dist) return res.status(404).json({ error: 'Not found' });
  Object.assign(dist, req.body);
  res.json({ ok: true });
});

router.delete('/distributions/:id', (req, res) => {
  const idx = store.distributions.findIndex(d => d.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  if (store.distributions[idx].id === 'default' && store.distributions.length === 1) {
    return res.status(400).json({ error: 'Cannot delete the only distribution' });
  }
  store.distributions.splice(idx, 1);
  res.json({ ok: true });
});

// ═════════════════════════════════════════════════════════════════════════════
//  ROUTES — КОНФИГИ СТРАТЕГИЙ
// ═════════════════════════════════════════════════════════════════════════════

router.get('/strategy-configs', (req, res) => res.json(store.strategyConfigs));

router.put('/strategy-configs/:strategyId', (req, res) => {
  const { strategyId } = req.params;
  store.strategyConfigs[strategyId] = { ...store.strategyConfigs[strategyId], ...req.body };
  res.json({ ok: true, config: store.strategyConfigs[strategyId] });
});

router.delete('/strategy-configs/:strategyId', (req, res) => {
  delete store.strategyConfigs[req.params.strategyId];
  res.json({ ok: true });
});

// ═════════════════════════════════════════════════════════════════════════════
//  ROUTES — ФОРМАТЫ
// ═════════════════════════════════════════════════════════════════════════════

router.get('/formats', (req, res) => res.json(store.formats));

router.post('/formats', (req, res) => {
  const { name, template, type } = req.body;
  if (!name || !template) return res.status(400).json({ error: 'name and template required' });
  const id = 'fmt_' + Date.now();
  const fields = (template.match(/\{(\w+)\}/g) || []).map(f => f.slice(1, -1));
  store.formats.push({ id, name, template, type: type || 'any', isDefault: false, fields });
  res.json({ id });
});

router.put('/formats/:id', (req, res) => {
  const fmt = store.formats.find(f => f.id === req.params.id);
  if (!fmt) return res.status(404).json({ error: 'Not found' });
  if (req.body.template) {
    req.body.fields = (req.body.template.match(/\{(\w+)\}/g) || []).map(f => f.slice(1,-1));
  }
  if (req.body.isDefault) {
    store.formats.filter(f => f.type === (req.body.type || fmt.type)).forEach(f => { f.isDefault = false; });
  }
  Object.assign(fmt, req.body);
  res.json({ ok: true });
});

router.delete('/formats/:id', (req, res) => {
  const f = store.formats.find(x => x.id === req.params.id);
  if (!f) return res.status(404).json({ error: 'Not found' });
  if (f.isDefault) return res.status(400).json({ error: 'Cannot delete a default format' });
  store.formats.splice(store.formats.indexOf(f), 1);
  res.json({ ok: true });
});

// ═════════════════════════════════════════════════════════════════════════════
//  ROUTES — ОТПРАВКА
// ═════════════════════════════════════════════════════════════════════════════

/** POST /api/telegram/send */
router.post('/send', async (req, res) => {
  const { text, distributionId, botId, chatId } = req.body;
  if (!text) return res.status(400).json({ error: 'text required' });

  // Прямая отправка на конкретный chatId через дефолтный бот
  if (chatId) {
    const bot = resolveBot(botId);
    if (!bot) return res.status(400).json({ error: 'No bot configured' });
    try {
      await sendTo(bot.token, chatId, text);
      return res.json({ ok: true });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Отправка через рассылку
  try {
    const results = await sendViaDistribution(distributionId || 'default', text, botId);
    res.json({ ok: true, results });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** POST /api/telegram/alert */
router.post('/alert', async (req, res) => {
  const { type, data, strategyId } = req.body;
  if (!type || !data) return res.status(400).json({ error: 'type and data required' });
  try {
    await tgAPI.sendAlert(type, data, strategyId);
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** POST /api/telegram/test */
router.post('/test', async (req, res) => {
  const { distributionId, botId } = req.body;
  try {
    const text = `✅ <b>BetQuant Pro</b> — тест рассылки\n⏱ ${new Date().toLocaleString('ru')}`;
    const results = await sendViaDistribution(distributionId || 'default', text, botId);
    res.json({ ok: true, results });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** GET /api/telegram/status */
router.get('/status', (req, res) => {
  res.json({
    bots:          store.bots.length,
    recipients:    store.recipients.length,
    distributions: store.distributions.length,
    formats:       store.formats.length,
    strategyConfigs: Object.keys(store.strategyConfigs).length,
    defaultBot:    store.bots.find(b => b.isDefault)?.botName || null,
    enabled:       store.bots.some(b => b.token) && store.distributions.some(d => d.recipientIds.length),
  });
});

/** POST /api/telegram/webhook */
router.post('/webhook', express.json(), async (req, res) => {
  res.sendStatus(200);
  const msg = req.body?.message;
  if (!msg?.text) return;
  const bot = store.bots.find(b => b.isDefault);
  if (!bot) return;
  const chatId = msg.chat.id;
  const text   = msg.text.trim().toLowerCase();
  const cmd = async t => { try { await sendTo(bot.token, chatId, t); } catch(e){} };
  if (text === '/start' || text === '/help') {
    await cmd(`🎯 <b>BetQuant Pro Bot</b>\n\n/status — статус\n/help — справка`);
  } else if (text === '/status') {
    await cmd(`✅ BetQuant Pro работает\n⏱ ${new Date().toLocaleString('ru')}`);
  }
});

module.exports = { router, tgAPI, store };