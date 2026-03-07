'use strict';
/**
 * BetQuant Pro — Telegram Bot & Push Alerts  /api/telegram/*
 *
 * Поддерживает два режима:
 *  1. Webhook (production) — Telegram шлёт POST на /api/telegram/webhook
 *  2. Polling  (dev)       — сервер сам опрашивает getUpdates каждые 3 сек
 *
 * Endpoints:
 *  POST /api/telegram/setup          — сохранить token + chat_id, настроить webhook
 *  POST /api/telegram/send           — отправить произвольное сообщение
 *  POST /api/telegram/alert          — отправить форматированный алерт
 *  GET  /api/telegram/status         — статус бота и конфигурации
 *  POST /api/telegram/test           — тест-сообщение
 *  POST /api/telegram/webhook        — входящий вебхук от Telegram
 *
 * Алерты интегрированы с:
 *  • Value Finder  → сигнал при edge% > порога
 *  • Live Monitor  → in-play сигнал выше порога уверенности
 *  • CLV Tracker   → напоминание о pending closing odds
 *  • Neural        → точность модели изменилась
 */

const express = require('express');
const router  = express.Router();

// ─── Config store (память; PostgreSQL при наличии) ────────────────────────
let CFG = {
  token:   process.env.TELEGRAM_BOT_TOKEN || '',
  chatId:  process.env.TELEGRAM_CHAT_ID   || '',
  enabled: !!(process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID),
  webhookUrl: '',
  alerts: {
    valueEdge:    { enabled: true, minEdge: 5 },
    liveSignal:   { enabled: true, minConf: 65 },
    clvPending:   { enabled: true, hoursOld: 24 },
    neuralRetrain:{ enabled: true },
    oddsSpike:    { enabled: true, minDrop: 10 },
  },
};

// Очередь сообщений для de-duplication (не спамим одинаковыми сигналами)
const sentCache = new Map(); // key → timestamp
const DEDUP_TTL = 60 * 60 * 1000; // 1 час

// ─── Telegram API helpers ─────────────────────────────────────────────────
function tgUrl(method) {
  return `https://api.telegram.org/bot${CFG.token}/${method}`;
}

async function tgCall(method, body = {}) {
  if (!CFG.token) throw new Error('Telegram token not configured');
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 10000);
  try {
    const r = await fetch(tgUrl(method), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
    const d = await r.json();
    if (!d.ok) throw new Error(d.description || 'Telegram API error');
    return d.result;
  } finally {
    clearTimeout(t);
  }
}

async function sendMessage(chatId, text, extra = {}) {
  return tgCall('sendMessage', {
    chat_id:    chatId || CFG.chatId,
    text,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    ...extra,
  });
}

// ─── Message formatters ───────────────────────────────────────────────────
function fmtValue(bet) {
  return `💎 <b>VALUE BET SIGNAL</b>

⚽ <b>${bet.match}</b>
🏆 ${bet.league}
📊 Рынок: <b>${bet.label}</b>
📈 Коэффициент: <b>${bet.odds}</b>

🔵 Implied: ${bet.impliedProb}%
🟢 Model:   <b>${bet.modelProb}%</b>
✅ Edge:    <b>+${bet.edge}%</b>
📐 Kelly:   ${bet.kelly}%

λ Хозяева: ${bet.lH || '?'} / Гости: ${bet.lA || '?'}
🤖 Поisson+ELO Ensemble`;
}

function fmtLive(signal, matchName) {
  const top = signal.topSignal;
  if (!top) return null;
  return `🔴 <b>IN-PLAY SIGNAL</b>

⚽ <b>${matchName}</b>
⏱ Минута: ${signal.minute}' | Счёт: ${signal.score}

📣 ${top.label}
💪 Уверенность: <b>${top.confidence.toFixed(0)}%</b>
📊 Рынок: ${top.market}${top.odds ? ` @ <b>${(+top.odds).toFixed(2)}</b>` : ''}

${top.rationale}

⚡ Давление Д/Г: ${signal.pressureIndex?.home}/${signal.pressureIndex?.away}
⚠️ Риск: ${signal.riskLevel === 'high' ? '🔴 Высокий' : signal.riskLevel === 'medium' ? '🟡 Средний' : '🟢 Низкий'}`;
}

function fmtOddsSpike(data) {
  return `📉 <b>ODDS MOVEMENT ALERT</b>

⚽ <b>${data.match}</b>
📊 ${data.market}

⬆️ Открытие:  <b>${data.openOdds}</b>
⬇️ Сейчас:    <b>${data.currentOdds}</b>
📉 Движение:  <b>${data.dropPct > 0 ? '+' : ''}${data.dropPct}%</b>

${Math.abs(data.dropPct) >= 15 ? '🚨 Значительное движение — возможна инсайдерская информация!' : ''}`;
}

function fmtCLVReminder(bets) {
  const list = bets.slice(0, 5).map(b =>
    `• ${b.match_name || b.match} @ <b>${(+b.bet_odds).toFixed(2)}</b> (${(b.bet_date || '').slice(0, 10)})`
  ).join('\n');
  return `⏰ <b>CLV REMINDER</b>

У тебя ${bets.length} ставок без closing odds:

${list}${bets.length > 5 ? `\n...и ещё ${bets.length - 5}` : ''}

Открой CLV Tracker → закрой ставки, пока помнишь!`;
}

function fmtNeural(data) {
  return `🧠 <b>NEURAL RETRAIN COMPLETE</b>

🏆 Спорт: <b>${data.sport}</b>
📊 Точность: <b>${data.accuracy}%</b>
📋 Обучено на: ${data.rowsUsed} матчах
⏱ Время: ${new Date().toLocaleTimeString('ru')}

${data.accuracy >= 70 ? '✅ Модель готова к использованию' : '⚠️ Точность ниже 70% — нужно больше данных'}`;
}

// ─── Dedup helper ─────────────────────────────────────────────────────────
function shouldSend(key) {
  const last = sentCache.get(key);
  if (last && Date.now() - last < DEDUP_TTL) return false;
  sentCache.set(key, Date.now());
  return true;
}

// ─── Public API (вызывается из других модулей) ────────────────────────────
const tgAPI = {
  isEnabled() { return CFG.enabled && !!CFG.token && !!CFG.chatId; },

  async sendValueAlert(bet) {
    if (!this.isEnabled()) return;
    if (!CFG.alerts.valueEdge.enabled) return;
    if (bet.edge < CFG.alerts.valueEdge.minEdge) return;
    const key = `value_${bet.match}_${bet.market}`;
    if (!shouldSend(key)) return;
    try {
      await sendMessage(CFG.chatId, fmtValue(bet));
      console.log(`[Telegram] Value alert sent: ${bet.match} +${bet.edge}%`);
    } catch(e) { console.warn('[Telegram] value alert error:', e.message); }
  },

  async sendLiveSignal(signal, matchName) {
    if (!this.isEnabled()) return;
    if (!CFG.alerts.liveSignal.enabled) return;
    if (!signal?.topSignal) return;
    if (signal.topSignal.confidence < CFG.alerts.liveSignal.minConf) return;
    const key = `live_${signal.matchId}_${signal.minute}`;
    if (!shouldSend(key)) return;
    const text = fmtLive(signal, matchName);
    if (!text) return;
    try {
      await sendMessage(CFG.chatId, text);
      console.log(`[Telegram] Live signal sent: ${matchName}`);
    } catch(e) { console.warn('[Telegram] live signal error:', e.message); }
  },

  async sendOddsSpike(data) {
    if (!this.isEnabled()) return;
    if (!CFG.alerts.oddsSpike.enabled) return;
    if (Math.abs(data.dropPct) < CFG.alerts.oddsSpike.minDrop) return;
    const key = `odds_${data.match}_${Math.floor(Date.now() / 3600000)}`;
    if (!shouldSend(key)) return;
    try {
      await sendMessage(CFG.chatId, fmtOddsSpike(data));
    } catch(e) { console.warn('[Telegram] odds spike error:', e.message); }
  },

  async sendNeuralRetrain(data) {
    if (!this.isEnabled()) return;
    if (!CFG.alerts.neuralRetrain.enabled) return;
    try {
      await sendMessage(CFG.chatId, fmtNeural(data));
    } catch(e) { console.warn('[Telegram] neural alert error:', e.message); }
  },

  async sendCLVReminder(bets) {
    if (!this.isEnabled()) return;
    if (!CFG.alerts.clvPending.enabled) return;
    if (!bets?.length) return;
    const key = `clv_reminder_${new Date().toDateString()}`;
    if (!shouldSend(key)) return;
    try {
      await sendMessage(CFG.chatId, fmtCLVReminder(bets));
    } catch(e) { console.warn('[Telegram] CLV reminder error:', e.message); }
  },
};

// ─── Scheduled CLV reminder (раз в день) ─────────────────────────────────
setInterval(async () => {
  if (!tgAPI.isEnabled()) return;
  const pgPool = global.__betquant_pg;
  if (!pgPool) return;
  try {
    const r = await pgPool.query(
      `SELECT * FROM clv_bets WHERE closing_odds IS NULL AND bet_date < NOW() - INTERVAL '${CFG.alerts.clvPending.hoursOld} hours' LIMIT 20`
    );
    if (r.rows.length) await tgAPI.sendCLVReminder(r.rows);
  } catch(e) {}
}, 24 * 60 * 60 * 1000);

// ─── Routes ───────────────────────────────────────────────────────────────

/** POST /api/telegram/setup */
router.post('/setup', async (req, res) => {
  const { token, chatId, webhookUrl, alerts } = req.body;
  if (!token || !chatId) return res.status(400).json({ error: 'token and chatId required' });

  CFG.token   = token;
  CFG.chatId  = chatId;
  CFG.enabled = true;
  if (alerts) Object.assign(CFG.alerts, alerts);

  // Persist to env-like store (если pgPool есть)
  const pg = req.app.locals.pgPool;
  if (pg) {
    try {
      await pg.query(
        `INSERT INTO app_settings(key, value) VALUES('telegram_config', $1)
         ON CONFLICT(key) DO UPDATE SET value=$1`,
        [JSON.stringify({ token, chatId, alerts: CFG.alerts })]
      );
    } catch(e) { /* таблица может не существовать */ }
  }

  // Set webhook if URL provided
  if (webhookUrl) {
    try {
      await tgCall('setWebhook', { url: `${webhookUrl}/api/telegram/webhook` });
      CFG.webhookUrl = webhookUrl;
    } catch(e) { return res.json({ ok: true, warning: `Webhook setup failed: ${e.message}` }); }
  }

  // Test connection
  try {
    const me = await tgCall('getMe');
    res.json({ ok: true, botName: me.username, chatId, webhookSet: !!webhookUrl });
  } catch(e) {
    CFG.enabled = false;
    res.status(400).json({ error: 'Telegram connection failed: ' + e.message });
  }
});

/** GET /api/telegram/status */
router.get('/status', async (req, res) => {
  const base = {
    configured: !!CFG.token,
    enabled:    CFG.enabled,
    chatId:     CFG.chatId ? CFG.chatId.slice(0, 4) + '****' : null,
    webhookUrl: CFG.webhookUrl || null,
    alerts:     CFG.alerts,
  };
  if (!CFG.token) return res.json({ ...base, botName: null });
  try {
    const me = await tgCall('getMe');
    res.json({ ...base, botName: me.username, botId: me.id });
  } catch(e) {
    res.json({ ...base, error: e.message });
  }
});

/** POST /api/telegram/send — произвольное сообщение */
router.post('/send', async (req, res) => {
  const { text, chatId } = req.body;
  if (!text) return res.status(400).json({ error: 'text required' });
  if (!CFG.token) return res.status(400).json({ error: 'Telegram not configured' });
  try {
    const r = await sendMessage(chatId || CFG.chatId, text);
    res.json({ ok: true, messageId: r.message_id });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** POST /api/telegram/alert — форматированный алерт */
router.post('/alert', async (req, res) => {
  const { type, data } = req.body;
  if (!type || !data) return res.status(400).json({ error: 'type and data required' });

  try {
    switch (type) {
      case 'value':   await tgAPI.sendValueAlert(data);          break;
      case 'live':    await tgAPI.sendLiveSignal(data.signal, data.matchName); break;
      case 'odds':    await tgAPI.sendOddsSpike(data);           break;
      case 'neural':  await tgAPI.sendNeuralRetrain(data);       break;
      case 'clv':     await tgAPI.sendCLVReminder(data.bets);    break;
      default: return res.status(400).json({ error: `Unknown alert type: ${type}` });
    }
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** POST /api/telegram/test */
router.post('/test', async (req, res) => {
  if (!CFG.token || !CFG.chatId)
    return res.status(400).json({ error: 'Configure Telegram first (token + chatId)' });
  try {
    await sendMessage(CFG.chatId,
      `✅ <b>BetQuant Pro</b> — тест успешен!\n\nВремя: ${new Date().toLocaleString('ru')}\n\nАлерты настроены и работают 🎯`
    );
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** PATCH /api/telegram/alerts — обновить настройки алертов */
router.patch('/alerts', (req, res) => {
  const { alerts } = req.body;
  if (alerts) {
    for (const [key, val] of Object.entries(alerts)) {
      if (CFG.alerts[key]) Object.assign(CFG.alerts[key], val);
    }
  }
  res.json({ ok: true, alerts: CFG.alerts });
});

/** POST /api/telegram/webhook — Telegram Webhook endpoint */
router.post('/webhook', express.json(), async (req, res) => {
  res.sendStatus(200); // Всегда 200 сразу

  const upd = req.body;
  const msg = upd?.message;
  if (!msg?.text) return;

  const chatId = msg.chat.id;
  const text   = msg.text.trim().toLowerCase();

  const cmd = async (reply) => {
    try { await sendMessage(chatId, reply); } catch(e) {}
  };

  if (text === '/start' || text === '/help') {
    await cmd(`🎯 <b>BetQuant Pro Bot</b>

Доступные команды:
/status — статус платформы
/value  — последние value ставки
/live   — активные матчи
/clv    — CLV статистика
/help   — эта справка`);
  } else if (text === '/status') {
    await cmd(`✅ BetQuant Pro работает\n⏱ ${new Date().toLocaleString('ru')}`);
  } else if (text === '/value') {
    await cmd('💎 Открой Value Finder в приложении: /app#value');
  } else if (text === '/live') {
    await cmd('🔴 Открой Live Monitor в приложении: /app#live');
  } else if (text === '/clv') {
    await cmd('📐 Открой CLV Tracker в приложении: /app#clv');
  }
});

module.exports = { router, tgAPI };