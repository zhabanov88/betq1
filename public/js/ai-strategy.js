'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  AI Strategy Generator — симбиоз прямых вызовов + серверного прокси
//
//  ЛОГИКА МАРШРУТИЗАЦИИ:
//  • ollama / lmstudio  → прямой fetch из браузера (localhost, нет CORS)
//  • anthropic с ключом → прямой fetch api.anthropic.com
//  • google с ключом    → прямой fetch generativelanguage.googleapis.com
//  • openrouter_free    → ТОЛЬКО через /api/ai/strategy (CORS блокирует браузер)
//  • все остальные      → /api/ai/strategy (сервер проксирует безопасно)
//  • нет ключа/ошибка   → localFallback с demo-стратегией
// ═══════════════════════════════════════════════════════════════════════════
const aiStrategy = {
  messages: [],
  generatedCode: '',

  SYSTEM_PROMPT: `You are BetQuant AI — expert sports betting strategy developer and quantitative analyst.
When creating a strategy ALWAYS produce a complete JavaScript evaluate() function inside a \`\`\`javascript block.

function evaluate(match, team, h2h, market) {
  // match: { team_home, team_away, league, date,
  //          odds_home, odds_draw, odds_away, odds_over, odds_under, odds_btts,
  //          prob_home, prob_draw, prob_away }
  // team:  { form(name,n)→['W','D','L'], goalsScored(name,n), goalsConceded(name,n), xG(name,n) }
  // h2h:   { results: [{home, away, home_goals, away_goals, result}] }
  // market:{ implied(odds), value(odds,prob)→edge, kelly(odds,prob)→fraction }
  return { signal: true, market: 'home', stake: 0.02, prob: 0.55 };
  // market: 'home'|'draw'|'away'|'over'|'under'|'btts' — return null to skip
}

Also include: strategy description, parameters to optimize, risk warnings.
Respond in Russian if the question is in Russian.`,

  // ── Читаем активный провайдер из llmSettings ──────────────────────────────
  getActiveCfg() {
    if (typeof llmSettings === 'undefined') {
      return { provider: 'openrouter_free', model: 'meta-llama/llama-4-maverick:free', apiKey: '', baseUrl: '', cfg: null };
    }
    const provider = llmSettings.getActiveProvider();
    const model    = llmSettings.getActiveModel();
    const apiKey   = llmSettings.getApiKey(provider);
    const baseUrl  = llmSettings.getBaseUrl(provider);
    const cfg      = llmSettings.PROVIDERS[provider] || null;
    return { provider, model, apiKey, baseUrl, cfg };
  },

  init() {
    if (typeof llmSettings !== 'undefined') llmSettings.updateAiPanelHeader();
  },

  // ════════════════════════════════════════════════════════════════════════════
  //  SEND
  // ════════════════════════════════════════════════════════════════════════════
  async send() {
    const input = document.getElementById('aiInput')?.value?.trim();
    if (!input) return;
    document.getElementById('aiInput').value = '';
    this.addMessage('user', input);

    const btn = document.getElementById('aiSendIcon');
    if (btn) { btn.textContent = '⏳'; btn.style.pointerEvents = 'none'; }

    const { provider, model, apiKey, baseUrl, cfg } = this.getActiveCfg();
    console.log(`[BetQuant AI] provider=${provider} | model=${model} | hasKey=${!!apiKey}`);

    try {
      const history = this.messages.slice(-7, -1).map(m => ({ role: m.role, content: m.content }));
      const lastMsg = this.messages[this.messages.length - 1]?.content || input;
      let text = '';

      if (provider === 'ollama' || provider === 'lmstudio') {
        // Локальные — всегда прямой вызов
        text = await this.callLocal(provider, model, baseUrl, history, lastMsg);

      } else if (cfg?.format === 'anthropic') {
        // Anthropic — прямой если есть ключ, иначе прокси
        text = apiKey
          ? await this.callAnthropic(model, apiKey, history, lastMsg)
          : await this.callViaProxy(provider, model, apiKey, baseUrl, history, lastMsg);

      } else if (cfg?.format === 'google') {
        // Google — прямой если есть ключ, иначе прокси
        text = apiKey
          ? await this.callGoogle(model, apiKey, history, lastMsg)
          : await this.callViaProxy(provider, model, apiKey, baseUrl, history, lastMsg);

      } else {
        // OpenAI-compatible (OpenRouter, Groq, DeepSeek, Mistral, xAI, OpenAI)
        // Поддерживают CORS — вызываем НАПРЯМУЮ из браузера, без прокси
        text = await this.callOpenAIDirect(provider, model, apiKey, baseUrl, cfg, history, lastMsg);
      }

      if (text) {
        this.addMessage('assistant', text);
        this.extractAndShowCode(text);
      }
    } catch (e) {
      console.error('[BetQuant AI]', e);
      this.addMessage('assistant', `❌ Ошибка (${provider}): ${e.message}`);
    } finally {
      if (btn) { btn.textContent = '▶'; btn.style.pointerEvents = ''; }
    }
  },

  // ════════════════════════════════════════════════════════════════════════════
  //  OpenAI-compatible — прямой вызов из браузера
  //  OpenRouter, Groq, DeepSeek, Mistral, xAI, OpenAI поддерживают CORS
  // ════════════════════════════════════════════════════════════════════════════
  async callOpenAIDirect(provider, model, apiKey, baseUrl, cfg, history, lastMsg) {
    // Определяем URL
    const URLS = {
      openrouter_free: 'https://openrouter.ai/api/v1/chat/completions',
      openrouter:      'https://openrouter.ai/api/v1/chat/completions',
      openai:          'https://api.openai.com/v1/chat/completions',
      deepseek:        'https://api.deepseek.com/v1/chat/completions',
      groq:            'https://api.groq.com/openai/v1/chat/completions',
      xai:             'https://api.x.ai/v1/chat/completions',
      mistral:         'https://api.mistral.ai/v1/chat/completions',
    };

    let url = cfg?.apiUrl || URLS[provider] || '';
    if (!url && baseUrl) url = baseUrl.replace(/\/$/, '') + '/v1/chat/completions';
    if (!url) return this.localFallback();

    const headers = { 'Content-Type': 'application/json' };
    if (apiKey) headers['Authorization'] = 'Bearer ' + apiKey;
    if (provider === 'openrouter_free' || provider === 'openrouter') {
      headers['HTTP-Referer'] = window.location.origin;
      headers['X-Title'] = 'BetQuant Pro';
    }

    const r = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        model,
        max_tokens: 2000,
        messages: [
          { role: 'system', content: this.SYSTEM_PROMPT },
          ...history,
          { role: 'user', content: lastMsg },
        ],
      }),
    });

    const d = await r.json();

    if (!r.ok) {
      // 401 без ключа — показываем fallback с инструкцией
      if (r.status === 401) return this.localFallback();
      const errMsg = d.error?.message || JSON.stringify(d.error) || `HTTP ${r.status}`;
      throw new Error(errMsg);
    }

    return d.choices?.[0]?.message?.content || '';
  },

  // ════════════════════════════════════════════════════════════════════════════
  //  Серверный прокси — fallback когда прямой вызов невозможен
  // ════════════════════════════════════════════════════════════════════════════
  async callViaProxy(provider, model, apiKey, baseUrl, history, lastMsg) {
    const r = await fetch('/api/ai/strategy', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ provider, model, apiKey, baseUrl, message: lastMsg, history }),
    });

    if (r.status === 404) {
      // Сервер старый — пробуем прямой вызов как последний шанс
      const cfg = typeof llmSettings !== 'undefined' ? llmSettings.PROVIDERS[provider] : null;
      return this.callOpenAIDirect(provider, model, apiKey, baseUrl, cfg, history, lastMsg);
    }
    if (r.status === 503) return this.localFallback();

    const d = await r.json();
    if (!r.ok || d.error) throw new Error(d.error || `HTTP ${r.status}`);
    return d.response || '';
  }, 

  // ════════════════════════════════════════════════════════════════════════════
  //  Прямые вызовы (только когда CORS не проблема)
  // ════════════════════════════════════════════════════════════════════════════

  // Anthropic — прямой (только когда есть apiKey в LLM Settings)
  async callAnthropic(model, apiKey, history, lastMsg) {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model,
        max_tokens: 2000,
        system: this.SYSTEM_PROMPT,
        messages: [...history, { role: 'user', content: lastMsg }],
      }),
    });
    const d = await r.json();
    if (!r.ok) {
      // CORS или auth ошибка → падаем на прокси
      if (r.status === 401 || r.status === 403) {
        console.warn('[BetQuant AI] Anthropic direct failed, trying proxy...');
        return this.callViaProxy('anthropic', model, apiKey, '', history, lastMsg);
      }
      throw new Error(d.error?.message || `HTTP ${r.status}`);
    }
    return d.content?.[0]?.text || '';
  },

  // Google Gemini — прямой (только когда есть apiKey)
  async callGoogle(model, apiKey, history, lastMsg) {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        systemInstruction: { parts: [{ text: this.SYSTEM_PROMPT }] },
        contents: [
          ...history.map(m => ({ role: m.role === 'assistant' ? 'model' : 'user', parts: [{ text: m.content }] })),
          { role: 'user', parts: [{ text: lastMsg }] },
        ],
      }),
    });
    const d = await r.json();
    if (!r.ok) {
      // Fallback на прокси при ошибке
      if (r.status === 401 || r.status === 403) {
        console.warn('[BetQuant AI] Google direct failed, trying proxy...');
        return this.callViaProxy('google', model, apiKey, '', history, lastMsg);
      }
      throw new Error(d.error?.message || `HTTP ${r.status}`);
    }
    return d.candidates?.[0]?.content?.parts?.[0]?.text || '';
  },

  // Ollama / LM Studio — прямой (localhost, нет CORS)
  async callLocal(provider, model, baseUrl, history, lastMsg) {
    const defaultBase = provider === 'ollama' ? 'http://localhost:11434' : 'http://localhost:1234';
    const url = (baseUrl || defaultBase).replace(/\/$/, '') + '/v1/chat/completions';
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model,
        max_tokens: 2000,
        messages: [
          { role: 'system', content: this.SYSTEM_PROMPT },
          ...history,
          { role: 'user', content: lastMsg },
        ],
      }),
    });
    if (!r.ok) {
      const d = await r.json().catch(() => ({}));
      throw new Error(d.error?.message || `${provider} недоступен. Убедись что сервер запущен на ${url}`);
    }
    const d = await r.json();
    return d.choices?.[0]?.message?.content || '';
  },

  // ── Demo fallback (ни ключей, ни сервера) ─────────────────────────────────
  localFallback() {
    const code = `// Value Betting Strategy (Demo)
// Настрой провайдер в LLM Settings для AI генерации
function evaluate(match, team, h2h, market) {
  const minEdge = 0.05;
  const homeForm = team.form(match.team_home, 5);
  const awayForm = team.form(match.team_away, 5);
  const homeWins = homeForm.filter(r => r === 'W').length;
  const awayWins = awayForm.filter(r => r === 'W').length;
  const homeProb = 0.3 + homeWins * 0.06 - awayWins * 0.03;
  if (match.odds_home >= 1.6 && match.odds_home <= 4.0) {
    if (market.value(match.odds_home, homeProb) > minEdge)
      return { signal: true, market: 'home', stake: market.kelly(match.odds_home, homeProb) * 0.5, prob: homeProb };
  }
  return null;
}`;
    const el = document.getElementById('aiGeneratedCode');
    if (el) el.value = code;
    this.generatedCode = code;
    return `**Demo стратегия** (AI провайдер не настроен)

Для работы AI выбери один из вариантов в **LLM Settings**:
- **OpenRouter Free** — работает сразу, без ключа
- **Groq** — бесплатный ключ на [console.groq.com](https://console.groq.com/keys)
- **Google Gemini** — бесплатный ключ на [aistudio.google.com](https://aistudio.google.com/app/apikey)
- **Ollama** — локально, полная приватность`;
  },

  // ════════════════════════════════════════════════════════════════════════════
  //  UI helpers
  // ════════════════════════════════════════════════════════════════════════════
  extractAndShowCode(text) {
    const m = text.match(/```(?:javascript|js)\n?([\s\S]*?)```/);
    if (m) {
      this.generatedCode = m[1].trim();
      const el = document.getElementById('aiGeneratedCode');
      if (el) el.value = this.generatedCode;
    }
    const a = document.getElementById('aiAnalysis');
    if (a) {
      a.innerHTML = text.replace(/```[\s\S]*?```/g, '').trim()
        .replace(/\n/g, '<br>')
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        .replace(/`([^`]+)`/g, '<code>$1</code>');
    }
  },

  addMessage(role, content) {
    this.messages.push({ role, content });
    const c = document.getElementById('aiMessages');
    if (!c) return;
    const div = document.createElement('div');
    div.className = 'ai-msg ' + role;
    const html = content
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/\n/g, '<br>')
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/`([^`]+)`/g, '<code>$1</code>');
    div.innerHTML = `<div class="ai-msg-avatar">${role === 'user' ? '👤' : '⬡'}</div>
      <div class="ai-msg-content">
        <strong>${role === 'user' ? 'You' : 'BetQuant AI'}</strong>
        <p>${html}</p>
      </div>`;
    c.appendChild(div);
    c.scrollTop = c.scrollHeight;
  },

  quickPrompts: {
    value:           'Создай стратегию Value Betting на футбол: ищи матчи где implied probability букмекера ниже расчётных вероятностей на основе формы и xG. Half Kelly bankroll management.',
    form:            'Стратегия на форму: ставь Home если 4+ побед из 5 дома, гость проиграл 3+ из 5 в гостях. Коэффициент 1.8–2.8.',
    h2h:             'Стратегия H2H: если одна команда выиграла 4+ из последних 5 встреч, ставь на неё. Только коэф до 2.5.',
    market_movement: 'Стратегия на движение линии: ставь когда Pinnacle срезал коэф более 15% с открытия.',
    xg:              'Стратегия xG Under: ставь Under 2.5 если xG обеих команд за 5 матчей < 1.1 в среднем.',
    poisson:         'Полная стратегия Пуассон: expected goals из attack/defense рейтингов, edge > 5% для ставки.',
    elo:             'ELO система: обновляй рейтинги после матчей, конвертируй в вероятности, ставь при расхождении с рынком.',
    corners:         'Стратегия угловые Over: если сумма средних угловых обеих команд за 5 игр > 11.',
  },

  quickPrompt(type) {
    const p = this.quickPrompts[type];
    if (p) { document.getElementById('aiInput').value = p; this.send(); }
  },

  copyCode() {
    if (this.generatedCode) navigator.clipboard.writeText(this.generatedCode).catch(() => {});
  },

  sendToBuilder() {
    if (!this.generatedCode) return;
    const el = document.getElementById('strategyCode');
    if (el) el.value = this.generatedCode;
    app.showPanel('strategy');
    if (typeof strategyBuilder !== 'undefined') strategyBuilder.showTab('code');
  },

  runBacktest() {
    if (!this.generatedCode) return;
    if (typeof backtestEngine !== 'undefined' && backtestEngine.activeStrategies?.length) {
      backtestEngine.activeStrategies[0].code = this.generatedCode;
      backtestEngine.saveActiveStrategies();
      backtestEngine.renderStrategySlots();
    }
    app.showPanel('backtest');
  },
};