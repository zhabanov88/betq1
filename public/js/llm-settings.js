'use strict';

// ═══════════════════════════════════════════════════════════════════════
//  LLM Settings — полная страница настройки провайдеров и моделей
//  Бесплатная модель по умолчанию: Gemini 2.0 Flash (Google AI Studio)
//  без ключа — OpenRouter :nitro (бесплатный tier)
// ═══════════════════════════════════════════════════════════════════════

const llmSettings = {

  // ── Полный реестр провайдеров ────────────────────────────────────────────
  PROVIDERS: {
    openrouter_free: {
      label: 'OpenRouter Free',
      badge: 'FREE',
      badgeColor: '#00e676',
      icon: '🆓',
      description: 'Бесплатные модели без API ключа. Работает сразу.',
      apiUrl: 'https://openrouter.ai/api/v1/chat/completions',
      noKey: true,
      format: 'openai',
      models: [
        { id: 'openrouter/free',                        label: '🔀 Auto Free Router (рекомендуется)' },
        { id: 'google/gemma-3-27b-it:free',             label: 'Gemma 3 27B (Free)' },
        { id: 'google/gemma-3-12b-it:free',             label: 'Gemma 3 12B (Free)' },
        { id: 'meta-llama/llama-3.3-70b-instruct:free', label: 'Llama 3.3 70B (Free)' },
        { id: 'mistralai/mistral-7b-instruct:free',     label: 'Mistral 7B (Free)' },
        { id: 'qwen/qwen3-8b:free',                     label: 'Qwen3 8B (Free)' },
        { id: 'qwen/qwen3-14b:free',                    label: 'Qwen3 14B (Free)' },
        { id: 'microsoft/phi-4:free',                   label: 'Phi-4 (Free)' },
        { id: 'deepseek/deepseek-r1:free',              label: 'DeepSeek R1 (Free)' },
      ],
      keyHint: 'Ключ не нужен. Опционально: openrouter.ai/keys для увеличения лимитов',
      setupUrl: 'https://openrouter.ai',
    },
    anthropic: {
      label: 'Anthropic',
      badge: 'PAID',
      badgeColor: '#fb923c',
      icon: '🟣',
      description: 'Claude — лучшие модели для кода и анализа.',
      apiUrl: 'https://api.anthropic.com/v1/messages',
      format: 'anthropic',
      keyPlaceholder: 'sk-ant-api03-...',
      keyHint: 'console.anthropic.com → API Keys',
      setupUrl: 'https://console.anthropic.com',
      models: [
        { id: 'claude-sonnet-4-20250514',   label: 'Claude Sonnet 4 ⭐' },
        { id: 'claude-opus-4-20250514',     label: 'Claude Opus 4 (top)' },
        { id: 'claude-haiku-4-5-20251001',  label: 'Claude Haiku 4.5 (fast)' },
        { id: 'claude-3-5-sonnet-20241022', label: 'Claude 3.5 Sonnet' },
      ],
    },
    openai: {
      label: 'OpenAI',
      badge: 'PAID',
      badgeColor: '#fb923c',
      icon: '🟢',
      description: 'GPT-4o, o1, o3 — самые популярные модели.',
      apiUrl: 'https://api.openai.com/v1/chat/completions',
      format: 'openai',
      keyPlaceholder: 'sk-proj-...',
      keyHint: 'platform.openai.com → API Keys',
      setupUrl: 'https://platform.openai.com/api-keys',
      models: [
        { id: 'gpt-4o',          label: 'GPT-4o ⭐' },
        { id: 'gpt-4o-mini',     label: 'GPT-4o mini (дешевле)' },
        { id: 'o1',              label: 'o1 (рассуждение)' },
        { id: 'o3-mini',         label: 'o3-mini (рассуждение)' },
        { id: 'gpt-4-turbo',     label: 'GPT-4 Turbo' },
      ],
    },
    google: {
      label: 'Google Gemini',
      badge: 'FREE tier',
      badgeColor: '#ffd740',
      icon: '🔵',
      description: 'Gemini 2.0 Flash — бесплатный tier с ключом.',
      format: 'google',
      keyPlaceholder: 'AIza...',
      keyHint: 'aistudio.google.com → Get API key (бесплатно)',
      setupUrl: 'https://aistudio.google.com/app/apikey',
      models: [
        { id: 'gemini-2.0-flash',          label: 'Gemini 2.0 Flash ⭐ (free tier)' },
        { id: 'gemini-2.5-pro-preview-03-25', label: 'Gemini 2.5 Pro' },
        { id: 'gemini-2.0-flash-thinking',  label: 'Gemini 2.0 Flash Thinking' },
        { id: 'gemini-1.5-pro',             label: 'Gemini 1.5 Pro' },
        { id: 'gemini-1.5-flash',           label: 'Gemini 1.5 Flash (free tier)' },
      ],
    },
    deepseek: {
      label: 'DeepSeek',
      badge: 'CHEAP',
      badgeColor: '#00d4ff',
      icon: '🔷',
      description: 'DeepSeek V3/R1 — очень дешевле GPT-4, сопоставимое качество.',
      apiUrl: 'https://api.deepseek.com/v1/chat/completions',
      format: 'openai',
      keyPlaceholder: 'sk-...',
      keyHint: 'platform.deepseek.com → API Keys',
      setupUrl: 'https://platform.deepseek.com',
      models: [
        { id: 'deepseek-chat',     label: 'DeepSeek V3 ⭐' },
        { id: 'deepseek-reasoner', label: 'DeepSeek R1 (рассуждение)' },
      ],
    },
    xai: {
      label: 'xAI (Grok)',
      badge: 'PAID',
      badgeColor: '#fb923c',
      icon: '⚫',
      description: 'Grok 3 от xAI — сильный в рассуждениях.',
      apiUrl: 'https://api.x.ai/v1/chat/completions',
      format: 'openai',
      keyPlaceholder: 'xai-...',
      keyHint: 'console.x.ai → API Keys',
      setupUrl: 'https://console.x.ai',
      models: [
        { id: 'grok-3',       label: 'Grok 3 ⭐' },
        { id: 'grok-3-mini',  label: 'Grok 3 Mini' },
        { id: 'grok-2',       label: 'Grok 2' },
      ],
    },
    mistral: {
      label: 'Mistral AI',
      badge: 'CHEAP',
      badgeColor: '#00d4ff',
      icon: '🟠',
      description: 'Mistral Large и Codestral — отличны для кода.',
      apiUrl: 'https://api.mistral.ai/v1/chat/completions',
      format: 'openai',
      keyPlaceholder: 'your-key...',
      keyHint: 'console.mistral.ai → API Keys',
      setupUrl: 'https://console.mistral.ai',
      models: [
        { id: 'mistral-large-latest',  label: 'Mistral Large ⭐' },
        { id: 'codestral-latest',      label: 'Codestral (код)' },
        { id: 'mistral-small-latest',  label: 'Mistral Small (fast)' },
        { id: 'open-mistral-nemo',     label: 'Mistral Nemo' },
      ],
    },
    openrouter: {
      label: 'OpenRouter',
      badge: 'AGGREGATOR',
      badgeColor: '#c084fc',
      icon: '🔀',
      description: 'Доступ к 200+ моделям через один ключ.',
      apiUrl: 'https://openrouter.ai/api/v1/chat/completions',
      format: 'openai',
      keyPlaceholder: 'sk-or-v1-...',
      keyHint: 'openrouter.ai/keys',
      setupUrl: 'https://openrouter.ai/keys',
      models: [
        { id: 'anthropic/claude-sonnet-4',      label: 'Claude Sonnet 4' },
        { id: 'openai/gpt-4o',                  label: 'GPT-4o' },
        { id: 'google/gemini-2.0-flash',        label: 'Gemini 2.0 Flash' },
        { id: 'deepseek/deepseek-chat-v3-0324', label: 'DeepSeek V3' },
        { id: 'meta-llama/llama-4-maverick',    label: 'Llama 4 Maverick' },
        { id: 'mistralai/mistral-large',        label: 'Mistral Large' },
        { id: 'x-ai/grok-3',                   label: 'Grok 3' },
        { id: 'qwen/qwen3-235b-a22b',           label: 'Qwen3 235B' },
      ],
    },
    groq: {
      label: 'Groq',
      badge: 'FAST',
      badgeColor: '#00e676',
      icon: '⚡',
      description: 'Groq — самый быстрый inference, бесплатный tier.',
      apiUrl: 'https://api.groq.com/openai/v1/chat/completions',
      format: 'openai',
      keyPlaceholder: 'gsk_...',
      keyHint: 'console.groq.com → API Keys (бесплатный tier)',
      setupUrl: 'https://console.groq.com/keys',
      models: [
        { id: 'llama-3.3-70b-versatile',   label: 'Llama 3.3 70B ⭐ (free)' },
        { id: 'llama-3.1-8b-instant',      label: 'Llama 3.1 8B (ultrafast)' },
        { id: 'deepseek-r1-distill-llama-70b', label: 'DeepSeek R1 70B' },
        { id: 'mixtral-8x7b-32768',        label: 'Mixtral 8x7B' },
        { id: 'gemma2-9b-it',              label: 'Gemma2 9B' },
      ],
    },
    ollama: {
      label: 'Ollama (Local)',
      badge: 'LOCAL',
      badgeColor: '#8892a4',
      icon: '🖥️',
      description: 'Локальные модели. Полная приватность, нет лимитов.',
      format: 'openai',
      noKey: true,
      customUrl: true,
      defaultUrl: 'http://localhost:11434',
      keyHint: 'Установи: ollama.ai → ollama serve → ollama pull llama3.2',
      setupUrl: 'https://ollama.ai',
      models: [
        { id: 'llama3.2:latest',        label: 'Llama 3.2 3B (быстро)' },
        { id: 'llama3.1:8b',            label: 'Llama 3.1 8B' },
        { id: 'llama3.1:70b',           label: 'Llama 3.1 70B (нужно 40GB RAM)' },
        { id: 'deepseek-r1:7b',         label: 'DeepSeek R1 7B' },
        { id: 'deepseek-r1:32b',        label: 'DeepSeek R1 32B' },
        { id: 'qwen2.5-coder:7b',       label: 'Qwen 2.5 Coder 7B' },
        { id: 'qwen2.5-coder:32b',      label: 'Qwen 2.5 Coder 32B' },
        { id: 'mistral:latest',         label: 'Mistral 7B' },
        { id: 'phi4:latest',            label: 'Phi-4 14B' },
        { id: 'gemma3:12b',             label: 'Gemma 3 12B' },
      ],
    },
    lmstudio: {
      label: 'LM Studio (Local)',
      badge: 'LOCAL',
      badgeColor: '#8892a4',
      icon: '🖥️',
      description: 'LM Studio с GUI. Загружай любую GGUF модель.',
      format: 'openai',
      noKey: true,
      customUrl: true,
      defaultUrl: 'http://localhost:1234',
      keyHint: 'lmstudio.ai → Download → Load model → Start server',
      setupUrl: 'https://lmstudio.ai',
      models: [
        { id: 'local-model', label: 'Автоопределение загруженной модели' },
      ],
    },
    custom: {
      label: 'Custom URL',
      badge: 'CUSTOM',
      badgeColor: '#8892a4',
      icon: '⚙️',
      description: 'Любой OpenAI-совместимый API: vLLM, Together, Fireworks, Perplexity...',
      format: 'openai',
      customUrl: true,
      keyPlaceholder: 'API ключ (если нужен)',
      keyHint: 'Любой сервис с /v1/chat/completions',
      models: [
        { id: 'custom', label: 'Укажи модель вручную' },
      ],
    },
  },

  DEFAULT_PROVIDER: 'openrouter_free',
  DEFAULT_MODEL: 'openrouter/free',

  // ── Инициализация ────────────────────────────────────────────────────────
  init() {
    this.renderProviderCards();
    this.loadActive();
    this.syncToAiStrategy();
  },

  // ── Текущие настройки ─────────────────────────────────────────────────────
  getSettings() {
    return JSON.parse(localStorage.getItem('bq_llm_settings') || '{}');
  },
  saveSettings(s) {
    localStorage.setItem('bq_llm_settings', JSON.stringify(s));
    this.syncToAiStrategy();
  },

  getActiveProvider() {
    return this.getSettings().activeProvider || this.DEFAULT_PROVIDER;
  },
  getActiveModel() {
    const s = this.getSettings();
    const p = s.activeProvider || this.DEFAULT_PROVIDER;
    return s[`model_${p}`] || this.DEFAULT_MODEL;
  },
  getApiKey(provider) {
    return this.getSettings()[`key_${provider}`] || '';
  },
  getBaseUrl(provider) {
    const s = this.getSettings();
    return s[`url_${provider}`] || this.PROVIDERS[provider]?.defaultUrl || '';
  },

  // ── Синхронизация в aiStrategy ─────────────────────────────────────────
  syncToAiStrategy() {
    if (typeof aiStrategy === 'undefined') return;
    const p = this.getActiveProvider();
    aiStrategy._activeProvider = p;
    aiStrategy._activeModel    = this.getActiveModel();
    aiStrategy._apiKey         = this.getApiKey(p);
    aiStrategy._baseUrl        = this.getBaseUrl(p);
    aiStrategy._providerCfg    = this.PROVIDERS[p];
  },

  // ── Загрузить активный провайдер на UI ────────────────────────────────────
  loadActive() {
    const s  = this.getSettings();
    const ap = s.activeProvider || this.DEFAULT_PROVIDER;

    // Подсветить карточку
    document.querySelectorAll('.llm-provider-card').forEach(c => {
      c.classList.toggle('active', c.dataset.provider === ap);
    });

    // Заполнить правую панель
    this.openProvider(ap);
  },

  // ── Рендер карточек провайдеров ──────────────────────────────────────────
  renderProviderCards() {
    const grid = document.getElementById('llmProviderGrid');
    if (!grid) return;
    const s = this.getSettings();
    const active = s.activeProvider || this.DEFAULT_PROVIDER;

    grid.innerHTML = Object.entries(this.PROVIDERS).map(([id, p]) => {
      const hasKey = p.noKey || !!s[`key_${id}`];
      const isActive = id === active;
      return `
      <div class="llm-provider-card ${isActive ? 'active' : ''}" data-provider="${id}"
           onclick="llmSettings.openProvider('${id}')">
        <div class="llm-card-top">
          <span class="llm-card-icon">${p.icon}</span>
          <div class="llm-card-name">${p.label}</div>
          <span class="llm-badge" style="background:${p.badgeColor}20;color:${p.badgeColor};border-color:${p.badgeColor}40">${p.badge}</span>
        </div>
        <div class="llm-card-desc">${p.description}</div>
        <div class="llm-card-status">
          ${isActive ? '<span class="llm-status-active">● Active</span>' : ''}
          ${hasKey && !p.noKey ? '<span class="llm-status-key">🔑 Key set</span>' : ''}
          ${p.noKey ? '<span class="llm-status-free">✓ No key needed</span>' : ''}
        </div>
      </div>`;
    }).join('');
  },

  // ── Открыть панель настройки провайдера ──────────────────────────────────
  openProvider(id) {
    const p = this.PROVIDERS[id];
    if (!p) return;
    const s = this.getSettings();

    // Подсветка карточки
    document.querySelectorAll('.llm-provider-card').forEach(c =>
      c.classList.toggle('active', c.dataset.provider === id));

    const panel = document.getElementById('llmProviderDetail');
    if (!panel) return;

    const isActive = (s.activeProvider || this.DEFAULT_PROVIDER) === id;
    const savedKey = s[`key_${id}`] || '';
    const savedUrl = s[`url_${id}`] || p.defaultUrl || '';
    const savedModel = s[`model_${id}`] || p.models[0]?.id || '';
    const savedCustomModel = s[`customModel_${id}`] || '';

    panel.innerHTML = `
      <div class="llm-detail-header">
        <span class="llm-detail-icon">${p.icon}</span>
        <div>
          <div class="llm-detail-title">${p.label}
            <span class="llm-badge" style="background:${p.badgeColor}20;color:${p.badgeColor};border-color:${p.badgeColor}40">${p.badge}</span>
          </div>
          <div class="llm-detail-desc">${p.description}</div>
        </div>
      </div>

      ${!p.noKey ? `
      <div class="llm-field-group">
        <label class="llm-field-label">API Key</label>
        <div style="display:flex;gap:8px">
          <input type="password" class="ctrl-input" id="llmKeyInput"
            placeholder="${p.keyPlaceholder || 'Paste API key...'}"
            value="${savedKey}"
            oninput="llmSettings.onKeyInput('${id}', this.value)">
          <button class="ctrl-btn sm" onclick="llmSettings.testConnection('${id}')">Test</button>
        </div>
        <div class="llm-field-hint">
          ${p.keyHint}
          ${p.setupUrl ? ` — <a href="${p.setupUrl}" target="_blank" style="color:var(--accent)">Открыть →</a>` : ''}
        </div>
        <div class="llm-test-result" id="llmTestResult_${id}"></div>
      </div>
      ` : `
      <div class="llm-field-group">
        <div class="llm-free-note">✅ Ключ не нужен — работает сразу</div>
        <div class="llm-field-hint">${p.keyHint}
          ${p.setupUrl ? ` — <a href="${p.setupUrl}" target="_blank" style="color:var(--accent)">Сайт →</a>` : ''}
        </div>
        <div style="margin-top:10px">
          <label class="llm-field-label" style="color:var(--text3);font-size:11px">API Key (опционально — для увеличения лимитов)</label>
          <div style="display:flex;gap:8px;margin-top:4px">
            <input type="password" class="ctrl-input" id="llmKeyInput"
              placeholder="${p.keyPlaceholder || 'sk-or-... (необязательно)'}"
              value="${savedKey}"
              oninput="llmSettings.onKeyInput('${id}', this.value)">
            <button class="ctrl-btn sm" onclick="llmSettings.testConnection('${id}')">Test</button>
          </div>
        </div>
      </div>
      `}

      ${p.customUrl ? `
      <div class="llm-field-group">
        <label class="llm-field-label">Base URL</label>
        <input type="text" class="ctrl-input" id="llmUrlInput"
          placeholder="${p.defaultUrl || 'http://...'}"
          value="${savedUrl}"
          oninput="llmSettings.onUrlInput('${id}', this.value)">
        <div class="llm-field-hint">URL сервера. Оставь пустым для значения по умолчанию.</div>
      </div>
      ` : ''}

      <div class="llm-field-group">
        <label class="llm-field-label">Модель</label>
        <select class="ctrl-select" id="llmModelSelect" onchange="llmSettings.onModelChange('${id}', this.value)">
          ${p.models.map(m => `<option value="${m.id}" ${m.id === savedModel ? 'selected' : ''}>${m.label}</option>`).join('')}
        </select>
      </div>

      ${id === 'custom' || id === 'ollama' ? `
      <div class="llm-field-group">
        <label class="llm-field-label">Название модели (вручную)</label>
        <input type="text" class="ctrl-input" id="llmCustomModelInput"
          placeholder="например: llama3.2:latest или gpt-4o"
          value="${savedCustomModel}"
          oninput="llmSettings.onCustomModelInput('${id}', this.value)">
        <div class="llm-field-hint">Переопределяет выбор выше</div>
      </div>
      ` : ''}

      <div class="llm-activate-row">
        <button class="ctrl-btn primary ${isActive ? 'active-provider-btn' : ''}"
          onclick="llmSettings.setActive('${id}')" id="llmActivateBtn">
          ${isActive ? '✅ Активный провайдер' : '▶ Использовать этот провайдер'}
        </button>
        ${isActive ? '<span style="color:var(--green);font-size:12px">Используется в AI Strategy</span>' : ''}
      </div>

      <div class="llm-models-info">
        <div class="llm-models-info-title">Все доступные модели</div>
        ${p.models.map(m => `
          <div class="llm-model-row ${m.id === savedModel ? 'selected' : ''}"
               onclick="document.getElementById('llmModelSelect').value='${m.id}';llmSettings.onModelChange('${id}','${m.id}')">
            <span>${m.label}</span>
          </div>`).join('')}
      </div>
    `;
  },

  // ── Handlers ──────────────────────────────────────────────────────────────
  onKeyInput(providerId, val) {
    const s = this.getSettings();
    s[`key_${providerId}`] = val;
    this.saveSettings(s);
  },

  onUrlInput(providerId, val) {
    const s = this.getSettings();
    s[`url_${providerId}`] = val;
    this.saveSettings(s);
  },

  onModelChange(providerId, val) {
    const s = this.getSettings();
    s[`model_${providerId}`] = val;
    this.saveSettings(s);
    // Обновляем строки модели в списке
    document.querySelectorAll('.llm-model-row').forEach(r =>
      r.classList.toggle('selected', r.querySelector('span')?.textContent === this.PROVIDERS[providerId]?.models.find(m => m.id === val)?.label));
  },

  onCustomModelInput(providerId, val) {
    const s = this.getSettings();
    s[`customModel_${providerId}`] = val;
    this.saveSettings(s);
  },

  setActive(providerId) {
    const s = this.getSettings();
    s.activeProvider = providerId;
    this.saveSettings(s);
    this.renderProviderCards();
    this.openProvider(providerId);
    // Показать тост
    this.showToast(`✅ Активный провайдер: ${this.PROVIDERS[providerId]?.label}`);
    // Обновить шапку AI панели
    this.updateAiPanelHeader();
  },

  // ── Тест соединения ─────────────────────────────────────────────────────
  async testConnection(providerId) {
    const el = document.getElementById(`llmTestResult_${providerId}`);
    if (el) { el.textContent = '⏳ Тестирую...'; el.className = 'llm-test-result testing'; }

    const p   = this.PROVIDERS[providerId];
    const key = this.getApiKey(providerId);
    const url = this.getBaseUrl(providerId);
    const model = this.getSettings()[`model_${providerId}`] || p.models[0]?.id;

    try {
      let ok = false;
      if (p.format === 'anthropic') {
        const r = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01' },
          body: JSON.stringify({ model, max_tokens: 5, messages: [{ role: 'user', content: 'hi' }] })
        });
        ok = r.status !== 401 && r.status !== 403;
      } else if (p.format === 'google') {
        const r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ contents: [{ parts: [{ text: 'hi' }] }] })
        });
        ok = r.status !== 400 && r.status !== 403;
      } else {
        const apiUrl = p.customUrl ? (url + '/v1/chat/completions') : p.apiUrl;
        const headers = { 'Content-Type': 'application/json' };
        if (key) headers['Authorization'] = 'Bearer ' + key;
        const r = await fetch(apiUrl, {
          method: 'POST', headers,
          body: JSON.stringify({ model, max_tokens: 5, messages: [{ role: 'user', content: 'hi' }] })
        });
        ok = r.status !== 401 && r.status !== 403;
      }
      if (el) { el.textContent = ok ? '✅ Соединение успешно' : '⚠️ Ключ неверный или нет доступа'; el.className = 'llm-test-result ' + (ok ? 'ok' : 'fail'); }
    } catch (e) {
      if (el) { el.textContent = '❌ Ошибка: ' + e.message; el.className = 'llm-test-result fail'; }
    }
  },

  // ── Обновить шапку в AI Strategy ─────────────────────────────────────────
  updateAiPanelHeader() {
    const p  = this.getActiveProvider();
    const m  = this.getActiveModel();
    const cfg = this.PROVIDERS[p];
    const el = document.getElementById('aiActiveProvider');
    if (el) el.textContent = `${cfg?.icon || ''} ${cfg?.label} — ${cfg?.models?.find(x => x.id === m)?.label || m}`;
  },

  showToast(msg) {
    const t = document.createElement('div');
    t.className = 'bq-toast';
    t.textContent = msg;
    document.body.appendChild(t);
    setTimeout(() => t.classList.add('show'), 10);
    setTimeout(() => { t.classList.remove('show'); setTimeout(() => t.remove(), 300); }, 2500);
  },
};