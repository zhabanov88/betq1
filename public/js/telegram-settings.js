'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Telegram Multi-Bot Settings Panel
//  Вкладки: Боты · Получатели · Рассылки · Стратегии · Форматы
// ═══════════════════════════════════════════════════════════════════════════
const telegramSettings = {
  tab:           'bots',    // bots | recipients | distributions | strategies | formats
  data: {
    bots:          [],
    recipients:    [],
    distributions: [],
    formats:       [],
    strategyConfigs: {},
  },
  strategies: [], // список стратегий из backtest

  // ── lifecycle ─────────────────────────────────────────────────────────────
  async init() {
    await this.loadAll();
    this.strategies = this._getStrategies();
    this.render();
  },

  async loadAll() {
    const [bots, recs, dists, fmts, cfgs] = await Promise.all([
      this._fetch('/api/telegram/bots'),
      this._fetch('/api/telegram/recipients'),
      this._fetch('/api/telegram/distributions'),
      this._fetch('/api/telegram/formats'),
      this._fetch('/api/telegram/strategy-configs'),
    ]);
    this.data.bots          = bots          || [];
    this.data.recipients    = recs          || [];
    this.data.distributions = dists         || [];
    this.data.formats       = fmts          || [];
    this.data.strategyConfigs = cfgs        || {};
  },

  // ── main render ───────────────────────────────────────────────────────────
  render() {
    const el = document.getElementById('tg-panel');
    if (!el) return;

    const tabs = [
      { id:'bots',          icon:'🤖', label:'Боты' },
      { id:'recipients',    icon:'👥', label:'Получатели' },
      { id:'distributions', icon:'📡', label:'Рассылки' },
      { id:'strategies',    icon:'🎯', label:'Стратегии' },
      { id:'formats',       icon:'🖋', label:'Форматы' },
    ];

    el.innerHTML = `
      <div class="tg-tabs">
        ${tabs.map(t => `
          <button class="tg-tab ${this.tab === t.id ? 'active' : ''}"
                  onclick="telegramSettings.setTab('${t.id}')">
            ${t.icon} ${t.label}
          </button>`).join('')}
      </div>
      <div id="tg-tab-content" class="tg-tab-content"></div>`;

    this._renderTab();
  },

  setTab(id) {
    this.tab = id;
    this.render();
  },

  _renderTab() {
    const el = document.getElementById('tg-tab-content');
    if (!el) return;
    switch (this.tab) {
      case 'bots':          el.innerHTML = this._botsHTML();          break;
      case 'recipients':    el.innerHTML = this._recipientsHTML();    break;
      case 'distributions': el.innerHTML = this._distributionsHTML(); break;
      case 'strategies':    el.innerHTML = this._strategiesHTML();    break;
      case 'formats':       el.innerHTML = this._formatsHTML();       break;
    }
  },

  // ═══════════════════════════════════════════════════════════════════════════
  //  БОТЫ
  // ═══════════════════════════════════════════════════════════════════════════
  _botsHTML() {
    const bots = this.data.bots;
    return `
      <div class="tg-section-head">
        <div>
          <div class="tg-sec-title">🤖 Telegram-боты</div>
          <div style="font-size:11px;color:var(--text3)">Каждый бот — отдельный токен. Один помечается дефолтным.</div>
        </div>
        <button class="ctrl-btn primary" onclick="telegramSettings.showBotForm()">+ Добавить бота</button>
      </div>

      ${!bots.length ? `<div class="tg-empty">Нет ботов. Создай бота в @BotFather и добавь его токен.</div>` : ''}

      <div class="tg-card-list">
        ${bots.map(b => `
          <div class="tg-card ${b.isDefault ? 'default' : ''}">
            <div class="tg-card-main">
              <span class="tg-card-icon">🤖</span>
              <div>
                <div class="tg-card-name">${b.name} ${b.isDefault ? '<span class="tg-badge default">Default</span>' : ''}</div>
                <div class="tg-card-sub">@${b.botName || '—'} · ${b.token || '—'}</div>
              </div>
            </div>
            <div class="tg-card-actions">
              ${!b.isDefault ? `<button class="ctrl-btn sm" onclick="telegramSettings.setDefaultBot('${b.id}')">Сделать дефолтным</button>` : ''}
              <button class="ctrl-btn sm" onclick="telegramSettings.testBot('${b.id}')">Тест</button>
              <button class="ctrl-btn sm" onclick="telegramSettings.showBotForm('${b.id}')">✏️</button>
              ${!b.isDefault ? `<button class="ctrl-btn sm danger" onclick="telegramSettings.deleteBot('${b.id}')">✕</button>` : ''}
            </div>
          </div>`).join('')}
      </div>

      <!-- Форма добавления/редактирования -->
      <div id="bot-form" style="display:none" class="tg-form">
        <div class="tg-form-title" id="bot-form-title">Добавить бота</div>
        <input type="hidden" id="bot-form-id">
        <div class="config-row"><label>Название</label>
          <input class="ctrl-input" id="bot-name" placeholder="Мой Value Bot" style="flex:1"></div>
        <div class="config-row"><label>Bot Token</label>
          <input type="password" class="ctrl-input" id="bot-token" placeholder="123456:AAF..." style="flex:1"></div>
        <div class="config-row">
          <label class="toggle-switch">
            <input type="checkbox" id="bot-default">
            <span class="toggle-slider"></span>
          </label>
          <span style="font-size:12px;margin-left:8px">Установить как дефолтный бот</span>
        </div>
        <div style="display:flex;gap:8px;margin-top:10px">
          <button class="ctrl-btn primary" onclick="telegramSettings.saveBot()">Сохранить</button>
          <button class="ctrl-btn" onclick="document.getElementById('bot-form').style.display='none'">Отмена</button>
          <span id="bot-form-status" class="tg-inline-status"></span>
        </div>
      </div>`;
  },

  showBotForm(id) {
    const f = document.getElementById('bot-form');
    if (!f) return;
    f.style.display = '';
    document.getElementById('bot-form-id').value   = id || '';
    document.getElementById('bot-form-title').textContent = id ? 'Редактировать бота' : 'Добавить бота';
    if (id) {
      const bot = this.data.bots.find(b => b.id === id);
      if (bot) {
        document.getElementById('bot-name').value     = bot.name;
        document.getElementById('bot-token').value    = '';
        document.getElementById('bot-default').checked = bot.isDefault;
      }
    } else {
      document.getElementById('bot-name').value  = '';
      document.getElementById('bot-token').value = '';
      document.getElementById('bot-default').checked = false;
    }
    f.scrollIntoView({ behavior:'smooth', block:'nearest' });
  },

  async saveBot() {
    const id      = document.getElementById('bot-form-id')?.value;
    const name    = document.getElementById('bot-name')?.value?.trim();
    const token   = document.getElementById('bot-token')?.value?.trim();
    const isDef   = document.getElementById('bot-default')?.checked;
    if (!name) { this._formStatus('bot-form-status', '⚠️ Введите название', 'warn'); return; }
    if (!id && !token) { this._formStatus('bot-form-status', '⚠️ Введите токен', 'warn'); return; }
    this._formStatus('bot-form-status', '⏳ Проверяем...', '');
    try {
      const body = { name, isDefault: isDef };
      if (token) body.token = token;
      if (id) {
        await this._fetch(`/api/telegram/bots/${id}`, 'PUT', body);
      } else {
        await this._fetch('/api/telegram/bots', 'POST', body);
      }
      document.getElementById('bot-form').style.display = 'none';
      await this.loadAll();
      this.render();
      this._toast(`✅ Бот сохранён`);
    } catch(e) {
      this._formStatus('bot-form-status', '❌ ' + e.message, 'err');
    }
  },

  async testBot(id) {
    const chatId = prompt('Chat ID для тестового сообщения\n(оставь пустым — возьмём первого получателя):')?.trim();
    try {
      await this._fetch(`/api/telegram/bots/${id}/test`, 'POST', { chatId: chatId || undefined });
      this._toast('✅ Тестовое сообщение отправлено');
    } catch(e) { this._toast('❌ ' + e.message); }
  },

  async setDefaultBot(id) {
    await this._fetch(`/api/telegram/bots/${id}`, 'PUT', { isDefault: true });
    await this.loadAll(); this.render();
  },

  async deleteBot(id) {
    if (!confirm('Удалить бота?')) return;
    await this._fetch(`/api/telegram/bots/${id}`, 'DELETE');
    await this.loadAll(); this.render();
  },

  // ═══════════════════════════════════════════════════════════════════════════
  //  ПОЛУЧАТЕЛИ
  // ═══════════════════════════════════════════════════════════════════════════
  _recipientsHTML() {
    return `
      <div class="tg-section-head">
        <div>
          <div class="tg-sec-title">👥 Получатели</div>
          <div style="font-size:11px;color:var(--text3)">Telegram chat_id — личка, группа или канал. Теги используются для фильтрации в рассылках.</div>
        </div>
        <button class="ctrl-btn primary" onclick="telegramSettings.showRecipientForm()">+ Добавить</button>
      </div>

      <div class="tg-how-to" style="margin-bottom:14px">
        <b>Как узнать Chat ID:</b>
        <ol>
          <li>Напиши <code>/start</code> боту <a href="https://t.me/userinfobot" target="_blank">@userinfobot</a> — он пришлёт твой ID</li>
          <li>Для канала: добавь бота как администратора, отправь в канал любое сообщение, открой:<br>
              <code>https://api.telegram.org/bot&lt;TOKEN&gt;/getUpdates</code></li>
          <li>Для группы: добавь бота в группу, /getUpdates покажет <code>"chat":{"id":-100...}</code></li>
        </ol>
      </div>

      <div class="tg-card-list">
        ${this.data.recipients.map(r => `
          <div class="tg-card ${r.isActive ? '' : 'inactive'}">
            <div class="tg-card-main">
              <span class="tg-card-icon">${r.chatId?.toString().startsWith('-100') ? '📢' : r.chatId?.toString().startsWith('-') ? '👥' : '👤'}</span>
              <div>
                <div class="tg-card-name">${r.label} ${!r.isActive ? '<span class="tg-badge off">Выкл</span>' : ''}</div>
                <div class="tg-card-sub">Chat ID: <code>${r.chatId}</code> · Теги: ${(r.tags||[]).join(', ') || '—'}</div>
              </div>
            </div>
            <div class="tg-card-actions">
              <button class="ctrl-btn sm" onclick="telegramSettings.toggleRecipient('${r.id}')">${r.isActive ? 'Отключить' : 'Включить'}</button>
              <button class="ctrl-btn sm" onclick="telegramSettings.showRecipientForm('${r.id}')">✏️</button>
              <button class="ctrl-btn sm danger" onclick="telegramSettings.deleteRecipient('${r.id}')">✕</button>
            </div>
          </div>`).join('') || '<div class="tg-empty">Нет получателей</div>'}
      </div>

      <div id="rec-form" style="display:none" class="tg-form">
        <div class="tg-form-title" id="rec-form-title">Добавить получателя</div>
        <input type="hidden" id="rec-form-id">
        <div class="config-row"><label>Название</label>
          <input class="ctrl-input" id="rec-label" placeholder="VIP группа" style="flex:1"></div>
        <div class="config-row"><label>Chat ID</label>
          <input class="ctrl-input" id="rec-chatid" placeholder="-1001234567890" style="flex:1"></div>
        <div class="config-row"><label>Теги</label>
          <input class="ctrl-input" id="rec-tags" placeholder="value, vip, football" style="flex:1">
          <span style="font-size:11px;color:var(--text3)">через запятую</span></div>
        <div style="display:flex;gap:8px;margin-top:10px">
          <button class="ctrl-btn primary" onclick="telegramSettings.saveRecipient()">Сохранить</button>
          <button class="ctrl-btn" onclick="document.getElementById('rec-form').style.display='none'">Отмена</button>
          <span id="rec-form-status" class="tg-inline-status"></span>
        </div>
      </div>`;
  },

  showRecipientForm(id) {
    const f = document.getElementById('rec-form');
    if (!f) return;
    f.style.display = '';
    document.getElementById('rec-form-title').textContent = id ? 'Редактировать' : 'Добавить получателя';
    document.getElementById('rec-form-id').value = id || '';
    if (id) {
      const r = this.data.recipients.find(x => x.id === id);
      if (r) {
        document.getElementById('rec-label').value  = r.label;
        document.getElementById('rec-chatid').value = r.chatId;
        document.getElementById('rec-tags').value   = (r.tags||[]).join(', ');
      }
    } else {
      document.getElementById('rec-label').value  = '';
      document.getElementById('rec-chatid').value = '';
      document.getElementById('rec-tags').value   = '';
    }
    f.scrollIntoView({ behavior:'smooth', block:'nearest' });
  },

  async saveRecipient() {
    const id     = document.getElementById('rec-form-id')?.value;
    const label  = document.getElementById('rec-label')?.value?.trim();
    const chatId = document.getElementById('rec-chatid')?.value?.trim();
    const tags   = document.getElementById('rec-tags')?.value?.split(',').map(t=>t.trim()).filter(Boolean);
    if (!label || !chatId) { this._formStatus('rec-form-status','⚠️ Заполни Название и Chat ID','warn'); return; }
    try {
      const body = { label, chatId, tags, isActive: true };
      if (id) await this._fetch(`/api/telegram/recipients/${id}`, 'PUT', body);
      else    await this._fetch('/api/telegram/recipients', 'POST', body);
      document.getElementById('rec-form').style.display = 'none';
      await this.loadAll(); this.render();
      this._toast('✅ Получатель сохранён');
    } catch(e) { this._formStatus('rec-form-status','❌ '+e.message,'err'); }
  },

  async toggleRecipient(id) {
    const r = this.data.recipients.find(x => x.id === id);
    if (r) { await this._fetch(`/api/telegram/recipients/${id}`, 'PUT', { isActive: !r.isActive }); await this.loadAll(); this.render(); }
  },

  async deleteRecipient(id) {
    if (!confirm('Удалить получателя?')) return;
    await this._fetch(`/api/telegram/recipients/${id}`, 'DELETE');
    await this.loadAll(); this.render();
  },

  // ═══════════════════════════════════════════════════════════════════════════
  //  РАССЫЛКИ
  // ═══════════════════════════════════════════════════════════════════════════
  _distributionsHTML() {
    const recs = this.data.recipients;
    return `
      <div class="tg-section-head">
        <div>
          <div class="tg-sec-title">📡 Рассылки (Distribution Lists)</div>
          <div style="font-size:11px;color:var(--text3)">Именованные наборы: бот + список получателей. Стратегии ссылаются на рассылки.</div>
        </div>
        <button class="ctrl-btn primary" onclick="telegramSettings.showDistForm()">+ Создать рассылку</button>
      </div>

      <div class="tg-card-list">
        ${this.data.distributions.map(d => {
          const bot  = this.data.bots.find(b => b.id === d.botId);
          const recp = recs.filter(r => d.recipientIds.includes(r.id));
          return `
            <div class="tg-card">
              <div class="tg-card-main" style="flex:1">
                <span class="tg-card-icon">📡</span>
                <div style="flex:1">
                  <div class="tg-card-name">${d.name} ${d.id==='default'?'<span class="tg-badge default">Default</span>':''}</div>
                  <div class="tg-card-sub">Бот: <b>${bot?.name || '—'}</b> (@${bot?.botName||'?'})</div>
                  <div class="tg-dist-recipients">
                    ${recp.map(r=>`<span class="tg-rec-chip">${r.chatId?.toString().startsWith('-100')?'📢':r.chatId?.toString().startsWith('-')?'👥':'👤'} ${r.label}</span>`).join('')}
                    ${!recp.length ? '<span style="font-size:11px;color:var(--text3)">Нет получателей</span>' : ''}
                  </div>
                  ${d.description ? `<div style="font-size:11px;color:var(--text3);margin-top:3px">${d.description}</div>` : ''}
                </div>
              </div>
              <div class="tg-card-actions">
                <button class="ctrl-btn sm" onclick="telegramSettings.testDist('${d.id}')">📨 Тест</button>
                <button class="ctrl-btn sm" onclick="telegramSettings.showDistForm('${d.id}')">✏️</button>
                ${d.id!=='default'?`<button class="ctrl-btn sm danger" onclick="telegramSettings.deleteDist('${d.id}')">✕</button>`:''}
              </div>
            </div>`;
        }).join('') || '<div class="tg-empty">Нет рассылок</div>'}
      </div>

      <!-- Форма рассылки -->
      <div id="dist-form" style="display:none" class="tg-form">
        <div class="tg-form-title" id="dist-form-title">Создать рассылку</div>
        <input type="hidden" id="dist-form-id">
        <div class="config-row"><label>Название</label>
          <input class="ctrl-input" id="dist-name" placeholder="Value Bets — VIP" style="flex:1"></div>
        <div class="config-row"><label>Описание</label>
          <input class="ctrl-input" id="dist-desc" placeholder="Опционально" style="flex:1"></div>
        <div class="config-row"><label>Бот</label>
          <select class="ctrl-select" id="dist-bot" style="flex:1">
            ${this.data.bots.map(b=>`<option value="${b.id}">${b.name} (@${b.botName||'—'})</option>`).join('')}
          </select>
        </div>
        <div class="tg-form-label" style="margin:10px 0 6px">Получатели:</div>
        <div id="dist-rec-checks" style="display:flex;flex-wrap:wrap;gap:8px">
          ${this.data.recipients.map(r=>`
            <label class="tg-check-label">
              <input type="checkbox" class="dist-rec-cb" value="${r.id}">
              ${r.chatId?.toString().startsWith('-')? '👥':'👤'} ${r.label}
              <span style="color:var(--text3);font-size:10px">${r.chatId}</span>
            </label>`).join('')}
        </div>
        ${!this.data.recipients.length ? '<div style="font-size:12px;color:var(--text3)">Сначала добавь получателей во вкладке «Получатели»</div>' : ''}
        <div style="display:flex;gap:8px;margin-top:12px">
          <button class="ctrl-btn primary" onclick="telegramSettings.saveDist()">Сохранить</button>
          <button class="ctrl-btn" onclick="document.getElementById('dist-form').style.display='none'">Отмена</button>
          <span id="dist-form-status" class="tg-inline-status"></span>
        </div>
      </div>`;
  },

  showDistForm(id) {
    const f = document.getElementById('dist-form');
    if (!f) return; f.style.display = '';
    document.getElementById('dist-form-title').textContent = id ? 'Редактировать рассылку' : 'Создать рассылку';
    document.getElementById('dist-form-id').value = id || '';
    if (id) {
      const d = this.data.distributions.find(x => x.id === id);
      if (d) {
        document.getElementById('dist-name').value = d.name;
        document.getElementById('dist-desc').value = d.description || '';
        document.getElementById('dist-bot').value  = d.botId;
        document.querySelectorAll('.dist-rec-cb').forEach(cb => {
          cb.checked = d.recipientIds.includes(cb.value);
        });
      }
    } else {
      document.getElementById('dist-name').value = '';
      document.getElementById('dist-desc').value = '';
      document.querySelectorAll('.dist-rec-cb').forEach(cb => cb.checked = false);
    }
    f.scrollIntoView({ behavior:'smooth', block:'nearest' });
  },

  async saveDist() {
    const id   = document.getElementById('dist-form-id')?.value;
    const name = document.getElementById('dist-name')?.value?.trim();
    const desc = document.getElementById('dist-desc')?.value?.trim();
    const botId = document.getElementById('dist-bot')?.value;
    const recipientIds = [...document.querySelectorAll('.dist-rec-cb:checked')].map(cb => cb.value);
    if (!name || !botId) { this._formStatus('dist-form-status','⚠️ Заполни Название и Бот','warn'); return; }
    try {
      const body = { name, description: desc, botId, recipientIds };
      if (id) await this._fetch(`/api/telegram/distributions/${id}`, 'PUT', body);
      else    await this._fetch('/api/telegram/distributions', 'POST', body);
      document.getElementById('dist-form').style.display = 'none';
      await this.loadAll(); this.render();
      this._toast('✅ Рассылка сохранена');
    } catch(e) { this._formStatus('dist-form-status','❌ '+e.message,'err'); }
  },

  async testDist(id) {
    const d = this.data.distributions.find(x => x.id === id);
    this._toast(`📨 Отправляем тест в "${d?.name}"...`);
    try {
      const r = await this._fetch('/api/telegram/test', 'POST', { distributionId: id });
      const ok  = r.results?.filter(x=>x.ok).length || 0;
      const err = r.results?.filter(x=>!x.ok).length || 0;
      this._toast(`✅ Отправлено: ${ok}${err ? ` · ❌ Ошибок: ${err}` : ''}`);
    } catch(e) { this._toast('❌ ' + e.message); }
  },

  async deleteDist(id) {
    if (!confirm('Удалить рассылку?')) return;
    await this._fetch(`/api/telegram/distributions/${id}`, 'DELETE');
    await this.loadAll(); this.render();
  },

  // ═══════════════════════════════════════════════════════════════════════════
  //  СТРАТЕГИИ
  // ═══════════════════════════════════════════════════════════════════════════
  _strategiesHTML() {
    const strategies = this.strategies;
    return `
      <div class="tg-section-head">
        <div>
          <div class="tg-sec-title">🎯 Привязка стратегий к рассылкам</div>
          <div style="font-size:11px;color:var(--text3)">
            Для каждой стратегии можно задать отдельного бота, список получателей и формат сообщения.
            Если не задано — используется дефолтная рассылка.
          </div>
        </div>
      </div>

      ${!strategies.length ? `
        <div class="tg-empty">
          Стратегии не найдены.<br>
          <span style="font-size:11px">Добавь стратегии в бэктест-движке — они автоматически появятся здесь.</span>
        </div>` : ''}

      <div class="tg-strat-grid">
        ${strategies.map(s => {
          const cfg = this.data.strategyConfigs[s.id] || {};
          const dist = this.data.distributions.find(d => d.id === cfg.distributionId);
          const fmt  = this.data.formats.find(f => f.id === cfg.formatId);
          const bot  = this.data.bots.find(b => b.id === cfg.botId);
          return `
            <div class="tg-strat-card ${cfg.enabled === false ? 'disabled' : ''}">
              <div class="tg-strat-head">
                <span class="tg-strat-color" style="background:${s.color||'#00d4ff'}"></span>
                <span class="tg-strat-name">${s.name}</span>
                <span class="tg-badge sport">${s.sport||'—'}</span>
                <label class="toggle-switch" style="margin-left:auto">
                  <input type="checkbox" ${cfg.enabled !== false ? 'checked' : ''}
                         onchange="telegramSettings.toggleStrategy('${s.id}', this.checked)">
                  <span class="toggle-slider"></span>
                </label>
              </div>

              ${cfg.enabled === false ? `<div class="tg-strat-off">Уведомления отключены</div>` : `
              <div class="tg-strat-body">
                <div class="tg-strat-row">
                  <span class="tg-strat-label">Рассылка</span>
                  <select class="ctrl-select sm" onchange="telegramSettings.updateStratConfig('${s.id}','distributionId',this.value)">
                    <option value="">— Дефолтная —</option>
                    ${this.data.distributions.map(d=>`<option value="${d.id}" ${cfg.distributionId===d.id?'selected':''}>${d.name}</option>`).join('')}
                  </select>
                  <span class="tg-strat-hint">${dist ? `📡 ${dist.name}` : '📡 Default'}</span>
                </div>
                <div class="tg-strat-row">
                  <span class="tg-strat-label">Бот</span>
                  <select class="ctrl-select sm" onchange="telegramSettings.updateStratConfig('${s.id}','botId',this.value)">
                    <option value="">— Из рассылки —</option>
                    ${this.data.bots.map(b=>`<option value="${b.id}" ${cfg.botId===b.id?'selected':''}>${b.name}</option>`).join('')}
                  </select>
                  <span class="tg-strat-hint">${bot ? `🤖 ${bot.name}` : '🤖 Default'}</span>
                </div>
                <div class="tg-strat-row">
                  <span class="tg-strat-label">Формат</span>
                  <select class="ctrl-select sm" onchange="telegramSettings.updateStratConfig('${s.id}','formatId',this.value)">
                    <option value="">— Дефолтный —</option>
                    ${this.data.formats.map(f=>`<option value="${f.id}" ${cfg.formatId===f.id?'selected':''}>${f.name}</option>`).join('')}
                  </select>
                  <span class="tg-strat-hint">${fmt ? `🖋 ${fmt.name}` : '🖋 Default'}</span>
                </div>
              </div>`}

              <div class="tg-strat-footer">
                ${cfg.distributionId || cfg.botId || cfg.formatId
                  ? `<span style="font-size:11px;color:var(--accent)">⚙️ Кастомные настройки</span>`
                  : `<span style="font-size:11px;color:var(--text3)">Использует Default</span>`}
                <button class="ctrl-btn sm" onclick="telegramSettings.sendTestAlert('${s.id}')">📨 Тест алерта</button>
              </div>
            </div>`;
        }).join('')}
      </div>`;
  },

  async toggleStrategy(strategyId, enabled) {
    await this._saveCfg(strategyId, { enabled });
  },

  async updateStratConfig(strategyId, field, value) {
    await this._saveCfg(strategyId, { [field]: value || undefined });
  },

  async _saveCfg(strategyId, patch) {
    const cur = this.data.strategyConfigs[strategyId] || {};
    const next = { ...cur, ...patch };
    // Удаляем undefined
    Object.keys(next).forEach(k => next[k] === undefined && delete next[k]);
    this.data.strategyConfigs[strategyId] = next;
    await this._fetch(`/api/telegram/strategy-configs/${strategyId}`, 'PUT', next);
  },

  async sendTestAlert(strategyId) {
    try {
      await this._fetch('/api/telegram/alert', 'POST', {
        type: 'value', strategyId,
        data: {
          match: 'Arsenal vs Chelsea', league: 'Premier League',
          label: 'Победа Arsenal', odds: 3.10, edge: 7.2, kelly: 3.8,
          impliedProb: 32.3, modelProb: 38.9, lH: 1.42, lA: 1.18,
        },
      });
      this._toast('✅ Тест отправлен');
    } catch(e) { this._toast('❌ ' + e.message); }
  },

  // ═══════════════════════════════════════════════════════════════════════════
  //  ФОРМАТЫ
  // ═══════════════════════════════════════════════════════════════════════════
  _formatsHTML() {
    return `
      <div class="tg-section-head">
        <div>
          <div class="tg-sec-title">🖋 Форматы сообщений</div>
          <div style="font-size:11px;color:var(--text3)">
            Шаблоны с переменными <code>{match}</code>, <code>{odds}</code>, <code>{edge}</code> и др.
            HTML-теги &lt;b&gt;, &lt;i&gt;, &lt;code&gt; поддерживаются.
          </div>
        </div>
        <button class="ctrl-btn primary" onclick="telegramSettings.showFmtForm()">+ Создать формат</button>
      </div>

      <div class="tg-card-list">
        ${this.data.formats.map(f => `
          <div class="tg-card">
            <div class="tg-card-main" style="flex:1">
              <span class="tg-card-icon">🖋</span>
              <div style="flex:1">
                <div class="tg-card-name">${f.name}
                  ${f.isDefault ? '<span class="tg-badge default">Default</span>' : ''}
                  <span class="tg-badge type">${f.type}</span>
                </div>
                <pre class="tg-template-preview">${f.template.replace(/</g,'&lt;').slice(0,120)}${f.template.length>120?'…':''}</pre>
                <div style="font-size:10px;color:var(--text3)">Переменные: ${(f.fields||[]).map(v=>`<code>{${v}}</code>`).join(' ')}</div>
              </div>
            </div>
            <div class="tg-card-actions">
              ${!f.isDefault?`<button class="ctrl-btn sm" onclick="telegramSettings.setDefaultFmt('${f.id}','${f.type}')">Default</button>`:''}
              <button class="ctrl-btn sm" onclick="telegramSettings.showFmtForm('${f.id}')">✏️</button>
              ${!f.isDefault?`<button class="ctrl-btn sm danger" onclick="telegramSettings.deleteFmt('${f.id}')">✕</button>`:''}
            </div>
          </div>`).join('')}
      </div>

      <!-- Форма формата -->
      <div id="fmt-form" style="display:none" class="tg-form">
        <div class="tg-form-title" id="fmt-form-title">Создать формат</div>
        <input type="hidden" id="fmt-form-id">
        <div class="config-row"><label>Название</label>
          <input class="ctrl-input" id="fmt-name" placeholder="Компактный для группы" style="flex:1"></div>
        <div class="config-row"><label>Тип</label>
          <select class="ctrl-select" id="fmt-type">
            <option value="any">Любой</option>
            <option value="value">Value Bet</option>
            <option value="live">Live Signal</option>
            <option value="odds">Odds Movement</option>
            <option value="neural">Neural</option>
          </select>
        </div>
        <div class="config-row" style="align-items:flex-start"><label style="padding-top:6px">Шаблон</label>
          <textarea class="ctrl-input" id="fmt-template" rows="6" style="flex:1;resize:vertical;font-family:var(--font-mono);font-size:12px"
                    oninput="telegramSettings.previewFmt()"
                    placeholder="💎 &lt;b&gt;{match}&lt;/b&gt; | {label} @ {odds} | Edge +{edge}%"></textarea>
        </div>
        <div class="tg-fmt-vars">
          Доступные переменные: 
          ${['match','league','label','odds','edge','kelly','impliedProb','modelProb','lH','lA','minute','score','signalLabel','confidence','sport']
            .map(v=>`<code onclick="telegramSettings.insertVar('{${v}}')">{${v}}</code>`).join(' ')}
        </div>
        <div class="tg-form-label" style="margin:10px 0 4px">Превью:</div>
        <div id="fmt-preview" class="tg-template-preview" style="min-height:40px;white-space:pre-wrap"></div>
        <div style="display:flex;gap:8px;margin-top:12px">
          <button class="ctrl-btn primary" onclick="telegramSettings.saveFmt()">Сохранить</button>
          <button class="ctrl-btn" onclick="document.getElementById('fmt-form').style.display='none'">Отмена</button>
        </div>
      </div>`;
  },

  showFmtForm(id) {
    const f = document.getElementById('fmt-form');
    if (!f) return; f.style.display = '';
    document.getElementById('fmt-form-title').textContent = id ? 'Редактировать формат' : 'Создать формат';
    document.getElementById('fmt-form-id').value = id || '';
    if (id) {
      const fmt = this.data.formats.find(x => x.id === id);
      if (fmt) {
        document.getElementById('fmt-name').value     = fmt.name;
        document.getElementById('fmt-type').value     = fmt.type;
        document.getElementById('fmt-template').value = fmt.template;
      }
    } else {
      document.getElementById('fmt-name').value     = '';
      document.getElementById('fmt-template').value = '';
    }
    this.previewFmt();
    f.scrollIntoView({ behavior:'smooth', block:'nearest' });
  },

  previewFmt() {
    const tpl = document.getElementById('fmt-template')?.value || '';
    const sample = { match:'Arsenal vs Chelsea', league:'Premier League', label:'Победа Arsenal',
      odds:'3.10', edge:'7.2', kelly:'3.8', impliedProb:'32.3', modelProb:'38.9',
      lH:'1.42', lA:'1.18', minute:'67', score:'2:1', signalLabel:'Домашняя победа', confidence:'78', sport:'Football' };
    const rendered = tpl.replace(/\{(\w+)\}/g, (_, k) => sample[k] || `{${k}}`);
    const el = document.getElementById('fmt-preview');
    if (el) el.textContent = rendered;
  },

  insertVar(v) {
    const ta = document.getElementById('fmt-template');
    if (!ta) return;
    const pos = ta.selectionStart || ta.value.length;
    ta.value = ta.value.slice(0, pos) + v + ta.value.slice(pos);
    ta.focus(); this.previewFmt();
  },

  async saveFmt() {
    const id       = document.getElementById('fmt-form-id')?.value;
    const name     = document.getElementById('fmt-name')?.value?.trim();
    const type     = document.getElementById('fmt-type')?.value;
    const template = document.getElementById('fmt-template')?.value?.trim();
    if (!name || !template) { this._toast('⚠️ Заполни Название и Шаблон'); return; }
    try {
      const body = { name, type, template };
      if (id) await this._fetch(`/api/telegram/formats/${id}`, 'PUT', body);
      else    await this._fetch('/api/telegram/formats', 'POST', body);
      document.getElementById('fmt-form').style.display = 'none';
      await this.loadAll(); this.render();
      this._toast('✅ Формат сохранён');
    } catch(e) { this._toast('❌ ' + e.message); }
  },

  async setDefaultFmt(id, type) {
    await this._fetch(`/api/telegram/formats/${id}`, 'PUT', { isDefault: true, type });
    await this.loadAll(); this.render();
  },

  async deleteFmt(id) {
    if (!confirm('Удалить формат?')) return;
    await this._fetch(`/api/telegram/formats/${id}`, 'DELETE');
    await this.loadAll(); this.render();
  },

  // ─── Helpers ──────────────────────────────────────────────────────────────
  _getStrategies() {
    // Берём из backtestEngine если есть, иначе возвращаем демо
    if (typeof backtestEngine !== 'undefined' && backtestEngine.activeStrategies?.length) {
      return backtestEngine.activeStrategies;
    }
    return [
      { id:'value_home', name:'Value Betting (Poisson)', sport:'football', color:'#00d4ff' },
      { id:'over25_xg',  name:'Over 2.5 xG Model',      sport:'football', color:'#00e676' },
      { id:'elo_value',  name:'ELO Rating Value',        sport:'football', color:'#c084fc' },
    ];
  },

  _formStatus(id, msg, cls) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = msg;
    el.className   = 'tg-inline-status ' + (cls === 'ok' ? 'ok' : cls === 'err' ? 'err' : cls === 'warn' ? 'warn' : '');
  },
  _toast(msg) {
    let t = document.getElementById('bq-toast');
    if (!t) { t = Object.assign(document.createElement('div'),{id:'bq-toast',className:'bq-toast'}); document.body.append(t); }
    t.textContent = msg; t.classList.add('show');
    clearTimeout(t._tm); t._tm = setTimeout(()=>t.classList.remove('show'), 3000);
  },
  async _fetch(url, method = 'GET', body = null) {
    const opts = { method, headers:{'Content-Type':'application/json','x-auth-token':localStorage.getItem('bq_token')||'demo'} };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    if (!r.ok) {
      const d = await r.json().catch(()=>({ error: r.statusText }));
      throw new Error(d.error || r.statusText);
    }
    return r.json();
  },
};