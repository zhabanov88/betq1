'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Панель управления маппингами
//  Связывание внешних ID (API) с внутренними сущностями
// ═══════════════════════════════════════════════════════════════════════════
const mappings = {
  _section: 'sports',    // sports | countries | tournaments | teams | stats | sub_events
  _meta: null,

  async init() {
    const r = await this._fetch('/api/matching/mappings/meta').catch(() => null);
    this._meta = r;
    this.render();
  },

  render() {
    const panel = document.getElementById('panel-mappings');
    if (!panel) return;

    panel.innerHTML = `
    <div class="panel-header">
      <h2>🔗 Маппинги данных</h2>
      <div class="panel-controls">
        <button class="ctrl-btn" onclick="mappings.init()">↻ Обновить</button>
        <button class="ctrl-btn primary" onclick="mappings.autoDetect()">⚡ Авто-маппинг</button>
      </div>
    </div>

    <div class="mappings-layout">
      <!-- Левая навигация -->
      <div class="mappings-sidebar">
        ${[
          ['sports',      '⚽', 'Виды спорта'],
          ['countries',   '🌍', 'Страны'],
          ['tournaments', '🏆', 'Турниры'],
          ['teams',       '👥', 'Команды'],
          ['stats',       '📊', 'Статистики'],
          ['sub_events',  '⚡', 'Подсобытия'],
          ['api_sources', '🔌', 'API Источники'],
        ].map(([id, icon, label]) => `
          <div class="mappings-sidebar-item ${this._section===id?'active':''}" onclick="mappings.showSection('${id}')">
            ${icon} ${label}
          </div>`).join('')}
      </div>

      <!-- Контент -->
      <div id="mappings-content" style="overflow-y:auto">
        ${this._renderSection()}
      </div>
    </div>
    `;
  },

  showSection(section) {
    this._section = section;
    const content = document.getElementById('mappings-content');
    if (content) content.innerHTML = this._renderSection();

    // Обновляем активный элемент в сайдбаре
    document.querySelectorAll('.mappings-sidebar-item').forEach((el, i) => {
      const sections = ['sports','countries','tournaments','teams','stats','sub_events','api_sources'];
      el.classList.toggle('active', sections[i] === section);
    });
  },

  _renderSection() {
    switch (this._section) {
      case 'sports':      return this._renderSports();
      case 'countries':   return this._renderCountries();
      case 'tournaments': return this._renderTournaments();
      case 'teams':       return this._renderTeams();
      case 'stats':       return this._renderStatEvents();
      case 'sub_events':  return this._renderSubEvents();
      case 'api_sources': return this._renderApiSources();
      default: return '';
    }
  },

  _renderSports() {
    const sports = this._meta?.sports || [];
    return `
    <div style="margin-bottom:16px">
      <h3 style="font-size:14px;margin-bottom:12px">⚽ Виды спорта</h3>
      <p style="font-size:13px;color:var(--text2);margin-bottom:16px">
        Маппинг внутренних видов спорта на идентификаторы в разных API.
        Это позволяет системе точно понимать какой спорт пришёл из любого источника.
      </p>
      <table class="mapping-table">
        <thead><tr>
          <th>Спорт</th>
          <th>Slug</th>
          ${(this._meta?.apiSources || []).slice(0,4).map(s => `<th>${s.name.split(' ')[0]}</th>`).join('')}
          <th>Действия</th>
        </tr></thead>
        <tbody>
          ${sports.map(s => `
          <tr>
            <td><span style="font-size:18px">${s.icon||'🏆'}</span> ${s.name}</td>
            <td><code style="background:var(--bg3);padding:2px 6px;border-radius:4px;font-size:11px">${s.slug}</code></td>
            ${(this._meta?.apiSources || []).slice(0,4).map(() => `<td><span style="color:var(--text3);font-size:12px">—</span></td>`).join('')}
            <td><button class="ctrl-btn sm" onclick="mappings.editSportMapping(${s.id})">Настроить</button></td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>`;
  },

  _renderCountries() {
    const countries = this._meta?.countries || [];
    return `
    <div>
      <h3 style="font-size:14px;margin-bottom:12px">🌍 Страны (${countries.length})</h3>
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:8px;max-height:70vh;overflow-y:auto">
        ${countries.map(c => `
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:10px">
          <div style="font-weight:600;font-size:13px">${c.name}</div>
          <div style="font-size:11px;color:var(--text3)">${c.iso2 || '—'} • ${c.continent || '—'}</div>
        </div>`).join('')}
      </div>
    </div>`;
  },

  _renderTournaments() {
    const tournaments = this._meta?.tournaments || [];
    return `
    <div>
      <h3 style="font-size:14px;margin-bottom:12px">🏆 Турниры (${tournaments.length})</h3>
      <table class="mapping-table">
        <thead><tr>
          <th>Турнир</th><th>Спорт</th><th>Страна</th><th>Уровень</th><th>Маппинги</th>
        </tr></thead>
        <tbody>
          ${tournaments.map(t => `
          <tr>
            <td><strong>${t.name}</strong> ${t.short_name ? `<span style="color:var(--text3);font-size:11px">(${t.short_name})</span>` : ''}</td>
            <td>${t.sport_slug || '—'}</td>
            <td>${t.country_name || '—'}</td>
            <td><span style="background:var(--bg3);padding:2px 6px;border-radius:4px;font-size:11px">Уровень ${t.tier || 1}</span></td>
            <td><button class="ctrl-btn sm" onclick="mappings.editTournamentMapping(${t.id})">Настроить</button></td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>`;
  },

  _renderTeams() {
    return `
    <div>
      <h3 style="font-size:14px;margin-bottom:8px">👥 Команды</h3>
      <div style="display:flex;gap:10px;margin-bottom:16px">
        <select class="ctrl-select" id="teamMappingSource">
          <option value="">Все источники</option>
          ${(this._meta?.apiSources || []).map(s => `<option value="${s.code}">${s.name}</option>`).join('')}
        </select>
        <select class="ctrl-select" id="teamMappingSport">
          <option value="">Все виды спорта</option>
          ${(this._meta?.sports || []).map(s => `<option value="${s.id}">${s.icon} ${s.name}</option>`).join('')}
        </select>
        <button class="ctrl-btn primary" onclick="mappings.loadTeamMappings()">Загрузить</button>
      </div>
      <div id="team-mappings-table">
        <div style="color:var(--text3);text-align:center;padding:40px">
          Выберите источник и нажмите Загрузить
        </div>
      </div>
    </div>`;
  },

  async loadTeamMappings() {
    const source = document.getElementById('teamMappingSource')?.value;
    const sportId = document.getElementById('teamMappingSport')?.value;
    const container = document.getElementById('team-mappings-table');
    if (!container) return;
    container.innerHTML = '<div style="color:var(--text3);text-align:center;padding:20px">⏳ Загрузка...</div>';

    try {
      const params = new URLSearchParams();
      if (source) params.set('source', source);
      if (sportId) params.set('sport_id', sportId);
      const r = await this._fetch(`/api/matching/mappings/teams?${params}`);
      const teams = r?.teams || [];

      if (!teams.length) {
        container.innerHTML = '<div style="color:var(--text3);text-align:center;padding:40px">Нет данных</div>';
        return;
      }

      container.innerHTML = `
      <table class="mapping-table">
        <thead><tr>
          <th>Команда</th><th>Источник</th><th>External ID</th><th>External Name</th><th>Уверенность</th><th>Verified</th>
        </tr></thead>
        <tbody>
          ${teams.map(t => `
          <tr>
            <td><strong>${t.name}</strong>${t.short_name ? ` <span style="color:var(--text3);font-size:11px">(${t.short_name})</span>` : ''}</td>
            <td>${t.source_name || '—'}</td>
            <td><code style="background:var(--bg3);padding:2px 4px;border-radius:3px;font-size:11px">${t.external_id || '—'}</code></td>
            <td>${t.external_name || '—'}</td>
            <td>
              <span class="mapping-confidence ${(t.confidence||0) >= .9 ? 'high' : (t.confidence||0) >= .7 ? 'medium' : 'low'}"></span>
              ${Math.round((t.confidence || 0) * 100)}%
            </td>
            <td>${t.is_verified ? '✅' : '<span style="color:var(--text3)">—</span>'}</td>
          </tr>`).join('')}
        </tbody>
      </table>`;
    } catch (e) {
      container.innerHTML = `<div style="color:var(--red);padding:16px">${e.message}</div>`;
    }
  },

  _renderStatEvents() {
    const events = this._meta?.statEventTypes || [];
    const grouped = {};
    events.forEach(e => {
      const key = e.sport_slug || 'other';
      if (!grouped[key]) grouped[key] = [];
      grouped[key].push(e);
    });

    return `
    <div>
      <h3 style="font-size:14px;margin-bottom:12px">📊 Счётные подсобытия (${events.length})</h3>
      <p style="font-size:13px;color:var(--text2);margin-bottom:16px">
        На что именно делается ставка: голы, угловые, ауты, пробросы, штрафные минуты и т.д.
        Каждый код имеет единый идентификатор для связки с любым API.
      </p>
      ${Object.entries(grouped).map(([sport, items]) => `
      <div style="margin-bottom:20px">
        <div style="font-weight:700;font-size:13px;margin-bottom:8px;color:var(--accent)">${sport.toUpperCase()}</div>
        <table class="mapping-table">
          <thead><tr><th>Код</th><th>Название</th><th>RU</th><th>Единица</th><th>Тотал линия</th><th>Область</th></tr></thead>
          <tbody>
            ${items.map(e => `
            <tr>
              <td><code style="background:rgba(99,102,241,.12);color:#818cf8;padding:2px 6px;border-radius:4px;font-size:11px">${e.code}</code></td>
              <td>${e.name}</td>
              <td>${e.name_ru || '—'}</td>
              <td>${e.unit || 'count'}</td>
              <td>${e.typical_over_under != null ? `<strong>${e.typical_over_under}</strong>` : '—'}</td>
              <td><span style="color:var(--text3);font-size:11px">${e.scope || 'match'}</span></td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div>`).join('')}
    </div>`;
  },

  _renderSubEvents() {
    const events = this._meta?.subEventTypes || [];
    return `
    <div>
      <h3 style="font-size:14px;margin-bottom:12px">⚡ Подсобытия (${events.length})</h3>
      <p style="font-size:13px;color:var(--text2);margin-bottom:16px">
        Типы игровых событий: гол, жёлтая карточка, угловой, замена, и т.д.
        Маппируются на коды из разных API для единообразной обработки.
      </p>
      <table class="mapping-table">
        <thead><tr><th>Код</th><th>Название</th><th>RU</th><th>Спорт</th><th>Категория</th><th>Считаемое</th></tr></thead>
        <tbody>
          ${events.map(e => `
          <tr>
            <td><code style="background:rgba(99,102,241,.12);color:#818cf8;padding:2px 6px;border-radius:4px;font-size:11px">${e.code}</code></td>
            <td>${e.name}</td>
            <td>${e.name_ru || '—'}</td>
            <td>${e.sport_slug || '—'}</td>
            <td>${e.category || '—'}</td>
            <td>${e.is_countable ? '✅' : '—'}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>`;
  },

  _renderApiSources() {
    const sources = this._meta?.apiSources || [];
    return `
    <div>
      <h3 style="font-size:14px;margin-bottom:12px">🔌 API Источники (${sources.length})</h3>
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px">
        ${sources.map(s => `
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:14px">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px">
            <strong style="font-size:14px">${s.name}</strong>
            ${s.is_realtime
              ? '<span class="source-badge realtime">🔴 Live</span>'
              : '<span class="source-badge historical">📂 Historical</span>'}
          </div>
          <div style="font-size:12px;color:var(--text3);margin-bottom:8px">
            <code style="background:var(--bg3);padding:2px 6px;border-radius:4px">${s.code}</code>
          </div>
          <div style="display:flex;flex-wrap:wrap;gap:4px">
            ${(s.sports || []).map(sp => `<span class="library-card-tag">${sp}</span>`).join('')}
          </div>
        </div>`).join('')}
      </div>
    </div>`;
  },

  async autoDetect() {
    this._toast('⏳ Запускаем авто-маппинг...', 'info');
    // В реальной реализации — запрос к серверу для fuzzy matching
    setTimeout(() => this._toast('✅ Авто-маппинг завершён (демо режим)', 'success'), 2000);
  },

  editSportMapping(sportId) {
    this._toast('Настройка маппинга спорта (скоро)', 'info');
  },

  editTournamentMapping(tournamentId) {
    this._toast('Настройка маппинга турнира (скоро)', 'info');
  },

  _toast(msg, type = 'info') {
    let t = document.getElementById('bq-toast');
    if (!t) { t = Object.assign(document.createElement('div'), { id: 'bq-toast', className: 'bq-toast' }); document.body.append(t); }
    t.textContent = msg;
    t.className = `bq-toast show ${type}`;
    clearTimeout(t._tm);
    t._tm = setTimeout(() => t.classList.remove('show'), 3000);
  },

  async _fetch(url, method = 'GET', body = null) {
    const opts = {
      method,
      headers: { 'Content-Type': 'application/json', 'x-auth-token': localStorage.getItem('bq_token') || 'demo' },
    };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  },
};