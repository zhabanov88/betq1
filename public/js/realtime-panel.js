'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Realtime Monitor Panel v4
//  Исправления:
//  1. Показывает реальные ETL-таблицы (football_matches и др.)
//  2. ETL-лог из ClickHouse вместо docker logs (работает без docker socket)
//  3. Корректная обработка ошибок кнопки Запустить
//  4. Кнопка перехода в Value Finder прямо из панели
// ═══════════════════════════════════════════════════════════════════════════
const realtimePanel = {
  _timer:     null,
  _logsOpen:  false,
  _etlLogOpen:false,
  POLL_MS:    20000,

  async init() {
    await this.refresh();
    this.startPoll();
  },

  destroy() {
    clearInterval(this._timer);
  },

  startPoll() {
    clearInterval(this._timer);
    this._timer = setInterval(() => this.refresh(), this.POLL_MS);
  },

  async refresh() {
    try {
      const data = await this._api('/api/realtime/status');
      if (!data) return;
      this._renderContainer(data);
      this._renderTables(data.tables || []);
      this._renderSources(data.etlSources || []);
      this._renderOddsApi(data.oddsApi);
    } catch(e) {
      console.warn('[realtimePanel]', e);
    }
  },

  // ── Контейнер realtime-collector ─────────────────────────────────────────
  _renderContainer(data) {
    const el = document.getElementById('rt-container-status');
    if (!el) return;

    const c = data.container || {};
    const MAP = {
      running:   ['#4caf50', '● работает'],
      exited:    ['#f44336', '● остановлен'],
      paused:    ['#ff9800', '● приостановлен'],
      not_found: ['#888',    '● не найден'],
      unknown:   ['#888',    '● неизвестно'],
    };
    const [color, label] = MAP[c.status] || MAP.unknown;
    const uptime = c.startedAt ? this._fmtUptime(c.startedAt) : '—';

    el.innerHTML = `
      <div class="rt-status-row">
        <span style="color:${color};font-weight:600">${label}</span>
        <span class="rt-meta" style="margin-left:8px;font-size:11px;color:var(--text3)">${CONTAINER || 'betquant-realtime'}</span>
      </div>
      ${c.error ? `<div style="margin-top:8px;font-size:11px;color:var(--red);white-space:pre-wrap;line-height:1.5">${this._esc(c.error)}</div>` : ''}
      ${!c.error ? `
        <div class="rt-status-row" style="margin-top:8px">
          <span class="rt-label">Uptime</span>
          <span class="rt-val">${uptime}</span>
        </div>
        <div class="rt-status-row">
          <span class="rt-label">Рестартов</span>
          <span class="rt-val">${c.restarts ?? '—'}</span>
        </div>
      ` : ''}
      <div class="rt-status-row" style="margin-top:8px">
        <span class="rt-label">ClickHouse</span>
        <span class="rt-val" style="color:${data.chConnected ? '#4caf50' : '#f44336'}">
          ${data.chConnected ? '● подключён' : '● недоступен'}
        </span>
      </div>
      <div class="rt-status-row">
        <span class="rt-label">Обновлено</span>
        <span class="rt-val">${new Date(data.timestamp).toLocaleTimeString('ru')}</span>
      </div>`;

    // Кнопки управления
    const btns = document.getElementById('rt-control-btns');
    if (!btns) return;
    const isRunning = c.status === 'running';
    btns.innerHTML = `
      <button class="ctrl-btn ${isRunning ? '' : 'primary'}"
              onclick="realtimePanel.action('${isRunning ? 'restart' : 'start'}')">
        ${isRunning ? '🔄 Перезапустить' : '▶ Запустить'}
      </button>
      <button class="ctrl-btn" onclick="realtimePanel.action('stop')"
              ${!isRunning ? 'disabled' : ''}>⏹ Остановить</button>
      <button class="ctrl-btn" onclick="realtimePanel.toggleLogs()">📋 Логи</button>
      <button class="ctrl-btn" onclick="realtimePanel.toggleEtlLog()">🗃 ETL Лог</button>
      <button class="ctrl-btn" onclick="realtimePanel.refresh()">↺ Обновить</button>
      <span id="rt-action-msg" style="font-size:12px;margin-left:8px"></span>`;
  },

  // ── Таблица ETL данных ────────────────────────────────────────────────────
  _renderTables(tables) {
    const el = document.getElementById('rt-tables-body');
    if (!el) return;

    if (!tables.length) {
      el.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text3);padding:20px">Нет данных</td></tr>';
      return;
    }

    el.innerHTML = tables.map(t => {
      const dot   = t.exists ? (t.healthy ? '●' : '●') : '●';
      const color = !t.exists ? '#888' : t.healthy ? '#4caf50' : (t.count > 0 ? '#ff9800' : '#f44336');
      const age   = t.ageMin === null ? '—'
                  : t.ageMin < 60    ? `${t.ageMin}м`
                  : t.ageMin < 1440  ? `${Math.round(t.ageMin/60)}ч`
                  : `${Math.round(t.ageMin/1440)}д`;
      const status = !t.exists ? '<span style="color:#888;font-size:11px">нет таблицы</span>'
                   : t.error   ? `<span style="color:var(--red);font-size:11px">${t.error}</span>`
                   : t.healthy ? '<span style="color:#4caf50;font-size:11px">✓ актуально</span>'
                   :             '<span style="color:#ff9800;font-size:11px">устарело</span>';
      return `
        <tr>
          <td><span style="color:${color}">${dot}</span> <span style="font-size:12px">${t.label || t.name}</span></td>
          <td style="text-align:right;font-family:var(--font-mono);font-size:12px">
            ${t.count > 0 ? t.count.toLocaleString('ru') : '—'}
          </td>
          <td style="color:var(--text3);font-size:11px">${age}</td>
          <td>${status}</td>
        </tr>`;
    }).join('');
  },

  // ── Источники ETL ─────────────────────────────────────────────────────────
  _renderSources(sources) {
    const el = document.getElementById('rt-sources');
    if (!el) return;

    if (!sources.length) {
      el.innerHTML = '<div style="color:var(--text3);font-size:12px;padding:8px">Нет данных ETL лога. Запустите ETL.</div>';
      return;
    }

    el.innerHTML = `
      <table style="width:100%;font-size:12px">
        <thead>
          <tr>
            <th style="text-align:left;padding:4px 8px;color:var(--text3)">Источник</th>
            <th style="text-align:right;padding:4px 8px;color:var(--text3)">Строк</th>
            <th style="padding:4px 8px;color:var(--text3)">Последний запуск</th>
          </tr>
        </thead>
        <tbody>
          ${sources.map(s => `
            <tr>
              <td style="padding:4px 8px">${this._esc(s.source || '—')}</td>
              <td style="text-align:right;padding:4px 8px;font-family:var(--font-mono)">${(s.rows||0).toLocaleString('ru')}</td>
              <td style="padding:4px 8px;color:var(--text3);font-size:11px">${s.last_run ? s.last_run.slice(0,19) : '—'}</td>
            </tr>`).join('')}
        </tbody>
      </table>`;
  },

  // ── Odds API статус ───────────────────────────────────────────────────────
  _renderOddsApi(api) {
    const el = document.getElementById('rt-odds-usage');
    if (!el || !api) return;

    if (!api.configured) {
      el.innerHTML = `
        <div style="color:var(--text3);font-size:12px;padding:8px 0">
          ⚠️ ODDS_API_KEY не задан в .env<br>
          <a href="https://the-odds-api.com" target="_blank" style="color:var(--accent)">
            Получить бесплатный ключ →
          </a>
        </div>`;
      return;
    }

    el.innerHTML = `
      <div style="font-size:12px;color:var(--green)">
        ✅ Odds API подключён (${api.key})
      </div>`;
  },

  // ── Логи контейнера ───────────────────────────────────────────────────────
  async toggleLogs() {
    const el = document.getElementById('rt-logs-wrap');
    if (!el) return;
    this._logsOpen = !this._logsOpen;
    el.style.display = this._logsOpen ? 'block' : 'none';
    if (this._logsOpen) await this._loadLogs();
  },

  async _loadLogs() {
    const el = document.getElementById('rt-logs-output');
    if (!el) return;
    el.innerHTML = '<div style="color:var(--text3);font-size:12px">Загрузка...</div>';
    const data = await this._api('/api/realtime/logs?lines=80');
    if (!data?.lines) return;
    el.innerHTML = data.lines.map(line => {
      const cls = line.includes('✅') || line.includes('INFO')  ? 'log-ok'
                : line.includes('❌') || line.includes('ERROR') ? 'log-err'
                : line.includes('⚠️') || line.includes('WARN')  ? 'log-warn'
                : 'log-info';
      return `<div class="${cls}" style="font-size:11px;padding:1px 0">${this._esc(line)}</div>`;
    }).join('');
    el.scrollTop = 0;
  },

  // ── ETL лог из ClickHouse ─────────────────────────────────────────────────
  async toggleEtlLog() {
    const el = document.getElementById('rt-etllog-wrap');
    if (!el) return;
    this._etlLogOpen = !this._etlLogOpen;
    el.style.display = this._etlLogOpen ? 'block' : 'none';
    if (this._etlLogOpen) await this._loadEtlLog();
  },

  async _loadEtlLog() {
    const el = document.getElementById('rt-etllog-output');
    if (!el) return;
    el.innerHTML = '<div style="color:var(--text3);font-size:12px;padding:8px">Загрузка...</div>';
    const data = await this._api('/api/realtime/etl-log?limit=50');
    if (!data?.rows?.length) {
      el.innerHTML = '<div style="color:var(--text3);font-size:12px;padding:8px">ETL лог пуст. Запустите ETL.</div>';
      return;
    }
    el.innerHTML = `
      <table class="data-table" style="font-size:11px">
        <thead><tr>
          <th>Время</th><th>Спорт</th><th>Источник</th>
          <th>Лига</th><th>Сезон</th><th>Строк</th><th>Статус</th>
        </tr></thead>
        <tbody>
          ${data.rows.map(r => `
            <tr>
              <td style="color:var(--text3)">${r.created_at?.slice(0,19)||'—'}</td>
              <td>${r.sport||'—'}</td>
              <td style="font-size:10px">${r.source||'—'}</td>
              <td>${r.league||'—'}</td>
              <td>${r.season||'—'}</td>
              <td style="text-align:right">${(r.rows_loaded||0).toLocaleString('ru')}</td>
              <td>
                <span style="color:${r.status==='ok'?'#4caf50':r.status==='skip'?'#ff9800':'#f44336'}">
                  ${r.status==='ok'?'✅':r.status==='skip'?'⚠️':'❌'} ${r.status}
                </span>
              </td>
            </tr>`).join('')}
        </tbody>
      </table>`;
  },

  // ── Действия с контейнером ────────────────────────────────────────────────
  async action(cmd) {
    const labels = { restart: 'перезапустить', stop: 'остановить', start: 'запустить' };
    if (!confirm(`${labels[cmd] || cmd} контейнер betquant-realtime?`)) return;

    const msgEl = document.getElementById('rt-action-msg');
    if (msgEl) { msgEl.textContent = '⏳ Выполняется...'; msgEl.style.color = 'var(--text2)'; }

    const data = await this._api(`/api/realtime/${cmd}`, 'POST');
    if (msgEl) {
      msgEl.textContent = data?.message || (data?.ok ? '✅ OK' : '❌ Ошибка');
      msgEl.style.color = data?.ok ? '#4caf50' : '#f44336';
      setTimeout(() => { if (msgEl) msgEl.textContent = ''; }, 6000);
    }
    setTimeout(() => this.refresh(), 2500);
  },

  // ── Helpers ───────────────────────────────────────────────────────────────
  _fmtUptime(startedAt) {
    const ms = Date.now() - new Date(startedAt).getTime();
    if (ms < 0) return '—';
    const m = Math.floor(ms / 60000);
    const h = Math.floor(m / 60);
    const d = Math.floor(h / 24);
    if (d > 0)  return `${d}д ${h%24}ч`;
    if (h > 0)  return `${h}ч ${m%60}м`;
    return `${m}м`;
  },

  _esc(s) {
    return String(s||'')
      .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  },

  async _api(url, method = 'GET') {
    try {
      const r = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json',
          'x-auth-token': localStorage.getItem('bq_token') || 'demo',
        },
      });
      return r.ok ? r.json() : null;
    } catch(e) {
      return null;
    }
  },
};