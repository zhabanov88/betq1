const realtimePanel = {
    _timer:      null,
    _logsTimer:  null,
    _logsOpen:   false,
    POLL_MS:     15000,
   
    async init() {
      await this.refresh();
      this.startPoll();
    },
   
    destroy() {
      clearInterval(this._timer);
      clearInterval(this._logsTimer);
    },
   
    startPoll() {
      clearInterval(this._timer);
      this._timer = setInterval(() => this.refresh(), this.POLL_MS);
    },
   
    async refresh() {
      try {
        const data = await this._api('/api/realtime/status');
        if (!data) return;
        this._renderStatus(data);
        this._renderTables(data.tables || []);
        this._renderOddsUsage(data.oddsApi);
      } catch (e) {
        console.warn('[realtimePanel]', e);
      }
    },
   
    // ── Рендер блока контейнера ────────────────────────────────────────────
    _renderStatus(data) {
      const el = document.getElementById('rt-container-status');
      if (!el) return;
   
      const c = data.container || {};
      const statusMap = {
        running:  ['#4caf50', '● работает'],
        exited:   ['#f44336', '● остановлен'],
        paused:   ['#ff9800', '● приостановлен'],
        unknown:  ['#888',    '● неизвестно'],
      };
      const [color, label] = statusMap[c.status] || statusMap.unknown;
   
      const uptime = c.startedAt ? this._fmtUptime(c.startedAt) : '—';
   
      el.innerHTML = `
        <div class="rt-status-row">
          <span style="color:${color};font-weight:500">${label}</span>
          <span class="rt-meta">betquant-realtime</span>
        </div>
        <div class="rt-status-row" style="margin-top:8px">
          <span class="rt-label">Uptime</span>
          <span class="rt-val">${uptime}</span>
        </div>
        <div class="rt-status-row">
          <span class="rt-label">Рестартов</span>
          <span class="rt-val">${c.restarts ?? '—'}</span>
        </div>
        <div class="rt-status-row">
          <span class="rt-label">Обновлено</span>
          <span class="rt-val">${new Date(data.timestamp).toLocaleTimeString('ru')}</span>
        </div>
      `;
   
      // Кнопки управления
      const btns = document.getElementById('rt-control-btns');
      if (btns) {
        const isRunning = c.status === 'running';
        btns.innerHTML = `
          <button class="ctrl-btn ${isRunning ? '' : 'primary'}"
                  onclick="realtimePanel.action('${isRunning ? 'restart' : 'start'}')"
                  style="background:${isRunning ? '' : 'var(--accent)'}">
            ${isRunning ? '🔄 Перезапустить' : '▶ Запустить'}
          </button>
          <button class="ctrl-btn" onclick="realtimePanel.action('stop')"
                  ${!isRunning ? 'disabled' : ''}>
            ⏹ Остановить
          </button>
          <button class="ctrl-btn" onclick="realtimePanel.toggleLogs()">
            📋 Логи
          </button>
          <button class="ctrl-btn" onclick="realtimePanel.refresh()">
            ↺ Обновить
          </button>
        `;
      }
    },
   
    // ── Рендер таблицы состояния данных ───────────────────────────────────
    _renderTables(tables) {
      const el = document.getElementById('rt-tables-body');
      if (!el) return;
   
      const expectedFreq = {
        live_stats:        'каждую минуту',
        odds:              'раз в сутки',
        odds_timeseries:   'раз в сутки',
        predictions:       'каждые 2ч',
        strategy_signals:  'каждые 2ч',
        player_stats:      'раз в сутки',
      };
   
      el.innerHTML = tables.map(t => {
        const dot   = t.healthy ? '●' : '●';
        const color = t.healthy ? '#4caf50' : (t.count > 0 ? '#ff9800' : '#f44336');
        const age   = t.ageMin === null ? '—'
                    : t.ageMin < 60    ? `${t.ageMin}м назад`
                    : t.ageMin < 1440  ? `${Math.round(t.ageMin/60)}ч назад`
                    : `${Math.round(t.ageMin/1440)}д назад`;
   
        return `
          <tr>
            <td><span style="color:${color}">${dot}</span> ${t.name}</td>
            <td style="text-align:right;font-family:var(--font-mono)">${t.count.toLocaleString('ru')}</td>
            <td style="color:var(--text3)">${age}</td>
            <td style="color:var(--text3);font-size:11px">${expectedFreq[t.name] || '—'}</td>
          </tr>
        `;
      }).join('');
    },
   
    // ── Рендер расхода The Odds API ────────────────────────────────────────
    _renderOddsUsage(api) {
      const el = document.getElementById('rt-odds-usage');
      if (!el || !api) return;
   
      const pctColor = api.pct > 80 ? '#f44336' : api.pct > 60 ? '#ff9800' : '#4caf50';
   
      el.innerHTML = `
        <div class="rt-status-row">
          <span class="rt-label">Запросов в этом месяце</span>
          <span class="rt-val" style="color:${pctColor}">${api.requestsThisMonth} / ${api.limit}</span>
        </div>
        <div style="margin:8px 0;background:var(--bg3);border-radius:4px;height:6px;overflow:hidden">
          <div style="height:100%;width:${api.pct}%;background:${pctColor};transition:width 0.5s"></div>
        </div>
        <div style="font-size:11px;color:var(--text3)">${api.pct}% использовано (лимит 400 для безопасности)</div>
      `;
    },
   
    // ── Логи ──────────────────────────────────────────────────────────────
    async toggleLogs() {
      const el = document.getElementById('rt-logs-wrap');
      if (!el) return;
      this._logsOpen = !this._logsOpen;
      el.style.display = this._logsOpen ? 'block' : 'none';
      if (this._logsOpen) {
        await this._loadLogs();
        this._logsTimer = setInterval(() => this._loadLogs(), 10000);
      } else {
        clearInterval(this._logsTimer);
      }
    },
   
    async _loadLogs() {
      const el = document.getElementById('rt-logs-output');
      if (!el) return;
      const data = await this._api('/api/realtime/logs?lines=80');
      if (!data?.lines) return;
   
      el.innerHTML = data.lines.map(line => {
        const cls = line.includes('✅') || line.includes('ℹ️') ? 'log-ok'
                  : line.includes('❌') || line.includes('error') ? 'log-err'
                  : line.includes('⚠️') ? 'log-warn'
                  : 'log-info';
        return `<div class="${cls}">${this._esc(line)}</div>`;
      }).join('');
    },
   
    // ── Действия с контейнером ─────────────────────────────────────────────
    async action(cmd) {
      const labels = { restart: 'Перезапустить', stop: 'Остановить', start: 'Запустить' };
      if (!confirm(`${labels[cmd]} контейнер betquant-realtime?`)) return;
   
      const msgEl = document.getElementById('rt-action-msg');
      if (msgEl) msgEl.textContent = '⏳ Выполняется...';
   
      const data = await this._api(`/api/realtime/${cmd}`, 'POST');
      if (msgEl) {
        msgEl.textContent = data?.message || (data?.ok ? 'OK' : 'Ошибка');
        msgEl.style.color = data?.ok ? '#4caf50' : '#f44336';
        setTimeout(() => { if (msgEl) msgEl.textContent = ''; }, 4000);
      }
   
      // Обновить статус через 2 секунды
      setTimeout(() => this.refresh(), 2000);
    },
   
    // ── Helpers ────────────────────────────────────────────────────────────
    async _api(url, method = 'GET') {
      try {
        const token = localStorage.getItem('bq_token') || 'demo';
        const resp = await fetch(url, {
          method,
          headers: { 'Content-Type': 'application/json', 'x-auth-token': token },
        });
        return resp.ok ? resp.json() : null;
      } catch { return null; }
    },
   
    _fmtUptime(startedAt) {
      const ms  = Date.now() - new Date(startedAt).getTime();
      const d   = Math.floor(ms / 86400000);
      const h   = Math.floor((ms % 86400000) / 3600000);
      const m   = Math.floor((ms % 3600000) / 60000);
      if (d > 0) return `${d}д ${h}ч`;
      if (h > 0) return `${h}ч ${m}м`;
      return `${m}м`;
    },
   
    _esc(s) {
      return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    },
  };