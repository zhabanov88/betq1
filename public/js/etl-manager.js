/**
 * BetQuant — ETL Manager UI
 * Управление загрузкой реальных данных через /api/etl/*
 * Поддерживает ETL v1 (футбол/хоккей/теннис/NBA/MLB) и
 * ETL v2 (баскетбол/крикет/регби/NFL/водное поло/волейбол)
 */

const etlManager = {
  _taskId: null,
  _pollInterval: null,

  // ── Загрузить статус БД ─────────────────────────────────────────
  async loadStatus() {
    const el = document.getElementById('etlDbStatus');
    if (!el) return;
    el.innerHTML = '<span style="color:var(--text3)">⏳ Загружаем...</span>';

    try {
      const data = await fetch('/api/stats/etl-status').then(r => r.json());

      const items = [
        { key: 'football_matches',      icon: '⚽', label: 'Футбол' },
        { key: 'hockey_matches',        icon: '🏒', label: 'Хоккей' },
        { key: 'tennis_extended',       icon: '🎾', label: 'Теннис' },
        { key: 'basketball_matches',    icon: '🏀', label: 'NBA v1' },
        { key: 'baseball_matches',      icon: '⚾', label: 'Бейсбол' },
        { key: 'basketball_matches_v2', icon: '🏀', label: 'NBA v2' },
        { key: 'cricket_matches',       icon: '🏏', label: 'Крикет' },
        { key: 'rugby_matches',         icon: '🏉', label: 'Регби' },
        { key: 'nfl_games',             icon: '🏈', label: 'NFL' },
        { key: 'waterpolo_matches',     icon: '🤽', label: 'Водное поло' },
        { key: 'volleyball_matches',    icon: '🏐', label: 'Волейбол' },
      ];

      const fmtN = n => n >= 1000000
        ? (n / 1000000).toFixed(1) + 'M'
        : n >= 1000
        ? (n / 1000).toFixed(0) + 'K'
        : String(n || 0);

      const totalRows = items.reduce((s, it) => s + (data[it.key] || 0), 0);

      let html = items.map(it => {
        const n = data[it.key] || 0;
        const color = n > 0 ? 'var(--green, #4caf50)' : 'var(--text3)';
        return `<span style="color:${color}">${it.icon} ${it.label}: <b>${fmtN(n)}</b></span>`;
      }).join('');

      const range = data._football_range;
      if (range && range.from_d && range.from_d !== '1970-01-01') {
        html += `<span style="color:var(--accent)">📅 Футбол: ${range.from_d} — ${range.to_d}</span>`;
      }

      if (totalRows === 0) {
        html = '<span style="color:var(--orange,#ff9800)">⚠️ БД пустая — нажми <b>▶ Запустить ETL</b> для загрузки данных</span>';
      } else {
        html += `<span style="font-weight:700;color:var(--accent)">ИТОГО: ${fmtN(totalRows)} строк</span>`;
      }

      el.innerHTML = html;
    } catch (e) {
      el.innerHTML = `<span style="color:var(--red,#f44)">❌ Ошибка: ${e.message}</span>`;
    }
  },

  // ── Запустить ETL ──────────────────────────────────────────────
  async run() {
    const sport   = document.getElementById('etlSport')?.value   || 'football';
    const seasons = document.getElementById('etlSeasons')?.value || '3';
    const mode    = document.getElementById('etlMode')?.value    || 'full';
    const version = document.getElementById('etlVersion')?.value || 'v1';
    const quick   = mode === 'quick';

    const btn = document.getElementById('etlRunBtn');
    if (btn) { btn.disabled = true; btn.textContent = '⏳ Запускаем...'; }

    const prog = document.getElementById('etlProgress');
    if (prog) prog.style.display = 'block';
    this._setProgress(0, 'Запускаем ETL...');

    try {
      const resp = await fetch('/api/etl/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sport, seasons: parseInt(seasons), quick, version }),
      });
      const data = await resp.json();

      if (!data.taskId) {
        this._setProgress(0, '❌ Ошибка запуска: ' + (data.error || 'unknown'));
        if (btn) { btn.disabled = false; btn.textContent = '▶ Запустить ETL'; }
        return;
      }

      this._taskId = data.taskId;
      this._startPolling();
    } catch (e) {
      this._setProgress(0, `❌ Ошибка: ${e.message}`);
      if (btn) { btn.disabled = false; btn.textContent = '▶ Запустить ETL'; }
    }
  },

  _startPolling() {
    if (this._pollInterval) clearInterval(this._pollInterval);
    this._pollInterval = setInterval(() => this._poll(), 2000);
  },

  async _poll() {
    if (!this._taskId) return;
    try {
      const data = await fetch(`/api/etl/progress/${this._taskId}`).then(r => r.json());
      this._setProgress(data.pct || 0, data.message || '');

      if (data.log && data.log.length) {
        const logEl = document.getElementById('etlLogOutput');
        if (logEl) {
          logEl.innerHTML = data.log.slice(-50).map(l => {
            const cls = l.startsWith('✅') || l.includes('✓') || l.includes('loaded')
                       ? 'color:var(--green,#4caf50)'
                       : l.startsWith('❌') || l.startsWith('ERR') ? 'color:var(--red,#f44)'
                       : l.startsWith('⚠') ? 'color:var(--orange,#ff9800)'
                       : 'color:var(--text2)';
            return `<div style="${cls}">${this._esc(l)}</div>`;
          }).join('');
          logEl.scrollTop = logEl.scrollHeight;
        }
      }

      if (data.status === 'done' || data.status === 'error') {
        clearInterval(this._pollInterval);
        this._pollInterval = null;
        const btn = document.getElementById('etlRunBtn');
        if (btn) { btn.disabled = false; btn.textContent = '▶ Запустить ETL'; }
        setTimeout(() => this.loadStatus(), 1000);
        if (data.status === 'done') {
          if (typeof app !== 'undefined' && app.showNotification) {
            setTimeout(() => app.showNotification('✅ ETL завершён! База данных заполнена.', 'success'), 500);
          }
        }
      }
    } catch (e) { /* игнорируем временные ошибки */ }
  },

  _setProgress(pct, msg) {
    const bar   = document.getElementById('etlProgressBar');
    const msgEl = document.getElementById('etlProgressMsg');
    const pctEl = document.getElementById('etlProgressPct');
    if (bar)   bar.style.width = `${pct}%`;
    if (msgEl) msgEl.textContent = msg || '';
    if (pctEl) pctEl.textContent = `${pct}%`;
  },

  _esc(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  },

  // ── Просмотр ETL лога ─────────────────────────────────────────
  async viewLog() {
    try {
      const rows = await fetch('/api/etl/log').then(r => r.json());
      if (!rows.length) {
        alert('ETL лог пустой. Запустите ETL чтобы загрузить данные.');
        return;
      }

      const html = `
        <div style="font-family:monospace;font-size:12px;max-height:60vh;overflow-y:auto">
          <table style="width:100%;border-collapse:collapse">
            <tr style="position:sticky;top:0;background:var(--bg2)">
              <th style="padding:6px;text-align:left">Время</th>
              <th>Спорт</th><th>Источник</th><th>Лига</th>
              <th>Сезон</th><th>Строк</th><th>Статус</th>
            </tr>
            ${rows.map(r => `
              <tr style="border-bottom:1px solid var(--border)">
                <td style="padding:4px 6px;color:var(--text3)">${(r.ts || '').slice(0, 16)}</td>
                <td style="padding:4px 6px">${r.sport}</td>
                <td style="padding:4px 6px;color:var(--text3)">${r.source}</td>
                <td style="padding:4px 6px">${r.league}</td>
                <td style="padding:4px 6px;color:var(--text3)">${r.season}</td>
                <td style="padding:4px 6px;text-align:right;color:var(--accent)">${Number(r.rows_loaded||0).toLocaleString()}</td>
                <td style="padding:4px 6px;color:${r.status==='ok'?'var(--green,#4c4)':r.status==='error'?'var(--red,#f44)':'var(--text3)'}">${r.status}</td>
              </tr>
            `).join('')}
          </table>
        </div>
      `;

      const modal = document.getElementById('sqlModal');
      if (modal) {
        const content = modal.querySelector('.modal-content');
        if (content) {
          content.innerHTML = `
            <div class="modal-header">
              <span>📋 ETL Log — история загрузок</span>
              <button onclick="this.closest('.modal').style.display='none'">×</button>
            </div>
            ${html}
          `;
          modal.style.display = 'flex';
        }
      } else {
        const win = window.open('', '_blank', 'width=900,height=600');
        win.document.write(`<html><body style="background:#111;color:#ccc">${html}</body></html>`);
      }
    } catch (e) {
      alert('Ошибка загрузки лога: ' + e.message);
    }
  },

  init() {
    this.loadStatus();
  }
};

// Автозапуск при загрузке страницы
document.addEventListener('DOMContentLoaded', () => {
  setTimeout(() => etlManager.loadStatus(), 2000);
});