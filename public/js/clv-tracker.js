'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — CLV Tracker
//  Closing Line Value — главный индикатор навыка беттора
// ═══════════════════════════════════════════════════════════════════════════
const clvTracker = {
  bets:    [],
  stats:   {},
  charts:  {},

  // ── init ──────────────────────────────────────────────────────────────────
  async init() {
    await this.load();
  },

  async load() {
    this._setLoading(true);
    try {
      const d = await this._fetch('/api/clv/bets');
      if (!d) return;
      this.bets  = d.bets  || [];
      this.stats = d.stats || {};
      this.renderStats();
      this.renderTable();
      this.renderChart();
    } catch(e) { console.warn('[clvTracker]', e); }
    finally { this._setLoading(false); }
  },

  // ── stats panel ───────────────────────────────────────────────────────────
  renderStats() {
    const s = this.stats;
    const set = (id, v, cls) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = v;
      if (cls) el.className = cls;
    };
    set('clvAvg',      (s.avgClv >= 0 ? '+' : '') + (s.avgClv || 0).toFixed(2) + '%',
        s.avgClv > 0 ? 'positive' : s.avgClv < 0 ? 'negative' : '');
    set('clvPosRate',  (s.positiveClvPct || 0).toFixed(1) + '%');
    set('clvTotal',    s.totalBets   || 0);
    set('clvSettled',  s.settledBets || 0);
    set('clvWinRate',  (s.winRate    || 0).toFixed(1) + '%');
    set('clvPnl',      (s.totalPnl >= 0 ? '+' : '') + (s.totalPnl || 0).toFixed(2),
        s.totalPnl > 0 ? 'positive' : s.totalPnl < 0 ? 'negative' : '');

    const interp = document.getElementById('clvInterpretation');
    if (interp && s.interpretation) {
      interp.textContent = s.interpretation;
      interp.className = 'clv-interp ' + (s.avgClv > 2 ? 'good' : s.avgClv > 0 ? 'ok' : 'bad');
    }
  },

  // ── table ─────────────────────────────────────────────────────────────────
  renderTable() {
    const el = document.getElementById('clvTable');
    if (!el) return;
    if (!this.bets.length) {
      el.innerHTML = '<div class="empty-state" style="padding:32px;text-align:center">Нет ставок. Добавь первую через "+ Добавить ставку".</div>';
      return;
    }
    const rows = [...this.bets].reverse().map(b => {
      const betOdds  = +(b.bet_odds  || b.betOdds  || 0);
      const closOdds = +(b.closing_odds || b.closingOdds || 0);
      const clv      = closOdds ? (((betOdds / closOdds) - 1) * 100).toFixed(2) : null;
      const clvCls   = clv > 0 ? 'positive' : clv < 0 ? 'negative' : '';
      const resCls   = b.result === 'win' ? 'positive' : b.result === 'loss' ? 'negative' : '';
      const pnl      = b.pnl || 0;
      return `<tr>
        <td>${(b.bet_date || b.betDate || '').slice(0,10)}</td>
        <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${b.match_name || b.match || ''}</td>
        <td>${b.market || ''}</td>
        <td>${b.selection || ''}</td>
        <td><strong>${betOdds.toFixed(2)}</strong></td>
        <td>${closOdds ? closOdds.toFixed(2) : '<span style="color:var(--text3)">—</span>'}</td>
        <td class="${clvCls}">${clv !== null ? (clv > 0 ? '+' : '') + clv + '%' : '—'}</td>
        <td>${b.stake}</td>
        <td class="${resCls}">${b.result ? b.result.toUpperCase() : '—'}</td>
        <td class="${pnl >= 0 ? 'positive' : 'negative'}">${pnl ? (pnl > 0 ? '+' : '') + (+pnl).toFixed(2) : '—'}</td>
        <td>
          ${!closOdds ? `<button class="ctrl-btn sm" onclick="clvTracker.openCloseModal(${b.id})">Закрыть</button>` : ''}
        </td>
      </tr>`;
    }).join('');

    el.innerHTML = `<table class="data-table">
      <thead><tr>
        <th>Дата</th><th>Матч</th><th>Рынок</th><th>Исход</th>
        <th>Коэф ставки</th><th>Closing</th><th>CLV%</th>
        <th>Ставка</th><th>Рез</th><th>P&L</th><th></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  },

  // ── CLV trend chart ───────────────────────────────────────────────────────
  renderChart() {
    if (this.charts.clv) { try { this.charts.clv.destroy(); } catch(e){} }
    const cvs = document.getElementById('clvTrendChart');
    if (!cvs) return;

    const settled = this.bets.filter(b => (b.closing_odds || b.closingOdds));
    if (!settled.length) return;

    const clvs = settled.map(b => {
      const bo = +(b.bet_odds || b.betOdds);
      const co = +(b.closing_odds || b.closingOdds);
      return +((bo / co - 1) * 100).toFixed(2);
    });

    // Кумулятивный CLV
    let cumSum = 0;
    const cumClv = clvs.map(v => { cumSum += v; return +cumSum.toFixed(2); });

    const dk = document.body.classList.contains('dark-mode');
    const tc = dk ? '#94a3b8' : '#475569', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.06)';

    this.charts.clv = new Chart(cvs, {
      type: 'line',
      data: {
        labels: settled.map((_, i) => `#${i + 1}`),
        datasets: [
          {
            label: 'CLV%', data: clvs,
            borderColor: '#00d4ff', borderWidth: 1.5, pointRadius: 3,
            backgroundColor: clvs.map(v => v >= 0 ? 'rgba(0,230,118,.6)' : 'rgba(255,69,96,.6)'),
            type: 'bar',
            yAxisID: 'y',
          },
          {
            label: 'Кумулятивный CLV', data: cumClv,
            borderColor: '#ffd740', borderWidth: 2, pointRadius: 0, tension: .4, fill: false,
            type: 'line',
            yAxisID: 'y',
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { color: tc, font: { size: 11 } } }, tooltip: { mode: 'index', intersect: false } },
        scales: {
          x: { ticks: { color: tc, font: { size: 10 } }, grid: { color: gc } },
          y: { ticks: { color: tc, font: { size: 10 } }, grid: { color: gc },
               title: { display: true, text: 'CLV%', color: tc, font: { size: 10 } } },
        },
      },
    });
  },

  // ── add bet modal ─────────────────────────────────────────────────────────
  openAddModal() {
    const m = this._modal('clvAddModal');
    m.innerHTML = `
      <div class="modal-content" style="max-width:460px">
        <div class="modal-header"><span>+ Добавить ставку для CLV</span><button onclick="this.closest('.modal').style.display='none'">×</button></div>
        <div class="config-section">
          <div class="config-row"><label>Матч</label><input class="ctrl-input" id="clvAddMatch" placeholder="Arsenal vs Chelsea"></div>
          <div class="config-row"><label>Рынок</label>
            <select class="ctrl-select" id="clvAddMarket">
              <option value="1X2">1X2</option><option value="O/U 2.5">O/U 2.5</option>
              <option value="BTTS">BTTS</option><option value="AH">Asian Handicap</option><option value="Other">Другой</option>
            </select></div>
          <div class="config-row"><label>Исход</label><input class="ctrl-input" id="clvAddSelection" placeholder="Arsenal (Победа)"></div>
          <div class="config-row"><label>Коэффициент ставки</label><input type="number" class="ctrl-input" id="clvAddOdds" step="0.01" placeholder="2.10" min="1.01"></div>
          <div class="config-row"><label>Ставка (ед.)</label><input type="number" class="ctrl-input" id="clvAddStake" value="10" min="0.01"></div>
          <div class="config-row"><label>Дата ставки</label><input type="date" class="ctrl-input" id="clvAddDate" value="${new Date().toISOString().slice(0,10)}"></div>
        </div>
        <div style="display:flex;gap:8px;justify-content:flex-end;padding-top:8px">
          <button class="ctrl-btn" onclick="this.closest('.modal').style.display='none'">Отмена</button>
          <button class="ctrl-btn primary" onclick="clvTracker.saveBet()">Сохранить</button>
        </div>
      </div>`;
    m.style.display = 'flex';
  },

  async saveBet() {
    const matchName = document.getElementById('clvAddMatch')?.value?.trim();
    const betOdds   = parseFloat(document.getElementById('clvAddOdds')?.value);
    if (!matchName || !betOdds || betOdds < 1.01) {
      alert('Заполните матч и коэффициент'); return;
    }
    await this._fetch('/api/clv/bet', 'POST', {
      matchName,
      market:    document.getElementById('clvAddMarket')?.value,
      selection: document.getElementById('clvAddSelection')?.value,
      betOdds,
      stake:     parseFloat(document.getElementById('clvAddStake')?.value || 10),
      betDate:   document.getElementById('clvAddDate')?.value,
    });
    document.getElementById('clvAddModal').style.display = 'none';
    await this.load();
  },

  // ── close bet (add closing odds) ──────────────────────────────────────────
  openCloseModal(id) {
    const m = this._modal('clvCloseModal');
    m.innerHTML = `
      <div class="modal-content" style="max-width:400px">
        <div class="modal-header"><span>Закрыть ставку #${id}</span><button onclick="this.closest('.modal').style.display='none'">×</button></div>
        <div class="config-section">
          <div class="config-row"><label>Closing Odds (коэф. перед стартом)</label>
            <input type="number" class="ctrl-input" id="clvCloseOdds" step="0.01" placeholder="1.95" min="1.01"></div>
          <div class="config-row"><label>Результат</label>
            <select class="ctrl-select" id="clvCloseResult">
              <option value="">Ещё не сыгран</option>
              <option value="win">WIN</option>
              <option value="loss">LOSS</option>
              <option value="void">VOID</option>
            </select></div>
        </div>
        <div style="display:flex;gap:8px;justify-content:flex-end;padding-top:8px">
          <button class="ctrl-btn" onclick="this.closest('.modal').style.display='none'">Отмена</button>
          <button class="ctrl-btn primary" onclick="clvTracker.closeBet(${id})">Сохранить</button>
        </div>
      </div>`;
    m.style.display = 'flex';
  },

  async closeBet(id) {
    const closingOdds = parseFloat(document.getElementById('clvCloseOdds')?.value);
    if (!closingOdds || closingOdds < 1.01) { alert('Введите closing odds'); return; }
    const result = document.getElementById('clvCloseResult')?.value || null;
    await this._fetch(`/api/clv/bet/${id}/close`, 'PUT', { closingOdds, result: result || undefined });
    document.getElementById('clvCloseModal').style.display = 'none';
    await this.load();
  },

  // ── utils ─────────────────────────────────────────────────────────────────
  _modal(id) {
    let m = document.getElementById(id);
    if (!m) {
      m = Object.assign(document.createElement('div'), { id, className: 'modal' });
      m.onclick = e => { if (e.target === m) m.style.display = 'none'; };
      document.body.appendChild(m);
    }
    return m;
  },
  _setLoading(on) {
    const el = document.getElementById('clvLoading');
    if (el) el.style.display = on ? 'flex' : 'none';
  },
  async _fetch(url, method = 'GET', body = null) {
    const opts = { method, headers: { 'Content-Type': 'application/json', 'x-auth-token': localStorage.getItem('bq_token') || 'demo' } };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(r.status);
    return r.json();
  },
};