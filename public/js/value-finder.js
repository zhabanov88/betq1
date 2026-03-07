'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Value Finder (реальные модели: Poisson + ELO)
//  Заменяет старый value-finder.js с демо-данными
// ═══════════════════════════════════════════════════════════════════════════
const valueFinder = {
  results:  [],
  scanning: false,
  charts:   {},
  calcMode: false,   // режим ручного калькулятора

  // ── init ──────────────────────────────────────────────────────────────────
  init() {
    this.renderFilters();
    this.scan();
  },

  // ── filters UI ────────────────────────────────────────────────────────────
  renderFilters() {
    const el = document.getElementById('vfFilters');
    if (!el) return;
    el.innerHTML = `
      <div class="config-row"><label>Мин. Edge %</label>
        <input type="number" class="ctrl-input" id="vfMinEdge" value="3" step="0.5" min="0"></div>
      <div class="config-row"><label>Лига</label>
        <select class="ctrl-select" id="vfLeague">
          <option value="">Все лиги</option>
          <option value="PL">Premier League</option>
          <option value="LL">La Liga</option>
          <option value="BL">Bundesliga</option>
          <option value="SA">Serie A</option>
          <option value="L1">Ligue 1</option>
        </select></div>
      <div class="config-row"><label>Рынок</label>
        <select class="ctrl-select" id="vfMarket">
          <option value="">Все рынки</option>
          <option value="homeWin">1 (Победа хозяев)</option>
          <option value="draw">X (Ничья)</option>
          <option value="awayWin">2 (Победа гостей)</option>
          <option value="over25">Тотал Больше 2.5</option>
          <option value="btts">BTTS</option>
        </select></div>
      <div class="config-row"><label>Модель</label>
        <select class="ctrl-select" id="vfModel">
          <option value="ensemble">Ensemble (Poisson+ELO)</option>
          <option value="poisson">Poisson (Dixon-Coles)</option>
          <option value="elo">ELO Rating</option>
        </select></div>`;
  },

  // ── scan ──────────────────────────────────────────────────────────────────
  async scan() {
    if (this.scanning) return;
    this.scanning = true;
    const btn = document.getElementById('vfScanBtn');
    if (btn) { btn.textContent = '⏳ Расчёт...'; btn.disabled = true; }
    this._progress(true);

    try {
      const minEdge = parseFloat(document.getElementById('vfMinEdge')?.value || 3);
      const league  = document.getElementById('vfLeague')?.value || '';
      const market  = document.getElementById('vfMarket')?.value || '';
      const model   = document.getElementById('vfModel')?.value || 'ensemble';

      const qs = new URLSearchParams({ minEdge });
      if (league) qs.set('league', league);

      const d = await this._fetch(`/api/value/scan?${qs}`);
      if (!d) return;

      let bets = d.bets || [];
      if (market) bets = bets.filter(b => b.market === market);

      this.results = bets;
      this.renderResults();
      this.renderChart();
      this.renderSummary(d.models);
    } catch(e) {
      console.warn('[valueFinder]', e);
      this._showError('Ошибка сканирования. Проверьте подключение к серверу.');
    } finally {
      this.scanning = false;
      this._progress(false);
      if (btn) { btn.textContent = '🔍 Сканировать'; btn.disabled = false; }
    }
  },

  // ── results table ─────────────────────────────────────────────────────────
  renderResults() {
    const el = document.getElementById('vfResultsTable');
    if (!el) return;
    if (!this.results.length) {
      el.innerHTML = '<div class="empty-state" style="padding:32px;text-align:center">Нет value ставок по заданным фильтрам. Снизьте Min Edge.</div>';
      return;
    }
    const rows = this.results.map(r => `
      <tr>
        <td><span class="bt-strat-sport-tag">${r.league}</span></td>
        <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${r.match}</td>
        <td><span class="bt-tag single">${this._mktLabel(r.market)}</span></td>
        <td><strong>${r.odds}</strong></td>
        <td>${r.impliedProb}%</td>
        <td class="positive"><strong>${r.modelProb}%</strong></td>
        <td class="positive"><strong>+${r.edge}%</strong></td>
        <td>${r.kelly}%</td>
        <td style="font-size:11px;color:var(--text3)">${r.lH} / ${r.lA}</td>
        <td>
          <button class="ctrl-btn sm" onclick="valueFinder.showMatrix('${r.match.replace(/'/g,"\\'")}',${r.lH},${r.lA})">Матрица</button>
          <button class="ctrl-btn sm" onclick="valueFinder.addWatchlist('${r.match.replace(/'/g,"\\'")}','${r.market}',${r.odds})">+ Watch</button>
        </td>
      </tr>`).join('');

    el.innerHTML = `<table class="data-table">
      <thead><tr>
        <th>Лига</th><th>Матч</th><th>Рынок</th>
        <th>Коэф</th><th>Рынок%</th><th>Модель%</th><th>Edge%</th>
        <th>Kelly%</th><th>λH/λA</th><th></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  },

  renderSummary(models) {
    const el = document.getElementById('vfSummary');
    if (!el || !this.results.length) return;
    const avg = (this.results.reduce((s, r) => s + r.edge, 0) / this.results.length).toFixed(1);
    const best = this.results[0];
    el.innerHTML = `
      Найдено: <strong>${this.results.length}</strong> value ставок &nbsp;|&nbsp;
      Средний Edge: <strong class="positive">+${avg}%</strong> &nbsp;|&nbsp;
      Лучшая: <strong>${best?.match} → ${this._mktLabel(best?.market)} +${best?.edge}%</strong>
      <span style="font-size:10px;color:var(--text3);margin-left:8px">Модели: ${(models||[]).join(', ')}</span>`;
  },

  // ── odds chart ────────────────────────────────────────────────────────────
  renderChart() {
    if (this.charts.value) { try { this.charts.value.destroy(); } catch(e){} }
    const cvs = document.getElementById('chartVFValue');
    if (!cvs || !this.results.length) return;
    const dk = document.body.classList.contains('dark-mode');
    const tc = dk ? '#8892a4' : '#4a5568', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.07)';
    const top = this.results.slice(0, 12);
    this.charts.value = new Chart(cvs, {
      type: 'bar',
      data: {
        labels: top.map(r => r.home + ' ' + this._mktLabel(r.market)),
        datasets: [
          { label: 'Edge %',     data: top.map(r => r.edge),       backgroundColor: 'rgba(0,212,255,.75)',  borderRadius: 4 },
          { label: 'Implied %',  data: top.map(r => r.impliedProb), backgroundColor: 'rgba(148,163,184,.4)', borderRadius: 4 },
          { label: 'Model %',    data: top.map(r => r.modelProb),   backgroundColor: 'rgba(0,230,118,.65)',  borderRadius: 4 },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { color: tc, font: { size: 11 } } } },
        scales: {
          x: { ticks: { color: tc, font: { size: 9 }, maxRotation: 35 }, grid: { color: gc } },
          y: { ticks: { color: tc, font: { size: 10 } }, grid: { color: gc } },
        },
      },
    });
  },

  // ── score matrix modal ────────────────────────────────────────────────────
  async showMatrix(matchName, lH, lA) {
    const d = await this._fetch(`/api/value/calculate`, 'POST', {
      home: matchName.split(' vs ')[0], away: matchName.split(' vs ')[1],
      homeAttack: lH / 1.45, homeDefense: 1, awayAttack: lA / 1.15, awayDefense: 1,
    });
    if (!d) return;

    const mat     = d.pois?.matrix || [];
    const scores  = d.pois?.topScores || [];
    const maxProb = Math.max(...mat.flat());

    const rows = mat.slice(0, 7).map((row, h) =>
      '<tr><td style="font-weight:600;background:var(--bg3)">' + h + '</td>' +
      row.slice(0, 7).map((p, a) => {
        const pct  = (p * 100).toFixed(1);
        const heat = Math.round((p / maxProb) * 100);
        const bg   = h > a
          ? `rgba(0,212,255,${p / maxProb * .6})`
          : h === a
          ? `rgba(255,215,64,${p / maxProb * .6})`
          : `rgba(255,69,96,${p / maxProb * .6})`;
        return `<td style="background:${bg};text-align:center;font-size:11px">${pct}%</td>`;
      }).join('') + '</tr>'
    ).join('');

    const header = '<tr><td></td>' + [0,1,2,3,4,5,6].map(a => `<td style="text-align:center;font-weight:600;background:var(--bg3)">${a}</td>`).join('') + '</tr>';

    const modal = document.getElementById('vfMatrixModal') || this._createMatrixModal();
    modal.innerHTML = `
      <div class="modal-content large" style="max-width:700px">
        <div class="modal-header">
          <span>Матрица счёта — ${matchName}</span>
          <span style="font-size:12px;color:var(--text3)">λ Хозяева: ${lH} &nbsp;|&nbsp; λ Гости: ${lA}</span>
          <button onclick="this.closest('.modal').style.display='none'">×</button>
        </div>
        <div style="overflow-x:auto;margin-bottom:16px">
          <div style="font-size:11px;color:var(--text3);margin-bottom:6px">Строки = голы хозяев, Столбцы = голы гостей</div>
          <table class="data-table" style="font-size:12px">${header}${rows}</table>
        </div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px">
          <div class="kpi-card"><span>1 (Хозяева)</span><strong class="positive">${(d.pois?.homeWin*100).toFixed(1)}%</strong></div>
          <div class="kpi-card"><span>X (Ничья)</span><strong>${(d.pois?.draw*100).toFixed(1)}%</strong></div>
          <div class="kpi-card"><span>2 (Гости)</span><strong class="negative">${(d.pois?.awayWin*100).toFixed(1)}%</strong></div>
          <div class="kpi-card"><span>Over 2.5</span><strong>${(d.pois?.over25*100).toFixed(1)}%</strong></div>
          <div class="kpi-card"><span>BTTS</span><strong>${(d.pois?.btts*100).toFixed(1)}%</strong></div>
          <div class="kpi-card"><span>ELO Хозяева/Гости</span><strong>${d.elo ? ((d.elo.homeWin||0)*100).toFixed(1)+'% / '+((d.elo.awayWin||0)*100).toFixed(1)+'%' : '—'}</strong></div>
        </div>
        <div>
          <div style="font-size:11px;font-weight:600;color:var(--text3);margin-bottom:6px">ТОП счётов по вероятности:</div>
          <div style="display:flex;gap:6px;flex-wrap:wrap">
            ${scores.map(sc => `<span class="chip">${sc.score} — ${(sc.prob*100).toFixed(1)}%</span>`).join('')}
          </div>
        </div>
      </div>`;
    modal.style.display = 'flex';
  },

  _createMatrixModal() {
    const m = document.createElement('div');
    m.id = 'vfMatrixModal'; m.className = 'modal';
    m.onclick = e => { if (e.target === m) m.style.display = 'none'; };
    document.body.appendChild(m);
    return m;
  },

  addWatchlist(match, market, odds) {
    const list = JSON.parse(localStorage.getItem('bq_watchlist') || '[]');
    list.push({ match, market, odds, added: new Date().toISOString() });
    localStorage.setItem('bq_watchlist', JSON.stringify(list));
    const btn = event?.target;
    if (btn) { btn.textContent = '✓ Добавлено'; btn.style.color = 'var(--green)'; }
  },

  // ── utils ─────────────────────────────────────────────────────────────────
  _mktLabel(k) {
    return { homeWin:'1 (Хозяева)', draw:'X (Ничья)', awayWin:'2 (Гости)',
             over25:'Over 2.5', over15:'Over 1.5', over35:'Over 3.5', btts:'BTTS' }[k] || k;
  },
  _progress(on) {
    const el = document.getElementById('valueScanProgress');
    if (el) el.style.display = on ? 'block' : 'none';
  },
  _showError(msg) {
    const el = document.getElementById('vfResultsTable');
    if (el) el.innerHTML = `<div class="empty-state" style="padding:32px;text-align:center;color:var(--red)">${msg}</div>`;
  },
  async _fetch(url, method = 'GET', body = null) {
    const opts = { method, headers: { 'Content-Type': 'application/json', 'x-auth-token': localStorage.getItem('bq_token') || 'demo' } };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(`${r.status}`);
    return r.json();
  },
};