'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Bankroll Manager
//  Kelly, Fixed, Martingale, D'Alembert стейкинг · Stop-loss · ROI тренд
// ═══════════════════════════════════════════════════════════════════════════
const bankrollManager = {
  config: {
    startBankroll: 1000,
    currentBankroll: 1000,
    staking: 'kelly',       // kelly | fixed | flat | martingale | dalembert
    kellyFraction: 0.25,    // четверть Келли
    fixedPct: 2,            // % от банка
    flatStake: 20,          // фиксированная сумма
    maxStakePct: 5,         // максимум % от банка на ставку
    stopLoss: 20,           // стоп-лосс % от начального банка
    stopLossEnabled: true,
    takeProfitPct: 50,      // тейк-профит %
    takeProfitEnabled: false,
    dailyLossLimit: 10,     // дневной лимит потерь %
    dailyLossEnabled: true,
  },
  history:  [],
  charts:   {},
  stopped:  false,

  // ── lifecycle ─────────────────────────────────────────────────────────────
  init() {
    this._loadState();
    this.render();
  },

  // ── render ────────────────────────────────────────────────────────────────
  render() {
    this._renderConfig();
    this._renderKPIs();
    this._renderChart();
    this._renderHistory();
    this._checkStopLoss();
  },

  _renderConfig() {
    const el = document.getElementById('bm-config');
    if (!el) return;
    const c = this.config;
    el.innerHTML = `
      <div class="config-section">
        <div class="config-title">💰 Параметры банка</div>
        <div class="config-row">
          <label>Начальный банк</label>
          <input type="number" class="ctrl-input" id="bmStart" value="${c.startBankroll}" min="1" step="10">
        </div>
        <div class="config-row">
          <label>Текущий банк</label>
          <input type="number" class="ctrl-input" id="bmCurrent" value="${c.currentBankroll.toFixed(2)}" min="0" step="0.01">
        </div>
      </div>

      <div class="config-section">
        <div class="config-title">📐 Метод стейкинга</div>
        <div class="config-row">
          <label>Метод</label>
          <select class="ctrl-select" id="bmStaking" onchange="bankrollManager.onStakingChange()">
            <option value="kelly"      ${c.staking==='kelly'       ? 'selected':''}>Kelly (рекомендуется)</option>
            <option value="fixed"      ${c.staking==='fixed'       ? 'selected':''}>Fixed % банка</option>
            <option value="flat"       ${c.staking==='flat'        ? 'selected':''}>Flat (фиксированная)</option>
            <option value="martingale" ${c.staking==='martingale'  ? 'selected':''}>Martingale ⚠️</option>
            <option value="dalembert"  ${c.staking==='dalembert'   ? 'selected':''}>D'Alembert</option>
          </select>
        </div>
        <div id="bm-staking-params">
          ${this._stakingParamsHtml()}
        </div>
      </div>

      <div class="config-section">
        <div class="config-title">🛡 Защита банка</div>
        <div class="config-row">
          <label>Stop-Loss %</label>
          <label class="toggle-switch" style="margin-left:auto">
            <input type="checkbox" id="bmStopLossEnabled" ${c.stopLossEnabled ? 'checked':''} onchange="bankrollManager.saveConfig()">
            <span class="toggle-slider"></span>
          </label>
          <input type="number" class="ctrl-input" id="bmStopLoss" value="${c.stopLoss}" min="1" max="50" style="width:80px">
          <span style="font-size:11px;color:var(--text3)">% от старта</span>
        </div>
        <div class="config-row">
          <label>Take-Profit %</label>
          <label class="toggle-switch" style="margin-left:auto">
            <input type="checkbox" id="bmTPEnabled" ${c.takeProfitEnabled ? 'checked':''} onchange="bankrollManager.saveConfig()">
            <span class="toggle-slider"></span>
          </label>
          <input type="number" class="ctrl-input" id="bmTakeProfit" value="${c.takeProfitPct}" min="1" max="500" style="width:80px">
          <span style="font-size:11px;color:var(--text3)">% прибыли</span>
        </div>
        <div class="config-row">
          <label>Дневной лимит %</label>
          <label class="toggle-switch" style="margin-left:auto">
            <input type="checkbox" id="bmDailyEnabled" ${c.dailyLossEnabled ? 'checked':''} onchange="bankrollManager.saveConfig()">
            <span class="toggle-slider"></span>
          </label>
          <input type="number" class="ctrl-input" id="bmDailyLoss" value="${c.dailyLossLimit}" min="1" max="30" style="width:80px">
          <span style="font-size:11px;color:var(--text3)">% потерь/день</span>
        </div>
        <div class="config-row">
          <label>Макс. ставка %</label>
          <input type="number" class="ctrl-input" id="bmMaxStake" value="${c.maxStakePct}" min="1" max="25" style="width:80px">
        </div>
      </div>

      <button class="ctrl-btn primary" onclick="bankrollManager.saveConfig()" style="width:100%;margin-top:8px">
        💾 Сохранить настройки
      </button>`;
  },

  _stakingParamsHtml() {
    const c = this.config;
    const map = {
      kelly:      `<div class="config-row"><label>Доля Келли</label><input type="number" class="ctrl-input" id="bmKellyFrac" value="${c.kellyFraction}" min="0.05" max="1" step="0.05"><span style="font-size:11px;color:var(--text3)">0.25 = четверть-Келли</span></div>`,
      fixed:      `<div class="config-row"><label>% банка</label><input type="number" class="ctrl-input" id="bmFixedPct" value="${c.fixedPct}" min="0.1" max="25" step="0.1"></div>`,
      flat:       `<div class="config-row"><label>Сумма (ед.)</label><input type="number" class="ctrl-input" id="bmFlatStake" value="${c.flatStake}" min="0.01"></div>`,
      martingale: `<div class="bm-warning">⚠️ Мартингейл увеличивает ставку после каждого проигрыша — высокий риск. Используй с осторожностью.</div>`,
      dalembert:  `<div class="config-row"><label>% банка (шаг)</label><input type="number" class="ctrl-input" id="bmDalembert" value="${c.fixedPct}" min="0.5" max="10" step="0.5"></div>`,
    };
    return map[c.staking] || '';
  },

  onStakingChange() {
    const v = document.getElementById('bmStaking')?.value;
    if (v) {
      this.config.staking = v;
      const el = document.getElementById('bm-staking-params');
      if (el) el.innerHTML = this._stakingParamsHtml();
    }
  },

  saveConfig() {
    const g = id => document.getElementById(id);
    const pf = (id, def) => parseFloat(g(id)?.value || def) || def;
    this.config = {
      ...this.config,
      startBankroll:   pf('bmStart',    1000),
      currentBankroll: pf('bmCurrent',  this.config.currentBankroll),
      staking:         g('bmStaking')?.value || 'kelly',
      kellyFraction:   pf('bmKellyFrac', 0.25),
      fixedPct:        pf('bmFixedPct',  2),
      flatStake:       pf('bmFlatStake', 20),
      maxStakePct:     pf('bmMaxStake',  5),
      stopLoss:        pf('bmStopLoss',  20),
      stopLossEnabled: g('bmStopLossEnabled')?.checked ?? true,
      takeProfitPct:   pf('bmTakeProfit', 50),
      takeProfitEnabled: g('bmTPEnabled')?.checked ?? false,
      dailyLossLimit:  pf('bmDailyLoss', 10),
      dailyLossEnabled: g('bmDailyEnabled')?.checked ?? true,
    };
    this._saveState();
    this.render();
    this._toast('✅ Настройки сохранены');
  },

  // ── KPI panel ─────────────────────────────────────────────────────────────
  _renderKPIs() {
    const el = document.getElementById('bm-kpis');
    if (!el) return;
    const c   = this.config;
    const pnl = c.currentBankroll - c.startBankroll;
    const roi  = (pnl / c.startBankroll * 100);
    const dd   = this._maxDrawdown();
    const slPct = c.stopLoss;
    const slLevel = c.startBankroll * (1 - slPct / 100);
    const ddPct  = this._currentDrawdown();
    const dailyPnl = this._dailyPnl();

    // Статус
    const status = this.stopped
      ? `<div class="bm-status stopped">🛑 СТОП-ЛОСС АКТИВИРОВАН</div>`
      : ddPct > slPct * 0.75
      ? `<div class="bm-status warning">⚠️ Приближаемся к стоп-лоссу (${ddPct.toFixed(1)}% / ${slPct}%)</div>`
      : `<div class="bm-status ok">✅ В норме</div>`;

    el.innerHTML = `
      ${status}
      <div class="clv-kpis">
        <div class="clv-kpi">
          <div class="clv-kpi-label">Текущий банк</div>
          <div class="clv-kpi-val">${c.currentBankroll.toFixed(2)}</div>
        </div>
        <div class="clv-kpi">
          <div class="clv-kpi-label">P&L всего</div>
          <div class="clv-kpi-val ${pnl >= 0 ? 'positive' : 'negative'}">${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}</div>
        </div>
        <div class="clv-kpi">
          <div class="clv-kpi-label">ROI</div>
          <div class="clv-kpi-val ${roi >= 0 ? 'positive' : 'negative'}">${roi >= 0 ? '+' : ''}${roi.toFixed(1)}%</div>
        </div>
        <div class="clv-kpi">
          <div class="clv-kpi-label">Max Drawdown</div>
          <div class="clv-kpi-val ${dd > 20 ? 'negative' : ''}">${dd.toFixed(1)}%</div>
        </div>
        <div class="clv-kpi">
          <div class="clv-kpi-label">Stop-Loss уровень</div>
          <div class="clv-kpi-val">${slLevel.toFixed(2)}</div>
        </div>
        <div class="clv-kpi">
          <div class="clv-kpi-label">P&L сегодня</div>
          <div class="clv-kpi-val ${dailyPnl >= 0 ? 'positive' : 'negative'}">${dailyPnl >= 0 ? '+' : ''}${dailyPnl.toFixed(2)}</div>
        </div>
      </div>`;
  },

  // ── Stake calculator ───────────────────────────────────────────────────────
  calcStake(odds, prob) {
    const c = this.config;
    if (this.stopped) return 0;

    let stake = 0;
    switch (c.staking) {
      case 'kelly': {
        const k = ((odds - 1) * prob - (1 - prob)) / (odds - 1);
        stake = Math.max(0, k * c.kellyFraction * c.currentBankroll);
        break;
      }
      case 'fixed':
        stake = c.currentBankroll * (c.fixedPct / 100);
        break;
      case 'flat':
        stake = c.flatStake;
        break;
      case 'martingale': {
        const lastBet = this.history.at(-1);
        stake = lastBet?.result === 'loss'
          ? (lastBet.stake * 2)
          : (c.currentBankroll * 0.02);
        break;
      }
      case 'dalembert': {
        const seq = this.history.slice(-10);
        const losses  = seq.filter(b => b.result === 'loss').length;
        const wins    = seq.filter(b => b.result === 'win').length;
        const base    = c.currentBankroll * (c.fixedPct / 100);
        stake = Math.max(base, base + (losses - wins) * base * 0.5);
        break;
      }
    }

    return Math.min(stake, c.currentBankroll * (c.maxStakePct / 100));
  },

  // ── Record bet ─────────────────────────────────────────────────────────────
  recordBet(bet) {
    const pnl = bet.result === 'win'
      ? +(bet.stake * (bet.odds - 1)).toFixed(2)
      : bet.result === 'void' ? 0 : -bet.stake;

    this.config.currentBankroll = +(this.config.currentBankroll + pnl).toFixed(2);
    this.history.push({ ...bet, pnl, bank: this.config.currentBankroll, date: new Date().toISOString() });
    this._saveState();
    this.render();
    return pnl;
  },

  openBetModal() {
    const m = this._modal('bmBetModal');
    m.innerHTML = `
      <div class="modal-content" style="max-width:440px">
        <div class="modal-header"><span>+ Записать ставку</span><button onclick="this.closest('.modal').style.display='none'">×</button></div>
        <div class="config-section">
          <div class="config-row"><label>Матч</label><input class="ctrl-input" id="bmBetMatch" placeholder="Arsenal vs Chelsea"></div>
          <div class="config-row"><label>Коэффициент</label><input type="number" class="ctrl-input" id="bmBetOdds" step="0.01" placeholder="2.10" min="1.01" oninput="bankrollManager.updateStakeSuggestion()"></div>
          <div class="config-row"><label>Вероятность (модель)</label><input type="number" class="ctrl-input" id="bmBetProb" step="0.01" placeholder="0.55" min="0.01" max="0.99" oninput="bankrollManager.updateStakeSuggestion()"></div>
          <div class="bm-stake-suggest" id="bmStakeSuggest"></div>
          <div class="config-row"><label>Ставка (ед.)</label><input type="number" class="ctrl-input" id="bmBetStake" step="0.01" min="0.01"></div>
          <div class="config-row"><label>Результат</label>
            <select class="ctrl-select" id="bmBetResult">
              <option value="pending">Ожидает</option>
              <option value="win">Win</option>
              <option value="loss">Loss</option>
              <option value="void">Void</option>
            </select>
          </div>
        </div>
        <div style="display:flex;gap:8px;justify-content:flex-end">
          <button class="ctrl-btn" onclick="this.closest('.modal').style.display='none'">Отмена</button>
          <button class="ctrl-btn primary" onclick="bankrollManager.saveBet()">Записать</button>
        </div>
      </div>`;
    m.style.display = 'flex';
  },

  updateStakeSuggestion() {
    const odds = parseFloat(document.getElementById('bmBetOdds')?.value) || 0;
    const prob = parseFloat(document.getElementById('bmBetProb')?.value) || 0;
    const el   = document.getElementById('bmStakeSuggest');
    if (!el || odds < 1.01 || prob < 0.01) return;
    const suggested = this.calcStake(odds, prob);
    const edge = prob - 1/odds;
    const stakeEl = document.getElementById('bmBetStake');
    if (stakeEl && !stakeEl.value) stakeEl.value = suggested.toFixed(2);
    el.innerHTML = `<span class="${edge > 0 ? 'positive' : 'negative'}">
      Рекомендованная ставка (${this.config.staking}): <strong>${suggested.toFixed(2)}</strong> |
      Edge: ${edge > 0 ? '+' : ''}${(edge*100).toFixed(1)}%
    </span>`;
  },

  saveBet() {
    const g = id => document.getElementById(id);
    const match  = g('bmBetMatch')?.value?.trim();
    const odds   = parseFloat(g('bmBetOdds')?.value);
    const stake  = parseFloat(g('bmBetStake')?.value);
    const result = g('bmBetResult')?.value;
    if (!match || !odds || !stake) { alert('Заполните матч, коэффициент и ставку'); return; }
    this.recordBet({ match, odds, stake, result: result || 'pending' });
    document.getElementById('bmBetModal').style.display = 'none';
    this._toast(`✅ Записано: ${match}`);
  },

  // ── Equity chart ──────────────────────────────────────────────────────────
  _renderChart() {
    if (this.charts.equity) { try { this.charts.equity.destroy(); } catch(e){} }
    const cvs = document.getElementById('bm-equity-chart');
    if (!cvs) return;

    const c   = this.config;
    const data = [c.startBankroll, ...this.history.map(b => b.bank)];
    const slLevel = c.startBankroll * (1 - c.stopLoss / 100);
    const dk  = document.body.classList.contains('dark-mode');
    const tc  = dk ? '#94a3b8' : '#475569', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.06)';

    this.charts.equity = new Chart(cvs, {
      type: 'line',
      data: {
        labels: data.map((_, i) => i === 0 ? 'Старт' : `#${i}`),
        datasets: [
          {
            label: 'Банк', data,
            borderColor: '#00d4ff', borderWidth: 2, pointRadius: data.length < 30 ? 3 : 0,
            backgroundColor: 'rgba(0,212,255,.06)', fill: true, tension: .3,
          },
          {
            label: 'Stop-Loss',
            data: data.map(() => slLevel),
            borderColor: 'rgba(255,69,96,.6)', borderWidth: 1.5, borderDash: [6, 3],
            pointRadius: 0, fill: false,
          },
          ...(c.takeProfitEnabled ? [{
            label: 'Take-Profit',
            data: data.map(() => c.startBankroll * (1 + c.takeProfitPct / 100)),
            borderColor: 'rgba(0,230,118,.5)', borderWidth: 1.5, borderDash: [6, 3],
            pointRadius: 0, fill: false,
          }] : []),
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: {
          legend: { labels: { color: tc, font: { size: 11 } } },
          tooltip: { mode: 'index', intersect: false },
        },
        scales: {
          x: { ticks: { color: tc, font: { size: 10 }, maxTicksLimit: 10 }, grid: { color: gc } },
          y: { ticks: { color: tc, font: { size: 10 } }, grid: { color: gc } },
        },
      },
    });
  },

  _renderHistory() {
    const el = document.getElementById('bm-history');
    if (!el) return;
    const hist = [...this.history].reverse().slice(0, 50);
    if (!hist.length) {
      el.innerHTML = '<div class="lm-empty">Нет записей. Добавь первую ставку!</div>';
      return;
    }
    const rows = hist.map(b => `<tr>
      <td>${(b.date||'').slice(0,10)}</td>
      <td>${b.match}</td>
      <td>${(+b.odds).toFixed(2)}</td>
      <td>${(+b.stake).toFixed(2)}</td>
      <td><span class="chip ${b.result==='win'?'green':b.result==='loss'?'red':''}">${b.result?.toUpperCase()}</span></td>
      <td class="${b.pnl>=0?'positive':'negative'}">${b.pnl>=0?'+':''}${(+b.pnl).toFixed(2)}</td>
      <td style="font-family:var(--font-mono)">${(+b.bank).toFixed(2)}</td>
    </tr>`).join('');
    el.innerHTML = `<table class="data-table"><thead><tr>
      <th>Дата</th><th>Матч</th><th>Коэф</th><th>Ставка</th><th>Рез</th><th>P&L</th><th>Банк</th>
    </tr></thead><tbody>${rows}</tbody></table>`;
  },

  // ── Stop-loss check ────────────────────────────────────────────────────────
  _checkStopLoss() {
    if (!this.config.stopLossEnabled) return;
    const slLevel = this.config.startBankroll * (1 - this.config.stopLoss / 100);
    if (this.config.currentBankroll <= slLevel && !this.stopped) {
      this.stopped = true;
      this._saveState();
      setTimeout(() => {
        const el = document.getElementById('bm-stopLossAlert');
        if (el) el.style.display = 'block';
      }, 100);
    }
    if (this.config.currentBankroll > slLevel) this.stopped = false;
  },

  resetStopLoss() {
    this.stopped = false;
    this._saveState();
    this.render();
    this._toast('✅ Стоп-лосс сброшен');
  },

  // ── Stats helpers ─────────────────────────────────────────────────────────
  _maxDrawdown() {
    const curve = [this.config.startBankroll, ...this.history.map(b => b.bank)];
    let peak = 0, maxDD = 0;
    for (const v of curve) {
      if (v > peak) peak = v;
      const dd = (peak - v) / peak * 100;
      if (dd > maxDD) maxDD = dd;
    }
    return +maxDD.toFixed(1);
  },
  _currentDrawdown() {
    const curve = [this.config.startBankroll, ...this.history.map(b => b.bank)];
    let peak = 0;
    for (const v of curve) if (v > peak) peak = v;
    const dd = (peak - this.config.currentBankroll) / peak * 100;
    return +dd.toFixed(1);
  },
  _dailyPnl() {
    const today = new Date().toDateString();
    return this.history
      .filter(b => new Date(b.date).toDateString() === today)
      .reduce((s, b) => s + (b.pnl || 0), 0);
  },

  // ── Persistence ────────────────────────────────────────────────────────────
  _saveState() {
    localStorage.setItem('bq_bankroll', JSON.stringify({ config: this.config, history: this.history.slice(-500), stopped: this.stopped }));
  },
  _loadState() {
    const saved = JSON.parse(localStorage.getItem('bq_bankroll') || 'null');
    if (saved) {
      this.config  = { ...this.config, ...saved.config };
      this.history = saved.history || [];
      this.stopped = saved.stopped || false;
    } else {
      // Demo data
      this.history = this._demoHistory();
      this.config.currentBankroll = this.history.at(-1)?.bank || 1000;
    }
  },
  _demoHistory() {
    const hist = [];
    let bank = 1000;
    const matches = ['Arsenal vs Chelsea','Bayern vs Dortmund','Real vs Atletico','PSG vs Monaco','Inter vs Juventus'];
    for (let i = 0; i < 20; i++) {
      const odds = 1.5 + Math.random() * 2;
      const stake = bank * 0.02;
      const result = Math.random() < (1/odds * 1.08) ? 'win' : 'loss';
      const pnl = result === 'win' ? stake*(odds-1) : -stake;
      bank = +(bank + pnl).toFixed(2);
      hist.push({ match: matches[i%5], odds:+odds.toFixed(2), stake:+stake.toFixed(2), result, pnl:+pnl.toFixed(2), bank, date: new Date(Date.now()-(20-i)*86400000).toISOString() });
    }
    return hist;
  },

  // ── utils ─────────────────────────────────────────────────────────────────
  _modal(id) {
    let m = document.getElementById(id);
    if (!m) { m = Object.assign(document.createElement('div'), { id, className:'modal' }); m.onclick = e=>{if(e.target===m)m.style.display='none';}; document.body.appendChild(m); }
    return m;
  },
  _toast(msg) {
    let t = document.getElementById('bq-toast');
    if (!t) { t = Object.assign(document.createElement('div'),{id:'bq-toast',className:'bq-toast'}); document.body.append(t); }
    t.textContent=msg; t.classList.add('show'); clearTimeout(t._tm); t._tm=setTimeout(()=>t.classList.remove('show'),3000);
  },
};