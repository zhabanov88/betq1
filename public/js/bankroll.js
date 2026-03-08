'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Bankroll Manager
//  Данные из PostgreSQL + localStorage. Демо — только при bq_demo_mode=true.
// ═══════════════════════════════════════════════════════════════════════════
const bankrollManager = {
  config: {
    startBankroll: 1000,
    currentBankroll: 1000,
    staking: 'kelly',
    kellyFraction: 0.25,
    fixedPct: 2,
    flatStake: 20,
    maxStakePct: 5,
    stopLoss: 20,
    stopLossEnabled: true,
    takeProfitPct: 50,
    takeProfitEnabled: false,
    dailyLossLimit: 10,
    dailyLossEnabled: true,
  },
  history:  [],
  charts:   {},
  stopped:  false,

  init() { this._loadState(); this.render(); },

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
        <div class="config-row"><label>Начальный банк</label>
          <input type="number" class="ctrl-input" id="bmStart" value="${c.startBankroll}" min="1" step="10"></div>
        <div class="config-row"><label>Текущий банк</label>
          <input type="number" class="ctrl-input" id="bmCurrent" value="${c.currentBankroll.toFixed(2)}" min="0" step="0.01"></div>
      </div>
      <div class="config-section">
        <div class="config-title">📐 Метод стейкинга</div>
        <div class="config-row"><label>Метод</label>
          <select class="ctrl-select" id="bmStaking" onchange="bankrollManager.onStakingChange()">
            <option value="kelly"      ${c.staking==='kelly'       ?'selected':''}>Kelly (рекомендуется)</option>
            <option value="fixed"      ${c.staking==='fixed'       ?'selected':''}>Fixed % банка</option>
            <option value="flat"       ${c.staking==='flat'        ?'selected':''}>Flat (фиксированная)</option>
            <option value="martingale" ${c.staking==='martingale'  ?'selected':''}>Martingale ⚠️</option>
            <option value="dalembert"  ${c.staking==='dalembert'   ?'selected':''}>D'Alembert</option>
          </select></div>
        <div id="bmStakingParams">${this._stakingParamsHtml()}</div>
      </div>
      <div class="config-section">
        <div class="config-title">🛡️ Управление рисками</div>
        <div class="config-row"><label>Стоп-лосс %</label>
          <input type="number" class="ctrl-input" id="bmStopLoss" value="${c.stopLoss}" min="1" max="100">
          <label class="toggle-switch" style="margin-left:8px"><input type="checkbox" id="bmStopLossOn" ${c.stopLossEnabled?'checked':''} onchange="bankrollManager.toggleStopLoss(this.checked)"><span class="toggle-slider"></span></label></div>
        <div class="config-row"><label>Тейк-профит %</label>
          <input type="number" class="ctrl-input" id="bmTakeProfit" value="${c.takeProfitPct}" min="1">
          <label class="toggle-switch" style="margin-left:8px"><input type="checkbox" id="bmTakeProfitOn" ${c.takeProfitEnabled?'checked':''} onchange="bankrollManager.toggleTakeProfit(this.checked)"><span class="toggle-slider"></span></label></div>
        <div class="config-row"><label>Дневной лимит потерь %</label>
          <input type="number" class="ctrl-input" id="bmDailyLimit" value="${c.dailyLossLimit}" min="1">
          <label class="toggle-switch" style="margin-left:8px"><input type="checkbox" id="bmDailyLossOn" ${c.dailyLossEnabled?'checked':''} onchange="bankrollManager.toggleDailyLoss(this.checked)"><span class="toggle-slider"></span></label></div>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">
        <button class="ctrl-btn primary" onclick="bankrollManager.applyConfig()">💾 Сохранить</button>
        <button class="ctrl-btn" onclick="bankrollManager.showAddBet()">➕ Добавить ставку</button>
        <button class="ctrl-btn" onclick="bankrollManager.exportCSV()">📥 Экспорт CSV</button>
      </div>`;
  },

  _stakingParamsHtml() {
    const c = this.config;
    if (c.staking === 'kelly')  return `<div class="config-row"><label>Доля Kelly</label><input type="number" class="ctrl-input" id="bmKelly" value="${c.kellyFraction}" min="0.01" max="1" step="0.05"></div>`;
    if (c.staking === 'fixed')  return `<div class="config-row"><label>% от банка</label><input type="number" class="ctrl-input" id="bmFixedPct" value="${c.fixedPct}" min="0.1" max="20" step="0.1"></div>`;
    if (c.staking === 'flat')   return `<div class="config-row"><label>Сумма ставки</label><input type="number" class="ctrl-input" id="bmFlatStake" value="${c.flatStake}" min="1" step="1"></div>`;
    return '';
  },

  onStakingChange() {
    this.config.staking = document.getElementById('bmStaking')?.value || 'kelly';
    const p = document.getElementById('bmStakingParams');
    if (p) p.innerHTML = this._stakingParamsHtml();
  },

  applyConfig() {
    const g = id => document.getElementById(id);
    this.config.startBankroll    = parseFloat(g('bmStart')?.value     || this.config.startBankroll);
    this.config.currentBankroll  = parseFloat(g('bmCurrent')?.value   || this.config.currentBankroll);
    this.config.staking          = g('bmStaking')?.value              || this.config.staking;
    this.config.kellyFraction    = parseFloat(g('bmKelly')?.value     || this.config.kellyFraction);
    this.config.fixedPct         = parseFloat(g('bmFixedPct')?.value  || this.config.fixedPct);
    this.config.flatStake        = parseFloat(g('bmFlatStake')?.value || this.config.flatStake);
    this.config.stopLoss         = parseFloat(g('bmStopLoss')?.value  || this.config.stopLoss);
    this.config.takeProfitPct    = parseFloat(g('bmTakeProfit')?.value|| this.config.takeProfitPct);
    this.config.dailyLossLimit   = parseFloat(g('bmDailyLimit')?.value|| this.config.dailyLossLimit);
    this._saveState();
    this.render();
    this._toast('✅ Параметры сохранены');
  },

  toggleStopLoss(v)   { this.config.stopLossEnabled  = v; },
  toggleTakeProfit(v) { this.config.takeProfitEnabled = v; },
  toggleDailyLoss(v)  { this.config.dailyLossEnabled  = v; },

  _renderKPIs() {
    const el = document.getElementById('bm-kpis');
    if (!el) return;
    const c = this.config;
    const hist = this.history;
    const wins   = hist.filter(b => b.result === 'win').length;
    const losses = hist.filter(b => b.result === 'loss').length;
    const total  = hist.length;
    const wr     = total ? +(wins/total*100).toFixed(1) : 0;
    const pnl    = hist.reduce((s,b)=>s+(+b.pnl||0),0);
    const staked = hist.reduce((s,b)=>s+(+b.stake||0),0);
    const roi    = staked ? +(pnl/staked*100).toFixed(1) : 0;
    const maxDD  = this._maxDrawdown();
    const dd     = this._currentDrawdown();
    const dailyPnl = this._dailyPnl();
    el.innerHTML = `
      <div class="kpi-card"><div class="kpi-val">${c.currentBankroll.toFixed(2)}</div><div class="kpi-label">Банк (текущий)</div></div>
      <div class="kpi-card"><div class="kpi-val ${pnl>=0?'positive':'negative'}">${pnl>=0?'+':''}${pnl.toFixed(2)}</div><div class="kpi-label">Общий P&L</div></div>
      <div class="kpi-card"><div class="kpi-val ${roi>=0?'positive':'negative'}">${roi>=0?'+':''}${roi}%</div><div class="kpi-label">ROI</div></div>
      <div class="kpi-card"><div class="kpi-val">${wr}%</div><div class="kpi-label">Винрейт (${wins}/${total})</div></div>
      <div class="kpi-card"><div class="kpi-val negative">-${maxDD}%</div><div class="kpi-label">Макс. просадка</div></div>
      <div class="kpi-card"><div class="kpi-val negative">-${dd}%</div><div class="kpi-label">Тек. просадка</div></div>
      <div class="kpi-card"><div class="kpi-val ${dailyPnl>=0?'positive':'negative'}">${dailyPnl>=0?'+':''}${dailyPnl.toFixed(2)}</div><div class="kpi-label">P&L сегодня</div></div>
      <div class="kpi-card"><div class="kpi-val">${total}</div><div class="kpi-label">Ставок всего</div></div>`;
  },

  calcStake(odds, prob) {
    const c = this.config;
    const bank = c.currentBankroll;
    let stake = 0;
    if (c.staking === 'kelly') {
      const edge = prob * odds - 1;
      stake = edge > 0 ? bank * c.kellyFraction * (edge / (odds - 1)) : 0;
    } else if (c.staking === 'fixed') {
      stake = bank * c.fixedPct / 100;
    } else if (c.staking === 'flat') {
      stake = c.flatStake;
    } else if (c.staking === 'martingale') {
      const lastLoss = [...this.history].reverse().find(b => b.result === 'loss');
      stake = lastLoss ? +lastLoss.stake * 2 : c.flatStake;
    } else if (c.staking === 'dalembert') {
      const consec = this._consecutiveLosses();
      stake = c.flatStake + consec * c.flatStake;
    }
    return Math.min(stake, bank * c.maxStakePct / 100);
  },

  showAddBet() {
    const m = this._modal('bmBetModal');
    const odds = 2.0, prob = 0.52;
    const suggested = this.calcStake(odds, prob);
    const edge = prob - 1/odds;
    m.innerHTML = `<div class="modal-content" style="width:400px;max-width:95vw">
      <div class="modal-header"><span>➕ Добавить ставку</span><button onclick="document.getElementById('bmBetModal').style.display='none'">×</button></div>
      <div style="padding:16px;display:flex;flex-direction:column;gap:10px">
        <div class="config-row"><label>Матч</label><input class="ctrl-input" id="bmBetMatch" placeholder="Команда А vs Команда Б" style="flex:1"></div>
        <div class="config-row"><label>Коэффициент</label><input type="number" class="ctrl-input" id="bmBetOdds" value="${odds}" step="0.01" min="1.01" oninput="bankrollManager._updateSuggestedStake()"></div>
        <div class="config-row"><label>Вероятность (0–1)</label><input type="number" class="ctrl-input" id="bmBetProb" value="${prob}" step="0.01" min="0" max="1" oninput="bankrollManager._updateSuggestedStake()"></div>
        <div id="bmSuggestedStake" class="${edge>0?'positive':'negative'}" style="font-size:12px;padding:4px 0">
          Рекомендовано (${this.config.staking}): <strong>${suggested.toFixed(2)}</strong> | Edge: ${edge>0?'+':''}${(edge*100).toFixed(1)}%</div>
        <div class="config-row"><label>Ставка</label><input type="number" class="ctrl-input" id="bmBetStake" value="${suggested.toFixed(2)}" step="0.01" min="0.01"></div>
        <div class="config-row"><label>Рынок</label><input class="ctrl-input" id="bmBetMarket" placeholder="1X2, O/U, BTTS..."></div>
        <div class="config-row"><label>Результат</label>
          <select class="ctrl-select" id="bmBetResult">
            <option value="pending">Ожидание</option>
            <option value="win">Победа ✅</option>
            <option value="loss">Поражение ❌</option>
            <option value="void">Возврат</option>
          </select></div>
        <button class="ctrl-btn primary" onclick="bankrollManager.saveBet()">💾 Сохранить</button>
      </div></div>`;
    m.style.display = 'flex';
  },

  _updateSuggestedStake() {
    const odds = parseFloat(document.getElementById('bmBetOdds')?.value||2);
    const prob = parseFloat(document.getElementById('bmBetProb')?.value||0.5);
    const suggested = this.calcStake(odds, prob);
    const edge = prob - 1/odds;
    const el = document.getElementById('bmSuggestedStake');
    if (el) {
      el.className = edge>0?'positive':'negative';
      el.innerHTML = `Рекомендовано (${this.config.staking}): <strong>${suggested.toFixed(2)}</strong> | Edge: ${edge>0?'+':''}${(edge*100).toFixed(1)}%`;
      const s = document.getElementById('bmBetStake');
      if (s) s.value = suggested.toFixed(2);
    }
  },

  recordBet(bet) {
    const c = this.config;
    const pnl = bet.result === 'win' ? bet.stake * (bet.odds - 1) : bet.result === 'loss' ? -bet.stake : 0;
    const bank = +(c.currentBankroll + pnl).toFixed(2);
    c.currentBankroll = bank;
    this.history.push({ ...bet, pnl: +pnl.toFixed(2), bank, date: bet.date || new Date().toISOString() });
    this._saveState();
    this.render();
    this._checkStopLoss();
    // Синхронизация с PostgreSQL
    this._syncBetToPG(bet, pnl, bank);
  },

  async _syncBetToPG(bet, pnl, bank) {
    try {
      await fetch('/api/journal/bets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-auth-token': localStorage.getItem('bq_token')||'demo' },
        body: JSON.stringify({ match: bet.match, odds: bet.odds, stake: bet.stake, result: bet.result, pnl, bank, market: bet.market||'1X2', date: bet.date }),
      });
    } catch(e) {}
  },

  saveBet() {
    const g = id => document.getElementById(id);
    const match  = g('bmBetMatch')?.value?.trim();
    const odds   = parseFloat(g('bmBetOdds')?.value);
    const stake  = parseFloat(g('bmBetStake')?.value);
    const result = g('bmBetResult')?.value;
    const market = g('bmBetMarket')?.value?.trim() || '1X2';
    if (!match || !odds || !stake) { alert('Заполните матч, коэффициент и ставку'); return; }
    this.recordBet({ match, odds, stake, result: result||'pending', market });
    document.getElementById('bmBetModal').style.display = 'none';
    this._toast(`✅ Записано: ${match}`);
  },

  exportCSV() {
    const headers = 'Дата,Матч,Рынок,Коэф,Ставка,Результат,P&L,Банк';
    const rows = this.history.map(b =>
      [b.date?.slice(0,10), b.match, b.market||'', b.odds, b.stake, b.result, b.pnl, b.bank].join(',')
    );
    const csv = [headers, ...rows].join('\n');
    const a = Object.assign(document.createElement('a'),{ href:'data:text/csv;charset=utf-8,'+encodeURIComponent(csv), download:'bankroll.csv' });
    a.click();
  },

  _checkStopLoss() {
    if (!this.config.stopLossEnabled) return;
    const slLevel = this.config.startBankroll * (1 - this.config.stopLoss / 100);
    if (this.config.currentBankroll <= slLevel && !this.stopped) {
      this.stopped = true; this._saveState();
      setTimeout(() => { const el = document.getElementById('bm-stopLossAlert'); if (el) el.style.display = 'block'; }, 100);
    }
    if (this.config.currentBankroll > slLevel) this.stopped = false;
  },

  resetStopLoss() { this.stopped = false; this._saveState(); this.render(); this._toast('✅ Стоп-лосс сброшен'); },

  _maxDrawdown() {
    const curve = [this.config.startBankroll, ...this.history.map(b => b.bank)];
    let peak = 0, maxDD = 0;
    for (const v of curve) { if (v > peak) peak = v; const dd = (peak-v)/peak*100; if (dd > maxDD) maxDD = dd; }
    return +maxDD.toFixed(1);
  },
  _currentDrawdown() {
    const curve = [this.config.startBankroll, ...this.history.map(b => b.bank)];
    let peak = 0; for (const v of curve) if (v > peak) peak = v;
    return +((peak - this.config.currentBankroll) / peak * 100).toFixed(1);
  },
  _dailyPnl() {
    const today = new Date().toDateString();
    return this.history.filter(b => new Date(b.date).toDateString() === today).reduce((s,b)=>s+(b.pnl||0),0);
  },
  _consecutiveLosses() {
    let n = 0;
    for (const b of [...this.history].reverse()) { if (b.result === 'loss') n++; else break; }
    return n;
  },

  _saveState() {
    localStorage.setItem('bq_bankroll', JSON.stringify({ config: this.config, history: this.history.slice(-500), stopped: this.stopped }));
  },

  _loadState() {
    // 1. Пробуем localStorage
    const saved = JSON.parse(localStorage.getItem('bq_bankroll') || 'null');
    if (saved) {
      this.config  = { ...this.config, ...saved.config };
      this.history = saved.history || [];
      this.stopped = saved.stopped || false;
      return;
    }
    // 2. Пробуем подтянуть из PostgreSQL (async, не блокирует)
    this._fetchHistoryFromPG();
    // 3. Если демо-режим — ставим тестовые данные
    if (localStorage.getItem('bq_demo_mode') === 'true') {
      this.history = this._demoHistory();
      this.config.currentBankroll = this.history.at(-1)?.bank || 1000;
    }
    // Иначе: пустая история, ждём загрузки из PG
  },

  async _fetchHistoryFromPG() {
    try {
      const r = await fetch('/api/journal/bets?limit=200', {
        headers: { 'x-auth-token': localStorage.getItem('bq_token')||'demo' },
      });
      if (!r.ok) return;
      const data = await r.json();
      const bets = data.bets || data;
      if (Array.isArray(bets) && bets.length) {
        let bank = this.config.startBankroll;
        this.history = bets.map(b => {
          bank = +(bank + (+b.pnl||0)).toFixed(2);
          return { ...b, bank: b.bank || bank };
        });
        this.config.currentBankroll = this.history.at(-1)?.bank || bank;
        this._saveState();
        this.render();
      }
    } catch(e) {}
  },

  _demoHistory() {
    const hist = []; let bank = 1000;
    const matches = ['Арсенал — Челси','Бавария — Дортмунд','Реал — Атлетико','ПСЖ — Монако','Интер — Ювентус'];
    for (let i = 0; i < 20; i++) {
      const odds = +(1.5 + Math.random()*2).toFixed(2);
      const stake = +(bank * 0.02).toFixed(2);
      const result = Math.random() < (1/odds * 1.08) ? 'win' : 'loss';
      const pnl = +(result === 'win' ? stake*(odds-1) : -stake).toFixed(2);
      bank = +(bank + pnl).toFixed(2);
      hist.push({ match: matches[i%5], odds, stake, result, pnl, bank, date: new Date(Date.now()-(20-i)*86400000).toISOString() });
    }
    return hist;
  },

  _renderChart() {
    if (this.charts.equity) { try { this.charts.equity.destroy(); } catch(e){} }
    const cvs = document.getElementById('bm-equity-chart');
    if (!cvs) return;
    const c    = this.config;
    const data = [c.startBankroll, ...this.history.map(b => b.bank)];
    const slLevel = c.startBankroll * (1 - c.stopLoss / 100);
    const dk = document.body.classList.contains('dark-mode');
    const tc = dk ? '#94a3b8' : '#475569', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.06)';
    this.charts.equity = new Chart(cvs, {
      type: 'line',
      data: { labels: data.map((_,i)=>i===0?'Старт':`#${i}`), datasets: [
        { label:'Банк', data, borderColor:'#00d4ff', borderWidth:2, pointRadius:data.length<30?3:0, backgroundColor:'rgba(0,212,255,.06)', fill:true, tension:.3 },
        { label:'Stop-Loss', data:data.map(()=>slLevel), borderColor:'rgba(255,69,96,.6)', borderWidth:1.5, borderDash:[6,3], pointRadius:0, fill:false },
        ...(c.takeProfitEnabled?[{ label:'Take-Profit', data:data.map(()=>c.startBankroll*(1+c.takeProfitPct/100)), borderColor:'rgba(0,230,118,.5)', borderWidth:1.5, borderDash:[6,3], pointRadius:0, fill:false }]:[]),
      ]},
      options: { responsive:true, maintainAspectRatio:false, animation:false,
        plugins: { legend:{ labels:{color:tc,font:{size:11}} }, tooltip:{ mode:'index', intersect:false } },
        scales: { x:{ ticks:{color:tc,font:{size:10},maxTicksLimit:10}, grid:{color:gc} }, y:{ ticks:{color:tc,font:{size:10}}, grid:{color:gc} } } },
    });
  },

  _renderHistory() {
    const el = document.getElementById('bm-history');
    if (!el) return;
    if (!this.history.length) {
      el.innerHTML = `<div class="lm-empty" style="padding:32px;text-align:center">
        <div style="font-size:28px;margin-bottom:8px">📋</div>
        <div style="color:var(--text2)">История ставок пуста</div>
        <div style="color:var(--text3);font-size:12px;margin-top:4px">Добавьте первую ставку или загрузите данные из PostgreSQL</div>
      </div>`;
      return;
    }
    const hist = [...this.history].reverse().slice(0, 100);
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

  _modal(id) {
    let m = document.getElementById(id);
    if (!m) { m = Object.assign(document.createElement('div'),{id,className:'modal'}); m.onclick=e=>{if(e.target===m)m.style.display='none';}; document.body.appendChild(m); }
    return m;
  },
  _toast(msg) {
    let t = document.getElementById('bq-toast');
    if (!t) { t = Object.assign(document.createElement('div'),{id:'bq-toast',className:'bq-toast'}); document.body.append(t); }
    t.textContent=msg; t.classList.add('show'); clearTimeout(t._tm); t._tm=setTimeout(()=>t.classList.remove('show'),3000);
  },
};