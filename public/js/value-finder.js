'use strict';
const valueFinder = {
  scanning: false,
  results:  [],
  charts:   {},

  init() { this.renderFilters(); },

  renderFilters() {
    const el = document.getElementById('vfFilters');
    if (!el) return;
    el.innerHTML = `
      <div class="config-row"><label>Мин. Value %</label>
        <input type="number" class="ctrl-input" id="vfMinValue" value="3" step="0.5" min="0"></div>
      <div class="config-row"><label>Мин. коэф</label>
        <input type="number" class="ctrl-input" id="vfMinOdds" value="1.5" step="0.1" min="1.01"></div>
      <div class="config-row"><label>Макс. коэф</label>
        <input type="number" class="ctrl-input" id="vfMaxOdds" value="8" step="0.1"></div>
      <div class="config-row"><label>Спорт</label>
        <select class="ctrl-select" id="vfSport">
          <option value="all">Все</option>
          <option value="football">Football</option>
          <option value="tennis">Tennis</option>
          <option value="basketball">Basketball</option>
          <option value="hockey">Hockey</option>
        </select>
      </div>
      <div class="config-row"><label>Рынок</label>
        <select class="ctrl-select" id="vfMarket">
          <option value="all">Все рынки</option>
          <option value="home">1 (Победа хозяев)</option>
          <option value="draw">X (Ничья)</option>
          <option value="away">2 (Победа гостей)</option>
          <option value="over">Over 2.5</option>
          <option value="under">Under 2.5</option>
          <option value="btts">BTTS</option>
        </select>
      </div>`;
  },

  async scan() {
    if (this.scanning) return;
    this.scanning = true;
    const btn = document.getElementById('vfScanBtn');
    if (btn) { btn.textContent = '⏳ Сканирование...'; btn.style.pointerEvents = 'none'; }

    const cfg = {
      minValue: parseFloat(document.getElementById('vfMinValue')?.value || 3) / 100,
      minOdds:  parseFloat(document.getElementById('vfMinOdds')?.value  || 1.5),
      maxOdds:  parseFloat(document.getElementById('vfMaxOdds')?.value  || 8),
      sport:    document.getElementById('vfSport')?.value  || 'all',
      market:   document.getElementById('vfMarket')?.value || 'all',
    };

    // Симулируем сканирование
    await new Promise(r => setTimeout(r, 800));
    this.results = this.generateResults(cfg);
    this.renderResults();
    this.renderChart();

    this.scanning = false;
    if (btn) { btn.textContent = '🔍 Сканировать'; btn.style.pointerEvents = ''; }
  },

  generateResults(cfg) {
    const sports  = cfg.sport === 'all' ? ['football','tennis','basketball','hockey'] : [cfg.sport];
    const markets = cfg.market === 'all' ? ['home','draw','away','over','under'] : [cfg.market];
    const teamsBySport = {
      football:   [['Arsenal','Chelsea'],['Real Madrid','Barcelona'],['Bayern','Dortmund'],['PSG','Marseille'],['Juventus','Inter'],['Liverpool','Man City']],
      tennis:     [['Djokovic','Alcaraz'],['Sinner','Medvedev'],['Zverev','Rublev'],['Tsitsipas','Fritz']],
      basketball: [['Lakers','Warriors'],['Celtics','Heat'],['Bucks','Nuggets'],['76ers','Nets']],
      hockey:     [['Rangers','Bruins'],['Oilers','Flames'],['Capitals','Penguins']],
    };
    const results = [];

    for (const sport of sports) {
      const pairs = teamsBySport[sport] || [['Team A','Team B']];
      for (const [home, away] of pairs) {
        for (const market of markets) {
          const odds   = cfg.minOdds + Math.random() * (cfg.maxOdds - cfg.minOdds);
          const truePr = 1/odds * (1 + cfg.minValue + Math.random() * 0.12);
          const value  = truePr - 1/odds;
          if (value < cfg.minValue) continue;
          const kelly = Math.max(0, ((odds-1)*truePr - (1-truePr)) / (odds-1));
          results.push({
            sport, match: `${home} vs ${away}`, market,
            odds: +odds.toFixed(2),
            trueProb: +(truePr * 100).toFixed(1),
            impliedProb: +(100/odds).toFixed(1),
            value: +(value * 100).toFixed(2),
            kelly: +(kelly * 100).toFixed(1),
            bookmaker: ['Pinnacle','Bet365','1xBet','Betfair'][Math.floor(Math.random()*4)],
            time: new Date(Date.now() + Math.random()*86400000*3).toLocaleDateString('ru'),
          });
        }
      }
    }
    return results.sort((a,b) => b.value - a.value).slice(0, 50);
  },

  renderResults() {
    const el = document.getElementById('vfResultsTable');
    if (!el) return;
    if (!this.results.length) {
      el.innerHTML = '<div class="empty-state" style="padding:32px;text-align:center">Ничего не найдено. Попробуй изменить фильтры.</div>';
      return;
    }
    const rows = this.results.map(r => `
      <tr>
        <td><span class="bt-strat-sport-tag">${r.sport}</span></td>
        <td>${r.match}</td>
        <td>${r.time}</td>
        <td><span class="bt-tag single">${r.market}</span></td>
        <td><strong>${r.odds}</strong></td>
        <td>${r.impliedProb}%</td>
        <td class="positive"><strong>${r.trueProb}%</strong></td>
        <td class="positive"><strong>+${r.value}%</strong></td>
        <td>${r.kelly}%</td>
        <td>${r.bookmaker}</td>
        <td><button class="ctrl-btn sm" onclick="valueFinder.addToWatchlist('${r.match}','${r.market}',${r.odds})">+ Watch</button></td>
      </tr>`).join('');
    el.innerHTML = `<table class="data-table">
      <thead><tr>
        <th>Спорт</th><th>Матч</th><th>Дата</th><th>Рынок</th>
        <th>Коэф</th><th>Implied%</th><th>True%</th><th>Value%</th>
        <th>Kelly%</th><th>Букмекер</th><th></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;

    const summary = document.getElementById('vfSummary');
    if (summary) {
      const avgVal = (this.results.reduce((s,r) => s+r.value, 0) / this.results.length).toFixed(1);
      summary.innerHTML = `Найдено: <strong>${this.results.length}</strong> ставок &nbsp;|&nbsp; Средний Value: <strong class="positive">+${avgVal}%</strong>`;
    }
  },

  renderChart() {
    if (this.charts.value) { try { this.charts.value.destroy(); } catch(e){} }
    const cvs = document.getElementById('chartVFValue');
    if (!cvs || !this.results.length) return;
    const isDark = document.body.classList.contains('dark-mode');
    const tc = isDark ? '#8892a4' : '#4a5568';
    const gc = isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.07)';
    const top10 = this.results.slice(0, 10);
    this.charts.value = new Chart(cvs, {
      type: 'bar',
      data: {
        labels: top10.map(r => r.match.split(' vs ')[0] + ' ' + r.market),
        datasets: [{ data: top10.map(r => r.value), backgroundColor: 'rgba(0,212,255,0.7)', borderRadius: 4 }],
      },
      options: { responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
        scales:{ x:{ticks:{color:tc,font:{size:9}},grid:{color:gc}}, y:{ticks:{color:tc,font:{size:9}},grid:{color:gc}} } },
    });
  },

  addToWatchlist(match, market, odds) {
    const list = JSON.parse(localStorage.getItem('bq_watchlist') || '[]');
    list.push({ match, market, odds, added: new Date().toISOString() });
    localStorage.setItem('bq_watchlist', JSON.stringify(list));
    const btn = event?.target;
    if (btn) { btn.textContent = '✓'; btn.style.color = '#00e676'; }
  },
};