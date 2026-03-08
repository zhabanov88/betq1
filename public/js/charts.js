'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Графики и коэффициенты + Статистика + История коэффициентов
//  ВСЕ данные — из реального API. Демо — только при bq_demo_mode=true.
// ═══════════════════════════════════════════════════════════════════════════

// ── Общие утилиты ─────────────────────────────────────────────────────────
function _chartColors() {
  const dk = document.body.classList.contains('dark-mode');
  return { tc: dk ? '#8892a4' : '#4a5568', gc: dk ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.06)' };
}
function _baseOpts() {
  const { tc, gc } = _chartColors();
  return {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { color: tc, font: { size: 10 } } } },
    scales: {
      x: { ticks: { color: tc, font: { size: 9 } }, grid: { color: gc } },
      y: { ticks: { color: tc, font: { size: 9 } }, grid: { color: gc } },
    },
  };
}
function _noData(containerId, msg) {
  const el = document.getElementById(containerId);
  if (el) el.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:100%;min-height:80px;color:var(--text3);font-size:12px">${msg}</div>`;
}

// ═══════════════════════════════════════════════════════════════════════════
//  ODDS CHART — движение коэффициентов
// ═══════════════════════════════════════════════════════════════════════════
const oddsChart = {
  charts: {},

  async load() {
    const team = document.getElementById('chartTeamSearch')?.value?.trim() || '';
    if (!team) {
      _noData('oddsChartTitle', '');
      const el = document.getElementById('oddsChartTitle');
      if (el) el.textContent = 'Введите название команды и нажмите «Загрузить»';
      return;
    }
    const el = document.getElementById('oddsChartTitle');
    if (el) el.textContent = team + ' — Движение коэффициентов';

    try {
      const r = await apiCall(`/api/odds-compare/movement/${encodeURIComponent(team)}`);
      if (r && r.history && r.history.length) {
        this.renderMovementFromAPI(r);
        return;
      }
    } catch(e) {}

    // Нет данных
    if (this.charts.movement) { try { this.charts.movement.destroy(); } catch(e){} delete this.charts.movement; }
    const cvs = document.getElementById('chartOddsMovement');
    if (cvs && cvs.parentElement) {
      const existing = cvs.parentElement.querySelector('.no-data-msg');
      if (existing) existing.remove();
      const msg = document.createElement('div');
      msg.className = 'no-data-msg';
      msg.style.cssText = 'position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:var(--text3);font-size:12px;';
      msg.textContent = '📭 Нет данных. Подключите ODDS_API_KEY или выберите другую команду.';
      cvs.parentElement.style.position = 'relative';
      cvs.parentElement.appendChild(msg);
    }

    // Очистить таблицу букмекеров
    const ct = document.getElementById('oddsComparisonTable');
    if (ct) ct.innerHTML = '<div style="color:var(--text3);font-size:12px;padding:12px">Нет данных букмекеров</div>';
  },

  renderMovementFromAPI(r) {
    if (this.charts.movement) { try { this.charts.movement.destroy(); } catch(e){} }
    const cvs = document.getElementById('chartOddsMovement');
    if (!cvs) return;
    // Убрать заглушку если была
    cvs.parentElement?.querySelector('.no-data-msg')?.remove();

    const labels = r.history.map(h => h.label || h.time || '');
    const { tc, gc } = _chartColors();
    this.charts.movement = new Chart(cvs, {
      type: 'line',
      data: { labels, datasets: [
        { label: '1 (Хозяева)', data: r.history.map(h => h.home), borderColor: '#00d4ff', borderWidth: 2, pointRadius: 3, tension: 0.3 },
        { label: 'X (Ничья)',   data: r.history.map(h => h.draw), borderColor: '#ffd740', borderWidth: 2, pointRadius: 3, tension: 0.3 },
        { label: '2 (Гости)',   data: r.history.map(h => h.away), borderColor: '#fb923c', borderWidth: 2, pointRadius: 3, tension: 0.3 },
      ]},
      options: { ..._baseOpts() },
    });

    // Таблица букмекеров
    if (r.bookmakers && Object.keys(r.bookmakers).length) {
      const ct = document.getElementById('oddsComparisonTable');
      if (ct) {
        const rows = Object.entries(r.bookmakers).map(([bm, odds]) => {
          const margin = ((1/odds.home + 1/odds.draw + 1/odds.away - 1) * 100).toFixed(1);
          return `<tr><td>${bm}</td><td>${odds.home||'—'}</td><td>${odds.draw||'—'}</td><td>${odds.away||'—'}</td><td>${margin}%</td></tr>`;
        }).join('');
        ct.innerHTML = `<table class="data-table"><thead><tr><th>Букмекер</th><th>1</th><th>X</th><th>2</th><th>Маржа%</th></tr></thead><tbody>${rows}</tbody></table>`;
      }
    }
  },
};

// ═══════════════════════════════════════════════════════════════════════════
//  STATS ENGINE — статистика лиг из ClickHouse
// ═══════════════════════════════════════════════════════════════════════════
const statsEngine = {
  charts: {},

  async load() {
    const league = document.getElementById('statsLeague')?.value || '';
    const season = document.getElementById('statsSeason')?.value || '';

    // Статистика команд
    try {
      const r = await apiCall(`/api/stats/teams?league=${encodeURIComponent(league)}&season=${encodeURIComponent(season)}&limit=20`);
      if (r && r.teams && r.teams.length) {
        this.renderLeagueTable(r.teams);
        this.renderXGChart(r.teams);
        this.renderHomeAwayChart(r.teams);
      } else {
        this._noDataAll();
      }
    } catch(e) {
      this._noDataAll();
    }

    // Голы по минутам
    try {
      const r = await apiCall(`/api/stats/goals-by-minute?league=${encodeURIComponent(league)}`);
      if (r && r.length) {
        this.renderGoalsChart(r);
      }
    } catch(e) {}
  },

  renderLeagueTable(teams) {
    const el = document.getElementById('statsLeagueTable');
    if (!el) return;
    if (!teams.length) { el.innerHTML = '<div style="color:var(--text3);padding:12px;font-size:12px">Нет данных. Загрузите статистику через ETL.</div>'; return; }
    const headers = ['Команда','М','П','Н','П','ГЗ','ГП','ГР','О','xG','xGA'];
    const keys    = ['team','matches','wins','draws','losses','goals_for','goals_against','gd','points','xg','xga'];
    el.innerHTML = makeTable(
      headers.map((h, i) => ({ label: h, key: keys[i] })),
      teams
    );
  },

  renderGoalsChart(data) {
    if (this.charts.goals) { try { this.charts.goals.destroy(); } catch(e){} }
    const cvs = document.getElementById('chartStatsGoals');
    if (!cvs || !data.length) return;
    const { tc, gc } = _chartColors();
    this.charts.goals = new Chart(cvs, {
      type: 'bar',
      data: {
        labels: data.map(d => d.minute + "'"),
        datasets: [{ label: 'Голы', data: data.map(d => d.goals), backgroundColor: 'rgba(0,212,255,0.6)', borderRadius: 2 }],
      },
      options: { ..._baseOpts(), plugins: { legend: { display: false } } },
    });
  },

  renderHomeAwayChart(teams) {
    if (this.charts.homeaway) { try { this.charts.homeaway.destroy(); } catch(e){} }
    const cvs = document.getElementById('chartStatsHomeAway');
    if (!cvs || !teams.length) return;
    const homeWins  = teams.reduce((s, t) => s + (+t.home_wins || 0), 0);
    const draws     = teams.reduce((s, t) => s + (+t.draws || 0), 0);
    const awayWins  = teams.reduce((s, t) => s + (+t.away_wins || 0), 0);
    const total = homeWins + draws + awayWins || 1;
    const { tc } = _chartColors();
    this.charts.homeaway = new Chart(cvs, {
      type: 'doughnut',
      data: {
        labels: ['Победа хозяев', 'Ничья', 'Победа гостей'],
        datasets: [{ data: [
          +(homeWins/total*100).toFixed(1),
          +(draws/total*100).toFixed(1),
          +(awayWins/total*100).toFixed(1),
        ], backgroundColor: ['rgba(0,212,255,0.8)', 'rgba(255,215,64,0.8)', 'rgba(251,146,60,0.8)'] }],
      },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: tc, font: { size: 10 } } } } },
    });
  },

  renderXGChart(teams) {
    if (this.charts.xg) { try { this.charts.xg.destroy(); } catch(e){} }
    const cvs = document.getElementById('chartStatsXG');
    if (!cvs || !teams.length) return;
    const top = teams.slice(0, 10);
    const { tc } = _chartColors();
    this.charts.xg = new Chart(cvs, {
      type: 'bar',
      data: {
        labels: top.map(t => t.team),
        datasets: [
          { label: 'xG', data: top.map(t => +t.xg || 0), backgroundColor: 'rgba(0,212,255,0.6)', borderRadius: 3 },
          { label: 'Голы забито', data: top.map(t => +t.goals_for || 0), backgroundColor: 'rgba(0,230,118,0.6)', borderRadius: 3 },
        ],
      },
      options: _baseOpts(),
    });
  },

  _noDataAll() {
    const msg = '<div style="color:var(--text3);padding:40px;text-align:center;font-size:12px">📭 Нет данных. Подключите ClickHouse и загрузите статистику через ETL-менеджер.</div>';
    const ids = ['statsLeagueTable','chartStatsGoals','chartStatsHomeAway','chartStatsXG'];
    ids.forEach(id => {
      const el = document.getElementById(id);
      if (el) {
        // Для canvas — скрыть и показать текст рядом
        if (el.tagName === 'CANVAS' && el.parentElement) {
          el.style.display = 'none';
          if (!el.parentElement.querySelector('.no-data-stats')) {
            const d = document.createElement('div'); d.className = 'no-data-stats';
            d.style.cssText = 'color:var(--text3);font-size:11px;padding:8px;text-align:center';
            d.textContent = 'Нет данных'; el.parentElement.appendChild(d);
          }
        } else { el.innerHTML = msg; }
      }
    });
  },
};

// ═══════════════════════════════════════════════════════════════════════════
//  ODDS HISTORY — история коэффициентов из API
// ═══════════════════════════════════════════════════════════════════════════
const oddsHistory = {
  async load() {
    const search    = document.getElementById('ohSearch')?.value?.trim() || '';
    const container = document.getElementById('oddsHistoryTable');
    if (!container) return;

    container.innerHTML = '<div style="color:var(--text3);padding:16px;font-size:12px">⏳ Загрузка...</div>';

    try {
      const qs = search ? `?search=${encodeURIComponent(search)}` : '';
      const r  = await apiCall(`/api/odds-compare/history${qs}`);
      if (r && r.records && r.records.length) {
        container.innerHTML = makeTable(
          [
            { label: 'Дата',           key: 'date' },
            { label: 'Матч',           key: 'match' },
            { label: 'Рынок',          key: 'market' },
            { label: 'Откр. 1',        key: 'open_home' },
            { label: 'Закр. 1',        key: 'close_home' },
            { label: 'Откр. X',        key: 'open_draw' },
            { label: 'Закр. X',        key: 'close_draw' },
            { label: 'Откр. 2',        key: 'open_away' },
            { label: 'Закр. 2',        key: 'close_away' },
            { label: 'Движение',       key: 'movement' },
            { label: 'Итог',           key: 'result' },
          ],
          r.records
        );
        return;
      }
    } catch(e) {}

    container.innerHTML = `
      <div style="padding:48px;text-align:center">
        <div style="font-size:32px;margin-bottom:12px">📭</div>
        <div style="color:var(--text2);margin-bottom:6px">Нет истории коэффициентов</div>
        <div style="color:var(--text3);font-size:12px">Подключите <strong>ODDS_API_KEY</strong> в .env для получения данных</div>
      </div>`;
  },
};

// ═══════════════════════════════════════════════════════════════════════════
//  Вспомогательные функции (если не объявлены в app.js)
// ═══════════════════════════════════════════════════════════════════════════

// makeTable — универсальная таблица. Если в app.js уже есть — эта не будет использована.
if (typeof makeTable === 'undefined') {
  window.makeTable = function(columns, rows) {
    if (!rows || !rows.length) return '<div style="color:var(--text3);padding:12px;font-size:12px">Нет данных</div>';
    const heads = columns.map(c => `<th>${typeof c === 'string' ? c : c.label}</th>`).join('');
    const bodyRows = rows.map(row => {
      const cells = columns.map(c => {
        const key = typeof c === 'string' ? c : c.key;
        return `<td>${row[key] ?? '—'}</td>`;
      }).join('');
      return `<tr>${cells}</tr>`;
    }).join('');
    return `<table class="data-table"><thead><tr>${heads}</tr></thead><tbody>${bodyRows}</tbody></table>`;
  };
}

// apiCall — базовый fetch с авторизацией
if (typeof apiCall === 'undefined') {
  window.apiCall = async function(url, method = 'GET', body = null) {
    const opts = {
      method,
      headers: { 'Content-Type': 'application/json', 'x-auth-token': localStorage.getItem('bq_token') || 'demo' },
      credentials: 'include',
    };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(`${r.status} ${url}`);
    return r.json();
  };
}