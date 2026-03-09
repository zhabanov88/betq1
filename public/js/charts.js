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

  // ── Главная точка входа ────────────────────────────────────────────────
  async load() {
    // Динамически подгружаем список лиг
    try {
      const lgData = await apiCall(`/api/stats/leagues?sport=${sport}`);
      const sel = document.getElementById('statsLeague');
      if (sel && lgData?.leagues?.length) {
        const current = sel.value;
        sel.innerHTML = lgData.leagues
          .map(l => `<option value="${l.code}" ${l.code===current?'selected':''}>${l.code} (${l.matches})</option>`)
          .join('');
        // Показываем хинт
        const hint = document.getElementById('statsLeagueHint');
        if (hint) hint.textContent = `${lgData.leagues.length} лиг в БД`;
      }
    } catch(e) {}

    const sport  = app?.currentSport  || 'football';
    const league = document.getElementById('statsLeague')?.value  || (sport === 'hockey' ? 'NHL' : 'E0');
    const season = document.getElementById('statsSeason')?.value  || '';
    const metric = document.getElementById('statsMetric')?.value  || 'goals';

    // Обновляем заголовок панели
    const header = document.querySelector('#panel-stats .panel-header h2');
    if (header) {
      const icons = { football:'📊', hockey:'🏒', tennis:'🎾', basketball:'🏀', baseball:'⚾', esports:'🎮' };
      header.textContent = `${icons[sport] || '📊'} Статистика`;
    }

    await Promise.all([
      this.renderLeagueTable(sport, league, season, metric),
      this.renderGoalsChart(sport, league, season),
      this.renderHomeAwayChart(sport, league, season),
      this.renderXGChart(sport, league, season),
    ]);
  },

  // ── Таблица команд ─────────────────────────────────────────────────────
  async renderLeagueTable(sport, league, season, metric) {
    const el = document.getElementById('statsLeagueTable');
    if (!el) return;
    el.innerHTML = '<div class="lm-empty" style="padding:20px">Загрузка…</div>';

    try {
      const params = new URLSearchParams({ sport, league, season, metric, limit: 30 });
      const d = await apiCall(`/api/stats/teams?${params}`);

      if (!d?.teams?.length) {
        el.innerHTML = `<div style="padding:32px;text-align:center">
          <div style="font-size:28px;margin-bottom:8px">📭</div>
          <div style="color:var(--text2);margin-bottom:6px">Нет данных в ClickHouse</div>
          <div style="color:var(--text3);font-size:12px">
            Загружено матчей: <strong>${d?.total || 0}</strong><br>
            Лига: <strong>${league}</strong> · Сезон: <strong>${season || 'все'}</strong><br><br>
            ${d?.hint || 'Проверьте ETL и фильтры лиги'}
          </div>
        </div>`;
        return;
      }

      // Конфигурация колонок под спорт
      const colsBySport = {
        football:   [['team','Команда'],['matches','М'],['wins','В'],['draws','Н'],['losses','П'],
                     ['goals_for','Голы'],['goals_against','Пропущено'],['gd','Разница'],
                     ['points','Очки'],['xg','xG'],['xga','xGA']],
        hockey:     [['team','Команда'],['matches','И'],['wins','В'],['ot_wins','В (ОТ)'],
                     ['losses','П'],['goals_for','Голы'],['goals_against','Пропущено'],
                     ['points','Очки'],['shots_for','Броски'],['pp_pct','ПП%'],['sv_pct','Сейвы%']],
        tennis:     [['player','Игрок'],['matches','М'],['wins','В'],['losses','П'],
                     ['win_pct','%'],['aces','Эйсы'],['dfs','ДО'],['first_in','1я%']],
        basketball: [['team','Команда'],['matches','И'],['wins','В'],['losses','П'],
                     ['pts','Очки'],['reb','Подборы'],['ast','Передачи'],
                     ['fg_pct','FG%'],['3p_pct','3P%']],
        baseball:   [['team','Команда'],['matches','И'],['wins','В'],['losses','П'],
                     ['runs','Раны'],['era','ERA'],['whip','WHIP']],
      };

      const cols = colsBySport[sport] || colsBySport.football;
      const heads = cols.map(([,l]) => `<th>${l}</th>`).join('');
      const rows = d.teams.map((row, idx) => {
        const cells = cols.map(([key]) => {
          const v = row[key];
          if (v === undefined || v === null) return '<td>—</td>';
          if (key === 'points' || key === 'wins')
            return `<td style="font-weight:600;color:var(--accent)">${v}</td>`;
          if (key === 'xg' || key === 'xga')
            return `<td style="color:var(--text2)">${parseFloat(v).toFixed(2)}</td>`;
          if (key === 'win_pct' || key === 'fg_pct' || key === '3p_pct' || key === 'pp_pct' || key === 'sv_pct')
            return `<td>${parseFloat(v).toFixed(1)}%</td>`;
          return `<td>${v}</td>`;
        }).join('');
        const medal = idx === 0 ? '🥇' : idx === 1 ? '🥈' : idx === 2 ? '🥉' : '';
        // заменяем первую ячейку имени на медаль + имя
        return `<tr>${cells.replace('<td>', `<td>${medal} `)}</tr>`;
      }).join('');

      const source = d.source === 'clickhouse' ? '✅ ClickHouse' : d.source;
      el.innerHTML = `
        <div style="font-size:11px;color:var(--text3);margin-bottom:6px;text-align:right">
          Источник: ${source} · ${d.teams.length} команд · ${d.total || '?'} матчей
        </div>
        <table class="data-table">
          <thead><tr>${heads}</tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
    } catch(e) {
      el.innerHTML = `<div style="padding:24px;text-align:center;color:var(--text3);font-size:12px">
        ⚠️ Ошибка загрузки: ${e.message}
      </div>`;
    }
  },

  // ── График распределения голов/очков по минутам ─────────────────────
  async renderGoalsChart(sport, league, season) {
    if (this.charts.goals) { try { this.charts.goals.destroy(); } catch(e){} }
    const cvs = document.getElementById('chartStatsGoals');
    if (!cvs) return;

    const dk = document.body.classList.contains('dark-mode');
    const tc = dk ? '#8892a4' : '#4a5568', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.06)';

    const labelsBySport = {
      football:   { url: `/api/stats/goals-by-minute?league=${league}&season=${season}`, key: 'goals',  label: 'Голы по минутам', xLabel: 'Минута' },
      hockey:     { url: `/api/stats/goals-by-minute?sport=hockey&league=${league}`,     key: 'goals',  label: 'Голы по периодам',xLabel: 'Период' },
      basketball: { url: `/api/stats/goals-by-minute?sport=basketball&league=${league}`, key: 'goals',  label: 'Очки по четвертям',xLabel: 'Четверть' },
      tennis:     { url: `/api/stats/goals-by-minute?sport=tennis&league=${league}`,     key: 'goals',  label: 'Геймы',           xLabel: 'Сет' },
    };
    const cfg = labelsBySport[sport] || labelsBySport.football;

    try {
      const d = await apiCall(cfg.url);
      const data = Array.isArray(d) ? d : [];

      if (!data.length) {
        // Пустые оси — данных нет, но оси отображаются
        this.charts.goals = new Chart(cvs, {
          type: 'bar',
          data: { labels: [], datasets: [{ label: cfg.label, data: [], backgroundColor: 'rgba(0,212,255,0.5)', borderRadius: 2 }] },
          options: { responsive:true, maintainAspectRatio:false, animation:false,
            plugins:{ legend:{labels:{color:tc,font:{size:10}}},
              subtitle:{ display:true, text:'Нет данных — загрузите матчи через ETL', color:tc, font:{size:11} }},
            scales:{ x:{ticks:{color:tc},grid:{color:gc}}, y:{ticks:{color:tc},grid:{color:gc}} } },
        });
        return;
      }

      const labels = data.map(r => String(r.minute ?? r.period ?? r.interval ?? r.label ?? ''));
      const values = data.map(r => +(r[cfg.key] || r.goals || r.count || 0));

      // Цвет по зонам (голы в конце матча — тёмно-красный)
      const bgColors = labels.map((l) => {
        const n = parseInt(l);
        if (sport === 'football') {
          if (n >= 76) return 'rgba(255,69,96,0.75)';
          if (n >= 61) return 'rgba(255,180,0,0.65)';
          if (n >= 46) return 'rgba(0,212,255,0.55)';
          return 'rgba(0,212,255,0.35)';
        }
        return 'rgba(0,212,255,0.55)';
      });

      this.charts.goals = new Chart(cvs, {
        type: 'bar',
        data: { labels, datasets: [{ label: cfg.label, data: values, backgroundColor: bgColors, borderRadius: 2 }] },
        options: { responsive:true, maintainAspectRatio:false, animation:false,
          plugins:{ legend:{ labels:{color:tc,font:{size:10}} } },
          scales:{ x:{ title:{display:true,text:cfg.xLabel,color:tc,font:{size:10}}, ticks:{color:tc,font:{size:9},maxTicksLimit:18}, grid:{color:gc} },
                   y:{ ticks:{color:tc,font:{size:10}}, grid:{color:gc} } } },
      });
    } catch(e) {
      this._chartError(cvs, tc, gc, cfg.label, e.message);
    }
  },

  // ── График Дом vs Гости ─────────────────────────────────────────────
  async renderHomeAwayChart(sport, league, season) {
    if (this.charts.homeaway) { try { this.charts.homeaway.destroy(); } catch(e){} }
    const cvs = document.getElementById('chartStatsHomeAway');
    if (!cvs) return;

    const dk = document.body.classList.contains('dark-mode');
    const tc = dk ? '#8892a4' : '#4a5568', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.06)';

    try {
      const d = await apiCall(`/api/stats/home-away?sport=${sport}&league=${league}&season=${season}`);
      const stats = d?.stats || {};

      // Если нет данных — показываем placeholder
      const hw = +(stats.home_wins  || 0);
      const dr = +(stats.draws      || 0);
      const aw = +(stats.away_wins  || 0);
      const total = hw + dr + aw;

      const labels    = sport === 'hockey' ? ['Хозяева','Гости'] : ['Хозяева','Ничья','Гости'];
      const values    = sport === 'hockey' ? [hw, aw] : [hw, dr, aw];
      const pcts      = values.map(v => total ? +(v / total * 100).toFixed(1) : 0);
      const bgColors  = ['rgba(0,212,255,0.7)','rgba(255,210,0,0.65)','rgba(255,69,96,0.7)'];

      this.charts.homeaway = new Chart(cvs, {
        type: 'doughnut',
        data: { labels, datasets: [{ data: total ? pcts : [33,34,33], backgroundColor: bgColors, borderWidth: 0 }] },
        options: { responsive:true, maintainAspectRatio:false, animation:false,
          plugins:{ legend:{ labels:{color:tc,font:{size:11}} },
            tooltip:{ callbacks:{ label:(ctx) => ` ${ctx.label}: ${ctx.raw}% (${values[ctx.dataIndex]})` } },
            subtitle: !total ? { display:true, text:'Нет данных', color:tc, font:{size:11} } : undefined },
        },
      });
    } catch(e) {
      this._chartError(cvs, tc, gc, 'Дом vs Гости', e.message);
    }
  },

  // ── xG vs фактические голы ──────────────────────────────────────────
  async renderXGChart(sport, league, season) {
    if (this.charts.xg) { try { this.charts.xg.destroy(); } catch(e){} }
    const cvs = document.getElementById('chartStatsXG');
    if (!cvs) return;

    const dk = document.body.classList.contains('dark-mode');
    const tc = dk ? '#8892a4' : '#4a5568', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.06)';

    // xG только для футбола/хоккея — для остальных показываем сводку
    if (!['football','hockey'].includes(sport)) {
      this.charts.xg = new Chart(cvs, {
        type: 'bar',
        data: { labels:[], datasets:[{data:[],label:'Нет данных'}] },
        options:{ responsive:true, maintainAspectRatio:false, animation:false,
          plugins:{ subtitle:{display:true, text:`xG недоступен для ${sport}`, color:tc} },
          scales:{ x:{ticks:{color:tc},grid:{color:gc}}, y:{ticks:{color:tc},grid:{color:gc}} } },
      });
      return;
    }

    try {
      const d = await apiCall(`/api/stats/xg-vs-actual?sport=${sport}&league=${league}&season=${season}&limit=20`);
      const teams = d?.teams || [];

      if (!teams.length) {
        this._chartError(cvs, tc, gc, 'xG vs Голы', 'Нет данных');
        return;
      }

      const labels = teams.map(t => t.team?.slice(0, 12) || '?');
      const xgData   = teams.map(t => +(t.xg  || t.xg_for   || 0).toFixed(2));
      const goalData = teams.map(t => +(t.goals || t.gf || 0));

      this.charts.xg = new Chart(cvs, {
        type: 'bar',
        data: { labels, datasets: [
          { label:'xG', data: xgData, backgroundColor:'rgba(0,212,255,0.55)', borderRadius:2 },
          { label:'Фактически', data: goalData, backgroundColor:'rgba(192,132,252,0.6)', borderRadius:2 },
        ]},
        options:{ responsive:true, maintainAspectRatio:false, animation:false, indexAxis:'y',
          plugins:{ legend:{labels:{color:tc,font:{size:10}}} },
          scales:{ x:{ticks:{color:tc,font:{size:10}},grid:{color:gc}}, y:{ticks:{color:tc,font:{size:9}},grid:{color:gc}} } },
      });
    } catch(e) {
      this._chartError(cvs, tc, gc, 'xG vs Голы', e.message);
    }
  },

  _chartError(cvs, tc, gc, label, msg) {
    try {
      new Chart(cvs, {
        type: 'bar',
        data: { labels: [], datasets: [{ label, data: [] }] },
        options: { responsive:true, maintainAspectRatio:false, animation:false,
          plugins:{ subtitle:{ display:true, text:`⚠️ ${msg}`, color:tc, font:{size:11} } },
          scales:{ x:{ticks:{color:tc},grid:{color:gc}}, y:{ticks:{color:tc},grid:{color:gc}} } },
      });
    } catch(e) {}
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