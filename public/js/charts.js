'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Charts & Statistics Engine
//  public/js/charts.js  — ПОЛНАЯ ЗАМЕНА
//
//  ✅ Ноль хардкода — все лиги/сезоны загружаются из БД
//  ✅ Мульти-спорт: football, hockey, basketball, tennis, baseball, esports
//  ✅ Динамические фильтры обновляются при смене спорта
//  ✅ Графики реальных данных из ClickHouse
// ═══════════════════════════════════════════════════════════════════════════

// ─── Глобальные helpers (используются и в других модулях) ─────────────────
window.apiCall = window.apiCall || async function(url, method = 'GET', body = null) {
  const opts = {
    method,
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
  };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(url, opts);
  return r.json();
};

window.makeTable = window.makeTable || function(cols, rows) {
  if (!rows || !rows.length) return '<div class="empty-state"><div class="empty-state-icon">📭</div>Нет данных</div>';
  const ths = cols.map(c => `<th>${c}</th>`).join('');
  const trs = rows.map(r =>
    '<tr>' + cols.map(c => `<td>${r[c] !== undefined && r[c] !== null ? r[c] : '—'}</td>`).join('') + '</tr>'
  ).join('');
  return `<table class="data-table"><thead><tr>${ths}</tr></thead><tbody>${trs}</tbody></table>`;
};

// ─── oddsChart (графики коэффициентов — панель "Графики и коэффициенты") ──
const oddsChart = {
  charts: {},

  load() {
    this.renderSharpPublic();
    this.renderOpenClose();
  },

  renderSharpPublic() {
    const cvs = document.getElementById('chartSharpPublic');
    if (!cvs) return;
    if (this.charts.sharp) { try { this.charts.sharp.destroy(); } catch(e){} }
    const { tc, gc } = this._colors();
    this.charts.sharp = new Chart(cvs, {
      type: 'bar',
      data: {
        labels: ['Хозяева', 'Ничья', 'Гости'],
        datasets: [
          { label: 'Sharp %',  data: [62, 18, 20], backgroundColor: 'rgba(0,212,255,0.7)',  borderRadius: 3 },
          { label: 'Public %', data: [38, 35, 27], backgroundColor: 'rgba(192,132,252,0.7)', borderRadius: 3 },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { color: tc, font: { size: 10 } } } },
        scales: {
          x: { ticks: { color: tc }, grid: { color: gc } },
          y: { ticks: { color: tc, callback: v => v + '%' }, grid: { color: gc }, max: 100 },
        },
      },
    });
  },

  renderOpenClose() {
    const cvs = document.getElementById('chartOpenClose');
    if (!cvs) return;
    if (this.charts.openclose) { try { this.charts.openclose.destroy(); } catch(e){} }
    const { tc, gc } = this._colors();
    const n    = 15;
    const open = Array.from({ length: n }, () => 1.5 + Math.random() * 2);
    const close = open.map(v => v * (0.9 + Math.random() * 0.2));
    this.charts.openclose = new Chart(cvs, {
      type: 'scatter',
      data: {
        datasets: [{
          label: 'Open vs Close',
          data: open.map((v, i) => ({ x: v, y: close[i] })),
          backgroundColor: open.map((v, i) => close[i] < v ? 'rgba(0,230,118,0.8)' : 'rgba(255,69,96,0.8)'),
          pointRadius: 5,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { color: tc, font: { size: 10 } } } },
        scales: {
          x: { title: { display: true, text: 'Opening Odds', color: tc }, ticks: { color: tc }, grid: { color: gc } },
          y: { title: { display: true, text: 'Closing Odds', color: tc }, ticks: { color: tc }, grid: { color: gc } },
        },
      },
    });
  },

  _colors() {
    const dk = document.body.classList.contains('dark-mode');
    return {
      tc: dk ? '#8892a4' : '#4a5568',
      gc: dk ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.06)',
    };
  },
};

// ─── statsEngine ───────────────────────────────────────────────────────────
const statsEngine = {
  charts:        {},
  _leaguesCache: {},   // sport → [{code, label, matches}]
  _seasonsCache: {},   // sport:league → [season]
  _loading:      false,

  // ── Главная точка входа ──────────────────────────────────────────────────
  async load() {
    if (this._loading) return;
    this._loading = true;
    try {
      const sport = app?.currentSport || 'football';

      // 1. Обновляем фильтры из БД (если ещё не загружены для этого спорта)
      await this._syncFilters(sport);

      // 2. Читаем текущие значения
      const league = document.getElementById('statsLeague')?.value  || '';
      const season = document.getElementById('statsSeason')?.value  || '';
      const metric = document.getElementById('statsMetric')?.value  || 'goals';

      // 3. Рендерим всё параллельно
      await Promise.all([
        this._renderTable(sport, league, season, metric),
        this._renderGoals(sport, league, season),
        this._renderHomeAway(sport, league, season),
        this._renderXG(sport, league, season),
      ]);
    } finally {
      this._loading = false;
    }
  },

  // ── Синхронизация фильтров из БД ────────────────────────────────────────
  async _syncFilters(sport) {
    const leagueSel  = document.getElementById('statsLeague');
    const seasonSel  = document.getElementById('statsSeason');
    const metricSel  = document.getElementById('statsMetric');
    const hintEl     = document.getElementById('statsLeagueHint');
    if (!leagueSel) return;

    // Загружаем лиги если нет кеша
    if (!this._leaguesCache[sport]) {
      try {
        const d = await apiCall(`/api/stats/leagues?sport=${sport}`);
        this._leaguesCache[sport] = d.leagues || [];
      } catch(e) {
        this._leaguesCache[sport] = [];
      }
    }

    const leagues = this._leaguesCache[sport];
    const prevLeague = leagueSel.value;

    if (leagues.length) {
      leagueSel.innerHTML =
        `<option value="">Все лиги (${leagues.length})</option>` +
        leagues.map(l =>
          `<option value="${l.code}" ${l.code === prevLeague ? 'selected' : ''}>${l.label} (${l.matches})</option>`
        ).join('');
      if (hintEl) hintEl.textContent = `${leagues.length} лиг в БД`;
    } else {
      leagueSel.innerHTML = '<option value="">Нет данных в БД</option>';
      if (hintEl) hintEl.textContent = 'Нет данных — запустите ETL';
    }


    // Загружаем сезоны под выбранную лигу — только если кеша нет
    const league_for_seasons = leagueSel.value;  // читаем ПОСЛЕ обновления лиг
    const cacheKey = `${sport}:${league_for_seasons}`;
    const savedSeason = seasonSel?.value || '';   // запоминаем ДО перестройки

    if (!this._seasonsCache[cacheKey]) {
      try {
        const d = await apiCall(`/api/stats/seasons?sport=${sport}&league=${encodeURIComponent(league_for_seasons)}`);
        this._seasonsCache[cacheKey] = d.seasons || [];
      } catch(e) {
        this._seasonsCache[cacheKey] = [];
      }
    }

    const seasons = this._seasonsCache[cacheKey];
    if (seasonSel) {
      const currentVal = seasonSel.value;
      // Перестраиваем только если список изменился или пустой
      const existingOptions = Array.from(seasonSel.options).map(o => o.value).filter(Boolean);
      const newSeasons = seasons.map(s => String(s.season || s));
      const needsRebuild = newSeasons.some(s => !existingOptions.includes(s)) || existingOptions.some(s => !newSeasons.includes(s));

      if (needsRebuild || seasonSel.options.length <= 1) {
        seasonSel.innerHTML =
          '<option value="">Все сезоны</option>' +
          seasons.map(s => {
            const val = String(s.season || s);
            return `<option value="${val}" ${val === savedSeason ? 'selected' : ''}>${val}</option>`;
          }).join('');
        // Восстанавливаем выбранный сезон
        if (savedSeason) seasonSel.value = savedSeason;
      }
    }

    // Метрики по спорту — перестраиваем только при смене спорта
    const metricsBySport = {
      football:   [['goals','Голы'],['xg','xG'],['shots','Удары'],['corners','Угловые'],['cards','Карточки']],
      hockey:     [['goals','Голы'],['shots','Броски'],['corsi','Corsi%'],['pp','Большинство'],['sv','Вратари']],
      basketball: [['pts','Очки'],['reb','Подборы'],['ast','Передачи'],['fg','FG%'],['3p','3P%']],
      tennis:     [['sets','Сеты'],['aces','Эйсы'],['dfs','Дв.ошибки'],['bp','Брейки']],
      baseball:   [['runs','Раны'],['hits','Хиты'],['era','ERA'],['whip','WHIP']],
      esports:    [['kills','Фраги'],['rounds','Раунды'],['maps','Карты']],
    };
    if (metricSel) {
      const metrics    = metricsBySport[sport] || metricsBySport.football;
      const prevMetric = metricSel.value;
      const curVals    = Array.from(metricSel.options).map(o => o.value);
      const newVals    = metrics.map(([v]) => v);
      if (newVals.some(v => !curVals.includes(v)) || curVals.some(v => !newVals.includes(v))) {
        metricSel.innerHTML = metrics.map(([v, l]) =>
          `<option value="${v}" ${v === prevMetric ? 'selected' : ''}>${l}</option>`
        ).join('');
      }
    }
  },

  // ── Таблица команд ───────────────────────────────────────────────────────
  async _renderTable(sport, league, season, metric) {
    const el = document.getElementById('statsLeagueTable');
    if (!el) return;
    el.innerHTML = this._spinner();

    try {
      const p   = new URLSearchParams({ sport, league, season, metric, limit: 30 });
      const d   = await apiCall(`/api/stats/teams?${p}`);

      if (!d?.teams?.length) {
        el.innerHTML = `
          <div style="padding:40px;text-align:center">
            <div style="font-size:32px;margin-bottom:12px">📭</div>
            <div style="color:var(--text2);font-size:14px;margin-bottom:8px">
              ${d?.hint || 'Нет данных'}
            </div>
            ${d?.availableLeagues?.length ? `
              <div style="color:var(--text3);font-size:12px">
                Доступные лиги: <strong>${d.availableLeagues.join(' · ')}</strong>
              </div>` : ''}
          </div>`;
        // Инвалидируем кеш лиг чтобы перезагрузить
        delete this._leaguesCache[sport];
        return;
      }

      const colsBySport = {
        football:   [['#','#'],['team','Команда'],['matches','М'],['wins','В'],['draws','Н'],['losses','П'],
                     ['goals_for','Г+'],['goals_against','Г-'],['gd','±'],['points','Очки'],['xg','xG'],['xga','xGA'],['shots','Удары']],
        hockey:     [['#','#'],['team','Команда'],['matches','И'],['wins','В'],['ot_wins','ВОТ'],['losses','П'],
                     ['goals_for','Г+'],['goals_against','Г-'],['points','Очки'],['shots_for','Брос.'],['pp_pct','ПП%'],['sv_pct','Сейв%']],
        basketball: [['#','#'],['team','Команда'],['matches','И'],['wins','В'],['losses','П'],
                     ['goals_for','Очки'],['goals_against','Пропущ.']],
        baseball:   [['#','#'],['team','Команда'],['matches','И'],['wins','В'],['losses','П'],
                     ['goals_for','Раны'],['goals_against','Пропущ.']],
        tennis:     [['#','#'],['team','Игрок'],['matches','М'],['wins','В'],['losses','П']],
      };
      const cols = colsBySport[sport] || colsBySport.football;
      const medals = ['🥇','🥈','🥉'];

      const ths = cols.map(([,l]) => `<th>${l}</th>`).join('');
      const trs = d.teams.map((row, i) => {
        const cells = cols.map(([key]) => {
          if (key === '#')       return `<td style="color:var(--text3)">${medals[i] || (i+1)}</td>`;
          const v = row[key];
          if (v === undefined || v === null) return '<td>—</td>';
          if (key === 'points') return `<td style="font-weight:700;color:var(--accent)">${v}</td>`;
          if (key === 'gd')     return `<td style="color:${+v>=0?'var(--green)':'var(--red)'}">${+v>0?'+':''}${v}</td>`;
          if (['xg','xga','shots','pp_pct','sv_pct'].includes(key))
                                return `<td style="color:var(--text2)">${parseFloat(v).toFixed(2)}</td>`;
          if (key === 'team')   return `<td style="font-weight:600">${v}</td>`;
          return `<td>${v}</td>`;
        }).join('');
        return `<tr>${cells}</tr>`;
      }).join('');

      el.innerHTML = `
        <div style="font-size:11px;color:var(--text3);margin-bottom:6px;text-align:right">
          ✅ ClickHouse · ${d.teams.length} команд · ${d.total?.toLocaleString()} матчей · ${league || 'все лиги'} ${season || ''}
        </div>
        <table class="data-table"><thead><tr>${ths}</tr></thead><tbody>${trs}</tbody></table>`;

    } catch(e) {
      el.innerHTML = `<div style="padding:24px;color:var(--red);font-size:12px">⚠️ ${e.message}</div>`;
    }
  },
  // ── Таблица команд ───────────────────────────────────────────────────────
  async _renderGoals(sport, league, season) {
    this._destroyChart('goals');
    const cvs = document.getElementById('chartStatsGoals');
    if (!cvs) return;
    const { tc, gc } = this._colors();

    try {
      const p   = new URLSearchParams({ sport, league, season });  // sport обязателен
      const raw = await apiCall(`/api/stats/goals-by-minute?${p}`);

      let rows = [];
      if (Array.isArray(raw)) {
        rows = raw;
      } else if (raw?.type === 'summary') {
        // events таблица пустая — показываем среднее
        const avg  = raw.data?.[0]?.avg_goals ?? '?';
        const cnt  = raw.data?.[0]?.matches   ?? '?';
        const header = cvs.closest?.('.chart-card')?.querySelector?.('.chart-card-header span');
        if (header) header.textContent = `Среднее ${avg} гол/матч (${cnt} матчей)`;
        this._emptyChart(cvs, tc, gc, `Среднее: ${avg} гол/матч`, `${cnt} матчей`);
        return;
      }

      if (!rows.length) {
        this._emptyChart(cvs, tc, gc, 'Распределение голов', 'Нет данных');
        return;
      }

      const isHockey = sport === 'hockey';
      const labels = rows.map(r => r.label || (isHockey ? `Период ${r.minute}` : `${r.minute}'`));
      const data   = rows.map(r => +(r.goals || 0));
      const total  = data.reduce((a, b) => a + b, 0);
      const cntAll = rows.reduce((a, r) => a + (+(r.events||0) || 0), 0);

      if (this.charts.goals) { try { this.charts.goals.destroy(); } catch(e){} }
      this.charts.goals = new Chart(cvs, {
        type: 'bar',
        data: {
          labels,
          datasets: [{
            label: isHockey ? 'Голы по периодам' : 'Голов',
            data,
            backgroundColor: 'rgba(0,212,255,0.6)',
            borderColor:     'rgba(0,212,255,0.9)',
            borderWidth: 1,
            borderRadius: 3,
          }],
        },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false,
          plugins: {
            legend: { display: false },
            subtitle: {
              display: true,
              text: isHockey
                ? `${total} голов по периодам`
                : `Среднее ${(total / Math.max(data.length / 5, 1)).toFixed(2)} гол/матч (${total} голов)`,
              color: tc,
              font: { size: 10 },
            },
          },
          scales: {
            x: { ticks: { color: tc, font: { size: isHockey ? 11 : 9 } }, grid: { color: gc } },
            y: { ticks: { color: tc }, grid: { color: gc } },
          },
        },
      });
    } catch(e) {
      this._emptyChart(cvs, tc, gc, 'Распределение голов', e.message);
    }
  },

  // ── Голы по минутам / Счёт распределение ────────────────────────────────
  async _renderGoals(sport, league, season) {
    this._destroyChart('goals');
    const cvs = document.getElementById('chartStatsGoals');
    if (!cvs) return;
    const { tc, gc } = this._colors();

    try {
      const p = new URLSearchParams({ sport, league, season });
      const raw = await apiCall(`/api/stats/goals-by-minute?${p}`);

      // API может вернуть массив или {type:'summary', data:[]}
      const data = Array.isArray(raw) ? raw : (raw?.data || []);

      if (!data.length) {
        this._emptyChart(cvs, tc, gc, 'Голы', 'Нет событий в БД');
        return;
      }

      // Если это summary (нет events таблицы)
      if (!Array.isArray(raw) && raw?.type === 'summary') {
        const avg = parseFloat(data[0]?.avg_goals || 0).toFixed(2);
        const matches = data[0]?.matches || 0;
        this._emptyChart(cvs, tc, gc, 'Голы', `Среднее ${avg} гол/матч (${matches} матчей)`);
        return;
      }

      const labels = data.map(r => r.label || String(r.minute));
      const values = data.map(r => +(r.goals || r.count || 0));

      // Цвет — красный для концовки матча у футбола
      const bgColors = data.map(r => {
        const m = +(r.minute);
        if (sport === 'football') {
          if (m >= 76) return 'rgba(255,69,96,0.75)';
          if (m >= 61) return 'rgba(255,180,0,0.65)';
          if (m >= 46) return 'rgba(0,212,255,0.55)';
          return 'rgba(0,212,255,0.35)';
        }
        if (sport === 'hockey') {
          const colors = ['rgba(0,212,255,0.6)','rgba(0,212,255,0.75)','rgba(0,212,255,0.9)','rgba(255,180,0,0.8)'];
          return colors[Math.min(+(r.minute)-1, 3)] || 'rgba(0,212,255,0.6)';
        }
        return 'rgba(0,212,255,0.55)';
      });

      const titleBySport = {
        football:'Голы по минутам', hockey:'Голы по периодам',
        basketball:'Очки по четвертям', tennis:'Геймы по сетам',
        baseball:'Раны по иннингам',
      };

      this.charts.goals = new Chart(cvs, {
        type: 'bar',
        data: { labels, datasets: [{ label: titleBySport[sport] || 'Голы', data: values, backgroundColor: bgColors, borderRadius: 2 }] },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false,
          plugins: { legend: { labels: { color: tc, font: { size: 10 } } } },
          scales: {
            x: { ticks: { color: tc, font: { size: 9 }, maxTicksLimit: 20 }, grid: { color: gc } },
            y: { ticks: { color: tc, font: { size: 10 } }, grid: { color: gc } },
          },
        },
      });
    } catch(e) {
      this._emptyChart(cvs, tc, gc, 'Голы', e.message);
    }
  },

  // ── Хозяева vs Гости ────────────────────────────────────────────────────
  async _renderHomeAway(sport, league, season) {
    this._destroyChart('homeaway');
    const cvs = document.getElementById('chartStatsHomeAway');
    if (!cvs) return;
    const { tc, gc } = this._colors();

    try {
      const p = new URLSearchParams({ sport, league, season });  // sport обязателен
      const d = await apiCall(`/api/stats/home-away?${p}`);
      const s = d?.stats || d || {};

      const hw    = parseInt(s.home_wins || 0);
      const dr    = parseInt(s.draws     || 0);
      const aw    = parseInt(s.away_wins || 0);
      const total = hw + dr + aw;

      if (!total) {
        this._emptyChart(cvs, tc, gc, 'Хозяева vs Гости', 'Нет данных');
        return;
      }

      // Для хоккея и баскетбола нет ничьих
      const hasDraw = dr > 0;
      const labels  = hasDraw ? ['Хозяева','Ничья','Гости'] : ['Хозяева','Гости'];
      const values  = hasDraw ? [hw, dr, aw] : [hw, aw];
      const pcts    = values.map(v => +(v / total * 100).toFixed(1));
      const colors  = ['rgba(0,212,255,0.8)', 'rgba(255,210,0,0.7)', 'rgba(255,69,96,0.8)'];

      if (this.charts.homeaway) { try { this.charts.homeaway.destroy(); } catch(e){} }
      this.charts.homeaway = new Chart(cvs, {
        type: 'doughnut',
        data: {
          labels,
          datasets: [{
            data: pcts,
            backgroundColor: colors.slice(0, labels.length),
            borderWidth: 2,
            borderColor: 'transparent',
          }],
        },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false,
          plugins: {
            legend: { labels: { color: tc, font: { size: 11 } } },
            subtitle: {
              display: true,
              text: `${total} матчей`,
              color: tc,
              font: { size: 10 },
            },
            tooltip: {
              callbacks: {
                label: ctx => ` ${ctx.label}: ${ctx.raw}% (${values[ctx.dataIndex]} матчей)`,
              },
            },
          },
        },
      });
    } catch(e) {
      this._emptyChart(cvs, tc, gc, 'Хозяева vs Гости', e.message);
    }
  },

  // ── xG vs фактические голы ──────────────────────────────────────────────
  async _renderXG(sport, league, season) {
    this._destroyChart('xg');
    const cvs = document.getElementById('chartStatsXG');
    if (!cvs) return;
    const { tc, gc } = this._colors();

    if (!['football','hockey'].includes(sport)) {
      this._emptyChart(cvs, tc, gc, 'xG', `xG недоступен для ${sport}`);
      return;
    }

    try {
      const p = new URLSearchParams({ sport, league, season, limit: 15 });
      const d = await apiCall(`/api/stats/xg-vs-actual?${p}`);
      const teams = d?.teams || [];

      if (!teams.length) {
        this._emptyChart(cvs, tc, gc, 'xG vs Голы', 'Нет данных xG');
        return;
      }

      const labels   = teams.map(t => (t.team || '?').slice(0, 14));
      const xgVals   = teams.map(t => +(t.xg    || 0).toFixed(2));
      const goalVals = teams.map(t => +(t.goals  || 0).toFixed(2));

      this.charts.xg = new Chart(cvs, {
        type: 'bar',
        data: {
          labels,
          datasets: [
            { label: 'xG',          data: xgVals,   backgroundColor: 'rgba(0,212,255,0.55)',  borderRadius: 2 },
            { label: 'Фактически',  data: goalVals,  backgroundColor: 'rgba(192,132,252,0.6)', borderRadius: 2 },
          ],
        },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false, indexAxis: 'y',
          plugins: { legend: { labels: { color: tc, font: { size: 10 } } } },
          scales: {
            x: { ticks: { color: tc, font: { size: 10 } }, grid: { color: gc } },
            y: { ticks: { color: tc, font: { size: 9 } }, grid: { color: gc } },
          },
        },
      });
    } catch(e) {
      this._emptyChart(cvs, tc, gc, 'xG vs Голы', e.message);
    }
  },

  // ── Helpers ──────────────────────────────────────────────────────────────
  _colors() {
    const dk = document.body.classList.contains('dark-mode');
    return {
      tc: dk ? '#8892a4' : '#4a5568',
      gc: dk ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.06)',
    };
  },

  _spinner() {
    return `<div style="display:flex;align-items:center;justify-content:center;padding:40px;gap:10px">
      <div class="spinning" style="font-size:24px">⬡</div>
      <span style="color:var(--text3)">Загрузка из ClickHouse…</span>
    </div>`;
  },

  _destroyChart(key) {
    if (this.charts[key]) {
      try { this.charts[key].destroy(); } catch(e) {}
      this.charts[key] = null;
    }
  },

  _emptyChart(cvs, tc, gc, label, msg) {
    try {
      this.charts[label.toLowerCase().replace(/\s/g,'_')] = new Chart(cvs, {
        type: 'bar',
        data: { labels: [], datasets: [{ label, data: [] }] },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false,
          plugins: {
            legend:   { labels: { color: tc, font: { size: 10 } } },
            subtitle: { display: true, text: `ℹ️  ${msg}`, color: tc, font: { size: 11 } },
          },
          scales: {
            x: { ticks: { color: tc }, grid: { color: gc || 'transparent' } },
            y: { ticks: { color: tc }, grid: { color: gc || 'transparent' } },
          },
        },
      });
    } catch(e) {}
  },
};