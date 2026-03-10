'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Neural Networks Panel  v2
//  public/js/neural_frontend.js
//
//  ИЗМЕНЕНИЯ v2:
//  • Новые группы признаков: elo / poisson / form5 / form10 / venue / h2h /
//    fatigue / xg / surface / serve_w / serve_l / rally_w / rally_l / market
//  • Карточки стратегий отображают: топ-признаки, inisightType, confidence,
//    ожидаемый ROI, объяснение — и передают РЕАЛЬНЫЙ код в бэктест
//  • sendToBacktest() правильно вызывает backtestEngine.addStrategyFromNeural()
//    с { id, name, sport, code, color }
//  • Группы признаков с цветовой кодировкой и иконками
//  • Детальный блок "Почему модель так решила" для каждой цели
// ═══════════════════════════════════════════════════════════════════════════

const neuralPanel = {
  activeSport:    'football',
  statusData:     {},
  weightsData:    {},
  strategiesData: {},
  _trainLoading:  {},

  // ── ГРУППЫ ПРИЗНАКОВ — иконки и цвета ──────────────────────────────────
  GROUP_META: {
    elo:      { icon: '⚡', label: 'ELO рейтинг',        color: '#f59e0b' },
    poisson:  { icon: '📐', label: 'Poisson (λ-голы)',    color: '#8b5cf6' },
    form5:    { icon: '📈', label: 'Форма L-5',           color: '#4ade80' },
    form10:   { icon: '📊', label: 'Форма L-10',          color: '#22d3ee' },
    venue:    { icon: '🏟️', label: 'Дом/Выезд',           color: '#fb923c' },
    h2h:      { icon: '🤜', label: 'H2H история',         color: '#e879f9' },
    fatigue:  { icon: '😮‍💨', label: 'Усталость',          color: '#94a3b8' },
    xg:       { icon: '🎯', label: 'xG / удары',          color: '#34d399' },
    market:   { icon: '💰', label: 'Рынок / коэф.',       color: '#60a5fa' },
    season:   { icon: '📅', label: 'Сезон',               color: '#f97316' },
    shots:    { icon: '🏒', label: 'Броски',              color: '#06b6d4' },
    special:  { icon: '⚡', label: 'PP/PK',               color: '#a78bfa' },
    goalie:   { icon: '🥅', label: 'Вратарь',             color: '#10b981' },
    puck:     { icon: '🏒', label: 'Владение шайбой',     color: '#6366f1' },
    rank:     { icon: '🏅', label: 'Рейтинг АТП/WTA',    color: '#f59e0b' },
    serve_w:  { icon: '🎾', label: 'Подача (победитель)', color: '#4ade80' },
    serve_l:  { icon: '🎾', label: 'Подача (проигравший)',color: '#f87171' },
    rally_w:  { icon: '🔁', label: 'Розыгрыш (п)',        color: '#34d399' },
    rally_l:  { icon: '🔁', label: 'Розыгрыш (пр)',       color: '#f87171' },
    surface:  { icon: '🌿', label: 'Покрытие',            color: '#84cc16' },
    offense:  { icon: '⚡', label: 'Атака',               color: '#f59e0b' },
    defense:  { icon: '🛡️', label: 'Защита',              color: '#60a5fa' },
    pace:     { icon: '🏃', label: 'Темп',                color: '#fb923c' },
    boards:   { icon: '💥', label: 'Подборы',             color: '#a78bfa' },
    turnovers:{ icon: '🔄', label: 'Потери',              color: '#f87171' },
  },

  // ── Init ────────────────────────────────────────────────────────────────
  async init() {
    await this.loadStatus();
    this.renderSportTabs();
    this.renderStatus();
    this.bindEvents();
  },

  bindEvents() {
    document.addEventListener('click', e => {
      const tab = e.target.closest('[data-neural-sport]');
      if (tab) this.setActiveSport(tab.dataset.neuralSport);
    });
  },

  async setActiveSport(sport) {
    this.activeSport = sport;
    document.querySelectorAll('[data-neural-sport]').forEach(el => {
      el.classList.toggle('nn-tab-active', el.dataset.neuralSport === sport);
    });
    await Promise.all([this.loadWeights(sport), this.loadStrategies(sport)]);
    this.renderWeights(sport);
    this.renderStrategies(sport);
  },

  // ── API ─────────────────────────────────────────────────────────────────
  async api(path, opts = {}) {
    const r = await fetch('/api/neural' + path, {
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      ...opts,
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  },

  async loadStatus() {
    try { const d = await this.api('/status'); this.statusData = d.status || {}; }
    catch(e) { this.statusData = {}; }
  },

  async loadWeights(sport) {
    try { this.weightsData[sport] = await this.api(`/weights/${sport}`); }
    catch(e) { this.weightsData[sport] = null; }
  },

  async loadStrategies(sport) {
    try { this.strategiesData[sport] = await this.api(`/strategy/${sport}`); }
    catch(e) { this.strategiesData[sport] = null; }
  },

  // ── Train ────────────────────────────────────────────────────────────────
  async trainSport(sport) {
    if (this._trainLoading[sport]) return;
    this._trainLoading[sport] = true;

    const btns = document.querySelectorAll(`[data-train-sport="${sport}"]`);
    btns.forEach(b => { b.disabled = true; b.innerHTML = `<span class="nn-spin">⏳</span> Обучение...`; });
    this.showToast(`🧠 Обучение ${sport === 'all' ? 'всех моделей' : sport}...`, 'info');

    try {
      const d = await this.api(`/train/${sport}`, { method: 'POST' });
      if (d.ok) {
        const acc = d.results
          ? Object.entries(d.results).map(([s, r]) => `${s}: ${r.accuracy || '—'}%`).join(' | ')
          : `${d.accuracy}%`;
        this.showToast(`✅ Готово — ${acc}`, 'success');
        await this.loadStatus();
        this.renderStatus();
        if (sport === this.activeSport || sport === 'all') {
          await this.setActiveSport(this.activeSport);
        }
      } else {
        this.showToast(`⚠️ Ошибка: ${d.error || 'неизвестная'}`, 'error');
      }
    } catch(e) {
      this.showToast(`❌ ${e.message}`, 'error');
    } finally {
      this._trainLoading[sport] = false;
      btns.forEach(b => { b.disabled = false; b.innerHTML = b.dataset.trained === '1' ? '🔄 Переобучить' : '▶ Обучить'; });
    }
  },

  // ── STATUS CARDS ─────────────────────────────────────────────────────────
  renderStatus() {
    const c = document.getElementById('nn-status-grid');
    if (!c) return;

    const sports = Object.entries(this.statusData);
    if (!sports.length) {
      c.innerHTML = '<div class="nn-empty">Статус недоступен — запустите обучение</div>';
      return;
    }

    c.innerHTML = `
      <div class="nn-status-wrap">
        ${sports.map(([sport, info]) => `
          <div class="nn-status-card ${info.trained ? 'trained' : 'untrained'}"
               onclick="neuralPanel.setActiveSport('${sport}')">
            <div class="nn-sc-icon">${this.sportIcon(sport)}</div>
            <div class="nn-sc-body">
              <div class="nn-sc-name">${info.label}</div>
              <div class="nn-sc-badges">
                ${info.trained
                  ? `<span class="nn-badge green">✓ Обучена</span>
                     <span class="nn-badge blue">${info.accuracy}% acc</span>
                     <span class="nn-badge grey">${(info.rowsUsed||0).toLocaleString()} матчей</span>
                     <span class="nn-badge grey">${info.features} признаков</span>`
                  : `<span class="nn-badge red">Не обучена</span>`}
              </div>
              ${info.trained && info.lossHistory?.length
                ? `<div class="nn-loss-mini" id="loss-mini-${sport}"></div>`
                : ''}
            </div>
            <div class="nn-sc-actions">
              <button class="nn-btn sm" data-train-sport="${sport}"
                      data-trained="${info.trained ? 1 : 0}"
                      onclick="event.stopPropagation(); neuralPanel.trainSport('${sport}')">
                ${info.trained ? '🔄 Переобучить' : '▶ Обучить'}
              </button>
            </div>
          </div>
        `).join('')}
      </div>
    `;

    sports.forEach(([sport, info]) => {
      if (info.trained && info.lossHistory?.length) {
        setTimeout(() => this.drawMiniLoss(`loss-mini-${sport}`, info.lossHistory), 60);
      }
    });
  },

  // ── SPORT TABS ────────────────────────────────────────────────────────────
  renderSportTabs() {
    const c = document.getElementById('nn-sport-tabs');
    if (!c) return;
    c.innerHTML = ['football','hockey','tennis','basketball'].map(s => `
      <button class="nn-tab ${s === this.activeSport ? 'nn-tab-active' : ''}"
              data-neural-sport="${s}">
        ${this.sportIcon(s)} ${this.sportName(s)}
      </button>
    `).join('');
  },

  // ── WEIGHTS PANEL ─────────────────────────────────────────────────────────
  renderWeights(sport) {
    const c = document.getElementById('nn-weights-panel');
    if (!c) return;

    const data = this.weightsData[sport];
    if (!data || data.error) {
      c.innerHTML = `
        <div class="nn-not-trained">
          <div class="nn-not-trained-icon">🧠</div>
          <div>Модель <b>${this.sportName(sport)}</b> не обучена</div>
          <div style="font-size:12px;color:var(--text3);margin:8px 0">
            Нужно: минимум 100 матчей в ClickHouse</div>
          <button class="nn-btn primary" onclick="neuralPanel.trainSport('${sport}')">▶ Обучить сейчас</button>
        </div>`;
      return;
    }

    const maxW = Math.max(...data.inputImportance.map(f => f.weight), 0.001);

    // Группируем признаки по group для сводной таблицы
    const groupSummary = {};
    data.inputImportance.forEach(f => {
      if (!groupSummary[f.group]) groupSummary[f.group] = { sum: 0, count: 0, topFeat: f.feature };
      groupSummary[f.group].sum   += f.weight;
      groupSummary[f.group].count += 1;
    });
    const groupsSorted = Object.entries(groupSummary)
      .map(([g, v]) => ({ group: g, total: v.sum, topFeat: v.topFeat }))
      .sort((a, b) => b.total - a.total);
    const maxGroup = groupsSorted[0]?.total || 1;

    c.innerHTML = `
      <div class="nn-weights-header">
        <div class="nn-weights-title">
          ${data.label} — Анализ весов нейросети
          <span class="nn-badge green">${data.accuracy}% точность</span>
          <span class="nn-badge blue">${(data.rowsUsed||0).toLocaleString()} матчей</span>
          <span class="nn-badge grey">[${(data.architecture||[]).join('→')}]</span>
        </div>
        <div class="nn-trained-at">Обучена: ${data.trainedAt ? new Date(data.trainedAt).toLocaleString('ru-RU') : '—'}</div>
      </div>

      <div class="nn-weights-layout">

        <!-- ── Топ-20 признаков ── -->
        <div class="nn-section" style="grid-column:1">
          <div class="nn-section-title">📊 Важность признаков (top-20)</div>
          <div class="nn-section-sub">Входные веса первого скрытого слоя (L2-норма по строке)</div>
          <div class="nn-feature-list">
            ${data.inputImportance.slice(0, 20).map((f, i) => {
              const gm = this.GROUP_META[f.group] || { icon: '•', label: f.group, color: '#818cf8' };
              return `
              <div class="nn-feature-row">
                <div class="nn-feature-rank">${i + 1}</div>
                <div class="nn-feature-label">
                  <span class="nn-feat-group-dot" style="background:${gm.color}" title="${gm.label}">
                    ${gm.icon}
                  </span>
                  <span class="nn-feature-name">${f.feature}</span>
                </div>
                <div class="nn-feature-bar-wrap">
                  <div class="nn-feature-bar" style="width:${(f.weight/maxW*100).toFixed(1)}%;background:${gm.color}88"></div>
                </div>
                <div class="nn-feature-val">${f.weight.toFixed(4)}</div>
              </div>`;
            }).join('')}
          </div>
        </div>

        <!-- ── Группы признаков ── -->
        <div class="nn-section" style="grid-column:2">
          <div class="nn-section-title">🏷️ Важность по группам</div>
          <div class="nn-section-sub">Суммарный вклад каждой категории</div>
          <div class="nn-groups">
            ${groupsSorted.map(g => {
              const gm = this.GROUP_META[g.group] || { icon: '•', label: g.group, color: '#818cf8' };
              return `
              <div class="nn-group-item">
                <span class="nn-group-icon">${gm.icon}</span>
                <div style="flex:1;min-width:0">
                  <div style="display:flex;justify-content:space-between;margin-bottom:3px">
                    <span class="nn-group-name">${gm.label}</span>
                    <span class="nn-group-total">${g.total.toFixed(3)}</span>
                  </div>
                  <div class="nn-group-bar-wrap">
                    <div class="nn-group-bar-fill" style="width:${(g.total/maxGroup*100).toFixed(1)}%;background:${gm.color}"></div>
                  </div>
                  <div class="nn-group-top-feat">${g.topFeat}</div>
                </div>
              </div>`;
            }).join('')}
          </div>

          <!-- Архитектура сети -->
          <div class="nn-section-title" style="margin-top:20px">🔗 Архитектура сети</div>
          <canvas id="nn-arch-canvas" width="340" height="180"
                  style="display:block;margin-top:8px"></canvas>
        </div>

        <!-- ── Объяснения по целям ── -->
        <div class="nn-section nn-full-width">
          <div class="nn-section-title">🎯 Почему модель так решает — по каждой цели</div>
          <div class="nn-targets-grid">
            ${(data.targetExplanations || []).map(t => `
              <div class="nn-target-card">
                <div class="nn-target-label">${t.label}</div>
                <div class="nn-target-features">
                  Топ-5 признаков:
                  ${(t.topFeatures || []).slice(0, 5).map(f => {
                    const imp = typeof f.importance === 'number'
                      ? `<span class="nn-feat-imp">${(f.importance*100).toFixed(1)}%</span>` : '';
                    return `<span class="nn-feat-pill">${f.name}${imp}</span>`;
                  }).join('')}
                </div>
                <div class="nn-target-explanation">${t.explanation || ''}</div>
              </div>
            `).join('')}
          </div>
        </div>

      </div>
    `;

    setTimeout(() => this.drawArchitecture('nn-arch-canvas', data.architecture || []), 60);
  },

  // ── STRATEGIES PANEL ──────────────────────────────────────────────────────
  renderStrategies(sport) {
    const c = document.getElementById('nn-strategies-panel');
    if (!c) return;

    const data = this.strategiesData[sport];
    if (!data || data.error || !data.strategies?.length) {
      c.innerHTML = `
        <div class="nn-empty">
          <div style="font-size:32px;margin-bottom:12px">🚀</div>
          <div>Обучите модель для генерации персонализированных стратегий</div>
          <button class="nn-btn primary" style="margin-top:12px"
                  onclick="neuralPanel.trainSport('${sport}')">▶ Обучить ${this.sportName(sport)}</button>
        </div>`;
      return;
    }

    c.innerHTML = `
      <div class="nn-strat-header">
        <div class="nn-strat-title">🚀 Нейросетевые стратегии — ${data.label}</div>
        <div style="display:flex;gap:8px">
          <button class="nn-btn sm" onclick="neuralPanel.exportStrategies('${sport}')">📥 Экспорт JSON</button>
          <button class="nn-btn sm" onclick="neuralPanel.sendAllToBacktest('${sport}')">📤 Все в бэктест</button>
        </div>
      </div>
      <div class="nn-strat-grid">
        ${data.strategies.map((s, idx) => this._renderStratCard(s, idx, sport)).join('')}
      </div>
    `;
  },

  _renderStratCard(s, idx, sport) {
    const roiColor  = (s.roi || '').startsWith('+') ? '#4ade80' : '#f87171';
    const confColor = s.confidence > 70 ? '#4ade80' : s.confidence > 50 ? '#f59e0b' : '#94a3b8';

    const insightLabels = {
      'Нестандартно':   { icon: '💡', color: '#f59e0b' },
      'H2H паттерн':    { icon: '🤜', color: '#e879f9' },
      'Рыночный сигнал':{ icon: '💰', color: '#60a5fa' },
      'Форма':          { icon: '📈', color: '#4ade80' },
    };
    const insight = insightLabels[s.insightType] || { icon: '🎯', color: '#818cf8' };

    const topFeats = (s.topFeatures || []).slice(0, 4);

    return `
      <div class="nn-strat-card" id="nn-strat-${idx}">
        <div class="nn-strat-top">
          <span class="nn-strat-name">${s.label || s.target}</span>
          <span class="nn-strat-roi" style="color:${roiColor}">ROI ${s.roi}</span>
        </div>

        <!-- Тип инсайта -->
        <div class="nn-strat-insight">
          <span class="nn-insight-badge" style="border-color:${insight.color};color:${insight.color}">
            ${insight.icon} ${s.insightType || ''}
          </span>
          <span class="nn-strat-conf-text" style="color:${confColor}">
            ${s.confidence}% уверенность
          </span>
        </div>

        <!-- Топ-признаки -->
        ${topFeats.length ? `
        <div class="nn-strat-features">
          <span style="font-size:11px;color:var(--text3)">Ключевые признаки:</span>
          ${topFeats.map(f => {
            const group = this._guessGroup(f);
            const gm = this.GROUP_META[group] || { icon: '•', color: '#818cf8' };
            return `<span class="nn-feat-pill" style="border-color:${gm.color}55">
              ${gm.icon} ${f}
            </span>`;
          }).join('')}
        </div>` : ''}

        <!-- Объяснение -->
        <div class="nn-strat-explanation">${s.explanation || ''}</div>

        <!-- Уверенность -->
        <div class="nn-conf-wrap">
          <div class="nn-conf-bar">
            <div class="nn-conf-fill" style="width:${s.confidence}%;background:${confColor}"></div>
          </div>
        </div>

        <!-- Действия -->
        <div class="nn-strat-footer">
          <button class="nn-btn xs preview"
                  onclick="neuralPanel.previewCode('${sport}', ${idx})">
            👁 Код
          </button>
          <button class="nn-btn xs primary"
                  onclick="neuralPanel.sendToBacktest('${sport}', ${idx})">
            → Бэктест
          </button>
        </div>
      </div>
    `;
  },

  /** Угадать группу признака по ключевому слову в имени */
  _guessGroup(featName) {
    const n = featName.toLowerCase();
    if (n.includes('elo'))      return 'elo';
    if (n.includes('poisson') || n.includes('λ') || n.includes('ожид')) return 'poisson';
    if (n.includes('h2h'))      return 'h2h';
    if (n.includes('l5') || n.includes('last-5'))  return 'form5';
    if (n.includes('l10') || n.includes('last-10')) return 'form10';
    if (n.includes('дома') || n.includes('гостях') || n.includes('venue')) return 'venue';
    if (n.includes('усталость') || n.includes('отдых') || n.includes('back-to')) return 'fatigue';
    if (n.includes('xg') || n.includes('удар'))    return 'xg';
    if (n.includes('коэф') || n.includes('implied') || n.includes('маржа')) return 'market';
    if (n.includes('эйс') || n.includes('serve') || n.includes('подача')) return 'serve_w';
    if (n.includes('покрытие') || n.includes('hard') || n.includes('clay') || n.includes('grass')) return 'surface';
    if (n.includes('рейтинг') || n.includes('rank')) return 'rank';
    return 'form5';
  },

  // ── PREVIEW CODE ──────────────────────────────────────────────────────────
  previewCode(sport, idx) {
    const data = this.strategiesData[sport];
    if (!data?.strategies?.[idx]) return;
    const s = data.strategies[idx];

    const modal = document.createElement('div');
    modal.style.cssText = `
      position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,0.7);
      display:flex;align-items:center;justify-content:center;padding:20px
    `;
    modal.innerHTML = `
      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:16px;
                  max-width:700px;width:100%;max-height:80vh;overflow:auto;padding:24px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
          <div style="font-weight:700;font-size:15px">${s.label} — код стратегии</div>
          <button onclick="this.closest('div[style]').remove()"
                  style="background:none;border:none;color:var(--text3);font-size:18px;cursor:pointer">✕</button>
        </div>
        <pre style="background:var(--bg3);border-radius:8px;padding:16px;font-size:12px;
                    overflow:auto;color:#e2e8f0;line-height:1.6;white-space:pre-wrap">${
          (s.code || '// Код не сгенерирован').replace(/</g,'&lt;').replace(/>/g,'&gt;')
        }</pre>
        <div style="margin-top:12px;display:flex;gap:8px;justify-content:flex-end">
          <button class="nn-btn sm"
                  onclick="navigator.clipboard.writeText(${JSON.stringify(s.code || '')}); neuralPanel.showToast('✅ Скопировано','success')">
            📋 Скопировать
          </button>
          <button class="nn-btn sm primary"
                  onclick="neuralPanel.sendToBacktest('${sport}',${idx}); this.closest('div[style]').remove()">
            → Запустить бэктест
          </button>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
  },

  // ── SEND TO BACKTEST ──────────────────────────────────────────────────────
  sendToBacktest(sport, idx) {
    const data = this.strategiesData[sport];
    if (!data?.strategies?.[idx]) return;
    const s = data.strategies[idx];

    if (!s.code) {
      this.showToast('⚠️ Стратегия без кода — обучите модель заново', 'warning');
      return;
    }

    if (typeof backtestEngine === 'undefined') {
      this.showToast('❌ backtestEngine не найден', 'error');
      return;
    }

    const strat = {
      id:      `nn_${sport}_${s.target}_${Date.now()}`,
      name:    `🧠 NN: ${s.label}`,
      sport,
      code:    s.code,
      color:   '#00d4ff',
      enabled: true,
    };

    // Ищем существующий слот с тем же target
    const existing = backtestEngine.activeStrategies.findIndex(
      x => x.id.startsWith(`nn_${sport}_${s.target}`)
    );
    if (existing >= 0) {
      Object.assign(backtestEngine.activeStrategies[existing], strat);
    } else {
      backtestEngine.activeStrategies.push(strat);
    }
    backtestEngine.saveActiveStrategies?.();
    backtestEngine.renderStrategySlots?.();

    this.showToast(`✅ "${strat.name}" добавлена в бэктест`, 'success');

    // Переключаем на панель бэктеста
    setTimeout(() => {
      if (typeof app !== 'undefined' && app.showPanel) {
        app.showPanel('backtest');
      }
    }, 600);
  },

  sendAllToBacktest(sport) {
    const data = this.strategiesData[sport];
    if (!data?.strategies?.length) return;
    data.strategies.forEach((_, idx) => this.sendToBacktest(sport, idx));
    this.showToast(`✅ ${data.strategies.length} стратегий добавлены в бэктест`, 'success');
    setTimeout(() => {
      if (typeof app !== 'undefined' && app.showPanel) app.showPanel('backtest');
    }, 800);
  },

  // ── EXPORT ────────────────────────────────────────────────────────────────
  exportStrategies(sport) {
    const data = this.strategiesData[sport];
    if (!data) return;
    const blob = new Blob([JSON.stringify(data.strategies, null, 2)], { type: 'application/json' });
    const url  = URL.createObjectURL(blob);
    Object.assign(document.createElement('a'), {
      href: url, download: `nn_strategies_${sport}_${Date.now()}.json`
    }).click();
    URL.revokeObjectURL(url);
  },

  // ── MINI CHARTS ───────────────────────────────────────────────────────────
  drawMiniLoss(containerId, history) {
    const el = document.getElementById(containerId);
    if (!el || !history?.length) return;
    el.innerHTML = `<canvas width="160" height="30"></canvas>`;
    const canvas = el.querySelector('canvas');
    const ctx    = canvas.getContext('2d');
    const mn = Math.min(...history), mx = Math.max(...history), range = mx - mn || 1;
    const pts = history.map((v, i) => ({
      x: (i / (history.length - 1)) * 158 + 1,
      y: 28 - ((v - mn) / range) * 26,
    }));
    ctx.strokeStyle = '#4ade80'; ctx.lineWidth = 1.5;
    ctx.beginPath();
    pts.forEach((p, i) => i === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y));
    ctx.stroke();
    // Начальная и конечная точки
    [pts[0], pts[pts.length - 1]].forEach((p, i) => {
      ctx.fillStyle = i === 0 ? '#f59e0b' : '#4ade80';
      ctx.beginPath(); ctx.arc(p.x, p.y, 3, 0, Math.PI * 2); ctx.fill();
    });
  },

  drawArchitecture(canvasId, layers) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !layers?.length) return;
    const ctx = canvas.getContext('2d');
    const W = canvas.width, H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    const cols     = layers.length;
    const colW     = W / cols;
    const nodeR    = 6;
    const colors   = ['#4ade80', '#818cf8', '#818cf8', '#8b5cf6', '#f59e0b'];

    const positions = layers.map((n, col) => {
      const visible = Math.min(n, 8);
      const spacing = (H - 40) / (visible + 1);
      return Array.from({ length: visible }, (_, row) => ({
        x: col * colW + colW / 2, y: 20 + spacing * (row + 1),
      }));
    });

    // Связи
    ctx.globalAlpha = 0.08; ctx.strokeStyle = '#818cf8'; ctx.lineWidth = 0.7;
    for (let l = 0; l < positions.length - 1; l++) {
      positions[l].slice(0, 5).forEach(f =>
        positions[l + 1].slice(0, 5).forEach(t => {
          ctx.beginPath(); ctx.moveTo(f.x, f.y); ctx.lineTo(t.x, t.y); ctx.stroke();
        })
      );
    }
    ctx.globalAlpha = 1;

    // Узлы
    positions.forEach((col, ci) => {
      col.forEach(({ x, y }) => {
        ctx.beginPath(); ctx.arc(x, y, nodeR, 0, Math.PI * 2);
        ctx.fillStyle = colors[Math.min(ci, colors.length - 1)]; ctx.fill();
      });
      // Метка
      ctx.fillStyle = '#94a3b8'; ctx.font = '10px monospace'; ctx.textAlign = 'center';
      const lbl = ci === 0 ? `In\n${layers[ci]}`
                : ci === layers.length - 1 ? `Out\n${layers[ci]}`
                : `H${ci}\n${layers[ci]}`;
      lbl.split('\n').forEach((line, li) => ctx.fillText(line, col[0]?.x || 0, H - 18 + li * 12));
    });
  },

  // ── HELPERS ───────────────────────────────────────────────────────────────
  sportIcon(s)  { return { football:'⚽', hockey:'🏒', tennis:'🎾', basketball:'🏀' }[s] || '🎯'; },
  sportName(s)  { return { football:'Футбол', hockey:'Хоккей', tennis:'Теннис', basketball:'Баскетбол' }[s] || s; },

  showToast(msg, type = 'info') {
    const colors = { info:'#818cf8', success:'#4ade80', error:'#f87171', warning:'#f59e0b' };
    const t = Object.assign(document.createElement('div'), {
      textContent: msg,
      style: `position:fixed;top:20px;right:20px;z-index:9999;padding:10px 18px;
              border-radius:8px;font-size:13px;font-weight:600;pointer-events:none;
              background:${colors[type] || colors.info};color:#0f172a;
              box-shadow:0 4px 20px rgba(0,0,0,0.4);transition:opacity 0.3s`,
    });
    document.body.appendChild(t);
    setTimeout(() => { t.style.opacity = '0'; setTimeout(() => t.remove(), 350); }, 3000);
  },
};

// ── Auto-init ─────────────────────────────────────────────────────────────
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('nn-status-grid')) neuralPanel.init();
  });
} else {
  if (document.getElementById('nn-status-grid')) neuralPanel.init();
}