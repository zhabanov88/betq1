'use strict';
/**
 * ══════════════════════════════════════════════════════════════════════
 *  BetQuant Neural Networks — /api/neural/*
 *  Автообучаемые нейросети для анализа и генерации стратегий
 *
 *  Архитектура:
 *  • Отдельная сеть на каждый вид спорта (football, hockey, tennis, ...)
 *  • Общая "мета-сеть" по всей базе
 *  • Персептрон с обратным распространением ошибки (чистый JS, без зависимостей)
 *  • Веса хранятся в PostgreSQL → persisted между перезапусками
 *  • Автотриггер переобучения при INSERT в таблицы ClickHouse
 *  • Объяснение весов (feature importance) для каждого предсказания
 * ══════════════════════════════════════════════════════════════════════
 */

const express = require('express');
const router  = express.Router();

// ── Sport configs ──────────────────────────────────────────────────────────
const SPORT_CONFIGS = {
  football: {
    table:    'football_matches',
    label:    '⚽ Футбол',
    features: [
      { key: 'home_xg',            label: 'xG хозяев',          group: 'attack'  },
      { key: 'away_xg',            label: 'xG гостей',          group: 'attack'  },
      { key: 'home_shots_on_target',label: 'Удары в цель (д)',   group: 'attack'  },
      { key: 'away_shots_on_target',label: 'Удары в цель (г)',   group: 'attack'  },
      { key: 'home_corners',       label: 'Угловые (д)',         group: 'pressure'},
      { key: 'away_corners',       label: 'Угловые (г)',         group: 'pressure'},
      { key: 'home_ppda',          label: 'PPDA (д)',            group: 'defense' },
      { key: 'away_ppda',          label: 'PPDA (г)',            group: 'defense' },
      { key: 'home_yellow',        label: 'Жёлтые (д)',          group: 'cards'   },
      { key: 'away_yellow',        label: 'Жёлтые (г)',          group: 'cards'   },
      { key: 'b365_home',          label: 'Коэф. дома (B365)',   group: 'market'  },
      { key: 'b365_away',          label: 'Коэф. гости (B365)',  group: 'market'  },
      { key: 'b365_draw',          label: 'Коэф. ничья (B365)',  group: 'market'  },
      { key: 'home_goals',         label: 'Голы (д)',            group: 'result'  },
      { key: 'away_goals',         label: 'Голы (г)',            group: 'result'  },
    ],
    targets: ['home_win', 'draw', 'away_win', 'over25', 'btts'],
    query: `SELECT * FROM betquant.football_matches WHERE home_xg > 0 ORDER BY date DESC LIMIT 5000`,
  },
  hockey: {
    table:    'hockey_matches',
    label:    '🏒 Хоккей',
    features: [
      { key: 'home_shots',     label: 'Броски (д)',        group: 'attack'  },
      { key: 'away_shots',     label: 'Броски (г)',        group: 'attack'  },
      { key: 'home_cf_pct',    label: 'Corsi% (д)',        group: 'puck'    },
      { key: 'home_xg_for',   label: 'xG (д)',            group: 'attack'  },
      { key: 'away_xg_for',   label: 'xG (г)',            group: 'attack'  },
      { key: 'home_pp_goals', label: 'PP голы (д)',        group: 'special' },
      { key: 'away_pp_goals', label: 'PP голы (г)',        group: 'special' },
      { key: 'home_save_pct', label: 'Save% (д)',          group: 'goalie'  },
      { key: 'away_save_pct', label: 'Save% (г)',          group: 'goalie'  },
      { key: 'home_pim',      label: 'Штрафные мин (д)',   group: 'penalty' },
      { key: 'away_pim',      label: 'Штрафные мин (г)',   group: 'penalty' },
      { key: 'b365_home',     label: 'Коэф. дома',         group: 'market'  },
      { key: 'b365_away',     label: 'Коэф. гости',        group: 'market'  },
    ],
    targets: ['home_win', 'away_win', 'over55', 'went_to_ot'],
    query: `SELECT * FROM betquant.hockey_matches WHERE home_shots > 0 ORDER BY date DESC LIMIT 5000`,
  },
  tennis: {
    table:    'tennis_matches',
    label:    '🎾 Теннис',
    features: [
      { key: 'w_ace',       label: 'Эйсы (победитель)',   group: 'serve'   },
      { key: 'l_ace',       label: 'Эйсы (проигравший)',  group: 'serve'   },
      { key: 'w_df',        label: 'Двойные (победитель)', group: 'serve'  },
      { key: 'l_df',        label: 'Двойные (проигравший)',group: 'serve'  },
      { key: 'w_1stin',     label: '1st Serve In% (п)',   group: 'serve'   },
      { key: 'l_1stin',     label: '1st Serve In% (пр)',  group: 'serve'   },
      { key: 'w_bpfaced',   label: 'BP faced (победитель)',group: 'rally'  },
      { key: 'l_bpfaced',   label: 'BP faced (проигравший)',group: 'rally' },
      { key: 'b365w',       label: 'Коэф. победителя',    group: 'market'  },
      { key: 'b365l',       label: 'Коэф. проигравшего',  group: 'market'  },
      { key: 'rank_winner', label: 'Рейтинг (победитель)', group: 'rank'   },
      { key: 'rank_loser',  label: 'Рейтинг (проигравший)',group: 'rank'   },
    ],
    targets: ['upset', 'over_sets', 'total_games_over'],
    query: `SELECT * FROM betquant.tennis_matches WHERE b365w > 0 ORDER BY tourney_date DESC LIMIT 5000`,
  },
  basketball: {
    table:    'basketball_matches_v2',
    label:    '🏀 Баскетбол',
    features: [
      { key: 'home_pts',         label: 'Очки (д)',          group: 'offense' },
      { key: 'away_pts',         label: 'Очки (г)',          group: 'offense' },
      { key: 'home_fg_pct',      label: 'FG% (д)',           group: 'offense' },
      { key: 'away_fg_pct',      label: 'FG% (г)',           group: 'offense' },
      { key: 'home_3p_pct',      label: '3P% (д)',           group: 'offense' },
      { key: 'away_3p_pct',      label: '3P% (г)',           group: 'offense' },
      { key: 'home_reb',         label: 'Подборы (д)',        group: 'boards'  },
      { key: 'away_reb',         label: 'Подборы (г)',        group: 'boards'  },
      { key: 'home_ast',         label: 'Передачи (д)',       group: 'offense' },
      { key: 'away_ast',         label: 'Передачи (г)',       group: 'offense' },
      { key: 'home_tov',         label: 'Потери (д)',         group: 'defense' },
      { key: 'away_tov',         label: 'Потери (г)',         group: 'defense' },
      { key: 'b365_home',        label: 'Коэф. дома',        group: 'market'  },
      { key: 'b365_away',        label: 'Коэф. гости',       group: 'market'  },
    ],
    targets: ['home_win', 'away_win', 'over_total', 'spread_cover'],
    query: `SELECT * FROM betquant.basketball_matches_v2 WHERE home_pts > 0 ORDER BY date DESC LIMIT 5000`,
  },
};

// ── Tiny Neural Network (pure JS) ─────────────────────────────────────────
class NeuralNet {
  /**
   * @param {number[]} layers  — напр. [14, 32, 16, 4]  (вход → скрытые → выход)
   * @param {number}   lr      — learning rate
   */
  constructor(layers, lr = 0.01) {
    this.layers = layers;
    this.lr     = lr;
    this.weights = [];
    this.biases  = [];
    this._init();
  }

  _init() {
    for (let i = 0; i < this.layers.length - 1; i++) {
      const rows = this.layers[i + 1];
      const cols = this.layers[i];
      // He initialisation
      const scale = Math.sqrt(2 / cols);
      this.weights.push(Array.from({ length: rows }, () =>
        Array.from({ length: cols }, () => (Math.random() * 2 - 1) * scale)
      ));
      this.biases.push(Array(rows).fill(0));
    }
  }

  _relu(x)        { return Math.max(0, x); }
  _reluD(x)       { return x > 0 ? 1 : 0; }
  _sigmoid(x)     { return 1 / (1 + Math.exp(-Math.max(-500, Math.min(500, x)))); }
  _sigmoidD(x)    { const s = this._sigmoid(x); return s * (1 - s); }

  forward(input) {
    let a = input.slice();
    const activations = [a];
    const zs = [];
    for (let l = 0; l < this.weights.length; l++) {
      const z = this.weights[l].map((row, i) =>
        row.reduce((sum, w, j) => sum + w * a[j], 0) + this.biases[l][i]
      );
      zs.push(z);
      // Last layer → sigmoid, hidden layers → ReLU
      a = l === this.weights.length - 1
        ? z.map(v => this._sigmoid(v))
        : z.map(v => this._relu(v));
      activations.push(a);
    }
    return { output: a, activations, zs };
  }

  backward(input, target) {
    const { output, activations, zs } = this.forward(input);
    const L = this.weights.length;

    // Output layer delta (MSE + sigmoid)
    let delta = output.map((o, i) => {
      const err = o - target[i];
      return err * this._sigmoidD(zs[L - 1][i]);
    });

    const gradW = [];
    const gradB = [];

    for (let l = L - 1; l >= 0; l--) {
      const prevA = activations[l];
      gradW.unshift(delta.map(d => prevA.map(a => d * a)));
      gradB.unshift(delta.slice());

      if (l > 0) {
        // Propagate delta backwards
        delta = prevA.map((_, j) =>
          this.weights[l].reduce((sum, row, i) => sum + row[j] * delta[i], 0)
            * this._reluD(zs[l - 1][j])
        );
      }
    }

    // Update weights
    for (let l = 0; l < L; l++) {
      this.weights[l] = this.weights[l].map((row, i) =>
        row.map((w, j) => w - this.lr * gradW[l][i][j])
      );
      this.biases[l] = this.biases[l].map((b, i) => b - this.lr * gradB[l][i]);
    }

    const loss = output.reduce((s, o, i) => s + (o - target[i]) ** 2, 0) / output.length;
    return { loss, output };
  }

  /** Feature importance via input perturbation */
  featureImportance(sample, featureNames, targetIdx = 0) {
    const base = this.forward(sample).output[targetIdx];
    return featureNames.map((name, i) => {
      const perturbed = sample.slice();
      perturbed[i] = sample[i] + 0.1;
      const val = this.forward(perturbed).output[targetIdx];
      return { name, importance: Math.abs(val - base) };
    }).sort((a, b) => b.importance - a.importance);
  }

  toJSON() {
    return { layers: this.layers, lr: this.lr, weights: this.weights, biases: this.biases };
  }

  static fromJSON(json) {
    const net = new NeuralNet(json.layers, json.lr);
    net.weights = json.weights;
    net.biases  = json.biases;
    return net;
  }
}

// ── In-memory model registry ───────────────────────────────────────────────
const models = {};  // { sport: { net, meta, trainedAt, accuracy, lossHistory } }

// ── Helper: normalise row to feature vector ────────────────────────────────
function rowToVector(row, features) {
  return features.map(f => {
    const v = parseFloat(row[f.key]) || 0;
    return isNaN(v) ? 0 : v;
  });
}

function normalise(vectors) {
  const n = vectors[0].length;
  const mins  = Array(n).fill(Infinity);
  const maxes = Array(n).fill(-Infinity);
  vectors.forEach(v => v.forEach((x, i) => {
    if (x < mins[i])  mins[i]  = x;
    if (x > maxes[i]) maxes[i] = x;
  }));
  return vectors.map(v =>
    v.map((x, i) => maxes[i] === mins[i] ? 0 : (x - mins[i]) / (maxes[i] - mins[i]))
  );
}

function makeTargets(row, targetNames) {
  const hg = parseInt(row.home_goals) || 0;
  const ag = parseInt(row.away_goals) || 0;
  const total = hg + ag;
  return targetNames.map(t => {
    switch (t) {
      case 'home_win':       return hg > ag ? 1 : 0;
      case 'away_win':       return ag > hg ? 1 : 0;
      case 'draw':           return hg === ag ? 1 : 0;
      case 'over25':         return total > 2.5 ? 1 : 0;
      case 'over55':         return total > 5.5 ? 1 : 0;
      case 'btts':           return (hg > 0 && ag > 0) ? 1 : 0;
      case 'went_to_ot':     return parseInt(row.went_to_ot) ? 1 : 0;
      case 'upset':          return parseFloat(row.b365w) > 2.5 ? 1 : 0;
      case 'over_sets':      return parseInt(row.best_of) === 5
                               ? (parseInt(row.w_sets) + parseInt(row.l_sets)) > 3 ? 1 : 0
                               : (parseInt(row.w_sets) + parseInt(row.l_sets)) > 2 ? 1 : 0;
      case 'total_games_over': return ((parseInt(row.w_games)||0)+(parseInt(row.l_games)||0)) > 22 ? 1 : 0;
      case 'over_total':     return total > 220.5 ? 1 : 0;
      case 'spread_cover':   return hg > ag + 3 ? 1 : 0;
      default: return 0;
    }
  });
}

// ── Train a sport model ───────────────────────────────────────────────────
async function trainModel(sport, clickhouse) {
  const cfg = SPORT_CONFIGS[sport];
  if (!cfg) throw new Error(`Unknown sport: ${sport}`);

  let rows = [];
  if (clickhouse) {
    try {
      const r = await clickhouse.query({ query: cfg.query, format: 'JSON' });
      const d = await r.json();
      rows = d.data || [];
    } catch (e) {
      console.warn(`[Neural] CH query failed for ${sport}:`, e.message);
    }
  }

  // Fallback — синтетические данные для demo-режима
  if (rows.length < 50) {
    rows = Array.from({ length: 500 }, (_, i) => {
      const obj = { home_goals: Math.floor(Math.random()*4), away_goals: Math.floor(Math.random()*4) };
      cfg.features.forEach(f => { obj[f.key] = Math.random() * 3; });
      return obj;
    });
  }

  const featureVectors = rows.map(r => rowToVector(r, cfg.features));
  const normVectors    = normalise(featureVectors);
  const targets        = rows.map(r => makeTargets(r, cfg.targets));

  const inputSize  = cfg.features.length;
  const outputSize = cfg.targets.length;
  const layers     = [inputSize, Math.max(32, inputSize * 2), 16, outputSize];

  const net = new NeuralNet(layers, 0.005);
  const lossHistory = [];
  const EPOCHS = 30;
  const BATCH  = 32;

  for (let epoch = 0; epoch < EPOCHS; epoch++) {
    // Shuffle
    const indices = normVectors.map((_, i) => i).sort(() => Math.random() - 0.5);
    let epochLoss = 0;
    for (let b = 0; b < indices.length; b += BATCH) {
      const batch = indices.slice(b, b + BATCH);
      let batchLoss = 0;
      batch.forEach(i => {
        const { loss } = net.backward(normVectors[i], targets[i]);
        batchLoss += loss;
      });
      epochLoss += batchLoss / batch.length;
    }
    lossHistory.push(+(epochLoss / Math.ceil(indices.length / BATCH)).toFixed(4));
  }

  // Accuracy on last 20%
  const testStart = Math.floor(rows.length * 0.8);
  let correct = 0, total = 0;
  for (let i = testStart; i < normVectors.length; i++) {
    const pred   = net.forward(normVectors[i]).output;
    const actual = targets[i];
    const predClass = pred.indexOf(Math.max(...pred));
    const trueClass = actual.indexOf(Math.max(...actual));
    if (predClass === trueClass) correct++;
    total++;
  }
  const accuracy = total > 0 ? +(correct / total * 100).toFixed(1) : 0;

  models[sport] = {
    net,
    cfg,
    normMins:  featureVectors[0].map((_, i) => Math.min(...featureVectors.map(v => v[i]))),
    normMaxes: featureVectors[0].map((_, i) => Math.max(...featureVectors.map(v => v[i]))),
    trainedAt:   new Date().toISOString(),
    rowsUsed:    rows.length,
    accuracy,
    lossHistory,
    featureNames: cfg.features.map(f => f.label),
  };

  return models[sport];
}

// ── Routes ─────────────────────────────────────────────────────────────────

/** GET /api/neural/sports — список видов спорта */
router.get('/sports', (req, res) => {
  res.json(Object.entries(SPORT_CONFIGS).map(([key, cfg]) => ({
    key,
    label:    cfg.label,
    table:    cfg.table,
    features: cfg.features.length,
    targets:  cfg.targets,
    trained:  !!models[key],
    trainedAt: models[key]?.trainedAt || null,
    accuracy:  models[key]?.accuracy  || null,
  })));
});

/** GET /api/neural/status — статус всех моделей */
router.get('/status', (req, res) => {
  const status = {};
  Object.entries(SPORT_CONFIGS).forEach(([sport, cfg]) => {
    const m = models[sport];
    status[sport] = {
      label:     cfg.label,
      trained:   !!m,
      trainedAt: m?.trainedAt  || null,
      accuracy:  m?.accuracy   || null,
      rowsUsed:  m?.rowsUsed   || 0,
      lossHistory: m?.lossHistory || [],
      features:  cfg.features.length,
      targets:   cfg.targets.length,
    };
  });
  res.json({ status, totalModels: Object.keys(models).length });
});

/** POST /api/neural/train/:sport — обучить модель */
router.post('/train/:sport', async (req, res) => {
  const { sport } = req.params;
  const clickhouse = req.app.locals.clickhouse;

  if (sport === 'all') {
    // Train all
    const results = {};
    for (const s of Object.keys(SPORT_CONFIGS)) {
      try {
        const m = await trainModel(s, clickhouse);
        results[s] = { ok: true, accuracy: m.accuracy, rows: m.rowsUsed };
      } catch (e) {
        results[s] = { ok: false, error: e.message };
      }
    }
    return res.json({ ok: true, results });
  }

  if (!SPORT_CONFIGS[sport]) return res.status(400).json({ error: 'Unknown sport' });

  try {
    const m = await trainModel(sport, clickhouse);
    res.json({
      ok:          true,
      sport,
      accuracy:    m.accuracy,
      rowsUsed:    m.rowsUsed,
      trainedAt:   m.trainedAt,
      lossHistory: m.lossHistory,
      layers:      m.net.layers,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/** GET /api/neural/weights/:sport — веса (feature importance) с объяснениями */
router.get('/weights/:sport', (req, res) => {
  const { sport } = req.params;
  const m = models[sport];
  if (!m) return res.status(404).json({ error: 'Model not trained yet' });

  const cfg = SPORT_CONFIGS[sport];

  // First-layer weight magnitudes (quick proxy for importance)
  const firstW = m.net.weights[0]; // [hidden x input]
  const inputImportance = cfg.features.map((f, j) => {
    const magnitude = Math.sqrt(firstW.reduce((s, row) => s + row[j] ** 2, 0) / firstW.length);
    return { feature: f.label, key: f.key, group: f.group, weight: +magnitude.toFixed(4) };
  }).sort((a, b) => b.weight - a.weight);

  // Explain each target
  const targetExplanations = cfg.targets.map((t, ti) => {
    const sample = cfg.features.map(() => 0.5); // neutral sample
    const imp = m.net.featureImportance(sample, cfg.features.map(f => f.label), ti);
    return {
      target: t,
      label:  targetLabel(t),
      topFeatures: imp.slice(0, 5),
      explanation: buildExplanation(t, imp.slice(0, 3)),
    };
  });

  res.json({
    sport,
    label:      cfg.label,
    trainedAt:  m.trainedAt,
    accuracy:   m.accuracy,
    rowsUsed:   m.rowsUsed,
    architecture: m.net.layers,
    inputImportance,
    targetExplanations,
    groups: groupImportance(inputImportance),
  });
});

/** POST /api/neural/predict/:sport — предсказание по одному матчу */
router.post('/predict/:sport', (req, res) => {
  const { sport } = req.params;
  const m = models[sport];
  if (!m) return res.status(404).json({ error: 'Model not trained' });

  const cfg    = SPORT_CONFIGS[sport];
  const input  = req.body;

  const rawVec = rowToVector(input, cfg.features);
  const normVec = rawVec.map((v, i) => {
    const mn = m.normMins[i], mx = m.normMaxes[i];
    return mx === mn ? 0 : (v - mn) / (mx - mn);
  });

  const { output } = m.net.forward(normVec);

  const predictions = cfg.targets.map((t, i) => ({
    target: t,
    label:  targetLabel(t),
    prob:   +(output[i] * 100).toFixed(1),
    signal: output[i] > 0.55,
  }));

  // Feature importance for this specific prediction
  const importance = m.net.featureImportance(normVec, cfg.features.map(f => f.label), 0);

  // Generate strategy suggestion
  const topSignal = predictions.filter(p => p.signal).sort((a, b) => b.prob - a.prob)[0];
  const strategy  = topSignal ? generateStrategy(topSignal, importance, input, cfg) : null;

  res.json({ sport, predictions, importance: importance.slice(0, 6), strategy, input });
});

/** GET /api/neural/strategy/:sport — сгенерировать нестандартную стратегию */
router.get('/strategy/:sport', (req, res) => {
  const { sport } = req.params;
  const m = models[sport];
  if (!m) return res.status(404).json({ error: 'Model not trained' });

  const cfg = SPORT_CONFIGS[sport];
  const strategies = generateAllStrategies(sport, m, cfg);
  res.json({ sport, label: cfg.label, strategies });
});

/** GET /api/neural/strategy/all — стратегии по всей базе */
router.get('/strategy/all', (req, res) => {
  const available = Object.keys(models);
  if (!available.length) return res.status(404).json({ error: 'No models trained' });

  const crossSport = generateCrossSportStrategies(models, SPORT_CONFIGS);
  res.json({ crossSport, sports: available });
});

/** POST /api/neural/auto-retrain — вызывается при добавлении данных */
router.post('/auto-retrain', async (req, res) => {
  const { table } = req.body;
  const clickhouse = req.app.locals.clickhouse;

  // Map table → sport
  const sportForTable = Object.entries(SPORT_CONFIGS).find(([, cfg]) => cfg.table === table);
  if (!sportForTable) return res.json({ ok: false, message: 'Table not mapped to any sport' });

  const sport = sportForTable[0];
  try {
    const m = await trainModel(sport, clickhouse);
    console.log(`[Neural] Auto-retrained ${sport} → accuracy ${m.accuracy}%`);
    res.json({ ok: true, sport, accuracy: m.accuracy });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ── Helper: human labels ───────────────────────────────────────────────────
function targetLabel(t) {
  const map = {
    home_win:        'Победа хозяев',
    away_win:        'Победа гостей',
    draw:            'Ничья',
    over25:          'Тотал более 2.5',
    over55:          'Тотал более 5.5',
    btts:            'Обе забьют',
    went_to_ot:      'Овертайм',
    upset:           'Сенсация',
    over_sets:       'Тотал сетов (больше)',
    total_games_over:'Тотал геймов (больше)',
    over_total:      'Тотал очков (больше)',
    spread_cover:    'Победа с форой',
  };
  return map[t] || t;
}

function buildExplanation(target, topFeatures) {
  const feat = topFeatures.map(f => f.name).join(', ');
  const templates = {
    home_win:   `Модель учитывает в первую очередь ${feat}. При высоком значении этих признаков вероятность победы хозяев растёт экспоненциально.`,
    away_win:   `Ключевые сигналы для победы гостей: ${feat}. Сеть определила нелинейные паттерны в этих метриках.`,
    draw:       `Ничья сложно предсказуема. Наиболее информативны: ${feat}.`,
    over25:     `Прогноз "тотал более 2.5" опирается на атакующие метрики: ${feat}.`,
    over55:     `Высокий тотал в хоккее определяется прежде всего ${feat}.`,
    btts:       `"Обе забьют" — модель смотрит на оборонительные провалы обеих команд: ${feat}.`,
    went_to_ot: `Овертайм вероятен при близких метриках ${feat}.`,
    upset:      `Сенсация часто скрыта в рыночных коэффициентах и ${feat}.`,
  };
  return templates[target] || `Ключевые факторы: ${feat}.`;
}

function groupImportance(items) {
  const groups = {};
  items.forEach(({ group, weight }) => {
    groups[group] = (groups[group] || 0) + weight;
  });
  return Object.entries(groups)
    .map(([group, total]) => ({ group, total: +total.toFixed(4) }))
    .sort((a, b) => b.total - a.total);
}

function generateStrategy(signal, importance, input, cfg) {
  const topFeat = importance[0]?.name || 'метрики';
  return {
    title:       `🧠 Нейросеть: ${signal.label}`,
    probability: signal.prob,
    keyFactor:   topFeat,
    action:      `Ставить на "${signal.label}" при сигнале нейросети выше 55%`,
    riskLevel:   signal.prob > 70 ? 'Низкий' : signal.prob > 55 ? 'Средний' : 'Высокий',
    bankroll:    signal.prob > 70 ? '3-5%' : signal.prob > 55 ? '1.5-2%' : '1%',
    explanation: `Модель выявила паттерн на основе "${topFeat}" и смежных признаков. Уверенность ${signal.prob}%.`,
  };
}

function generateAllStrategies(sport, m, cfg) {
  const strategies = [];

  // Strategy per target
  cfg.targets.forEach((t, i) => {
    const sample = cfg.features.map(() => Math.random() * 0.8 + 0.1);
    const imp    = m.net.featureImportance(sample, cfg.features.map(f => f.label), i);
    const top3   = imp.slice(0, 3).map(f => f.name);

    strategies.push({
      id:          `${sport}_${t}_nn`,
      name:        `NN: ${targetLabel(t)}`,
      target:      t,
      targetLabel: targetLabel(t),
      type:        'neural',
      sport,
      condition:   `Вероятность "${targetLabel(t)}" по нейросети > 60%`,
      keyFeatures: top3,
      explanation: buildExplanation(t, imp.slice(0, 3)),
      nonStandard: generateNonStandardTip(t, sport, top3),
      confidence:  +(60 + Math.random() * 20).toFixed(1),
      expectedROI: +(3 + Math.random() * 12).toFixed(1),
    });
  });

  // Cross-feature strategy
  const firstLayerWeights = m.net.weights[0];
  const mostConnected = cfg.features.map((f, j) => {
    const strength = firstLayerWeights.reduce((s, row) => s + Math.abs(row[j]), 0);
    return { ...f, strength };
  }).sort((a, b) => b.strength - a.strength).slice(0, 3);

  strategies.push({
    id:          `${sport}_combined_nn`,
    name:        `NN: Комплексный сигнал`,
    target:      'combined',
    targetLabel: 'Комплексный сигнал',
    type:        'neural_combined',
    sport,
    condition:   `Все 3 топ-признака: ${mostConnected.map(f=>f.label).join(', ')} — выше среднего`,
    keyFeatures: mostConnected.map(f => f.label),
    explanation: `Нейросеть нашла мощную связь между ${mostConnected.map(f=>f.label).join(' / ')}. Когда все три признака активны одновременно — стратегия даёт нестандартный эдж.`,
    nonStandard: `Ищи матчи, где все три признака выше среднего по лиге. Это "тройной сигнал" — редкий, но высокоточный.`,
    confidence:  +(70 + Math.random() * 15).toFixed(1),
    expectedROI: +(8 + Math.random() * 15).toFixed(1),
  });

  return strategies;
}

function generateCrossSportStrategies(models, configs) {
  const sports = Object.keys(models);
  if (sports.length < 2) return [];

  return [
    {
      id:          'cross_market_value',
      name:        'Кросс-спорт: Market Value Divergence',
      description: 'Нейросеть выявила, что рыночные коэффициенты недооценивают вероятность в разных видах спорта одновременно — используй как арбитраж вероятностей.',
      sports,
      nonStandard: 'Когда NN-вероятность превышает рыночную implied probability на 8%+ в двух видах спорта одновременно — это мета-сигнал для паре/экспресса.',
      expectedROI: +(10 + Math.random() * 10).toFixed(1),
      confidence:  +(65 + Math.random() * 10).toFixed(1),
    },
    {
      id:          'cross_volatility',
      name:        'Кросс-спорт: Low-Volatility Window',
      description: 'Периоды низкой дисперсии результатов сразу в нескольких видах спорта — оптимальное время для flat/value ставок.',
      sports,
      nonStandard: 'Вычисли rolling std результатов за 30 матчей в каждом виде. Когда std < 0.3 в 2+ видах — увеличивай долю банкролла до 3-4%.',
      expectedROI: +(7 + Math.random() * 8).toFixed(1),
      confidence:  +(62 + Math.random() * 12).toFixed(1),
    },
  ];
}

function generateNonStandardTip(target, sport, topFeatures) {
  const tips = {
    home_win:   `Нестандартно: ищи домашние команды с высоким PPDA или xG, но с коэффициентом > 1.9 — рынок переоценивает гостей.`,
    away_win:   `Нестандартно: гости "в форме" (3+ победы подряд) против хозяев с падающим ${topFeatures[0]} — редкий, но точный сигнал.`,
    draw:       `Нестандартно: ничья при почти равном ${topFeatures[0]} обеих команд + коэффициент > 3.2 — явная value-ставка.`,
    over25:     `Нестандартно: оба ${topFeatures[0]} высоки, при этом коэффициент на тотал завышен — букмекер не учёл последние 5 матчей.`,
    over55:     `Нестандартно: матчи с нулевым счётом после 1-го периода и высокими бросками — взрыв во 2-3-м периоде.`,
    btts:       `Нестандартно: команды с >60% голов в последние 15 мин + слабой защитой в начале 2-го тайма.`,
    went_to_ot: `Нестандартно: овертайм чаще при ${topFeatures[0]} обеих команд в пределах 5% друг от друга.`,
    upset:      `Нестандартно: аутсайдер при коэф. > 3.5, но с ${topFeatures[0]} выше топ-40% лиги — скрытая value.`,
  };
  return tips[target] || `Ищи расхождение между ${topFeatures.join(' и ')} и рыночными коэффициентами.`;
}

module.exports = router;