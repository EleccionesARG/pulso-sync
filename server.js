/**
 * pulso-sync server v3.1
 *
 * FIXES v3.1:
 *  - autoRecoverSyncConfigs(): al arrancar lee v4config de Firebase y restaura syncConfigs
 *    automaticamente, sin necesitar que el dashboard llame a /sync/config manualmente.
 *  - startConfigListener(): escucha cambios en v4config en tiempo real para agregar/actualizar
 *    encuestas sin reiniciar el server.
 *  - Endpoint POST /sync/recover para trigger manual de recovery.
 */

const express = require('express');
const axios   = require('axios');
const admin   = require('firebase-admin');
const cron    = require('node-cron');

const app = express();
app.use(express.json({ limit: '10mb' }));

// ══════════════════════════════════════════
// CORS
// ══════════════════════════════════════════
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ══════════════════════════════════════════
// FIREBASE INIT
// ══════════════════════════════════════════
let db = null;
try {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: process.env.FIREBASE_DATABASE_URL,
  });
  db = admin.database();
  console.log('✓ Firebase conectado');
} catch (e) {
  console.warn('⚠ Firebase no configurado:', e.message);
}

// ══════════════════════════════════════════
// SURVEYMONKEY CLIENT
// ══════════════════════════════════════════
const SM_TOKEN = process.env.SURVEYMONKEY_TOKEN;
const SM_BASE  = 'https://api.surveymonkey.com/v3';

function smGet(path, params = {}) {
  return axios.get(`${SM_BASE}${path}`, {
    headers: { Authorization: `Bearer ${SM_TOKEN}` },
    params,
    timeout: 30000,
  });
}
function smPost(path, data = {}) {
  return axios.post(`${SM_BASE}${path}`, data, {
    headers: { Authorization: `Bearer ${SM_TOKEN}`, 'Content-Type': 'application/json' },
    timeout: 60000,
  });
}

// ══════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════
function norm(s) {
  return (s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
}
const PROV_ALIASES = {
  'ciudad autonoma de buenos aires': 'caba', 'ciudad de buenos aires': 'caba',
  'capital federal': 'caba', 'buenos aires ciudad': 'caba', 'c.a.b.a.': 'caba',
  'buenos aires provincia': 'buenos aires', 'provincia de buenos aires': 'buenos aires',
  'santiago del estero': 'stgo del estero',
  'entre rios': 'entre rios', 'entre ríos': 'entre rios',
  'neuquén': 'neuquen', 'córdoba': 'cordoba', 'tucumán': 'tucuman',
  'río negro': 'rio negro',
};
function normProv(s) { const n = norm(s); return PROV_ALIASES[n] || n; }

function getAgeGrp(edad, bounds, groups) {
  for (let i = 0; i < bounds.length; i++) {
    if (edad >= bounds[i][0] && edad <= bounds[i][1]) return groups[i];
  }
  return '?';
}

function lookupEst(prov, depto, muestra) {
  if (!muestra || !muestra.refTable || !muestra.refTable.length) return null;
  const pn = normProv(prov  || '');
  const dn = norm    (depto || '');
  if (muestra.fixedEstrato) {
    if (pn && muestra.fixedEstrato[pn] !== undefined) return String(muestra.fixedEstrato[pn]);
    if (dn && muestra.fixedEstrato[dn] !== undefined) return String(muestra.fixedEstrato[dn]);
  }
  const rt = muestra.refTable;
  const eq  = (a, b) => a === b;
  const inc = (a, b) => a.length > 0 && b.length > 0 && (a.includes(b) || b.includes(a));
  const hasNivel1 = rt.some(x => x.nivel1 && x.nivel1.trim() !== '');
  let r = null;
  if (hasNivel1 && pn && dn) {
    r = rt.find(x => eq(normProv(x.nivel1), pn) && eq(norm(x.nivel2), dn));
    if (!r) r = rt.find(x => eq(normProv(x.nivel1), pn) && inc(norm(x.nivel2), dn));
    if (!r) r = rt.find(x => inc(normProv(x.nivel1), pn) && inc(norm(x.nivel2), dn));
  }
  if (r) return String(r.estrato);
  if (!hasNivel1) {
    const val = dn || pn;
    if (val) {
      r = rt.find(x => eq(norm(x.nivel2), val));
      if (!r) r = rt.find(x => inc(norm(x.nivel2), val));
    }
  } else if (hasNivel1 && !dn && pn) {
    r = rt.find(x => eq(norm(x.nivel2), pn));
    if (!r) r = rt.find(x => inc(norm(x.nivel2), pn));
    if (!r) r = rt.find(x => eq(normProv(x.nivel1), pn));
    if (!r) r = rt.find(x => inc(normProv(x.nivel1), pn));
  }
  if (r) return String(r.estrato);
  if (hasNivel1 && pn && rt.every(x => !x.nivel2 || x.nivel2 === '')) {
    r = rt.find(x => eq(normProv(x.nivel1), pn));
    if (!r) r = rt.find(x => inc(normProv(x.nivel1), pn));
  }
  if (r) return String(r.estrato);
  return null;
}

// ══════════════════════════════════════════════════════════════
// CSV EXPORT MODULE
// ══════════════════════════════════════════════════════════════
async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function requestCsvExport(surveyId) {
  const res = await smPost(`/surveys/${surveyId}/exports`, {
    format: 'csv',
    language: 'es',
    all_answered: false,
  });
  return res.data.id;
}

async function pollExportJob(surveyId, jobId, maxWaitMs = 120000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    await sleep(3000);
    const res = await smGet(`/surveys/${surveyId}/exports/${jobId}`);
    const { status, url } = res.data;
    console.log(`  Export job ${jobId}: ${status}`);
    if (status === 'completed' && url) return url;
    if (status === 'failed') throw new Error('Export job falló en SurveyMonkey');
  }
  throw new Error('Timeout esperando export CSV (>2 min)');
}

async function downloadCsv(url) {
  const res = await axios.get(url, {
    responseType: 'text',
    timeout: 60000,
    headers: { 'Accept-Encoding': 'identity' },
  });
  return res.data;
}

function parseCsv(raw) {
  const text = raw.replace(/^\uFEFF/, '');
  const rows = [];
  let cur = [], cell = '', inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i], n = text[i+1];
    if (inQ) {
      if (c === '"' && n === '"') { cell += '"'; i++; }
      else if (c === '"')          inQ = false;
      else                         cell += c;
    } else {
      if      (c === '"')  { inQ = true; }
      else if (c === ',')  { cur.push(cell); cell = ''; }
      else if (c === '\n') { cur.push(cell); rows.push(cur); cur = []; cell = ''; }
      else if (c === '\r') { /* ignorar CR */ }
      else                 cell += c;
    }
  }
  if (cell || cur.length) { cur.push(cell); rows.push(cur); }
  return rows.filter(r => r.some(c => c.trim() !== ''));
}

function detectCsvColumns(headers, colMap, questionsMeta) {
  const idToHeading = {};
  (questionsMeta || []).forEach(q => { idToHeading[q.id] = q.heading; });

  function findCol(questionId) {
    if (!questionId) return -1;
    const heading = idToHeading[questionId];
    if (heading) {
      const idx = headers.findIndex(h => norm(h) === norm(heading));
      if (idx >= 0) return idx;
      const idx2 = headers.findIndex(h => norm(h).includes(norm(heading)) || norm(heading).includes(norm(h)));
      if (idx2 >= 0) return idx2;
    }
    return -1;
  }

  const deptoColMap = {};
  if (colMap.depto) {
    for (const [prov, qId] of Object.entries(colMap.depto)) {
      const col = findCol(qId);
      if (col >= 0) deptoColMap[prov] = col;
    }
  }

  return {
    gen:       findCol(colMap.gen),
    edad:      findCol(colMap.edad),
    prov:      findCol(colMap.prov),
    idCol:     findCol(colMap.idCol),
    filterCol: findCol(colMap.filterCol),
    depto:     deptoColMap,
  };
}

function csvRowsToRawCases(rows, colIdx, colMap) {
  if (rows.length < 2) return [];
  const filterExclude = (colMap.filterVal || 'not answered').trim().toLowerCase();
  const rawCases = [];

  rows.slice(1).forEach((row, i) => {
    if (colIdx.filterCol >= 0) {
      const val = (row[colIdx.filterCol] || '').trim().toLowerCase();
      if (val === filterExclude || val === '') return;
    }
    const gen     = (row[colIdx.gen]  || '').trim();
    const edadRaw = parseInt(row[colIdx.edad] || '');
    const prov    = (row[colIdx.prov] || '').trim();
    if (!gen || isNaN(edadRaw)) return;

    let depto = '';
    if (colIdx.depto && prov) {
      const provNorm = normProv(prov);
      for (const [pk, ci] of Object.entries(colIdx.depto)) {
        if (normProv(pk) === provNorm) {
          const v = (row[ci] || '').trim();
          if (v && v !== '-' && v !== '–') { depto = v; break; }
        }
      }
      if (!depto) {
        for (const [pk, ci] of Object.entries(colIdx.depto)) {
          const pkn = normProv(pk);
          if (provNorm.includes(pkn) || pkn.includes(provNorm)) {
            const v = (row[ci] || '').trim();
            if (v && v !== '-' && v !== '–') { depto = v; break; }
          }
        }
      }
    }

    const id = colIdx.idCol >= 0 && row[colIdx.idCol]
      ? row[colIdx.idCol].trim()
      : `R${String(i+1).padStart(4,'0')}`;

    rawCases.push({ id, gen, edad: edadRaw, prov, depto, ts: Date.now() });
  });

  return rawCases;
}

// ══════════════════════════════════════════════════════════════
// FETCH QUESTION METADATA
// ══════════════════════════════════════════════════════════════
async function fetchQuestionsMeta(surveyId) {
  const res = await smGet(`/surveys/${surveyId}/details`);
  const questions = [];
  (res.data.pages || []).forEach(page => {
    (page.questions || []).forEach(q => {
      questions.push({
        id: q.id,
        heading: q.headings?.[0]?.heading || q.id,
        type: q.family,
        choices: (q.answers?.choices || []).map(c => c.text),
      });
    });
  });
  return questions;
}

// ══════════════════════════════════════════════════════════════
// syncSurveyCSV: flujo completo usando CSV export
// ══════════════════════════════════════════════════════════════
async function syncSurveyCSV(surveyId, colMap, muestra) {
  console.log(`→ [CSV] Sincronizando encuesta ${surveyId}...`);

  const questionsMeta = await fetchQuestionsMeta(surveyId);
  console.log(`  ${questionsMeta.length} preguntas en metadata`);

  const jobId = await requestCsvExport(surveyId);
  console.log(`  Export job creado: ${jobId}`);

  const csvUrl = await pollExportJob(surveyId, jobId);
  const csvRaw = await downloadCsv(csvUrl);
  console.log(`  CSV descargado (${Math.round(csvRaw.length/1024)}KB)`);

  const rows = parseCsv(csvRaw);
  console.log(`  ${rows.length - 1} filas en CSV (sin header)`);
  if (rows.length < 2) {
    console.warn('  ⚠ CSV vacío o sin respuestas');
    return { surveyId, rawCases: [], lastSync: new Date().toISOString(), syncStats: { total: 0, valid: 0 } };
  }

  const headers = rows[0];
  const colIdx  = detectCsvColumns(headers, colMap, questionsMeta);
  console.log(`  Columnas detectadas: gen=${colIdx.gen} edad=${colIdx.edad} prov=${colIdx.prov} depto_keys=${Object.keys(colIdx.depto).length}`);

  if (colIdx.gen < 0 || colIdx.edad < 0 || colIdx.prov < 0) {
    console.warn('  ⚠ No se pudieron detectar las columnas principales. Headers:');
    console.warn('  ', headers.slice(0, 15).join(' | '));
  }

  const rawCases = csvRowsToRawCases(rows, colIdx, colMap);
  console.log(`  ✓ ${rawCases.length} casos válidos`);

  const newState = {
    surveyId,
    rawCases,
    lastSync: new Date().toISOString(),
    syncStats: { total: rows.length - 1, valid: rawCases.length },
    source: 'csv_export',
  };

  if (db) {
    await db.ref(`pulso/v4sync/${surveyId}`).set(JSON.stringify(newState));
    console.log(`  ✓ Firebase actualizado [pulso/v4sync/${surveyId}]`);
  }

  return newState;
}

// ══════════════════════════════════════════════════════════════
// syncSurveyAPI: flujo original por API (fallback)
// ══════════════════════════════════════════════════════════════
async function fetchChoiceMap(surveyId) {
  const res = await smGet(`/surveys/${surveyId}/details`);
  const choiceMap = {};
  (res.data.pages || []).forEach(page => {
    (page.questions || []).forEach(q => {
      const qid = q.id;
      choiceMap[qid] = {};
      const choices = (q.answers && q.answers.choices) || q.choices || [];
      choices.forEach(c => { choiceMap[qid][c.id] = c.text; });
      const rows = (q.answers && q.answers.rows) || q.rows || [];
      rows.forEach(r => { choiceMap[qid][r.id] = r.text; });
    });
  });
  return choiceMap;
}

function parseResponse(response, colMap, choiceMap) {
  const pages = response.pages || [];
  const answers = {};
  pages.forEach(page => {
    (page.questions || []).forEach(q => {
      const qid = q.id;
      const qAnswers = q.answers || [];
      if (!qAnswers.length) return;
      const first = qAnswers[0];
      let val = '';
      if (first.choice_id && choiceMap?.[qid]?.[first.choice_id]) val = choiceMap[qid][first.choice_id];
      else if (first.text)        val = first.text;
      else if (first.simple_text) val = first.simple_text;
      else if (first.choice_id)   val = String(first.choice_id);
      answers[qid] = val.trim();
    });
  });
  if (colMap.filterCol && colMap.filterVal) {
    const resp    = (answers[colMap.filterCol] || '').trim().toLowerCase();
    const exclude = colMap.filterVal.trim().toLowerCase();
    if (resp === exclude || resp === '') return null;
  }
  const gen     = (answers[colMap.gen] || '').trim();
  const edadRaw = parseInt(answers[colMap.edad]);
  const prov    = (answers[colMap.prov] || '').trim();
  if (!gen || isNaN(edadRaw)) return null;
  let depto = '';
  if (colMap.depto && prov) {
    const provNorm = normProv(prov);
    for (const [pk, qid] of Object.entries(colMap.depto)) {
      if (normProv(pk) === provNorm) {
        const v = (answers[qid] || '').trim();
        if (v && v !== '-' && v !== '–') { depto = v; break; }
      }
    }
    if (!depto) {
      for (const [pk, qid] of Object.entries(colMap.depto)) {
        const pkn = normProv(pk);
        if (provNorm.includes(pkn) || pkn.includes(provNorm)) {
          const v = (answers[qid] || '').trim();
          if (v && v !== '-' && v !== '–') { depto = v; break; }
        }
      }
    }
  }
  const id = colMap.idCol ? (answers[colMap.idCol] || response.id) : response.id;
  return { id, gen, edad: edadRaw, prov, depto, ts: Date.now() };
}

async function syncSurveyAPI(surveyId, colMap, muestra, appState) {
  console.log(`→ [API] Sincronizando encuesta ${surveyId}...`);
  const choiceMap = await fetchChoiceMap(surveyId);
  let allResponses = [], page = 1;
  while (true) {
    const res = await smGet(`/surveys/${surveyId}/responses/bulk`, { per_page: 100, page });
    const data = res.data.data || [];
    allResponses.push(...data);
    if (data.length < 100) break;
    page++;
    await sleep(300);
  }
  console.log(`  ${allResponses.length} respuestas obtenidas`);
  const rawCases = [];
  allResponses.forEach(r => {
    const c = parseResponse(r, colMap, choiceMap);
    if (c) rawCases.push(c);
  });
  console.log(`  ✓ ${rawCases.length} válidos`);
  const newState = { surveyId, rawCases, lastSync: new Date().toISOString(), source: 'api' };
  if (db) {
    await db.ref(`pulso/v4sync/${surveyId}`).set(JSON.stringify(newState));
  }
  return newState;
}

async function syncSurvey(surveyId, colMap, muestra, appState) {
  const cfg = syncConfigs[surveyId] || {};
  return cfg.useCSV !== false
    ? syncSurveyCSV(surveyId, colMap, muestra)
    : syncSurveyAPI(surveyId, colMap, muestra, appState);
}

// ══════════════════════════════════════════
// SYNC STATE
// ══════════════════════════════════════════
const syncConfigs = {};
const cronJobs    = {};

async function getAppState() {
  if (!db) return {};
  try {
    const snap = await db.ref('pulso/v4config').once('value');
    const val  = snap.val();
    return val ? (typeof val === 'string' ? JSON.parse(val) : val) : {};
  } catch (e) { return {}; }
}

async function runSyncForSurvey(surveyId) {
  const cfg = syncConfigs[surveyId];
  if (!cfg) return;
  try {
    const appState = await getAppState();
    const muestra  = (appState.muestras || []).find(m => m.id === cfg.muestraId) || null;
    await syncSurvey(surveyId, cfg.colMap, muestra, appState);
  } catch (e) {
    console.error(`Error en sync [${surveyId}]:`, e.message);
  }
}

function startCronForSurvey(surveyId, minutes) {
  if (cronJobs[surveyId]) { cronJobs[surveyId].stop(); delete cronJobs[surveyId]; }
  if (!minutes || minutes < 1) return;
  const expr = `*/${Math.max(1, minutes)} * * * *`;
  cronJobs[surveyId] = cron.schedule(expr, () => runSyncForSurvey(surveyId));
  console.log(`✓ Cron activo [${surveyId}]: cada ${minutes} min (modo CSV)`);
}

async function runSync() {
  for (const id of Object.keys(syncConfigs)) {
    await runSyncForSurvey(id);
  }
}

// ══════════════════════════════════════════════════════════════
// FIX: AUTO-RECOVERY desde Firebase al arrancar
// Resuelve el problema de "Railway reinicia y pierde syncConfigs"
// sin necesitar que el dashboard llame manualmente a /sync/config
// ══════════════════════════════════════════════════════════════
async function autoRecoverSyncConfigs() {
  if (!db) { console.log('[recovery] Firebase no disponible, saltando auto-recovery'); return; }
  try {
    console.log('[recovery] Leyendo v4config desde Firebase...');
    const snap = await db.ref('pulso/v4config').once('value');
    const val  = snap.val();
    if (!val) { console.log('[recovery] v4config vacío, sin nada que restaurar'); return; }

    const config  = typeof val === 'string' ? JSON.parse(val) : val;
    const surveys = config.activeSurveys || [];
    let recovered = 0;

    for (const sv of surveys) {
      const sid = String(sv.smSurveyId);
      if (!sid || !sv.colMap) continue;
      if (syncConfigs[sid]) continue; // ya configurado (p.ej. por /sync/config manual)

      const minutes = sv.syncIntervalMinutes || 60;
      syncConfigs[sid] = {
        surveyId: sid,
        colMap: sv.colMap,
        muestraId: sv.muestraId,
        intervalMinutes: minutes,
        useCSV: sv.useCSV !== false,
      };
      startCronForSurvey(sid, minutes);
      console.log(`  ✓ Recuperado: ${sid} (${sv.smTitle || sid}, cada ${minutes}min)`);
      recovered++;
    }

    if (recovered > 0) {
      console.log(`[recovery] ${recovered} configuracion(es) restaurada(s) desde Firebase`);
      // Sync inmediato para tener datos frescos tras el reinicio
      console.log('[recovery] Ejecutando sync inicial...');
      runSync().catch(e => console.error('[recovery] Error en sync inicial:', e.message));
    } else {
      console.log('[recovery] Sin configuraciones nuevas que restaurar');
    }
  } catch (e) {
    console.warn('[recovery] Error en auto-recovery:', e.message);
  }
}

// FIX: listener en tiempo real para v4config
// Si el dashboard agrega/modifica una encuesta, el server la pica automáticamente
function startConfigListener() {
  if (!db) return;
  db.ref('pulso/v4config').on('value', async (snap) => {
    const val = snap.val();
    if (!val) return;
    try {
      const config  = typeof val === 'string' ? JSON.parse(val) : val;
      const surveys = config.activeSurveys || [];
      for (const sv of surveys) {
        const sid = String(sv.smSurveyId);
        if (!sid || !sv.colMap) continue;
        const existing = syncConfigs[sid];
        const minutes  = sv.syncIntervalMinutes || 60;
        const changed  = !existing
          || existing.intervalMinutes !== minutes
          || JSON.stringify(existing.colMap) !== JSON.stringify(sv.colMap);
        if (changed) {
          syncConfigs[sid] = { surveyId: sid, colMap: sv.colMap, muestraId: sv.muestraId, intervalMinutes: minutes, useCSV: sv.useCSV !== false };
          startCronForSurvey(sid, minutes);
          console.log(`[config] ${existing ? 'Actualizado' : 'Nuevo'}: ${sid} (${sv.smTitle || sid})`);
        }
      }
    } catch (e) {
      console.error('[config listener]', e.message);
    }
  });
}

// ══════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════

app.get('/', (req, res) => {
  const activeSurveys = Object.keys(syncConfigs).map(id => ({
    surveyId: id,
    muestraId: syncConfigs[id].muestraId,
    intervalMinutes: syncConfigs[id].intervalMinutes,
    mode: syncConfigs[id].useCSV === false ? 'API' : 'CSV',
    cronActive: !!cronJobs[id],
  }));
  res.json({
    status: 'ok',
    service: 'Pulso Sync Server v3.1 (CSV mode + auto-recovery)',
    firebase: !!db,
    sm_token: !!SM_TOKEN,
    activeSurveys,
    totalActive: activeSurveys.length,
  });
});

app.get('/surveys', async (req, res) => {
  if (!SM_TOKEN) return res.status(500).json({ error: 'SURVEYMONKEY_TOKEN no configurado' });
  try {
    const r = await smGet('/surveys', { per_page: 50 });
    res.json({ surveys: (r.data.data || []).map(s => ({
      id: s.id, title: s.title,
      response_count: s.response_count, date_modified: s.date_modified,
    }))});
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/surveys/:id/questions', async (req, res) => {
  if (!SM_TOKEN) return res.status(500).json({ error: 'SURVEYMONKEY_TOKEN no configurado' });
  try {
    const questions = await fetchQuestionsMeta(req.params.id);
    res.json({ questions });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/csv/upload', async (req, res) => {
  const { surveyId, csvContent, colMap } = req.body;
  if (!surveyId || !csvContent) {
    return res.status(400).json({ error: 'surveyId y csvContent requeridos' });
  }
  try {
    const rows = parseCsv(csvContent);
    if (rows.length < 2) return res.status(400).json({ error: 'CSV vacío' });
    const headers       = rows[0];
    const questionsMeta = [];
    const colIdx        = detectCsvColumns(headers, colMap || {}, questionsMeta);
    const rawCases      = csvRowsToRawCases(rows, colIdx, colMap || {});
    const newState = {
      surveyId, rawCases,
      lastSync: new Date().toISOString(),
      syncStats: { total: rows.length - 1, valid: rawCases.length },
      source: 'manual_upload',
    };
    if (db) {
      await db.ref(`pulso/v4sync/${surveyId}`).set(JSON.stringify(newState));
    }
    console.log(`[CSV upload] ${surveyId}: ${rawCases.length} casos procesados`);
    res.json({ ok: true, total: rows.length - 1, valid: rawCases.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /sync/config — registrar encuesta manualmente (sigue funcionando igual)
app.post('/sync/config', async (req, res) => {
  const { surveyId, colMap, muestraId, intervalMinutes, useCSV } = req.body;
  if (!surveyId || !colMap) return res.status(400).json({ error: 'surveyId y colMap requeridos' });
  const minutes = intervalMinutes || 720;
  syncConfigs[surveyId] = { surveyId, colMap, muestraId, intervalMinutes: minutes, useCSV: useCSV !== false };
  startCronForSurvey(surveyId, minutes);
  runSyncForSurvey(surveyId).catch(console.error);
  res.json({ ok: true, message: `Sync configurado: ${surveyId} cada ${minutes} min (modo ${syncConfigs[surveyId].useCSV?'CSV':'API'})` });
});

app.post('/sync/now', async (req, res) => {
  const { surveyId } = req.body || {};
  if (!Object.keys(syncConfigs).length) {
    return res.status(400).json({ error: 'No hay sync configurado. Esperando auto-recovery o usá /sync/config.' });
  }
  try {
    if (surveyId && syncConfigs[surveyId]) {
      await runSyncForSurvey(surveyId);
      res.json({ ok: true, message: `Sync ejecutado para encuesta ${surveyId}` });
    } else {
      await runSync();
      res.json({ ok: true, message: `Sync ejecutado para ${Object.keys(syncConfigs).length} encuesta(s)` });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// FIX: endpoint para disparar manualmente el auto-recovery
app.post('/sync/recover', async (req, res) => {
  try {
    await autoRecoverSyncConfigs();
    res.json({ ok: true, active: Object.keys(syncConfigs).length, surveys: Object.keys(syncConfigs) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/sync/status', (req, res) => {
  res.json({
    active: Object.keys(cronJobs).length > 0,
    surveys: Object.keys(syncConfigs).map(id => ({
      surveyId: id,
      intervalMinutes: syncConfigs[id].intervalMinutes,
      mode: syncConfigs[id].useCSV === false ? 'API' : 'CSV',
      cronActive: !!cronJobs[id],
    })),
  });
});

app.post('/sync/stop', (req, res) => {
  const { surveyId } = req.body || {};
  if (surveyId) {
    if (cronJobs[surveyId]) { cronJobs[surveyId].stop(); delete cronJobs[surveyId]; }
    delete syncConfigs[surveyId];
    res.json({ ok: true, message: `Sync detenido: ${surveyId}` });
  } else {
    Object.values(cronJobs).forEach(j => j.stop());
    Object.keys(cronJobs).forEach(k => delete cronJobs[k]);
    Object.keys(syncConfigs).forEach(k => delete syncConfigs[k]);
    res.json({ ok: true, message: 'Todos los syncs detenidos' });
  }
});

// Meta Ads proxy (sin cambios)
const META_TOKEN   = process.env.META_TOKEN;
const META_ACCOUNT = process.env.META_ACCOUNT_ID;

app.get('/meta/adsets', async (req, res) => {
  if (!META_TOKEN || !META_ACCOUNT) {
    return res.json({ ok: false, error: 'META_TOKEN o META_ACCOUNT_ID no configurados' });
  }
  try {
    const url = `https://graph.facebook.com/v19.0/${META_ACCOUNT}/adsets` +
      `?fields=id,name,status,effective_status,daily_budget,lifetime_budget,campaign_id` +
      `&limit=100&access_token=${META_TOKEN}`;
    const r = await axios.get(url);
    res.json({ ok: true, adsets: (r.data.data || []).map(a => ({
      id: a.id, name: a.name, status: a.status, effectiveStatus: a.effective_status,
      dailyBudget: a.daily_budget, lifetimeBudget: a.lifetime_budget, campaignId: a.campaign_id,
    })), fetchedAt: Date.now() });
  } catch (e) {
    res.json({ ok: false, error: e.response?.data?.error?.message || e.message });
  }
});

// ══════════════════════════════════════════
// START
// ══════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`\nPulso Sync Server v3.1 en puerto ${PORT}`);
  console.log(`SM Token:  ${SM_TOKEN ? '✓' : '✗ falta SURVEYMONKEY_TOKEN'}`);
  console.log(`Firebase:  ${db    ? '✓' : '✗ falta FIREBASE_SERVICE_ACCOUNT'}`);
  console.log(`Meta Ads:  ${META_TOKEN ? '✓' : '✗ falta META_TOKEN'}`);
  console.log(`Modo:      CSV export por defecto\n`);

  // FIX: auto-recovery al arrancar (resuelve pérdida de config tras reinicio Railway)
  await autoRecoverSyncConfigs();

  // FIX: listener en tiempo real para nuevas encuestas desde el dashboard
  startConfigListener();
});
