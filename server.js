const express = require('express');
const axios = require('axios');
const admin = require('firebase-admin');
const cron = require('node-cron');

const app = express();
app.use(express.json());

// ══════════════════════════════════════════
// CORS — permite llamadas desde GitHub Pages
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
const SM_BASE = 'https://api.surveymonkey.com/v3';

function smGet(path, params = {}) {
  return axios.get(`${SM_BASE}${path}`, {
    headers: { Authorization: `Bearer ${SM_TOKEN}` },
    params,
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

function parseAgeBounds(groups) {
  return groups.map(g => {
    if (g.endsWith('+')) return [parseInt(g), 999];
    const p = g.split('-'); return [parseInt(p[0]), parseInt(p[1])];
  });
}

// Lookup estrato from muestra refTable — smart, handles nacional/provincial/subnacional
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

  // Estrategia 1: nivel1+nivel2 (tabla nacional)
  if (hasNivel1 && pn && dn) {
    r = rt.find(x => eq(normProv(x.nivel1), pn) && eq(norm(x.nivel2), dn));
    if (!r) r = rt.find(x => eq(normProv(x.nivel1), pn) && inc(norm(x.nivel2), dn));
    if (!r) r = rt.find(x => inc(normProv(x.nivel1), pn) && inc(norm(x.nivel2), dn));
  }
  if (r) return String(r.estrato);

  // Estrategia 2: solo nivel2 (tabla subnacional o prov sin depto)
  if (!hasNivel1) {
    const val = dn || pn;
    if (val) {
      r = rt.find(x => eq(norm(x.nivel2), val));
      if (!r) r = rt.find(x => inc(norm(x.nivel2), val));
    }
  } else if (hasNivel1 && !dn && pn) {
    // Tabla nacional pero depto vacío: prov podría ser el depto (caso Perú)
    r = rt.find(x => eq(norm(x.nivel2), pn));
    if (!r) r = rt.find(x => inc(norm(x.nivel2), pn));
    if (!r) r = rt.find(x => eq(normProv(x.nivel1), pn));
    if (!r) r = rt.find(x => inc(normProv(x.nivel1), pn));
  }
  if (r) return String(r.estrato);

  // Estrategia 3: tabla nivel1 sin nivel2
  if (hasNivel1 && pn && rt.every(x => !x.nivel2 || x.nivel2 === '')) {
    r = rt.find(x => eq(normProv(x.nivel1), pn));
    if (!r) r = rt.find(x => inc(normProv(x.nivel1), pn));
  }
  if (r) return String(r.estrato);

  return null;
}

// ══════════════════════════════════════════
// PARSE SM RESPONSE → raw answers by questionId
// Estrato lookup and age grouping happens in the dashboard (HTML)
// because the reference table lives there
// ══════════════════════════════════════════
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
      // 1. Use choiceMap (pre-loaded from survey details) to resolve choice_id
      if (first.choice_id && choiceMap && choiceMap[qid] && choiceMap[qid][first.choice_id]) {
        val = choiceMap[qid][first.choice_id];
      }
      // 2. Fallback: direct text fields
      else if (first.text) val = first.text;
      else if (first.simple_text) val = first.simple_text;
      // 3. Last resort: raw choice_id (will show as number — shouldn't happen with choiceMap)
      else if (first.choice_id) val = String(first.choice_id);

      answers[qid] = val.trim();
    });
  });

  // Eligibility filter
  if (colMap.filterCol && colMap.filterVal) {
    const resp = (answers[colMap.filterCol] || '').trim().toLowerCase();
    const exclude = colMap.filterVal.trim().toLowerCase();
    if (resp === exclude || resp === '') return null;
  }

  const gen = (answers[colMap.gen] || '').trim();
  const edadRaw = parseInt(answers[colMap.edad]);
  const prov = (answers[colMap.prov] || '').trim();

  if (!gen || isNaN(edadRaw)) return null;

  // Find depto: match per-province question using respondent's province
  let depto = '';
  if (colMap.depto && prov) {
    const provNorm = normProv(prov);
    // Exact normProv match
    for (const [pk, qid] of Object.entries(colMap.depto)) {
      if (normProv(pk) === provNorm) {
        const v = (answers[qid] || '').trim();
        if (v && v !== '-' && v !== '–') { depto = v; break; }
      }
    }
    // Partial match fallback
    if (!depto) {
      for (const [pk, qid] of Object.entries(colMap.depto)) {
        const pkn = normProv(pk);
        if (provNorm.includes(pkn) || pkn.includes(provNorm)) {
          const v = (answers[qid] || '').trim();
          if (v && v !== '-' && v !== '–') { depto = v; break; }
        }
      }
    }
    // No global fallback — wrong province depto is worse than empty
  }

  const id = colMap.idCol ? (answers[colMap.idCol] || response.id) : response.id;

  // Return raw — dashboard will assign estrato, ageGrp, key
  return { id, gen, edad: edadRaw, prov, depto, ts: Date.now() };
}

// ══════════════════════════════════════════
// FETCH CHOICE MAP (questionId → choiceId → text)
// ══════════════════════════════════════════
async function fetchChoiceMap(surveyId) {
  const res = await smGet(`/surveys/${surveyId}/details`);
  const choiceMap = {}; // questionId → { choiceId → text }
  (res.data.pages || []).forEach(page => {
    (page.questions || []).forEach(q => {
      const qid = q.id;
      choiceMap[qid] = {};
      // choices can be in answers.choices or rows or cols
      const choices = (q.answers && q.answers.choices) || q.choices || [];
      choices.forEach(c => { choiceMap[qid][c.id] = c.text; });
      // also map rows (for matrix questions)
      const rows = (q.answers && q.answers.rows) || q.rows || [];
      rows.forEach(r => { choiceMap[qid][r.id] = r.text; });
    });
  });
  console.log(`  ✓ Mapa de opciones cargado: ${Object.keys(choiceMap).length} preguntas`);
  return choiceMap;
}

// ══════════════════════════════════════════
// FETCH ALL RESPONSES (paginated)
// ══════════════════════════════════════════
async function fetchAllResponses(surveyId) {
  const allResponses = [];
  let page = 1;
  const perPage = 100;

  while (true) {
    const res = await smGet(`/surveys/${surveyId}/responses/bulk`, {
      per_page: perPage,
      page,
    });
    const data = res.data.data || [];
    allResponses.push(...data);
    if (data.length < perPage) break;
    page++;
    await new Promise(r => setTimeout(r, 300));
  }
  return allResponses;
}

// ══════════════════════════════════════════
// SYNC: fetch SM → send raw cases to Firebase
// Estrato + ageGrp assigned by dashboard (HTML)
// ══════════════════════════════════════════
async function syncSurvey(surveyId, colMap, muestra, appState) {
  console.log(`→ Sincronizando encuesta ${surveyId}...`);

  // Fetch both completed and partial responses
  // Load choice map first so we can resolve option texts
  const choiceMap = await fetchChoiceMap(surveyId);

  const responses = await fetchAllResponses(surveyId);
  console.log(`  ${responses.length} respuestas obtenidas`);

  const rawCases = [];
  let excluded = 0;

  responses.forEach(r => {
    const c = parseResponse(r, colMap, choiceMap);
    if (!c) { excluded++; return; }
    rawCases.push(c);
  });

  const withDepto = rawCases.filter(c => c.depto).length;
  const noDepto = rawCases.length - withDepto;
  console.log(`  ✓ ${rawCases.length} válidos · ${excluded} excluidos · ${withDepto} con depto · ${noDepto} sin depto`);
  // Debug: show first 5 cases
  rawCases.slice(0,5).forEach((c,i) => console.log(`    [${i+1}] gen=${c.gen} edad=${c.edad} prov="${c.prov}" depto="${c.depto||'(vacío)'}"`));
  // Debug: show colMap.depto keys
  if(colMap.depto && Object.keys(colMap.depto).length > 0){
    console.log(`  colMap.depto keys: ${Object.keys(colMap.depto).join(', ')}`);
  } else {
    console.log('  ⚠ colMap.depto está vacío o no configurado');
  }
  // Debug: show distinct provs in responses
  const distinctProvs = [...new Set(rawCases.map(c=>c.prov).filter(Boolean))].slice(0,8);
  console.log(`  Provincias en respuestas: ${distinctProvs.join(', ')}`);

  // Write raw cases to Firebase at a per-survey path
  // surveyId is included so the dashboard can route data to the correct survey
  const newState = {
    surveyId,           // ★ critical: dashboard uses this to identify which survey to update
    rawCases,
    lastSync: new Date().toISOString(),
    syncStats: { total: responses.length, valid: rawCases.length, excluded },
  };

  if (db) {
    // Each survey writes to its own path: pulso/v4sync/{surveyId}
    await db.ref(`pulso/v4sync/${surveyId}`).set(JSON.stringify(newState));
    console.log(`  ✓ Firebase [pulso/v4sync/${surveyId}] actualizado con ${rawCases.length} casos raw`);
  }

  return newState;
}

// ══════════════════════════════════════════
// SYNC STATE — one config per surveyId
// ══════════════════════════════════════════
// syncConfigs: { [surveyId]: { surveyId, colMap, muestraId, intervalMinutes } }
// cronJobs:    { [surveyId]: CronJob }
const syncConfigs = {};
const cronJobs    = {};

async function getAppState() {
  if (!db) return {};
  try {
    const snap = await db.ref('pulso/v4config').once('value');
    const val = snap.val();
    return val ? JSON.parse(val) : {};
  } catch (e) {
    return {};
  }
}

async function runSyncForSurvey(surveyId) {
  const cfg = syncConfigs[surveyId];
  if (!cfg) return;
  try {
    const appState = await getAppState();
    const muestra = (appState.muestras || []).find(m => m.id === cfg.muestraId) || null;
    await syncSurvey(surveyId, cfg.colMap, muestra, appState);
  } catch (e) {
    console.error(`Error en sync [${surveyId}]:`, e.message);
  }
}

function startCronForSurvey(surveyId, minutes) {
  // Stop existing cron for this survey if any
  if (cronJobs[surveyId]) { cronJobs[surveyId].stop(); delete cronJobs[surveyId]; }
  if (!minutes || minutes < 1) return;
  const expr = `*/${Math.max(1, minutes)} * * * *`;
  cronJobs[surveyId] = cron.schedule(expr, () => runSyncForSurvey(surveyId));
  console.log(`✓ Cron activo [${surveyId}]: cada ${minutes} minutos`);
}

// Legacy runSync — runs all configured surveys (used by /sync/now without body)
async function runSync() {
  const ids = Object.keys(syncConfigs);
  if (!ids.length) return;
  for (const id of ids) {
    await runSyncForSurvey(id);
  }
}

// ══════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════

// Health check
app.get('/', (req, res) => {
  const activeSurveys = Object.keys(syncConfigs).map(id => ({
    surveyId: id,
    muestraId: syncConfigs[id].muestraId,
    intervalMinutes: syncConfigs[id].intervalMinutes,
    cronActive: !!cronJobs[id],
  }));
  res.json({
    status: 'ok',
    service: 'Pulso Sync Server v2 (multi-survey)',
    firebase: !!db,
    sm_token: !!SM_TOKEN,
    activeSurveys,
    totalActive: activeSurveys.length,
  });
});

// GET /surveys — list all surveys from SM account
app.get('/surveys', async (req, res) => {
  if (!SM_TOKEN) return res.status(500).json({ error: 'SURVEYMONKEY_TOKEN no configurado' });
  try {
    const r = await smGet('/surveys', { per_page: 50 });
    const surveys = (r.data.data || []).map(s => ({
      id: s.id,
      title: s.title,
      response_count: s.response_count,
      date_modified: s.date_modified,
    }));
    res.json({ surveys });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /surveys/:id/questions — get question list for column mapping
app.get('/surveys/:id/questions', async (req, res) => {
  if (!SM_TOKEN) return res.status(500).json({ error: 'SURVEYMONKEY_TOKEN no configurado' });
  try {
    const r = await smGet(`/surveys/${req.params.id}/details`);
    const questions = [];
    (r.data.pages || []).forEach(page => {
      (page.questions || []).forEach(q => {
        questions.push({
          id: q.id,
          heading: q.headings?.[0]?.heading || q.id,
          type: q.family,
          choices: (q.answers?.choices || []).map(c => c.text),
        });
      });
    });
    res.json({ questions });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /sync/config — register a survey for periodic sync (one config per surveyId)
app.post('/sync/config', async (req, res) => {
  const { surveyId, colMap, muestraId, intervalMinutes } = req.body;
  if (!surveyId || !colMap) return res.status(400).json({ error: 'surveyId y colMap requeridos' });

  const minutes = intervalMinutes || 15;
  syncConfigs[surveyId] = { surveyId, colMap, muestraId, intervalMinutes: minutes };
  startCronForSurvey(surveyId, minutes);

  console.log(`✓ Configurado sync para encuesta ${surveyId} (${Object.keys(syncConfigs).length} total activas)`);

  // Run immediately for this survey
  runSyncForSurvey(surveyId).catch(console.error);

  res.json({ ok: true, message: `Sync configurado: encuesta ${surveyId} cada ${minutes} min` });
});

// POST /sync/now — manual trigger (all surveys, or specific one via body.surveyId)
app.post('/sync/now', async (req, res) => {
  const { surveyId } = req.body || {};
  if (!Object.keys(syncConfigs).length) {
    return res.status(400).json({ error: 'No hay sync configurado. Usá /sync/config primero.' });
  }
  try {
    if (surveyId && syncConfigs[surveyId]) {
      await runSyncForSurvey(surveyId);
      res.json({ ok: true, message: `Sync ejecutado para encuesta ${surveyId}` });
    } else {
      // Sync all surveys
      await runSync();
      res.json({ ok: true, message: `Sync ejecutado para ${Object.keys(syncConfigs).length} encuesta(s)` });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /sync/status
app.get('/sync/status', (req, res) => {
  res.json({
    active: Object.keys(cronJobs).length > 0,
    surveys: Object.keys(syncConfigs).map(id => ({
      surveyId: id,
      intervalMinutes: syncConfigs[id].intervalMinutes,
      cronActive: !!cronJobs[id],
    })),
  });
});

// POST /sync/stop — detiene una encuesta específica (body.surveyId) o todas
app.post('/sync/stop', (req, res) => {
  const { surveyId } = req.body || {};
  if (surveyId) {
    if (cronJobs[surveyId]) { cronJobs[surveyId].stop(); delete cronJobs[surveyId]; }
    delete syncConfigs[surveyId];
    res.json({ ok: true, message: `Sync detenido para encuesta ${surveyId}` });
  } else {
    // Stop all
    Object.values(cronJobs).forEach(j => j.stop());
    Object.keys(cronJobs).forEach(k => delete cronJobs[k]);
    Object.keys(syncConfigs).forEach(k => delete syncConfigs[k]);
    res.json({ ok: true, message: 'Todos los syncs detenidos' });
  }
});

// ══════════════════════════════════════════
// META ADS
// ══════════════════════════════════════════
const META_TOKEN   = process.env.META_TOKEN;
const META_ACCOUNT = process.env.META_ACCOUNT_ID; // e.g. act_1234567890

app.get('/meta/adsets', async (req, res) => {
  if (!META_TOKEN || !META_ACCOUNT) {
    return res.json({ ok: false, error: 'META_TOKEN o META_ACCOUNT_ID no configurados en Railway' });
  }
  try {
    const url = `https://graph.facebook.com/v19.0/${META_ACCOUNT}/adsets` +
      `?fields=id,name,status,effective_status,daily_budget,lifetime_budget,campaign_id` +
      `&limit=100&access_token=${META_TOKEN}`;
    const r = await axios.get(url);
    const adsets = (r.data.data || []).map(a => ({
      id:              a.id,
      name:            a.name,
      status:          a.status,
      effectiveStatus: a.effective_status,
      dailyBudget:     a.daily_budget,
      lifetimeBudget:  a.lifetime_budget,
      campaignId:      a.campaign_id,
    }));
    console.log(`Meta: ${adsets.length} conjuntos obtenidos`);
    res.json({ ok: true, adsets, fetchedAt: Date.now() });
  } catch (e) {
    const errMsg = e.response?.data?.error?.message || e.message;
    console.error('Meta API error:', errMsg);
    res.json({ ok: false, error: errMsg });
  }
});

// ══════════════════════════════════════════
// START
// ══════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Pulso Sync Server corriendo en puerto ${PORT}`);
  console.log(`SM Token: ${SM_TOKEN ? '✓ configurado' : '✗ falta SURVEYMONKEY_TOKEN'}`);
  console.log(`Firebase: ${db ? '✓ conectado' : '✗ falta FIREBASE_SERVICE_ACCOUNT'}`);
  console.log(`Meta Ads: ${META_TOKEN ? '✓ configurado' : '✗ falta META_TOKEN'}`);
});
