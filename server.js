
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

// Lookup estrato from muestra refTable
function lookupEst(prov, depto, muestra) {
  if (!muestra) return null;
  const pn = normProv(prov);

  // Fixed overrides
  if (muestra.fixedEstrato && muestra.fixedEstrato[pn] !== undefined) {
    return muestra.fixedEstrato[pn];
  }

  const dn = norm(depto);
  let r;
  if (muestra.cobertura === 'provincial' || muestra.cobertura === 'municipal') {
    r = muestra.refTable.find(x => norm(x.nivel2) === dn);
    if (!r) r = muestra.refTable.find(x => norm(x.nivel2).includes(dn) || dn.includes(norm(x.nivel2)));
  } else {
    r = muestra.refTable.find(x => normProv(x.nivel1) === pn && norm(x.nivel2) === dn);
    if (!r) r = muestra.refTable.find(x => normProv(x.nivel1) === pn && (norm(x.nivel2).includes(dn) || dn.includes(norm(x.nivel2))));
  }
  return r ? r.estrato : null;
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

  // Write raw cases to Firebase — dashboard handles estrato + quota logic
  const newState = {
    ...appState,
    rawCases,           // new field: raw cases without estrato
    lastSync: new Date().toISOString(),
    syncStats: { total: responses.length, valid: rawCases.length, excluded },
  };

  if (db) {
    await db.ref('pulso/v4sync').set(JSON.stringify(newState));
    console.log(`  ✓ Firebase actualizado con ${rawCases.length} casos raw`);
  }

  return newState;
}

// ══════════════════════════════════════════
// SYNC STATE (in-memory, also persisted to Firebase)
// ══════════════════════════════════════════
let syncConfig = null; // { surveyId, colMap, muestraId, intervalMinutes }
let cronJob = null;

async function getAppState() {
  if (!db) return {};
  try {
    const snap = await db.ref('pulso/v4').once('value');
    const val = snap.val();
    return val ? JSON.parse(val) : {};
  } catch (e) {
    return {};
  }
}

async function runSync() {
  if (!syncConfig) return;
  try {
    const appState = await getAppState();
    const muestra = (appState.muestras || []).find(m => m.id === syncConfig.muestraId) || null;
    await syncSurvey(syncConfig.surveyId, syncConfig.colMap, muestra, appState);
  } catch (e) {
    console.error('Error en sync:', e.message);
  }
}

function startCron(minutes) {
  if (cronJob) { cronJob.stop(); cronJob = null; }
  if (!minutes || minutes < 1) return;
  // Cron every N minutes
  const expr = `*/${Math.max(1, minutes)} * * * *`;
  cronJob = cron.schedule(expr, runSync);
  console.log(`✓ Cron activo: cada ${minutes} minutos`);
}

// ══════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════

// Health check
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'Pulso Sync Server',
    firebase: !!db,
    sm_token: !!SM_TOKEN,
    syncActive: !!cronJob,
    syncConfig: syncConfig ? { surveyId: syncConfig.surveyId, muestraId: syncConfig.muestraId, intervalMinutes: syncConfig.intervalMinutes } : null,
    lastSync: syncConfig?.lastSync || null,
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

// POST /sync/config — set which survey + muestra to sync
app.post('/sync/config', async (req, res) => {
  const { surveyId, colMap, muestraId, intervalMinutes } = req.body;
  if (!surveyId || !colMap) return res.status(400).json({ error: 'surveyId y colMap requeridos' });

  syncConfig = { surveyId, colMap, muestraId, intervalMinutes: intervalMinutes || 15 };
  startCron(syncConfig.intervalMinutes);

  // Run immediately
  runSync().catch(console.error);

  res.json({ ok: true, message: `Sync configurado: encuesta ${surveyId} cada ${syncConfig.intervalMinutes} min` });
});

// POST /sync/now — manual trigger
app.post('/sync/now', async (req, res) => {
  if (!syncConfig) return res.status(400).json({ error: 'No hay sync configurado. Usá /sync/config primero.' });
  try {
    await runSync();
    res.json({ ok: true, message: 'Sync ejecutado' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /sync/status
app.get('/sync/status', (req, res) => {
  res.json({
    active: !!cronJob,
    config: syncConfig,
  });
});

// POST /sync/stop
app.post('/sync/stop', (req, res) => {
  if (cronJob) { cronJob.stop(); cronJob = null; }
  syncConfig = null;
  res.json({ ok: true, message: 'Sync detenido' });
});

// ══════════════════════════════════════════
// START
// ══════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Pulso Sync Server corriendo en puerto ${PORT}`);
  console.log(`SM Token: ${SM_TOKEN ? '✓ configurado' : '✗ falta SURVEYMONKEY_TOKEN'}`);
  console.log(`Firebase: ${db ? '✓ conectado' : '✗ falta FIREBASE_SERVICE_ACCOUNT'}`);
});
