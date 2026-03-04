
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
// PARSE SM RESPONSE → case object
// ══════════════════════════════════════════
function parseResponse(response, colMap, muestra) {
  /*
    colMap: {
      gen: questionId,
      edad: questionId,
      prov: questionId,
      depto: { [provinciaKey]: questionId },  // one col per province
      filterCol: questionId,
      filterVal: 'not answered',
      idCol: questionId (optional)
    }
  */
  const pages = response.pages || [];
  const answers = {};

  pages.forEach(page => {
    (page.questions || []).forEach(q => {
      const qid = q.id;
      const rows = q.answers || [];
      if (!rows.length) return;
      // For single-choice / open-text, just grab the first answer text
      const first = rows[0];
      answers[qid] = first.text || first.simple_text || first.choice_id || '';

      // Also store by choice text if available
      if (first.text) answers[qid] = first.text;
      else if (first.simple_text) answers[qid] = first.simple_text;
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

  // Find depto: look in per-province mapping
  let depto = '';
  if (colMap.depto) {
    const provKey = normProv(prov);
    // Try exact match first, then alias
    for (const [pk, qid] of Object.entries(colMap.depto)) {
      if (normProv(pk) === provKey) {
        depto = (answers[qid] || '').trim();
        if (depto) break;
      }
    }
  }

  const estrato = lookupEst(prov, depto, muestra) || '?';
  const ageBounds = muestra ? parseAgeBounds(muestra.ageGroups) : [];
  const ageGrp = muestra ? getAgeGrp(edadRaw, ageBounds, muestra.ageGroups) : '?';
  const key = `${gen}||${ageGrp}||${estrato}`;
  const id = colMap.idCol ? (answers[colMap.idCol] || response.id) : response.id;

  return { id, gen, edad: edadRaw, ageGrp, prov, depto, estrato, key, ts: Date.now() };
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
      status: 'completed',
    });
    const data = res.data.data || [];
    allResponses.push(...data);
    if (data.length < perPage) break;
    page++;
    // Respect rate limits
    await new Promise(r => setTimeout(r, 200));
  }
  return allResponses;
}

// ══════════════════════════════════════════
// SYNC: fetch SM → process → write Firebase
// ══════════════════════════════════════════
async function syncSurvey(surveyId, colMap, muestra, appState) {
  console.log(`→ Sincronizando encuesta ${surveyId}...`);

  const responses = await fetchAllResponses(surveyId);
  console.log(`  ${responses.length} respuestas completadas`);

  const cases = [];
  let excluded = 0, noEst = 0;

  responses.forEach(r => {
    const c = parseResponse(r, colMap, muestra);
    if (!c) { excluded++; return; }
    cases.push(c);
    if (c.estrato === '?') noEst++;
  });

  console.log(`  ✓ ${cases.length} válidos · ${excluded} excluidos · ${noEst} sin estrato`);

  // Compute quotas from muestra + N
  const n = appState.n || 2500;
  const quotas = {};
  if (muestra && muestra.distribution) {
    Object.entries(muestra.distribution).forEach(([k, pct]) => {
      quotas[k] = Math.round(pct / 100 * n);
    });
  }

  // Merge into APP state
  const newState = {
    ...appState,
    cases,
    quotas,
    nextId: cases.length + 1,
    lastSync: new Date().toISOString(),
    syncStats: { total: responses.length, valid: cases.length, excluded, noEst },
  };

  // Write to Firebase
  if (db) {
    await db.ref('pulso/v4').set(JSON.stringify(newState));
    console.log(`  ✓ Firebase actualizado`);
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
