// erenworldWorker.js
export default {
  async scheduled(event, env) {
    try {
      await mainCron(env)
    } catch (err) {
      console.error("Cron failed:", err)
    }
  }
}

// ---------- CONFIG ----------
const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1";
const BATCH_SIZE = 50;

// Paginated endpoints
const PAGINATED_ENDPOINTS = [
  "/animes/top-airing",
  "/animes/most-popular",
  "/animes/most-favorite",
  "/animes/tv",
  "/animes/ova",
  "/animes/movie"
];

// Single fetch endpoints
const SINGLE_FETCH_ENDPOINTS = [
  "/home"
];

// Sequential IDs for anime info, characters, episodes
const SEQUENTIAL_ENDPOINTS = {
  anime_info: "/anime/:id",
  characters: "/characters/:id",
  episodes: "/episodes/:id"
};

// ---------- MAIN CRON ----------
async function mainCron(env) {
  await log("system", "🟢 Cron started", env);

  // 1️⃣ Home endpoint
  await processSingleFetch("/home", env);

  // 2️⃣ Paginated endpoints
  for (const endpoint of PAGINATED_ENDPOINTS) {
    await processPaginated(endpoint, env);
  }

  // 3️⃣ Sequential endpoints
  for (const key in SEQUENTIAL_ENDPOINTS) {
    await processSequential(SEQUENTIAL_ENDPOINTS[key], key, env);
  }

  await log("system", "🔵 Cron finished", env);
}

// ---------- PROCESS SINGLE FETCH ----------
async function processSingleFetch(endpoint, env) {
  try {
    const data = await fetchApi(endpoint);
    if (!data || data.length === 0) return;

    await ensureTable(endpoint, data, env);

    for (let i = 0; i < data.length; i += BATCH_SIZE) {
      const batch = data.slice(i, i + BATCH_SIZE);
      await insertOrReplaceBatch(endpoint, batch, env);
    }

    await log(endpoint, `✅ ${data.length} records processed`, env);
  } catch (err) {
    await log(endpoint, `❌ ${err.message}`, env);
  }
}

// ---------- PROCESS PAGINATED ENDPOINT ----------
async function processPaginated(endpoint, env) {
  try {
    let lastPageObj = await env.DB.prepare(
      `SELECT last_page FROM last_fetched_pages WHERE endpoint = ?`
    ).bind(endpoint).first();
    let page = lastPageObj ? lastPageObj.last_page + 1 : 1;
    let hasNext = true;

    while (hasNext) {
      const url = `${BASE_URL}${endpoint}?page=${page}`;
      const res = await fetch(url);
      if (!res.ok) break;
      const json = await res.json();
      const data = json.data || [];
      hasNext = json.hasNextPage !== undefined ? json.hasNextPage : page < (json.totalPages || page);

      if (data.length === 0) break;

      await ensureTable(endpoint, data, env);

      if (endpoint === "/animes/top-airing") {
        for (const item of data) {
          await upsertIfChanged(endpoint, item, env);
        }
      } else {
        for (let i = 0; i < data.length; i += BATCH_SIZE) {
          const batch = data.slice(i, i + BATCH_SIZE);
          await insertOrReplaceBatch(endpoint, batch, env);
        }
      }

      // Update last fetched page
      await env.DB.prepare(
        `INSERT INTO last_fetched_pages(endpoint, last_page) VALUES (?, ?) 
        ON CONFLICT(endpoint) DO UPDATE SET last_page=excluded.last_page`
      ).bind(endpoint, page).run();

      page++;
      if (!hasNext) break;
    }

    await log(endpoint, `✅ Pagination done`, env);
  } catch (err) {
    await log(endpoint, `❌ ${err.message}`, env);
  }
}

// ---------- PROCESS SEQUENTIAL IDS ----------
async function processSequential(endpointPattern, key, env) {
  try {
    let lastIdObj = await env.DB.prepare(
      `SELECT last_id FROM last_fetched_id WHERE endpoint_key = ?`
    ).bind(key).first();
    let id = lastIdObj ? lastIdObj.last_id + 1 : 1;

    while (true) {
      const url = `${BASE_URL}${endpointPattern.replace(":id", id)}`;
      const res = await fetch(url);
      if (!res.ok) break;
      const json = await res.json();
      const data = json.data || null;
      if (!data) break;

      await ensureTable(key, [data], env);
      await insertOrReplaceBatch(key, [data], env);

      // Update last fetched id
      await env.DB.prepare(
        `INSERT INTO last_fetched_id(endpoint_key, last_id) VALUES (?, ?) 
        ON CONFLICT(endpoint_key) DO UPDATE SET last_id=excluded.last_id`
      ).bind(key, id).run();

      id++;
    }

    await log(key, `✅ Sequential fetch done`, env);
  } catch (err) {
    await log(key, `❌ ${err.message}`, env);
  }
}

// ---------- FETCH API ----------
async function fetchApi(endpoint) {
  const res = await fetch(`${BASE_URL}${endpoint}`);
  if (!res.ok) throw new Error(`Failed fetch: ${endpoint}`);
  const json = await res.json();
  return json.data || json;
}

// ---------- TABLE CREATION ----------
async function ensureTable(tableName, data, env) {
  const tableExists = await env.DB.prepare(
    `SELECT name FROM sqlite_master WHERE type='table' AND name=?`
  ).bind(tableName).first();
  if (!tableExists) {
    const columnsSet = new Set();
    data.forEach(item => Object.keys(item).forEach(k => columnsSet.add(k)));
    const columns = Array.from(columnsSet);
    let sql = `CREATE TABLE IF NOT EXISTS ${tableName} (`;
    columns.forEach(k => {
      sql += k === "id" ? "id INTEGER PRIMARY KEY, " : `${k} TEXT, `;
    });
    sql += "data_hash TEXT);";
    await env.DB.prepare(sql).run();
  }
}

// ---------- UPSERT IF CHANGED (Top Airing) ----------
async function upsertIfChanged(table, record, env) {
  const existing = await env.DB.prepare(`SELECT * FROM ${table} WHERE id=?`).bind(record.id).first();
  const recordHash = hashRecord(record);
  if (!existing) {
    await env.DB.prepare(buildInsertSQL(table, record, recordHash)).run();
  } else {
    const existingHash = existing.data_hash || "";
    if (existingHash !== recordHash) {
      await env.DB.prepare(buildUpsertSQL(table, record, recordHash)).run();
    }
  }
}

// ---------- INSERT OR REPLACE BATCH ----------
async function insertOrReplaceBatch(table, batch, env) {
  for (const record of batch) {
    await env.DB.prepare(buildUpsertSQL(table, record)).run();
  }
}

// ---------- SQL BUILDERS ----------
function buildInsertSQL(table, record, hash = "") {
  record.data_hash = hash;
  const keys = Object.keys(record);
  const values = keys.map(k => `'${sanitize(record[k])}'`).join(",");
  return `INSERT INTO ${table} (${keys.join(",")}) VALUES (${values})`;
}

function buildUpsertSQL(table, record, hash = "") {
  if (hash) record.data_hash = hash;
  const keys = Object.keys(record);
  const values = keys.map(k => `'${sanitize(record[k])}'`).join(",");
  const updates = keys.filter(k => k !== "id").map(k => `${k}=excluded.${k}`).join(", ");
  return `INSERT INTO ${table} (${keys.join(",")}) VALUES (${values}) ON CONFLICT(id) DO UPDATE SET ${updates}`;
}

// ---------- HASH FUNCTION ----------
function hashRecord(obj) {
  return btoa(JSON.stringify(obj));
}

// ---------- SANITIZE ----------
function sanitize(val) {
  if (val === null || val === undefined) return "";
  return val.toString().replace(/'/g, "''");
}

// ---------- LOGGING ----------
async function log(endpoint, message, env) {
  const timestamp = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO cron_logs(timestamp, endpoint, status) VALUES (?, ? ,?)`
  ).bind(timestamp, endpoint, message).run();
}
