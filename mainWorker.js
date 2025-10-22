// erenworldWorker.js

export default {
  async scheduled(event, env) {
    try {
      await initSystemTables(env); // Auto-create base tables
      await mainCron(env);
    } catch (err) {
      console.error("Cron failed:", err);
      await log("system", `❌ Cron failed: ${err.message}`, env);
    }
  },
};

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
  "/animes/movie",
];

// Single fetch endpoints
const SINGLE_FETCH_ENDPOINTS = ["/home"];

// Sequential IDs for anime info, characters, episodes
const SEQUENTIAL_ENDPOINTS = {
  anime_info: "/anime/:id",
  characters: "/characters/:id",
  episodes: "/episodes/:id",
};

// ---------- MAIN CRON ----------
async function mainCron(env) {
  await log("system", "🟢 Cron started", env);

  // 1️⃣ Single endpoints
  for (const endpoint of SINGLE_FETCH_ENDPOINTS) {
    await processSingleFetch(endpoint, env);
  }

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

// ---------- INIT SYSTEM TABLES ----------
async function initSystemTables(env) {
  const systemTables = {
    cron_logs: `
      CREATE TABLE IF NOT EXISTS cron_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        endpoint TEXT,
        status TEXT
      );`,
    last_fetched_pages: `
      CREATE TABLE IF NOT EXISTS last_fetched_pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        endpoint TEXT UNIQUE,
        last_page INTEGER DEFAULT 1,
        updated_at TEXT
      );`,
    last_fetched_id: `
      CREATE TABLE IF NOT EXISTS last_fetched_id (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        endpoint_key TEXT UNIQUE,
        last_id INTEGER DEFAULT 1
      );`,
  };

  for (const [name, sql] of Object.entries(systemTables)) {
    await env.DB.prepare(sql).run();
  }

  console.log("✅ System tables ensured.");
}

// ---------- PROCESS SINGLE FETCH ----------
async function processSingleFetch(endpoint, env) {
  try {
    const data = await fetchApi(endpoint);
    if (!Array.isArray(data) || data.length === 0) {
      await log(endpoint, "⚠️ No valid data found", env);
      return;
    }

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

// ---------- PROCESS PAGINATED ----------
async function processPaginated(endpoint, env) {
  try {
    const row = await env.DB.prepare(
      `SELECT last_page FROM last_fetched_pages WHERE endpoint = ?`
    ).bind(endpoint).first();

    let page = row ? row.last_page + 1 : 1;
    let hasNext = true;

    while (hasNext) {
      const res = await fetch(`${BASE_URL}${endpoint}?page=${page}`);
      if (!res.ok) break;

      const json = await res.json();
      const data = Array.isArray(json.data) ? json.data : [];
      hasNext = json.hasNextPage ?? page < (json.totalPages || page);

      if (data.length === 0) break;

      await ensureTable(endpoint, data, env);

      for (let i = 0; i < data.length; i += BATCH_SIZE) {
        const batch = data.slice(i, i + BATCH_SIZE);
        await insertOrReplaceBatch(endpoint, batch, env);
      }

      await env.DB.prepare(
        `INSERT INTO last_fetched_pages(endpoint, last_page, updated_at)
         VALUES (?, ?, ?)
         ON CONFLICT(endpoint)
         DO UPDATE SET last_page=excluded.last_page, updated_at=excluded.updated_at`
      ).bind(endpoint, page, new Date().toISOString()).run();

      page++;
      if (!hasNext) break;
    }

    await log(endpoint, "✅ Pagination done", env);
  } catch (err) {
    await log(endpoint, `❌ ${err.message}`, env);
  }
}

// ---------- PROCESS SEQUENTIAL ----------
async function processSequential(pattern, key, env) {
  try {
    const row = await env.DB.prepare(
      `SELECT last_id FROM last_fetched_id WHERE endpoint_key = ?`
    ).bind(key).first();

    let id = row ? row.last_id + 1 : 1;

    while (true) {
      const res = await fetch(`${BASE_URL}${pattern.replace(":id", id)}`);
      if (!res.ok) break;

      const json = await res.json();
      const data = json.data || null;
      if (!data) break;

      await ensureTable(key, [data], env);
      await insertOrReplaceBatch(key, [data], env);

      await env.DB.prepare(
        `INSERT INTO last_fetched_id(endpoint_key, last_id)
         VALUES (?, ?)
         ON CONFLICT(endpoint_key)
         DO UPDATE SET last_id=excluded.last_id`
      ).bind(key, id).run();

      id++;
    }

    await log(key, "✅ Sequential fetch done", env);
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

// ---------- ENSURE TABLE ----------
async function ensureTable(name, data, env) {
  if (!Array.isArray(data) || data.length === 0) return;

  const exists = await env.DB.prepare(
    `SELECT name FROM sqlite_master WHERE type='table' AND name=?`
  ).bind(name).first();

  if (!exists) {
    const keys = new Set();
    data.forEach((item) => Object.keys(item).forEach((k) => keys.add(k)));
    let sql = `CREATE TABLE IF NOT EXISTS ${name} (`;

    for (const k of keys) {
      sql += k === "id" ? "id INTEGER PRIMARY KEY, " : `${k} TEXT, `;
    }
    sql += "data_hash TEXT);";
    await env.DB.prepare(sql).run();
  }
}

// ---------- UPSERT ----------
async function insertOrReplaceBatch(table, batch, env) {
  for (const record of batch) {
    const hash = hashRecord(record);
    record.data_hash = hash;

    const keys = Object.keys(record);
    const values = keys.map((k) => `'${sanitize(record[k])}'`).join(",");
    const updates = keys.filter((k) => k !== "id").map((k) => `${k}=excluded.${k}`).join(", ");

    const sql = `
      INSERT INTO ${table} (${keys.join(",")})
      VALUES (${values})
      ON CONFLICT(id) DO UPDATE SET ${updates};
    `;
    await env.DB.prepare(sql).run();
  }
}

// ---------- HELPERS ----------
function hashRecord(obj) {
  return btoa(JSON.stringify(obj));
}

function sanitize(v) {
  return v == null ? "" : v.toString().replace(/'/g, "''");
}

async function log(endpoint, msg, env) {
  const time = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO cron_logs(timestamp, endpoint, status) VALUES (?, ?, ?)`
  ).bind(time, endpoint, msg).run();
  console.log(`[${endpoint}] ${msg}`);
}
