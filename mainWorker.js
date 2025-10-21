// mainWorker.js
export default {
  async scheduled(event, env) {
    try {
      await runMainCron(env)
    } catch (err) {
      console.error("Cron failed:", err)
    }
  }
}

// ---------- CONFIG ----------
const ENDPOINTS = [
  { name: "trending", url: "/api/v1/trending", mode: "replace_all" },
  { name: "upcoming", url: "/api/v1/upcoming", mode: "replace_all" },
  { name: "latest", url: "/api/v1/latest", mode: "replace_all" },
  { name: "popular", url: "/api/v1/popular", mode: "replace_all" },
  { name: "anime_info", url: "/api/v1/anime/all", mode: "insert_once" },
  { name: "episodes", url: "/api/v1/episodes/all", mode: "insert_once" }
]

const BATCH_SIZE = 50

// ---------- MAIN CRON ----------
async function runMainCron(env) {
  await log("system", "🟢", "Cron started", env)

  for (const endpoint of ENDPOINTS) {
    await processEndpoint(endpoint, env)
  }

  await cleanOldLogs(env, 30)
  await log("system", "🔵", "Cron finished", env)
}

// ---------- PROCESS ENDPOINT ----------
async function processEndpoint(endpoint, env) {
  try {
    const data = await fetchApi(endpoint.url)
    if (!data || data.length === 0) {
      await log(endpoint.name, "⚠️", "Empty response", env)
      return
    }

    await ensureTableExists(endpoint.name, data, env)

    // Smart batching: only batch if more than BATCH_SIZE
    for (let i = 0; i < data.length; i += BATCH_SIZE) {
      const batch = data.slice(i, i + BATCH_SIZE)
      if (endpoint.mode === "replace_all") {
        await replaceAllBatch(endpoint.name, batch, env)
      } else if (endpoint.mode === "insert_once") {
        await insertIfNewBatch(endpoint.name, batch, env)
      }
    }

    await log(endpoint.name, "✅", `${data.length} processed in batches`, env)
  } catch (err) {
    await log(endpoint.name, "❌", err.message, env)
  }
}

// ---------- FETCH API ----------
async function fetchApi(endpointUrl) {
  const baseUrl = "https://erenworld-proxy.example.com"
  const res = await fetch(baseUrl + endpointUrl)
  if (!res.ok) throw new Error("API failed: " + endpointUrl)
  const json = await res.json()
  return json.data || json
}

// ---------- ENSURE TABLE EXISTS ----------
async function ensureTableExists(tableName, data, env) {
  const tableExists = await env.DB.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).bind(tableName).first()
  if (!tableExists) {
    // Collect all unique keys from data
    const columnsSet = new Set()
    data.forEach(item => Object.keys(item).forEach(k => columnsSet.add(k)))
    const columns = Array.from(columnsSet)

    // Build CREATE TABLE SQL
    let sql = `CREATE TABLE IF NOT EXISTS ${tableName} (`
    columns.forEach(k => {
      if (k === "id") sql += `id INTEGER PRIMARY KEY, `
      else sql += `${k} TEXT, `
    })
    sql = sql.replace(/, $/, "") + ");"

    await env.DB.prepare(sql).run()
    // Optional: save structure in __meta_tables
    const now = new Date().toISOString()
    await env.DB.prepare(`INSERT OR REPLACE INTO __meta_tables (name, columns, created_at) VALUES (?, ?, ?)`)
      .bind(tableName, columns.join(","), now).run()
  }
}

// ---------- BATCH INSERT / UPSERT ----------
async function replaceAllBatch(table, batch, env) {
  for (const record of batch) {
    await env.DB.prepare(buildUpsertSQL(table, record)).run()
  }
}

async function insertIfNewBatch(table, batch, env) {
  for (const record of batch) {
    const exists = await env.DB.prepare(`SELECT id FROM ${table} WHERE id=?`).bind(record.id).first()
    if (!exists) await env.DB.prepare(buildInsertSQL(table, record)).run()
  }
}

// ---------- SQL BUILDERS ----------
function buildInsertSQL(table, record) {
  const keys = Object.keys(record)
  const values = keys.map(k => `'${sanitize(record[k])}'`).join(",")
  return `INSERT INTO ${table} (${keys.join(",")}) VALUES (${values})`
}

function buildUpsertSQL(table, record) {
  const keys = Object.keys(record)
  const values = keys.map(k => `'${sanitize(record[k])}'`).join(",")
  const updates = keys.filter(k => k !== "id").map(k => `${k}=excluded.${k}`).join(", ")
  return `INSERT INTO ${table} (${keys.join(",")}) VALUES (${values}) ON CONFLICT(id) DO UPDATE SET ${updates}`
}

function sanitize(val) {
  if (val === null || val === undefined) return ""
  return val.toString().replace(/'/g, "''")
}

// ---------- LOGGING ----------
async function log(endpoint, status, message, env) {
  const timestamp = new Date().toISOString()
  await env.DB.prepare(`INSERT INTO cron_logs (timestamp, endpoint, status, message) VALUES (?, ?, ?, ?)`)
    .bind(timestamp, endpoint, status, message).run()
}

// ---------- CLEAN OLD LOGS ----------
async function cleanOldLogs(env, days) {
  await env.DB.prepare(`DELETE FROM cron_logs WHERE timestamp < datetime('now', ?)`).bind(`-${days} days`).run()
}
