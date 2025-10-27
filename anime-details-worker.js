export default {
  async scheduled(event, env) {
    await handleCron(env)
  },
  async fetch(request, env) {
    const url = new URL(request.url)
    if (url.pathname === "/test") {
      await ensureTables(env)
      await handleCron(env, true)
      return new Response("✅ Test run complete", { status: 200 })
    }
    return new Response("Not found", { status: 404 })
  },
}

const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1/anime/one-piece-"
const MAX_PER_CRON = 70
const BATCH_SIZE = 10
const WAIT_BETWEEN_BATCHES = 2500 // 2.5s between batches

// ========== MAIN CRON HANDLER ==========
async function handleCron(env, testMode = false) {
  const db = env.DB
  await ensureTables(env)

  // get last processed id
  let { results } = await db.prepare("SELECT last_id FROM meta WHERE name = 'progress'").first()
  let startId = results ? results : 1
  let endId = startId + MAX_PER_CRON - 1

  console.log(`🚀 Starting from ID ${startId} → ${endId}`)

  for (let i = startId; i <= endId; i += BATCH_SIZE) {
    const batch = Array.from({ length: Math.min(BATCH_SIZE, endId - i + 1) }, (_, k) => i + k)
    console.log(`📦 Fetching batch ${batch.join(", ")}`)

    const results = []
    for (const id of batch) {
      const url = `${BASE_URL}${id}`
      try {
        const res = await fetch(url)
        if (!res.ok) throw new Error(`❌ ${res.status}`)
        const json = await res.json()
        const data = json.data || {}
        await db.prepare(`
          INSERT INTO anime (id, title, synopsis, image, rating, type, status)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `).bind(
          id,
          data.title || "",
          data.synopsis || "",
          data.image || "",
          data.rating || "",
          data.type || "",
          data.status || ""
        ).run()

        console.log(`✅ Saved ID ${id}`)
      } catch (err) {
        console.error(`⚠️ Error for ID ${id}: ${err.message}`)
      }
    }

    await db.prepare(`UPDATE meta SET last_id = ? WHERE name = 'progress'`).bind(batch.at(-1)).run()

    console.log(`🕒 Waiting ${WAIT_BETWEEN_BATCHES / 1000}s before next batch...`)
    await new Promise(r => setTimeout(r, WAIT_BETWEEN_BATCHES))
  }

  console.log("✅ Completed batch run.")
  if (testMode) return new Response("Done")
}

// ========== CREATE TABLES IF NOT EXIST ==========
async function ensureTables(env) {
  const db = env.DB

  await db.exec(`
    CREATE TABLE IF NOT EXISTS anime (
      id INTEGER PRIMARY KEY,
      title TEXT,
      synopsis TEXT,
      image TEXT,
      rating TEXT,
      type TEXT,
      status TEXT
    );
  `)

  await db.exec(`
    CREATE TABLE IF NOT EXISTS meta (
      name TEXT PRIMARY KEY,
      last_id INTEGER DEFAULT 1
    );
  `)

  // initialize meta
  const check = await db.prepare("SELECT * FROM meta WHERE name = 'progress'").first()
  if (!check) {
    await db.prepare("INSERT INTO meta (name, last_id) VALUES ('progress', 1)").run()
  }
}
