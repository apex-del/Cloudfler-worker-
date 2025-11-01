const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1/anime/one-piece-"
const BATCH_LIMIT = 50
const DELAY = 2000 // 2 seconds delay between each fetch

async function safeRun(env, test = false) {
  try {
    const db = env.DB
    await initTables(db)

    const meta = await db.prepare("SELECT last_id FROM meta WHERE name='progress'").first()
    let startId = meta?.last_id || 1
    const endId = startId + BATCH_LIMIT - 1

    console.log(`🚀 Fetching anime ${startId} → ${endId}`)

    for (let id = startId; id <= endId; id++) {
      const ok = await fetchAndSaveAnime(db, id)
      if (ok) {
        await db.prepare(`UPDATE meta SET last_id = ? WHERE name='progress'`).bind(id).run()
      }
      await sleep(DELAY)
    }

    console.log("✅ Batch complete!")
  } catch (err) {
    console.error("❌ Worker crashed:", err)
  }
}

async function initTables(db) {
  try {
    await db.prepare(`
      CREATE TABLE IF NOT EXISTS anime (
        id INTEGER PRIMARY KEY,
        title TEXT,
        synopsis TEXT,
        image TEXT,
        rating TEXT,
        type TEXT,
        status TEXT
      )
    `).run()

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS meta (
        name TEXT PRIMARY KEY,
        last_id INTEGER DEFAULT 1
      )
    `).run()

    const check = await db.prepare("SELECT name FROM meta WHERE name='progress'").first()
    if (!check) {
      await db.prepare("INSERT INTO meta (name, last_id) VALUES ('progress', 1)").run()
      console.log("🆕 Initialized meta table.")
    }
  } catch (err) {
    console.error("⚠️ Error creating tables:", err)
  }
}

async function fetchAndSaveAnime(db, id) {
  const url = `${BASE_URL}${id}`
  try {
    const res = await fetch(url, { cf: { cacheTtl: 0 } })
    if (!res.ok) {
      console.warn(`⚠️ Skipped ${id}: HTTP ${res.status}`)
      return false
    }

    let json
    try {
      json = await res.json()
    } catch {
      console.warn(`⚠️ Invalid JSON for ${id}`)
      return false
    }

    // FIX: Correct data access - navigate through the nested structure
    const animeData = json?.data?.data
    if (!animeData?.title) {
      console.warn(`⚠️ Missing data for ${id}`)
      return false
    }

    // FIX: Use 'poster' instead of 'image' and access correct nested data
    await db.prepare(`
      INSERT OR REPLACE INTO anime (id, title, synopsis, image, rating, type, status)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).bind(
      id,
      animeData.title || "",
      animeData.synopsis || "",
      animeData.poster || "", // ← Changed from data.image to data.poster
      animeData.rating || "",
      animeData.type || "",
      animeData.status || ""
    ).run()

    console.log(`✅ Saved: ${animeData.title} (ID ${id})`)
    return true
  } catch (err) {
    console.error(`❌ Failed ${id}: ${err.message}`)
    return false
  }
}

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms))
}

export default {
  async scheduled(event, env) {
    await safeRun(env)
  },

  async fetch(request, env) {
    const url = new URL(request.url)
    if (url.pathname === "/test") {
      await safeRun(env, true)
      return new Response("✅ Test run complete", { status: 200 })
    }
    return new Response("Not found", { status: 404 })
  },
                  }
