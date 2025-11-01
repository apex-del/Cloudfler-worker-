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
    if (url.pathname === "/status") {
      return await getStatus(env)
    }
    if (url.pathname === "/reset") {
      return await resetDatabase(env)
    }
    return new Response("Not found", { status: 404 })
  },
}

const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1/anime/one-piece-"
const BATCH_LIMIT = 50
const DELAY = 2000

async function safeRun(env, test = false) {
  console.log(`🚀 Starting anime scraper (test mode: ${test})`)
  
  try {
    const db = env.DB
    if (!db) {
      throw new Error("Database not available - check DB binding")
    }

    const dbReady = await initTables(db)
    if (!dbReady) {
      throw new Error("Failed to initialize database")
    }

    const meta = await db.prepare("SELECT last_id FROM meta WHERE name = 'progress'").first()
    let startId = meta?.last_id || 1
    
    if (isNaN(startId)) {
      console.warn("⚠️ Invalid last_id found, resetting to 1")
      startId = 1
      await db.prepare("UPDATE meta SET last_id = 1 WHERE name = 'progress'").run()
    }

    const endId = startId + BATCH_LIMIT - 1
    console.log(`📊 Progress: Fetching anime ${startId} → ${endId}`)

    let successCount = 0
    let failCount = 0
    const failedIds = []

    for (let id = startId; id <= endId; id++) {
      try {
        const ok = await fetchAndSaveAnime(db, id)
        if (ok) {
          successCount++
          await db.prepare(`UPDATE meta SET last_id = ? WHERE name = 'progress'`).bind(id).run()
        } else {
          failCount++
          failedIds.push(id)
        }
      } catch (err) {
        console.error(`❌ Error processing ID ${id}:`, err.message)
        failCount++
        failedIds.push(id)
      }

      if (!test || id % 5 === 0) {
        await sleep(DELAY)
      }
    }

    console.log(`✅ Batch complete! Success: ${successCount}, Failed: ${failCount}`)
    
    if (failedIds.length > 0) {
      console.log(`❌ Failed IDs: ${failedIds.join(', ')}`)
    }

    return {
      success: true,
      processed: successCount + failCount,
      successCount,
      failCount,
      failedIds,
      nextStartId: endId + 1
    }

  } catch (err) {
    console.error("❌ Worker crashed:", err)
    return {
      success: false,
      error: err.message
    }
  }
}

async function initTables(db) {
  let retryCount = 0
  const maxRetries = 3

  while (retryCount < maxRetries) {
    try {
      console.log("🔄 Initializing database tables...")
      
      // Main anime table with EXACT field mapping
      const animeTable = await db.prepare(`
        CREATE TABLE IF NOT EXISTS anime (
          -- Basic Info
          id INTEGER PRIMARY KEY,
          api_id TEXT UNIQUE,
          title TEXT NOT NULL,
          alternative_title TEXT,
          japanese_title TEXT,
          
          -- Media Info
          poster_url TEXT,
          rating TEXT,
          type TEXT,
          is18Plus BOOLEAN DEFAULT FALSE,
          duration TEXT,
          status TEXT,
          MAL_score REAL,
          
          -- Episode Info
          episodes_sub INTEGER,
          episodes_dub INTEGER,
          episodes_total INTEGER,
          
          -- Content Info
          synopsis TEXT,
          synonyms TEXT,
          genres TEXT,
          
          -- Release Info
          aired_from TEXT,
          aired_to TEXT,
          premiered TEXT,
          
          -- Production Info
          studios TEXT,
          producers TEXT,
          
          -- Additional API Fields
          moreSeasons TEXT,
          related TEXT,
          mostPopular TEXT,
          recommended TEXT,
          
          -- Timestamps
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `).run()

      if (!animeTable.success) {
        throw new Error("Failed to create anime table")
      }

      // Create meta table
      const metaTable = await db.prepare(`
        CREATE TABLE IF NOT EXISTS meta (
          name TEXT PRIMARY KEY,
          last_id INTEGER DEFAULT 1,
          updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `).run()

      if (!metaTable.success) {
        throw new Error("Failed to create meta table")
      }

      // Initialize progress record
      const check = await db.prepare("SELECT name FROM meta WHERE name = 'progress'").first()
      if (!check) {
        const insert = await db.prepare("INSERT INTO meta (name, last_id) VALUES ('progress', 1)").run()
        if (!insert.success) {
          throw new Error("Failed to insert progress record")
        }
        console.log("🆕 Created initial progress record")
      }
      
      console.log("✅ Database tables initialized successfully")
      return true

    } catch (err) {
      retryCount++
      console.error(`⚠️ Database init attempt ${retryCount} failed:`, err.message)
      
      if (retryCount >= maxRetries) {
        console.error("❌ All database initialization attempts failed")
        return false
      }
      
      await sleep(1000 * retryCount)
    }
  }
}

async function fetchAndSaveAnime(db, id) {
  const url = `${BASE_URL}${id}`
  console.log(`🔍 Fetching anime ID: ${id}`)

  try {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 15000)
    
    const res = await fetch(url, { 
      cf: { cacheTtl: 0 },
      signal: controller.signal
    })
    
    clearTimeout(timeout)

    if (!res.ok) {
      if (res.status === 404) {
        console.log(`➖ Skipped ${id}: Not found (404)`)
      } else {
        console.warn(`⚠️ Skipped ${id}: HTTP ${res.status} - ${res.statusText}`)
      }
      return false
    }

    let json
    try {
      json = await res.json()
    } catch (parseError) {
      console.warn(`⚠️ Invalid JSON for ${id}:`, parseError.message)
      return false
    }

    // Navigate nested data structure
    const animeData = json?.data?.data
    if (!animeData) {
      console.warn(`⚠️ Missing data structure for ${id}`)
      return false
    }

    if (!animeData.title) {
      console.warn(`⚠️ Missing title for ${id}`)
      return false
    }

    // Extract nested data safely
    const episodes = animeData.episodes || {}
    const aired = animeData.aired || {}
    
    // Save COMPLETE data to database with EXACT field mapping
    const result = await db.prepare(`
      INSERT OR REPLACE INTO anime (
        id, api_id, title, alternative_title, japanese_title, poster_url, rating, 
        type, is18Plus, duration, status, MAL_score, episodes_sub, episodes_dub, 
        episodes_total, synopsis, synonyms, genres, aired_from, aired_to, premiered, 
        studios, producers, moreSeasons, related, mostPopular, recommended
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      id,                                // Your numeric primary key
      animeData.id || "",                // API's string ID
      animeData.title || "",
      animeData.alternativeTitle || "",
      animeData.japanese || "",
      animeData.poster || "",
      animeData.rating || "",
      animeData.type || "",
      animeData.is18Plus ? 1 : 0,
      animeData.duration || "",
      animeData.status || "",
      parseFloat(animeData.MAL_score) || 0,
      episodes.sub || 0,
      episodes.dub || 0,
      episodes.eps || 0,
      animeData.synopsis || "",
      animeData.synonyms || "",
      JSON.stringify(animeData.genres || []),
      aired.from || "",
      aired.to || "",
      animeData.premiered || "",
      animeData.studios || "",
      JSON.stringify(animeData.producers || []),
      JSON.stringify(animeData.moreSeasons || []),
      JSON.stringify(animeData.related || []),
      JSON.stringify(animeData.mostPopular || []),
      JSON.stringify(animeData.recommended || [])
    ).run()

    if (!result.success) {
      console.error(`❌ Database save failed for ${id}:`, result.error)
      return false
    }

    // Log some stats about the saved data
    const moreSeasonsCount = animeData.moreSeasons?.length || 0
    const relatedCount = animeData.related?.length || 0
    const popularCount = animeData.mostPopular?.length || 0
    const recommendedCount = animeData.recommended?.length || 0
    
    console.log(`✅ Saved: ${animeData.title} (ID ${id}, MAL: ${animeData.MAL_score || 'N/A'})`)
    console.log(`   📊 Stats: ${moreSeasonsCount} seasons, ${relatedCount} related, ${popularCount} popular, ${recommendedCount} recommended`)

    return true

  } catch (err) {
    if (err.name === 'AbortError') {
      console.warn(`⏰ Timeout fetching ${id}`)
    } else {
      console.error(`❌ Failed ${id}:`, err.message)
    }
    return false
  }
}

async function getStatus(env) {
  try {
    const db = env.DB
    const animeCount = await db.prepare("SELECT COUNT(*) as count FROM anime").first()
    const progress = await db.prepare("SELECT last_id FROM meta WHERE name = 'progress'").first()
    const stats = await db.prepare(`
      SELECT 
        COUNT(*) as total,
        AVG(MAL_score) as avg_score,
        SUM(episodes_total) as total_episodes,
        COUNT(DISTINCT studios) as unique_studios
      FROM anime 
      WHERE MAL_score > 0
    `).first()
    
    const status = {
      animeCount: animeCount?.count || 0,
      lastProcessedId: progress?.last_id || 0,
      nextBatchStart: (progress?.last_id || 0) + 1,
      batchSize: BATCH_LIMIT,
      statistics: {
        averageScore: stats?.avg_score ? parseFloat(stats.avg_score).toFixed(2) : 0,
        totalEpisodes: stats?.total_episodes || 0,
        uniqueStudios: stats?.unique_studios || 0
      }
    }
    
    return new Response(JSON.stringify(status, null, 2), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    })
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    })
  }
}

async function resetDatabase(env) {
  try {
    const db = env.DB
    await db.prepare("DELETE FROM anime").run()
    await db.prepare("UPDATE meta SET last_id = 1 WHERE name = 'progress'").run()
    
    return new Response(JSON.stringify({ 
      message: "Database reset successfully",
      animeCount: 0,
      lastProcessedId: 1
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    })
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    })
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
