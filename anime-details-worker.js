export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runBatch(env));
  },
  async fetch(request, env) {
    return new Response("⚙️ ErenWorld Worker is running fine!");
  },
};

const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1/anime/one-piece-";
const TOTAL_BATCH = 70;           // fetch 70 anime per cron
const CONCURRENT_REQUESTS = 7;    // 7 at a time
const BATCH_DELAY = 300;          // ms delay between batches

async function runBatch(env) {
  const db = env.DB;

  // 1️⃣ Auto-create tables
  await db.exec(`
    CREATE TABLE IF NOT EXISTS anime (
      anime_id TEXT PRIMARY KEY,
      title TEXT,
      alternativeTitle TEXT,
      japanese TEXT,
      poster TEXT,
      rating TEXT,
      type TEXT,
      is18Plus INTEGER,
      episodes_sub INTEGER,
      episodes_dub INTEGER,
      episodes_eps INTEGER,
      synopsis TEXT,
      synonyms TEXT,
      aired_from TEXT,
      aired_to TEXT,
      premiered TEXT,
      duration TEXT,
      status TEXT,
      MAL_score TEXT,
      genres TEXT,
      studios TEXT,
      producers TEXT,
      moreSeasons_json TEXT,
      related_json TEXT,
      mostPopular_json TEXT,
      recommended_json TEXT,
      raw_json TEXT
    );

    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT
    );

    INSERT OR IGNORE INTO meta (key, value) VALUES ('last_id', '0');
  `);

  // 2️⃣ Get last processed ID
  const metaRow = await db.prepare("SELECT value FROM meta WHERE key = 'last_id'").first();
  let lastId = parseInt(metaRow?.value || "0");
  let currentId = lastId + 1;

  console.log(`🚀 Starting batch from one-piece-${currentId} ...`);

  // 3️⃣ Wake-up first request
  try {
    const first = await fetch(`${BASE_URL}${currentId}`);
    await first.json(); // just to wake up
    console.log(`☕ First anime fetched to wake up render`);
  } catch (err) {
    console.error(`💥 Error waking up first anime:`, err.message || err);
  }

  // 4️⃣ Fetch 70 anime in small concurrent batches
  let remaining = TOTAL_BATCH;

  while (remaining > 0) {
    const batchCount = Math.min(CONCURRENT_REQUESTS, remaining);
    const promises = [];

    for (let i = 0; i < batchCount; i++) {
      const id = currentId + i;
      promises.push(fetchAnime(id, db));
    }

    const results = await Promise.allSettled(promises);
    results.forEach(res => {
      if (res.status === "fulfilled" && res.value) console.log(`✅ Stored anime ${res.value.anime_id}`);
    });

    currentId += batchCount;
    remaining -= batchCount;

    // small pause between batches to avoid API hot-limit
    await new Promise(r => setTimeout(r, BATCH_DELAY));
  }

  // 5️⃣ Update last processed ID
  await db.prepare("UPDATE meta SET value = ? WHERE key = 'last_id'")
    .bind(String(currentId - 1))
    .run();

  console.log(`🎯 Finished batch up to one-piece-${currentId - 1}`);
}

// Helper: fetch and save a single anime
async function fetchAnime(id, db) {
  const url = `${BASE_URL}${id}`;
  try {
    const response = await fetch(url, { timeout: 20000 });
    if (!response.ok) {
      console.warn(`❌ HTTP ${response.status} for ${url}`);
      return null;
    }

    const json = await response.json();
    const anime = json?.data?.data;
    if (!anime) {
      console.warn(`⚠️ No valid data for ${url}`);
      return null;
    }

    await db.prepare(`
      INSERT OR REPLACE INTO anime (
        anime_id, title, alternativeTitle, japanese, poster, rating, type, is18Plus,
        episodes_sub, episodes_dub, episodes_eps, synopsis, synonyms,
        aired_from, aired_to, premiered, duration, status, MAL_score,
        genres, studios, producers,
        moreSeasons_json, related_json, mostPopular_json, recommended_json, raw_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `)
    .bind(
      anime.id,
      anime.title,
      anime.alternativeTitle,
      anime.japanese,
      anime.poster,
      anime.rating,
      anime.type,
      anime.is18Plus ? 1 : 0,
      anime.episodes?.sub || 0,
      anime.episodes?.dub || 0,
      anime.episodes?.eps || 0,
      anime.synopsis || null,
      anime.synonyms || null,
      anime.aired?.from || null,
      anime.aired?.to || null,
      anime.premiered || null,
      anime.duration || null,
      anime.status || null,
      anime.MAL_score || null,
      Array.isArray(anime.genres) ? anime.genres.join(", ") : anime.genres || "",
      Array.isArray(anime.studios) ? anime.studios.join(", ") : anime.studios || "",
      Array.isArray(anime.producers) ? anime.producers.join(", ") : anime.producers || "",
      JSON.stringify(anime.moreSeasons || []),
      JSON.stringify(anime.related || []),
      JSON.stringify(anime.mostPopular || []),
      JSON.stringify(anime.recommended || []),
      JSON.stringify(anime)
    )
    .run();

    return anime;
  } catch (err) {
    console.error(`💥 Error fetching ${url}:`, err.message || err);
    return null;
  }
}
