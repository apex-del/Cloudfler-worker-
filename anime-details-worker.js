export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(main(env));
  },

  async fetch(req, env) {
    return new Response("✅ ErenWorld Anime Worker Active", { status: 200 });
  },
};

// ---- CONFIG ----
const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1/anime/one-piece-";
const TOTAL_BATCH = 70;
const CONCURRENT = 7;
const DELAY = 300;

// ---- MAIN ----
async function main(env) {
  const db = env.DB;

  // 🧱 Auto-create tables safely
  await db.batch([
    db.prepare(`
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
      )
    `),
    db.prepare(`
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
      )
    `),
    db.prepare(`INSERT OR IGNORE INTO meta (key, value) VALUES ('last_id', '0')`)
  ]);

  // 🔁 get last ID
  const meta = await db.prepare(`SELECT value FROM meta WHERE key='last_id'`).first();
  let currentId = parseInt(meta?.value || "0") + 1;

  console.log(`🚀 Starting fetch from ID: ${currentId}`);

  // 💤 wake Render
  await safeFetch(`${BASE_URL}${currentId}`);

  // 🔄 fetch in batches
  for (let done = 0; done < TOTAL_BATCH; done += CONCURRENT) {
    const promises = [];
    for (let i = 0; i < CONCURRENT; i++) {
      const id = currentId + i;
      promises.push(storeAnime(id, db));
    }
    await Promise.all(promises);
    currentId += CONCURRENT;
    await sleep(DELAY);
  }

  await db.prepare(`UPDATE meta SET value=? WHERE key='last_id'`)
    .bind(String(currentId - 1))
    .run();

  console.log(`✅ Completed till one-piece-${currentId - 1}`);
}

// ---- FUNCTION TO STORE ANIME ----
async function storeAnime(id, db) {
  const url = `${BASE_URL}${id}`;
  try {
    const res = await fetch(url);
    if (!res.ok) {
      console.warn(`❌ [${res.status}] ${url}`);
      return;
    }

    const json = await res.json();
    const anime = json?.data?.data;
    if (!anime) {
      console.warn(`⚠️ Missing data for ID: ${id}`);
      return;
    }

    // 🧩 Safe data extraction with fallbacks
    const safe = (v) => (v !== undefined && v !== null ? String(v) : "");
    const safeArr = (v) => (Array.isArray(v) ? v.join(", ") : safe(v));
    const safeJSON = (v) => JSON.stringify(v || []);

    // ✅ Insert safely with blanks for missing fields
    await db.prepare(`
      INSERT OR REPLACE INTO anime (
        anime_id, title, alternativeTitle, japanese, poster, rating, type, is18Plus,
        episodes_sub, episodes_dub, episodes_eps, synopsis, synonyms,
        aired_from, aired_to, premiered, duration, status, MAL_score,
        genres, studios, producers,
        moreSeasons_json, related_json, mostPopular_json, recommended_json, raw_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      safe(anime.id || `one-piece-${id}`),
      safe(anime.title),
      safe(anime.alternativeTitle),
      safe(anime.japanese),
      safe(anime.poster),
      safe(anime.rating),
      safe(anime.type),
      anime.is18Plus ? 1 : 0,
      anime.episodes?.sub || 0,
      anime.episodes?.dub || 0,
      anime.episodes?.eps || 0,
      safe(anime.synopsis),
      safe(anime.synonyms),
      safe(anime.aired?.from),
      safe(anime.aired?.to),
      safe(anime.premiered),
      safe(anime.duration),
      safe(anime.status),
      safe(anime.MAL_score),
      safeArr(anime.genres),
      safeArr(anime.studios),
      safeArr(anime.producers),
      safeJSON(anime.moreSeasons),
      safeJSON(anime.related),
      safeJSON(anime.mostPopular),
      safeJSON(anime.recommended),
      safeJSON(anime)
    ).run();

    console.log(`✅ Saved: one-piece-${id}`);
  } catch (err) {
    console.error(`⚠️ Error saving one-piece-${id}: ${err.message}`);
  }
}

// ---- HELPERS ----
async function safeFetch(url) {
  try {
    const res = await fetch(url);
    return await res.json();
  } catch {
    return null;
  }
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
