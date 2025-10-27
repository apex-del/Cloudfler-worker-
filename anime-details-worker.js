export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(main(env));
  },
  async fetch(req, env) {
    return new Response("✅ ErenWorld Worker running");
  },
};

const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1/anime/one-piece-";
const TOTAL_BATCH = 70;
const CONCURRENT = 7;
const DELAY = 300;

async function main(env) {
  const db = env.DB;

  // ✅ Auto-create tables (safe batch)
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

  // ✅ Get last id
  const meta = await db.prepare(`SELECT value FROM meta WHERE key='last_id'`).first();
  let currentId = parseInt(meta?.value || "0") + 1;

  console.log(`🚀 Starting from ${currentId}`);

  // 🔥 Wake Render
  await safeFetch(`${BASE_URL}${currentId}`);

  // ✅ Fetch batches safely
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

  console.log(`✅ Done till ${currentId - 1}`);
}

async function storeAnime(id, db) {
  const url = `${BASE_URL}${id}`;
  try {
    const res = await fetch(url);
    if (!res.ok) {
      console.warn(`❌ ${res.status} for ${url}`);
      return;
    }
    const json = await res.json();
    const anime = json?.data?.data;
    if (!anime) return;

    await db.prepare(`
      INSERT OR REPLACE INTO anime (
        anime_id, title, alternativeTitle, japanese, poster, rating, type, is18Plus,
        episodes_sub, episodes_dub, episodes_eps, synopsis, synonyms,
        aired_from, aired_to, premiered, duration, status, MAL_score,
        genres, studios, producers,
        moreSeasons_json, related_json, mostPopular_json, recommended_json, raw_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
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
      anime.synopsis || "",
      anime.synonyms || "",
      anime.aired?.from || "",
      anime.aired?.to || "",
      anime.premiered || "",
      anime.duration || "",
      anime.status || "",
      anime.MAL_score || "",
      Array.isArray(anime.genres) ? anime.genres.join(",") : anime.genres || "",
      Array.isArray(anime.studios) ? anime.studios.join(",") : anime.studios || "",
      Array.isArray(anime.producers) ? anime.producers.join(",") : anime.producers || "",
      JSON.stringify(anime.moreSeasons || []),
      JSON.stringify(anime.related || []),
      JSON.stringify(anime.mostPopular || []),
      JSON.stringify(anime.recommended || []),
      JSON.stringify(anime)
    ).run();

    console.log(`✅ Saved one-piece-${id}`);
  } catch (e) {
    console.error(`⚠️ ${url}: ${e.message}`);
  }
}

async function safeFetch(url) {
  try {
    const res = await fetch(url);
    return await res.json();
  } catch (_) {
    return null;
  }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
                                }
