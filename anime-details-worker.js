export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runBatch(env));
  },

  async fetch(request, env) {
    return new Response("⚙️ ErenWorld Worker is running fine!");
  },
};

const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1/anime/one-piece-";
const BATCH_SIZE = 100;

async function runBatch(env) {
  const db = env.DB;

  // 🧱 Auto-create tables if not exist
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

  // 🧠 Get last stored ID
  const metaRow = await db.prepare("SELECT value FROM meta WHERE key = 'last_id'").first();
  let lastId = parseInt(metaRow?.value?.replace('one-piece-', '') || "0");
  let startId = lastId + 1;
  let endId = startId + BATCH_SIZE - 1;

  console.log(`🚀 Starting batch: one-piece-${startId} → one-piece-${endId}`);

  for (let id = startId; id <= endId; id++) {
    const anime_id = `one-piece-${id}`;
    const url = `${BASE_URL}${id}`;
    console.log(`🌐 Fetching: ${url}`);

    try {
      const response = await fetch(url, { timeout: 20000 });
      if (!response.ok) {
        console.warn(`❌ HTTP ${response.status} for ${url}`);
        continue;
      }

      const json = await response.json();
      const anime = json?.data?.data;

      if (!anime) {
        console.warn(`⚠️ No valid data for ${url}`);
        continue;
      }

      // 📝 Insert into D1
      await db.prepare(`
        INSERT OR REPLACE INTO anime (
          anime_id, title, alternativeTitle, japanese, poster, rating, type, is18Plus,
          episodes_sub, episodes_dub, episodes_eps, synopsis, synonyms,
          aired_from, aired_to, premiered, duration, status, MAL_score,
          genres, studios, producers,
          moreSeasons_json, related_json, mostPopular_json, recommended_json, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `)
      .bind(
        anime.id || anime_id,
        anime.title || "",
        anime.alternativeTitle || "",
        anime.japanese || "",
        anime.poster || "",
        anime.rating || "",
        anime.type || "",
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

      console.log(`✅ Saved: ${anime.title || anime_id}`);

      // 🔄 Update last_id only after successful insert
      await db.prepare("UPDATE meta SET value = ? WHERE key = 'last_id'")
        .bind(anime_id)
        .run();

    } catch (err) {
      console.error(`💥 Error fetching ${url}:`, err.message || err);
    }
  }

  console.log(`🎯 Finished batch up to one-piece-${endId}`);
}
