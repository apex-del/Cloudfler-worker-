export default {
  async scheduled(event, env) {
    try {
      await initTables(env);
      await fetchNextBatch(env);
    } catch (err) {
      console.error("❌ Cron failed:", err);
    }
  },
};

// ================= CONFIG =================
const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1";
const BATCH_SIZE = 10;  // fetch 10 at a time (safe)
const RANGE_SIZE = 70;  // process 70 per cron (increase later if stable)

// ================= MAIN =================
async function fetchNextBatch(env) {
  // get last inserted id
  const { results } = await env.DB.prepare("SELECT MAX(id) AS lastId FROM anime").all();
  const lastId = results?.[0]?.lastId || 0;
  const start = lastId + 1;
  const end = start + RANGE_SIZE - 1;

  console.log(`🚀 Fetching range ${start} → ${end}`);

  for (let batchStart = start; batchStart <= end; batchStart += BATCH_SIZE) {
    const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, end);
    const ids = Array.from({ length: batchEnd - batchStart + 1 }, (_, i) => batchStart + i);

    console.log(`📦 Fetching IDs ${batchStart} → ${batchEnd}`);

    await Promise.all(
      ids.map(async (id) => {
        try {
          const res = await fetch(`${BASE_URL}/anime/${id}`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);

          const json = await res.json();
          const a = json?.data || {};

          const safe = {
            id,
            title: a.title || "",
            altTitle: a.alternativeTitle || "",
            japanese: a.japanese || "",
            synopsis: a.synopsis || "",
            image: a.image || "",
            status: a.status || "",
            totalEpisodes: a.totalEpisodes || "",
            type: a.type || "",
            releaseDate: a.releaseDate || "",
            subOrDub: a.subOrDub || "",
            url: a.url || "",
          };

          await saveAnime(env, safe);
          console.log(`✅ Saved Anime ID ${id}`);
        } catch (err) {
          console.warn(`⚠️ Skipped ID ${id}: ${err.message}`);
        }
      })
    );

    // prevent rate limit
    await sleep(2000);
  }

  console.log(`🏁 Finished range ${start} → ${end}`);
}

// ================= DB =================
async function initTables(env) {
  const createAnimeTable = `
    CREATE TABLE IF NOT EXISTS anime (
      id INTEGER PRIMARY KEY,
      title TEXT,
      altTitle TEXT,
      japanese TEXT,
      synopsis TEXT,
      image TEXT,
      status TEXT,
      totalEpisodes TEXT,
      type TEXT,
      releaseDate TEXT,
      subOrDub TEXT,
      url TEXT
    );
  `;
  await env.DB.exec(createAnimeTable);
  console.log("✅ Anime table ready");
}

async function saveAnime(env, a) {
  const stmt = `
    INSERT OR REPLACE INTO anime 
    (id, title, altTitle, japanese, synopsis, image, status, totalEpisodes, type, releaseDate, subOrDub, url)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;
  await env.DB.prepare(stmt)
    .bind(
      a.id,
      a.title,
      a.altTitle,
      a.japanese,
      a.synopsis,
      a.image,
      a.status,
      a.totalEpisodes,
      a.type,
      a.releaseDate,
      a.subOrDub,
      a.url
    )
    .run();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
