export default {
    async scheduled(event, env, ctx) {
        try {
            console.log('🟢 Anime Worker started');
            await initTables(env);
            const result = await processAnimeBatch(env);
            console.log('✅ Anime Worker completed:', result);
        } catch (err) {
            console.error('❌ Anime Worker failed:', err);
        }
    },

    async fetch(request, env) {
        // Manual trigger
        if (request.url.includes('/trigger')) {
            try {
                await initTables(env);
                const result = await processAnimeBatch(env);
                return new Response(JSON.stringify({ 
                    success: true,
                    ...result
                }), {
                    headers: { 'Content-Type': 'application/json' }
                });
            } catch (err) {
                return new Response(JSON.stringify({ 
                    success: false, 
                    error: err.message 
                }), { 
                    status: 500
                });
            }
        }

        // Test endpoint
        if (request.url.includes('/test')) {
            const url = new URL(request.url);
            const animeId = url.searchParams.get('id') || 'one-piece-1';
            try {
                const data = await fetchAnimeData(animeId);
                return new Response(JSON.stringify({
                    success: true,
                    data: data
                }));
            } catch (err) {
                return new Response(JSON.stringify({
                    success: false,
                    error: err.message
                }), { status: 500 });
            }
        }

        // Status endpoint
        if (request.url.includes('/status')) {
            const progress = await getProgress(env);
            const count = await env.DB.prepare('SELECT COUNT(*) as count FROM anime').first();
            return new Response(JSON.stringify({
                progress,
                totalAnime: count.count
            }));
        }

        return new Response('Anime Worker - Use /trigger, /test?id=one-piece-1, /status');
    }
};

// ---------- CONFIG ----------
const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1/anime";
const BATCH_SIZE = 100; // Process 100 anime per run

// ---------- TABLES ----------
async function initTables(env) {
    // Complete anime table with all fields from your JSON
    await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS anime (
            id TEXT PRIMARY KEY,
            title TEXT,
            alternative_title TEXT,
            japanese_title TEXT,
            poster TEXT,
            rating TEXT,
            type TEXT,
            is_18_plus BOOLEAN DEFAULT FALSE,
            synopsis TEXT,
            synonyms TEXT,
            aired_from TEXT,
            aired_to TEXT,
            premiered TEXT,
            duration TEXT,
            status TEXT,
            mal_score REAL,
            studio TEXT,
            sub_episodes INTEGER DEFAULT 0,
            dub_episodes INTEGER DEFAULT 0,
            total_episodes INTEGER DEFAULT 0,
            genres TEXT,  -- Store as JSON string
            producers TEXT,  -- Store as JSON string
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    `).run();

    // Progress table
    await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS progress (
            last_id INTEGER DEFAULT 1
        )
    `).run();

    // Insert default progress if not exists
    await env.DB.prepare(`
        INSERT OR IGNORE INTO progress (last_id) VALUES (1)
    `).run();
}

// ---------- PROCESSING ----------
async function processAnimeBatch(env) {
    console.log('🟢 Starting batch processing...');
    
    const progress = await getProgress(env);
    const startId = progress.last_id + 1;
    const endId = startId + BATCH_SIZE - 1;
    
    console.log(`📦 Processing IDs: ${startId} to ${endId}`);
    
    let successCount = 0;
    let failedCount = 0;
    let lastId = progress.last_id;

    for (let currentId = startId; currentId <= endId; currentId++) {
        const animeSlug = `one-piece-${currentId}`;
        try {
            console.log(`🔄 Fetching: ${animeSlug}`);
            const animeData = await fetchAnimeData(animeSlug);
            
            if (animeData && animeData.id) {
                await insertAnime(animeData, env);
                successCount++;
                lastId = currentId;
                console.log(`✅ Added: ${animeData.title}`);
            } else {
                failedCount++;
                console.log(`❌ No data: ${animeSlug}`);
            }
            
        } catch (error) {
            failedCount++;
            console.error(`❌ Failed: ${animeSlug} -`, error.message);
        }
    }
    
    // Update progress
    if (lastId > progress.last_id) {
        await updateProgress(env, lastId);
    }
    
    const result = {
        batchProcessed: BATCH_SIZE,
        successCount,
        failedCount,
        lastProcessedId: lastId,
        nextStartId: lastId + 1
    };
    
    return result;
}

// ---------- API CALL ----------
async function fetchAnimeData(animeSlug) {
    const url = `${BASE_URL}/${animeSlug}`;
    console.log(`🔗 Fetching: ${url}`);
    
    const response = await fetch(url);
    
    if (!response.ok) {
        if (response.status === 404) {
            return null; // Anime not found - normal
        }
        throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    
    // Navigate through the nested structure
    if (data && data.success && data.data && data.data.data) {
        return data.data.data;
    }
    
    return null;
}

// ---------- DATABASE OPERATIONS ----------
async function insertAnime(animeData, env) {
    await env.DB.prepare(`
        INSERT INTO anime (
            id, title, alternative_title, japanese_title, poster, rating, type,
            is_18_plus, synopsis, synonyms, aired_from, aired_to, premiered,
            duration, status, mal_score, studio, sub_episodes, dub_episodes,
            total_episodes, genres, producers
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            title = excluded.title,
            alternative_title = excluded.alternative_title,
            japanese_title = excluded.japanese_title,
            poster = excluded.poster,
            rating = excluded.rating,
            type = excluded.type,
            is_18_plus = excluded.is_18_plus,
            synopsis = excluded.synopsis,
            synonyms = excluded.synonyms,
            aired_from = excluded.aired_from,
            aired_to = excluded.aired_to,
            premiered = excluded.premiered,
            duration = excluded.duration,
            status = excluded.status,
            mal_score = excluded.mal_score,
            studio = excluded.studio,
            sub_episodes = excluded.sub_episodes,
            dub_episodes = excluded.dub_episodes,
            total_episodes = excluded.total_episodes,
            genres = excluded.genres,
            producers = excluded.producers
    `).bind(
        animeData.id,
        animeData.title,
        animeData.alternativeTitle,
        animeData.japanese || '',
        animeData.poster || '',
        animeData.rating || '',
        animeData.type || '',
        animeData.is18Plus ? 1 : 0,
        animeData.synopsis || '',
        animeData.synonyms || '',
        animeData.aired?.from || '',
        animeData.aired?.to || '',
        animeData.premiered || '',
        animeData.duration || '',
        animeData.status || '',
        animeData.MAL_score ? parseFloat(animeData.MAL_score) : null,
        animeData.studios || '',
        animeData.episodes?.sub || 0,
        animeData.episodes?.dub || 0,
        animeData.episodes?.eps || 0,
        animeData.genres ? JSON.stringify(animeData.genres) : '[]',
        animeData.producers ? JSON.stringify(animeData.producers) : '[]'
    ).run();
}

async function getProgress(env) {
    const result = await env.DB.prepare('SELECT last_id FROM progress').first();
    return { last_id: result?.last_id || 0 };
}

async function updateProgress(env, lastId) {
    await env.DB.prepare('UPDATE progress SET last_id = ?').bind(lastId).run();
                        }
