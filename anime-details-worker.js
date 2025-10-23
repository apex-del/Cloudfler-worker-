export default {
    async scheduled(event, env, ctx) {
        try {
            console.log('🟢 Anime Details Worker started');
            await initSystemTables(env);
            await processAnimeBatch(env);
            console.log('✅ Anime Details Worker completed');
        } catch (err) {
            console.error('❌ Anime Details Worker failed:', err);
            await log('system', `❌ Worker failed: ${err.message}`, env);
        }
    },

    async fetch(request, env) {
        // Manual trigger endpoint
        if (request.url.includes('/trigger-anime-update')) {
            try {
                const result = await processAnimeBatch(env);
                return new Response(JSON.stringify({ 
                    success: true, 
                    message: 'Anime batch processed successfully',
                    ...result
                }), {
                    headers: { 'Content-Type': 'application/json' }
                });
            } catch (err) {
                return new Response(JSON.stringify({ 
                    success: false, 
                    error: err.message 
                }), { 
                    status: 500,
                    headers: { 'Content-Type': 'application/json' }
                });
            }
        }

        // Status endpoint
        if (request.url.includes('/status')) {
            try {
                const status = await getWorkerStatus(env);
                return new Response(JSON.stringify(status), {
                    headers: { 'Content-Type': 'application/json' }
                });
            } catch (err) {
                return new Response(JSON.stringify({ error: err.message }), {
                    status: 500,
                    headers: { 'Content-Type': 'application/json' }
                });
            }
        }

        return new Response('Anime Details Worker - Use /trigger-anime-update or /status');
    }
};

// ---------- CONFIG ----------
const BASE_URL = "https://erenworld-proxy.onrender.com/api/v1/anime";
const BATCH_SIZE = 100; // Process 100 anime per run
const CONCURRENT_REQUESTS = 5; // Number of parallel requests

// ---------- INIT SYSTEM TABLES ----------
async function initSystemTables(env) {
    const systemTables = {
        cron_logs: `
            CREATE TABLE IF NOT EXISTS cron_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                endpoint TEXT,
                status TEXT
            )`,
        anime_details_progress: `
            CREATE TABLE IF NOT EXISTS anime_details_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                last_processed_id INTEGER DEFAULT 0,
                total_processed INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )`,
        anime_batch_history: `
            CREATE TABLE IF NOT EXISTS anime_batch_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_start INTEGER,
                batch_end INTEGER,
                processed_count INTEGER,
                success_count INTEGER,
                failed_count INTEGER,
                executed_at TEXT DEFAULT CURRENT_TIMESTAMP
            )`
    };

    for (const [name, sql] of Object.entries(systemTables)) {
        await env.DB.prepare(sql).run();
    }

    // Create anime details table
    await createAnimeDetailsTable(env);
    console.log("✅ System tables ensured");
}

async function createAnimeDetailsTable(env) {
    const animeTableSQL = `
        CREATE TABLE IF NOT EXISTS anime_details (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
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
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )`;

    const episodesTableSQL = `
        CREATE TABLE IF NOT EXISTS anime_episodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT,
            sub_episodes INTEGER DEFAULT 0,
            dub_episodes INTEGER DEFAULT 0,
            total_episodes INTEGER DEFAULT 0,
            FOREIGN KEY (anime_id) REFERENCES anime_details(id) ON DELETE CASCADE
        )`;

    const genresTableSQL = `
        CREATE TABLE IF NOT EXISTS anime_genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT,
            genre TEXT,
            FOREIGN KEY (anime_id) REFERENCES anime_details(id) ON DELETE CASCADE
        )`;

    const producersTableSQL = `
        CREATE TABLE IF NOT EXISTS anime_producers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT,
            producer TEXT,
            FOREIGN KEY (anime_id) REFERENCES anime_details(id) ON DELETE CASCADE
        )`;

    const moreSeasonsTableSQL = `
        CREATE TABLE IF NOT EXISTS anime_more_seasons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT,
            season_title TEXT,
            season_alternative_title TEXT,
            season_id TEXT,
            season_poster TEXT,
            is_active BOOLEAN,
            FOREIGN KEY (anime_id) REFERENCES anime_details(id) ON DELETE CASCADE
        )`;

    const relatedTableSQL = `
        CREATE TABLE IF NOT EXISTS anime_related (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT,
            related_title TEXT,
            related_alternative_title TEXT,
            related_id TEXT,
            related_poster TEXT,
            related_type TEXT,
            sub_episodes INTEGER DEFAULT 0,
            dub_episodes INTEGER DEFAULT 0,
            total_episodes INTEGER DEFAULT 0,
            FOREIGN KEY (anime_id) REFERENCES anime_details(id) ON DELETE CASCADE
        )`;

    const tables = [
        animeTableSQL, episodesTableSQL, genresTableSQL, 
        producersTableSQL, moreSeasonsTableSQL, relatedTableSQL
    ];

    for (const sql of tables) {
        try {
            await env.DB.prepare(sql).run();
        } catch (error) {
            console.error('Error creating table:', error);
        }
    }
    console.log("✅ Anime details tables ensured");
}

// ---------- MAIN PROCESSING ----------
async function processAnimeBatch(env) {
    console.log('🟢 Starting anime batch processing...');
    
    // Get last processed ID
    const progress = await getProgress(env);
    const startId = progress.last_processed_id + 1;
    const endId = startId + BATCH_SIZE - 1;
    
    console.log(`📦 Processing batch: IDs ${startId} to ${endId}`);
    
    let successCount = 0;
    let failedCount = 0;
    let lastSuccessfulId = progress.last_processed_id;
    
    // Process anime IDs sequentially
    for (let currentId = startId; currentId <= endId; currentId++) {
        try {
            const animeData = await fetchAnimeData(currentId);
            if (animeData) {
                await insertAnimeData(animeData, env);
                successCount++;
                lastSuccessfulId = currentId;
                console.log(`✅ Processed anime ID ${currentId}: ${animeData.title}`);
            } else {
                failedCount++;
                console.log(`❌ No data for anime ID ${currentId}`);
            }
            
            // Small delay to avoid rate limiting
            await new Promise(resolve => setTimeout(resolve, 100));
            
        } catch (error) {
            failedCount++;
            console.error(`❌ Failed to process anime ID ${currentId}:`, error.message);
        }
    }
    
    // Update progress
    if (lastSuccessfulId > progress.last_processed_id) {
        await updateProgress(env, lastSuccessfulId, progress.total_processed + successCount);
    }
    
    // Record batch history
    await recordBatchHistory(env, startId, endId, BATCH_SIZE, successCount, failedCount);
    
    const result = {
        batchProcessed: BATCH_SIZE,
        successCount,
        failedCount,
        lastProcessedId: lastSuccessfulId,
        nextStartId: lastSuccessfulId + 1
    };
    
    await log('batch', `✅ Batch completed: ${successCount} success, ${failedCount} failed. Last ID: ${lastSuccessfulId}`, env);
    
    return result;
}

// ---------- ANIME DATA PROCESSING ----------
async function fetchAnimeData(animeId) {
    const url = `${BASE_URL}/${animeId}`;
    
    try {
        const response = await fetch(url, {
            headers: {
                'User-Agent': 'Anime-Details-Worker/1.0',
                'Accept': 'application/json'
            }
        });

        if (!response.ok) {
            if (response.status === 404) {
                console.log(`⚠️ Anime ID ${animeId} not found (404)`);
                return null;
            }
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        
        // Navigate through nested structure
        if (data && data.success && data.data && data.data.data) {
            return data.data.data;
        } else {
            console.log(`⚠️ Invalid data structure for anime ID ${animeId}`);
            return null;
        }
        
    } catch (error) {
        console.error(`❌ Fetch failed for anime ID ${animeId}:`, error.message);
        throw error;
    }
}

async function insertAnimeData(animeData, env) {
    const connection = env.DB;
    
    // Insert main anime data
    await connection.prepare(`
        INSERT INTO anime_details (
            id, title, alternative_title, japanese_title, poster, rating, type,
            is_18_plus, synopsis, synonyms, aired_from, aired_to, premiered,
            duration, status, mal_score, studio
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            updated_at = CURRENT_TIMESTAMP
    `).bind(
        animeData.id,
        animeData.title,
        animeData.alternativeTitle,
        animeData.japanese,
        animeData.poster,
        animeData.rating,
        animeData.type,
        animeData.is18Plus ? 1 : 0,
        animeData.synopsis,
        animeData.synonyms,
        animeData.aired?.from,
        animeData.aired?.to,
        animeData.premiered,
        animeData.duration,
        animeData.status,
        animeData.MAL_score ? parseFloat(animeData.MAL_score) : null,
        animeData.studios
    ).run();

    // Insert episodes
    if (animeData.episodes) {
        await connection.prepare(`
            INSERT INTO anime_episodes (anime_id, sub_episodes, dub_episodes, total_episodes)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(anime_id) DO UPDATE SET
                sub_episodes = excluded.sub_episodes,
                dub_episodes = excluded.dub_episodes,
                total_episodes = excluded.total_episodes
        `).bind(
            animeData.id,
            animeData.episodes.sub || 0,
            animeData.episodes.dub || 0,
            animeData.episodes.eps || 0
        ).run();
    }

    // Insert genres
    if (animeData.genres && Array.isArray(animeData.genres)) {
        await connection.prepare('DELETE FROM anime_genres WHERE anime_id = ?').bind(animeData.id).run();
        for (const genre of animeData.genres) {
            await connection.prepare(`
                INSERT INTO anime_genres (anime_id, genre) VALUES (?, ?)
            `).bind(animeData.id, genre).run();
        }
    }

    // Insert producers
    if (animeData.producers && Array.isArray(animeData.producers)) {
        await connection.prepare('DELETE FROM anime_producers WHERE anime_id = ?').bind(animeData.id).run();
        for (const producer of animeData.producers) {
            await connection.prepare(`
                INSERT INTO anime_producers (anime_id, producer) VALUES (?, ?)
            `).bind(animeData.id, producer).run();
        }
    }

    // Insert more seasons
    if (animeData.moreSeasons && Array.isArray(animeData.moreSeasons)) {
        await connection.prepare('DELETE FROM anime_more_seasons WHERE anime_id = ?').bind(animeData.id).run();
        for (const season of animeData.moreSeasons) {
            await connection.prepare(`
                INSERT INTO anime_more_seasons (anime_id, season_title, season_alternative_title, season_id, season_poster, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
            `).bind(
                animeData.id,
                season.title,
                season.alternativeTitle,
                season.id,
                season.poster,
                season.isActive ? 1 : 0
            ).run();
        }
    }

    // Insert related anime
    if (animeData.related && Array.isArray(animeData.related)) {
        await connection.prepare('DELETE FROM anime_related WHERE anime_id = ?').bind(animeData.id).run();
        for (const related of animeData.related) {
            await connection.prepare(`
                INSERT INTO anime_related (anime_id, related_title, related_alternative_title, related_id, related_poster, related_type, sub_episodes, dub_episodes, total_episodes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).bind(
                animeData.id,
                related.title,
                related.alternativeTitle,
                related.id,
                related.poster,
                related.type,
                related.episodes?.sub || 0,
                related.episodes?.dub || 0,
                related.episodes?.eps || 0
            ).run();
        }
    }
}

// ---------- PROGRESS TRACKING ----------
async function getProgress(env) {
    const result = await env.DB.prepare(`
        SELECT last_processed_id, total_processed 
        FROM anime_details_progress 
        WHERE id = 1
    `).first();

    return result || { last_processed_id: 0, total_processed: 0 };
}

async function updateProgress(env, lastId, totalProcessed) {
    await env.DB.prepare(`
        INSERT INTO anime_details_progress (id, last_processed_id, total_processed, updated_at)
        VALUES (1, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            last_processed_id = excluded.last_processed_id,
            total_processed = excluded.total_processed,
            updated_at = excluded.updated_at
    `).bind(lastId, totalProcessed).run();
}

async function recordBatchHistory(env, startId, endId, processed, success, failed) {
    await env.DB.prepare(`
        INSERT INTO anime_batch_history (batch_start, batch_end, processed_count, success_count, failed_count)
        VALUES (?, ?, ?, ?, ?)
    `).bind(startId, endId, processed, success, failed).run();
}

async function getWorkerStatus(env) {
    const progress = await getProgress(env);
    const latestBatch = await env.DB.prepare(`
        SELECT * FROM anime_batch_history 
        ORDER BY executed_at DESC 
        LIMIT 1
    `).first();

    const totalAnime = await env.DB.prepare(`
        SELECT COUNT(*) as count FROM anime_details
    `).first();

    return {
        progress: {
            lastProcessedId: progress.last_processed_id,
            totalProcessed: progress.total_processed,
            nextStartId: progress.last_processed_id + 1
        },
        latestBatch,
        statistics: {
            totalAnime: totalAnime?.count || 0
        },
        config: {
            batchSize: BATCH_SIZE,
            baseUrl: BASE_URL
        }
    };
}

// ---------- HELPER FUNCTIONS ----------
async function log(endpoint, msg, env) {
    const timestamp = new Date().toISOString();
    try {
        await env.DB.prepare(`
            INSERT INTO cron_logs (timestamp, endpoint, status) VALUES (?, ?, ?)
        `).bind(timestamp, endpoint, msg).run();
    } catch (error) {
        console.error('Failed to write log:', error);
    }
    console.log(`[${endpoint}] ${msg}`);
}
