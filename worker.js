// erenworld-home-worker.js

export default {
    async scheduled(event, env, ctx) {
        try {
            console.log('🟢 Home Data Worker started');
            await initSystemTables(env);
            await processHomeData(env);
            console.log('✅ Home Data Worker completed');
        } catch (err) {
            console.error('❌ Home Data Worker failed:', err);
            await log('system', `❌ Worker failed: ${err.message}`, env);
        }
    },

    async fetch(request, env) {
        // Manual trigger endpoint
        if (request.url.includes('/trigger-home-update')) {
            try {
                await processHomeData(env);
                return new Response(JSON.stringify({ 
                    success: true, 
                    message: 'Home data updated successfully' 
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

        return new Response('Home Data Worker is running');
    }
};

// ---------- CONFIG ----------
const HOME_API_URL = "https://erenworld-proxy.onrender.com/api/v1/home";
const BATCH_SIZE = 10;

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
        home_data_updates: `
            CREATE TABLE IF NOT EXISTS home_data_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                updated_at TEXT,
                records_processed INTEGER,
                success BOOLEAN
            )`
    };

    for (const [name, sql] of Object.entries(systemTables)) {
        await env.DB.prepare(sql).run();
    }

    // Create home data tables
    await createHomeDataTables(env);
    console.log("✅ System tables ensured");
}

async function createHomeDataTables(env) {
    const homeTables = {
        anime: `
            CREATE TABLE IF NOT EXISTS anime (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                alternative_title TEXT,
                poster TEXT,
                type TEXT,
                quality TEXT,
                duration TEXT,
                aired TEXT,
                synopsis TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )`,
        anime_episodes: `
            CREATE TABLE IF NOT EXISTS anime_episodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                sub_episodes INTEGER DEFAULT 0,
                dub_episodes INTEGER DEFAULT 0,
                total_episodes INTEGER DEFAULT 0,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        spotlight_anime: `
            CREATE TABLE IF NOT EXISTS spotlight_anime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                rank INTEGER,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        trending_anime: `
            CREATE TABLE IF NOT EXISTS trending_anime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                rank INTEGER,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        top_airing_anime: `
            CREATE TABLE IF NOT EXISTS top_airing_anime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        most_popular_anime: `
            CREATE TABLE IF NOT EXISTS most_popular_anime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        most_favorite_anime: `
            CREATE TABLE IF NOT EXISTS most_favorite_anime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        latest_completed_anime: `
            CREATE TABLE IF NOT EXISTS latest_completed_anime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        latest_episode_anime: `
            CREATE TABLE IF NOT EXISTS latest_episode_anime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        new_added_anime: `
            CREATE TABLE IF NOT EXISTS new_added_anime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        top_upcoming_anime: `
            CREATE TABLE IF NOT EXISTS top_upcoming_anime (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        top10_today: `
            CREATE TABLE IF NOT EXISTS top10_today (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                rank INTEGER,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        top10_week: `
            CREATE TABLE IF NOT EXISTS top10_week (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                rank INTEGER,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        top10_month: `
            CREATE TABLE IF NOT EXISTS top10_month (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anime_id TEXT,
                rank INTEGER,
                FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
            )`,
        genres: `
            CREATE TABLE IF NOT EXISTS genres (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )`
    };

    for (const [name, sql] of Object.entries(homeTables)) {
        try {
            await env.DB.prepare(sql).run();
            console.log(`✅ Table ${name} ensured`);
        } catch (error) {
            console.error(`❌ Failed to create table ${name}:`, error);
        }
    }
}

// ---------- MAIN PROCESSING ----------
async function processHomeData(env) {
    let totalRecords = 0;
    
    try {
        // Fetch home data from API
        const homeData = await fetchHomeData();
        if (!homeData) {
            throw new Error('No data received from API');
        }

        // Clear existing data
        await clearExistingData(env);

        // Process all categories
        const categories = [
            'spotlight', 'trending', 'topAiring', 'mostPopular', 
            'mostFavorite', 'latestCompleted', 'latestEpisode', 
            'newAdded', 'topUpcoming'
        ];

        for (const category of categories) {
            if (homeData[category] && Array.isArray(homeData[category])) {
                const count = await processCategory(category, homeData[category], env);
                totalRecords += count;
                await log(category, `✅ Processed ${count} items`, env);
            }
        }

        // Process Top 10 data
        if (homeData.top10) {
            const top10Count = await processTop10Data(homeData.top10, env);
            totalRecords += top10Count;
            await log('top10', `✅ Processed Top 10 data`, env);
        }

        // Process genres
        if (homeData.genres && Array.isArray(homeData.genres)) {
            await processGenres(homeData.genres, env);
            await log('genres', `✅ Processed ${homeData.genres.length} genres`, env);
        }

        // Record successful update
        await env.DB.prepare(
            `INSERT INTO home_data_updates (updated_at, records_processed, success) 
             VALUES (?, ?, ?)`
        ).bind(new Date().toISOString(), totalRecords, true).run();

        await log('system', `✅ Home data update completed: ${totalRecords} records`, env);
        return totalRecords;

    } catch (error) {
        // Record failed update
        await env.DB.prepare(
            `INSERT INTO home_data_updates (updated_at, records_processed, success) 
             VALUES (?, ?, ?)`
        ).bind(new Date().toISOString(), totalRecords, false).run();
        
        throw error;
    }
}

// ---------- DATA PROCESSING FUNCTIONS ----------
async function fetchHomeData() {
    console.log('📡 Fetching home data from API...');
    
    const response = await fetch(HOME_API_URL, {
        headers: {
            'User-Agent': 'Cloudflare-Home-Worker/1.0',
            'Accept': 'application/json'
        }
    });

    if (!response.ok) {
        throw new Error(`API request failed: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    
    // Navigate through the nested structure
    if (data && data.success && data.data && data.data.data) {
        return data.data.data;
    } else {
        throw new Error('Invalid API response structure');
    }
}

async function clearExistingData(env) {
    console.log('🧹 Clearing existing data...');
    
    const tables = [
        'spotlight_anime', 'trending_anime', 'top_airing_anime',
        'most_popular_anime', 'most_favorite_anime', 'latest_completed_anime',
        'latest_episode_anime', 'new_added_anime', 'top_upcoming_anime',
        'top10_today', 'top10_week', 'top10_month', 'anime_episodes', 'genres'
    ];

    for (const table of tables) {
        try {
            await env.DB.prepare(`DELETE FROM ${table}`).run();
        } catch (error) {
            console.warn(`Could not clear table ${table}:`, error.message);
        }
    }
}

async function processCategory(categoryName, items, env) {
    console.log(`🔄 Processing ${categoryName} with ${items.length} items...`);
    
    let processed = 0;
    
    for (let i = 0; i < items.length; i += BATCH_SIZE) {
        const batch = items.slice(i, i + BATCH_SIZE);
        
        for (const item of batch) {
            try {
                // Insert anime data
                await insertAnime(item, env);
                
                // Insert into category table
                const tableName = getTableName(categoryName);
                const rank = item.rank || (i + 1);
                
                await env.DB.prepare(
                    `INSERT INTO ${tableName} (anime_id, rank) VALUES (?, ?)`
                ).bind(item.id, rank).run();
                
                processed++;
            } catch (error) {
                console.error(`Error processing item in ${categoryName}:`, error);
            }
        }
    }
    
    return processed;
}

async function insertAnime(animeData, env) {
    const { 
        id, title, alternativeTitle, poster, type, quality, 
        duration, aired, synopsis, episodes 
    } = animeData;

    // Insert into anime table
    await env.DB.prepare(
        `INSERT INTO anime (id, title, alternative_title, poster, type, quality, duration, aired, synopsis) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) 
         ON CONFLICT(id) DO UPDATE SET 
         title = excluded.title, alternative_title = excluded.alternative_title, 
         poster = excluded.poster, type = excluded.type, quality = excluded.quality,
         duration = excluded.duration, aired = excluded.aired, synopsis = excluded.synopsis,
         updated_at = CURRENT_TIMESTAMP`
    ).bind(
        id, title, alternativeTitle, poster, type, quality, 
        duration, aired, synopsis || ''
    ).run();

    // Insert episodes data if available
    if (episodes) {
        await env.DB.prepare(
            `INSERT INTO anime_episodes (anime_id, sub_episodes, dub_episodes, total_episodes) 
             VALUES (?, ?, ?, ?) 
             ON CONFLICT(anime_id) DO UPDATE SET 
             sub_episodes = excluded.sub_episodes, dub_episodes = excluded.dub_episodes,
             total_episodes = excluded.total_episodes`
        ).bind(
            id, 
            episodes.sub || 0, 
            episodes.dub || 0, 
            episodes.eps || 0
        ).run();
    }
}

async function processTop10Data(top10Data, env) {
    let processed = 0;
    
    if (top10Data.today) {
        for (const item of top10Data.today) {
            await insertAnime(item, env);
            await env.DB.prepare(
                `INSERT INTO top10_today (anime_id, rank) VALUES (?, ?)`
            ).bind(item.id, item.rank).run();
            processed++;
        }
    }
    
    if (top10Data.week) {
        for (const item of top10Data.week) {
            await insertAnime(item, env);
            await env.DB.prepare(
                `INSERT INTO top10_week (anime_id, rank) VALUES (?, ?)`
            ).bind(item.id, item.rank).run();
            processed++;
        }
    }
    
    if (top10Data.month) {
        for (const item of top10Data.month) {
            await insertAnime(item, env);
            await env.DB.prepare(
                `INSERT INTO top10_month (anime_id, rank) VALUES (?, ?)`
            ).bind(item.id, item.rank).run();
            processed++;
        }
    }
    
    return processed;
}

async function processGenres(genres, env) {
    for (const genre of genres) {
        await env.DB.prepare(
            `INSERT INTO genres (name) VALUES (?) ON CONFLICT(name) DO NOTHING`
        ).bind(genre).run();
    }
}

// ---------- HELPER FUNCTIONS ----------
function getTableName(categoryName) {
    const tableMap = {
        'spotlight': 'spotlight_anime',
        'trending': 'trending_anime',
        'topAiring': 'top_airing_anime',
        'mostPopular': 'most_popular_anime',
        'mostFavorite': 'most_favorite_anime',
        'latestCompleted': 'latest_completed_anime',
        'latestEpisode': 'latest_episode_anime',
        'newAdded': 'new_added_anime',
        'topUpcoming': 'top_upcoming_anime'
    };
    
    return tableMap[categoryName] || categoryName;
}

async function log(endpoint, msg, env) {
    const timestamp = new Date().toISOString();
    try {
        await env.DB.prepare(
            `INSERT INTO cron_logs (timestamp, endpoint, status) VALUES (?, ?, ?)`
        ).bind(timestamp, endpoint, msg).run();
    } catch (error) {
        console.error('Failed to write log:', error);
    }
    console.log(`[${endpoint}] ${msg}`);
}
