const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ V18-STATE-SERVER: Socket.IO with stable WebSocket ★★★
// ═══════════════════════════════════════════════════════════════════════════
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ['polling', 'websocket'],
    allowUpgrades: true,
    upgradeTimeout: 30000,
    pingTimeout: 120000,
    pingInterval: 25000,
    maxHttpBufferSize: 5e6,
    connectTimeout: 60000,
    allowEIO3: true
});

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ PostgreSQL Setup ★★★
// ═══════════════════════════════════════════════════════════════════════════
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 15,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 15000,
});

pool.on('error', (err) => {
    console.error('⚠️ PostgreSQL pool error:', err.message);
});

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ V18-STATE-SERVER: Full Database Schema ★★★
// ═══════════════════════════════════════════════════════════════════════════

async function initDatabase() {
    const client = await pool.connect();
    try {
        // ──────────────────────────────────────────────────────────────
        // 1. EXISTING TABLES (نحافظ عليها كما هي)
        // ──────────────────────────────────────────────────────────────
        await client.query(`
            CREATE TABLE IF NOT EXISTS message_buffer (
                id SERIAL PRIMARY KEY,
                seq INTEGER UNIQUE NOT NULL,
                uuid VARCHAR(64) UNIQUE NOT NULL,
                type VARCHAR(100) NOT NULL,
                data JSONB NOT NULL,
                hash VARCHAR(64) NOT NULL,
                sender VARCHAR(100),
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                acked_by TEXT[] DEFAULT '{}',
                ack_status VARCHAR(20) DEFAULT 'pending'
            )
        `);

        await client.query(`
            CREATE TABLE IF NOT EXISTS machine_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                status VARCHAR(50) DEFAULT 'UNKNOWN',
                speed REAL DEFAULT 0,
                last_data JSONB DEFAULT '{}',
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        `);

        await client.query(`
            CREATE TABLE IF NOT EXISTS sequence_counter (
                id INTEGER PRIMARY KEY DEFAULT 1,
                current_seq INTEGER DEFAULT 0
            )
        `);

        await client.query(`
            CREATE TABLE IF NOT EXISTS dead_letter_queue (
                id SERIAL PRIMARY KEY,
                uuid VARCHAR(64) UNIQUE NOT NULL,
                type VARCHAR(100) NOT NULL,
                data JSONB NOT NULL,
                sender VARCHAR(100),
                failure_reason TEXT,
                retry_count INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        `);

        // ──────────────────────────────────────────────────────────────
        // 2. ★ NEW STATE TABLES ★
        // ──────────────────────────────────────────────────────────────

        // 2.1 Ficha State Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS ficha_state (
                ficha_id INTEGER PRIMARY KEY,
                tailor_name VARCHAR(200) NOT NULL,
                clothes_type_name VARCHAR(200) NOT NULL,
                total_quantity INTEGER NOT NULL DEFAULT 0,
                created_date DATE NOT NULL,
                last_updated TIMESTAMPTZ DEFAULT NOW(),
                is_deleted BOOLEAN DEFAULT FALSE,
                last_seq INTEGER,
                last_uuid VARCHAR(64)
            )
        `);

        // 2.2 Production State Table (each line = size+color for a ficha)
        await client.query(`
            CREATE TABLE IF NOT EXISTS production_state (
                id SERIAL PRIMARY KEY,
                ficha_id INTEGER NOT NULL,
                size TEXT NOT NULL,
                color TEXT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 0,
                notes TEXT DEFAULT '',
                line_uuid VARCHAR(64) UNIQUE NOT NULL,
                date DATE NOT NULL,
                is_deleted BOOLEAN DEFAULT FALSE,
                last_updated TIMESTAMPTZ DEFAULT NOW(),
                last_seq INTEGER,
                UNIQUE(ficha_id, size, color, date)
            )
        `);

        // 2.3 Daily Basket Summary Table (aggregated by date, tailor, clothes_type)
        await client.query(`
            CREATE TABLE IF NOT EXISTS daily_basket_summary (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                tailor_name VARCHAR(200) NOT NULL,
                clothes_type_name VARCHAR(200) NOT NULL,
                total_quantity INTEGER NOT NULL DEFAULT 0,
                inspector_name VARCHAR(200) DEFAULT '',
                batch_quantity VARCHAR(100) DEFAULT '',
                defect_count VARCHAR(100) DEFAULT '',
                last_updated TIMESTAMPTZ DEFAULT NOW(),
                last_seq INTEGER,
                UNIQUE(date, tailor_name, clothes_type_name)
            )
        `);

        // ──────────────────────────────────────────────────────────────
        // 3. Indexes for State Tables
        // ──────────────────────────────────────────────────────────────
        
        await client.query(`CREATE INDEX IF NOT EXISTS idx_ficha_state_tailor ON ficha_state(tailor_name)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_ficha_state_clothes ON ficha_state(clothes_type_name)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_ficha_state_deleted ON ficha_state(is_deleted)`);
        
        await client.query(`CREATE INDEX IF NOT EXISTS idx_prod_state_ficha ON production_state(ficha_id)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_prod_state_line_uuid ON production_state(line_uuid)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_prod_state_date ON production_state(date)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_prod_state_deleted ON production_state(is_deleted)`);
        
        await client.query(`CREATE INDEX IF NOT EXISTS idx_daily_basket_date ON daily_basket_summary(date)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_daily_basket_tailor ON daily_basket_summary(tailor_name)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_daily_basket_clothes ON daily_basket_summary(clothes_type_name)`);

        // Existing indexes
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_seq ON message_buffer(seq)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_uuid ON message_buffer(uuid)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_timestamp ON message_buffer(timestamp)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_type ON message_buffer(type)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_ack_status ON message_buffer(ack_status)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_dead_letter_uuid ON dead_letter_queue(uuid)`);

        try {
            await client.query(`ALTER TABLE message_buffer ADD COLUMN IF NOT EXISTS ack_status VARCHAR(20) DEFAULT 'pending'`);
        } catch (e) { /* Column may already exist */ }

        await client.query(`
            INSERT INTO machine_state (id, status, last_data)
            VALUES (1, 'UNKNOWN', '{"tailor":"---","embroidery":"---","color":"---","ficha_id":"---"}')
            ON CONFLICT (id) DO NOTHING
        `);
        await client.query(`
            INSERT INTO sequence_counter (id, current_seq) VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING
        `);

        console.log('✅ Database initialized (V18-STATE-SERVER)');
        const countResult = await client.query('SELECT COUNT(*) as count FROM message_buffer');
        const seqResult = await client.query('SELECT current_seq FROM sequence_counter WHERE id=1');
        const pendingResult = await client.query("SELECT COUNT(*) as count FROM message_buffer WHERE ack_status='pending'");
        const fichaCount = await client.query('SELECT COUNT(*) as count FROM ficha_state WHERE is_deleted=false');
        const prodCount = await client.query('SELECT COUNT(*) as count FROM production_state WHERE is_deleted=false');
        console.log(`   📦 Total messages: ${countResult.rows[0].count}`);
        console.log(`   🔢 Last SEQ: ${seqResult.rows[0]?.current_seq || 0}`);
        console.log(`   ⏳ Pending ACK: ${pendingResult.rows[0].count}`);
        console.log(`   📋 Active Fichas: ${fichaCount.rows[0].count}`);
        console.log(`   🧵 Active Production lines: ${prodCount.rows[0].count}`);
    } catch (err) {
        console.error('❌ DB init error:', err.message);
    } finally {
        client.release();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ State Management (existing) ★★★
// ═══════════════════════════════════════════════════════════════════════════
let machineStatus = 'UNKNOWN';
let lastData = { tailor: '---', embroidery: '---', color: '---', ficha_id: '---' };
let sequenceCounter = 0;
let dbAvailable = false;

function stableStringify(obj) {
    if (obj === null || obj === undefined) return JSON.stringify(obj);
    if (typeof obj !== 'object') return JSON.stringify(obj);
    if (Array.isArray(obj)) {
        return '[' + obj.map(item => stableStringify(item)).join(',') + ']';
    }
    const keys = Object.keys(obj).sort();
    const pairs = keys.map(key => JSON.stringify(key) + ':' + stableStringify(obj[key]));
    return '{' + pairs.join(',') + '}';
}

function computeStableHash(data) {
    const stableStr = stableStringify(data);
    return crypto.createHash('sha256').update(stableStr, 'utf8').digest('hex');
}

async function loadStateFromDB() {
    try {
        const stateResult = await pool.query('SELECT * FROM machine_state WHERE id=1');
        if (stateResult.rows.length > 0) {
            machineStatus = stateResult.rows[0].status || 'UNKNOWN';
            lastData = stateResult.rows[0].last_data || lastData;
        }
        const seqResult = await pool.query('SELECT current_seq FROM sequence_counter WHERE id=1');
        if (seqResult.rows.length > 0) {
            sequenceCounter = seqResult.rows[0].current_seq || 0;
        }
        dbAvailable = true;
        console.log(`✅ State loaded: status=${machineStatus}, seq=${sequenceCounter}`);
    } catch (err) {
        console.error('⚠️ State load failed:', err.message);
        dbAvailable = false;
    }
}

async function getNextSeq() {
    sequenceCounter++;
    if (dbAvailable) {
        try {
            await pool.query('UPDATE sequence_counter SET current_seq=$1 WHERE id=1', [sequenceCounter]);
        } catch (err) {
            console.error('⚠️ SEQ update failed:', err.message);
        }
    }
    return sequenceCounter;
}

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ ★★★ ★★★ PROJECTION LAYER — NEW ★★★ ★★★ ★★★
// ═══════════════════════════════════════════════════════════════════════════
// These functions update the state tables based on incoming events
// They are called AFTER the message is stored in message_buffer
// ═══════════════════════════════════════════════════════════════════════════

// ──────────────────────────────────────────────────────────────────────────
// Helper: Parse TX notes to extract inspector, batch_qty, defects
// ──────────────────────────────────────────────────────────────────────────
function parseTxNotes(notesStr) {
    let inspector = '';
    let batchQty = '';
    let defects = '';
    if (!notesStr) return { inspector, batchQty, defects };
    const parts = notesStr.split('|').map(p => p.trim());
    for (const part of parts) {
        if (part.startsWith('عامل الفحص:')) {
            inspector = part.substring('عامل الفحص:'.length).trim();
        } else if (part.startsWith('كمية الدفعة:')) {
            batchQty = part.substring('كمية الدفعة:'.length).trim();
        } else if (part.startsWith('الأعطاب:')) {
            defects = part.substring('الأعطاب:'.length).trim();
        }
    }
    return { inspector, batchQty, defects };
}

// ──────────────────────────────────────────────────────────────────────────
// Projection: ficha_saved (create or update ficha_state)
// ──────────────────────────────────────────────────────────────────────────
async function projectFichaSaved(data, seq, eventUuid) {
    if (!dbAvailable) return;
    const fichaId = data.ficha_id || data.id;
    if (!fichaId) return;
    
    const tailor = data.tailor || data.tailor_name;
    const clothesType = data.clothes_type || data.clothes_type_name || data.embroidery;
    const totalQty = data.quantity || data.total_quantity || 0;
    const createdDate = data.date || new Date().toISOString().split('T')[0];
    
    try {
        await pool.query(`
            INSERT INTO ficha_state (ficha_id, tailor_name, clothes_type_name, total_quantity, created_date, last_updated, last_seq, last_uuid, is_deleted)
            VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7, false)
            ON CONFLICT (ficha_id) DO UPDATE SET
                tailor_name = EXCLUDED.tailor_name,
                clothes_type_name = EXCLUDED.clothes_type_name,
                total_quantity = EXCLUDED.total_quantity,
                created_date = EXCLUDED.created_date,
                last_updated = NOW(),
                last_seq = EXCLUDED.last_seq,
                last_uuid = EXCLUDED.last_uuid,
                is_deleted = false
        `, [fichaId, tailor, clothesType, totalQty, createdDate, seq, eventUuid]);
        console.log(`   📋 Projection: ficha_state UPSERT #${fichaId} (seq=${seq})`);
    } catch (err) {
        console.error(`   ⚠️ Projection ficha_saved failed: ${err.message}`);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Projection: ficha_deleted (soft delete from ficha_state)
// ──────────────────────────────────────────────────────────────────────────
async function projectFichaDeleted(data, seq, eventUuid) {
    if (!dbAvailable) return;
    const fichaId = data.ficha_id || data.id;
    if (!fichaId) return;
    
    try {
        await pool.query(`
            UPDATE ficha_state 
            SET is_deleted = true, last_updated = NOW(), last_seq = $2, last_uuid = $3
            WHERE ficha_id = $1
        `, [fichaId, seq, eventUuid]);
        console.log(`   📋 Projection: ficha_state DELETED #${fichaId} (seq=${seq})`);
    } catch (err) {
        console.error(`   ⚠️ Projection ficha_deleted failed: ${err.message}`);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Projection: production_saved (add/update production lines + update daily basket)
// ──────────────────────────────────────────────────────────────────────────
async function projectProductionSaved(data, seq, eventUuid) {
    if (!dbAvailable) return;
    const fichaId = data.ficha_id;
    const lines = data.lines || [];
    const tailor = data.tailor || data.tailor_name;
    const clothesType = data.clothes_type || data.clothes_type_name;
    
    if (!fichaId) return;
    
    for (const line of lines) {
        const lineUuid = line.line_uuid || eventUuid;
        const size = line.size;
        const color = line.color;
        const quantity = line.quantity;
        const prodDate = line.date || new Date().toISOString().split('T')[0];
        const notes = line.notes || '';
        const { inspector, batchQty, defects } = parseTxNotes(notes);
        
        // UPSERT production_state
        try {
            await pool.query(`
                INSERT INTO production_state (ficha_id, size, color, quantity, notes, line_uuid, date, last_updated, last_seq, is_deleted)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8, false)
                ON CONFLICT (ficha_id, size, color, date) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    notes = EXCLUDED.notes,
                    line_uuid = EXCLUDED.line_uuid,
                    last_updated = NOW(),
                    last_seq = EXCLUDED.last_seq,
                    is_deleted = false
            `, [fichaId, size, color, quantity, notes, lineUuid, prodDate, seq]);
        } catch (err) {
            console.error(`   ⚠️ Projection production_state failed: ${err.message}`);
            continue;
        }
        
        // Update daily_basket_summary if we have tailor and clothesType
        if (tailor && clothesType && prodDate) {
            try {
                // Check if entry exists
                const existing = await pool.query(
                    `SELECT id, total_quantity, inspector_name, batch_quantity, defect_count 
                     FROM daily_basket_summary 
                     WHERE date = $1 AND tailor_name = $2 AND clothes_type_name = $3`,
                    [prodDate, tailor, clothesType]
                );
                
                if (existing.rows.length > 0) {
                    const currentQty = existing.rows[0].total_quantity;
                    const newQty = currentQty + quantity;
                    const currentInsp = existing.rows[0].inspector_name || '';
                    const currentBatch = existing.rows[0].batch_quantity || '';
                    const currentDefects = existing.rows[0].defect_count || '';
                    
                    await pool.query(`
                        UPDATE daily_basket_summary 
                        SET total_quantity = $1,
                            inspector_name = COALESCE(NULLIF($2, ''), inspector_name),
                            batch_quantity = COALESCE(NULLIF($3, ''), batch_quantity),
                            defect_count = COALESCE(NULLIF($4, ''), defect_count),
                            last_updated = NOW(),
                            last_seq = $5
                        WHERE date = $6 AND tailor_name = $7 AND clothes_type_name = $8
                    `, [newQty, inspector, batchQty, defects, seq, prodDate, tailor, clothesType]);
                } else {
                    await pool.query(`
                        INSERT INTO daily_basket_summary (date, tailor_name, clothes_type_name, total_quantity, inspector_name, batch_quantity, defect_count, last_updated, last_seq)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8)
                    `, [prodDate, tailor, clothesType, quantity, inspector, batchQty, defects, seq]);
                }
            } catch (err) {
                console.error(`   ⚠️ Projection daily_basket_summary failed: ${err.message}`);
            }
        }
    }
    console.log(`   🧺 Projection: production_state UPSERT ${lines.length} lines (ficha=${fichaId}, seq=${seq})`);
}

// ──────────────────────────────────────────────────────────────────────────
// Projection: production_line_edited (update quantity in production_state and daily basket)
// ──────────────────────────────────────────────────────────────────────────
async function projectProductionLineEdited(data, seq, eventUuid) {
    if (!dbAvailable) return;
    const lineUuid = data.line_uuid;
    const newQuantity = data.new_quantity;
    const oldQuantity = data.old_quantity;
    const fichaId = data.ficha_id;
    const size = data.size;
    const color = data.color;
    const notes = data.notes || '';
    const prodDate = data.date || new Date().toISOString().split('T')[0];
    
    if (!lineUuid) return;
    
    // Get current production line to know tailor and clothesType for basket update
    let tailor = null;
    let clothesType = null;
    let oldQtyForBasket = oldQuantity;
    
    try {
        const prodResult = await pool.query(`
            SELECT p.ficha_id, p.size, p.color, p.quantity as old_qty, p.date,
                   f.tailor_name, f.clothes_type_name, p.notes
            FROM production_state p
            JOIN ficha_state f ON p.ficha_id = f.ficha_id
            WHERE p.line_uuid = $1 AND p.is_deleted = false
        `, [lineUuid]);
        
        if (prodResult.rows.length > 0) {
            const row = prodResult.rows[0];
            tailor = row.tailor_name;
            clothesType = row.clothes_type_name;
            if (oldQtyForBasket === undefined) oldQtyForBasket = row.old_qty;
        }
    } catch (err) {
        console.error(`   ⚠️ Projection line_edited fetch failed: ${err.message}`);
    }
    
    // Update production_state
    try {
        await pool.query(`
            UPDATE production_state 
            SET quantity = $1, notes = $2, last_updated = NOW(), last_seq = $3
            WHERE line_uuid = $4 AND is_deleted = false
        `, [newQuantity, notes, seq, lineUuid]);
        console.log(`   ✏️ Projection: production_state UPDATED qty ${newQuantity} (uuid=${lineUuid.substring(0,8)}..., seq=${seq})`);
    } catch (err) {
        console.error(`   ⚠️ Projection line_edited update failed: ${err.message}`);
    }
    
    // Update daily_basket_summary
    if (tailor && clothesType && prodDate && oldQtyForBasket !== undefined) {
        const diff = newQuantity - oldQtyForBasket;
        if (diff !== 0) {
            try {
                const { inspector, batchQty, defects } = parseTxNotes(notes);
                const existing = await pool.query(
                    `SELECT id, total_quantity FROM daily_basket_summary 
                     WHERE date = $1 AND tailor_name = $2 AND clothes_type_name = $3`,
                    [prodDate, tailor, clothesType]
                );
                
                if (existing.rows.length > 0) {
                    const newTotal = Math.max(0, existing.rows[0].total_quantity + diff);
                    await pool.query(`
                        UPDATE daily_basket_summary 
                        SET total_quantity = $1,
                            inspector_name = COALESCE(NULLIF($2, ''), inspector_name),
                            batch_quantity = COALESCE(NULLIF($3, ''), batch_quantity),
                            defect_count = COALESCE(NULLIF($4, ''), defect_count),
                            last_updated = NOW(),
                            last_seq = $5
                        WHERE date = $6 AND tailor_name = $7 AND clothes_type_name = $8
                    `, [newTotal, inspector, batchQty, defects, seq, prodDate, tailor, clothesType]);
                }
            } catch (err) {
                console.error(`   ⚠️ Projection line_edited basket update failed: ${err.message}`);
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Projection: production_line_deleted (soft delete from production_state and update daily basket)
// ──────────────────────────────────────────────────────────────────────────
async function projectProductionLineDeleted(data, seq, eventUuid) {
    if (!dbAvailable) return;
    const lineUuid = data.line_uuid;
    const fichaId = data.ficha_id;
    const size = data.size;
    const color = data.color;
    const deletedQuantity = data.quantity || data.old_quantity;
    const prodDate = data.date || new Date().toISOString().split('T')[0];
    
    if (!lineUuid) return;
    
    // Get production line details before deletion
    let tailor = null;
    let clothesType = null;
    let quantity = deletedQuantity;
    
    try {
        const prodResult = await pool.query(`
            SELECT p.ficha_id, p.size, p.color, p.quantity, p.date,
                   f.tailor_name, f.clothes_type_name
            FROM production_state p
            JOIN ficha_state f ON p.ficha_id = f.ficha_id
            WHERE p.line_uuid = $1 AND p.is_deleted = false
        `, [lineUuid]);
        
        if (prodResult.rows.length > 0) {
            const row = prodResult.rows[0];
            tailor = row.tailor_name;
            clothesType = row.clothes_type_name;
            if (quantity === undefined) quantity = row.quantity;
        }
    } catch (err) {
        console.error(`   ⚠️ Projection line_deleted fetch failed: ${err.message}`);
    }
    
    // Soft delete production_state (or hard delete - keeping soft for audit)
    try {
        await pool.query(`
            UPDATE production_state 
            SET is_deleted = true, last_updated = NOW(), last_seq = $1
            WHERE line_uuid = $2
        `, [seq, lineUuid]);
        console.log(`   🗑️ Projection: production_state DELETED (uuid=${lineUuid.substring(0,8)}..., seq=${seq})`);
    } catch (err) {
        console.error(`   ⚠️ Projection line_deleted update failed: ${err.message}`);
    }
    
    // Update daily_basket_summary (subtract quantity)
    if (tailor && clothesType && prodDate && quantity) {
        try {
            const existing = await pool.query(
                `SELECT id, total_quantity FROM daily_basket_summary 
                 WHERE date = $1 AND tailor_name = $2 AND clothes_type_name = $3`,
                [prodDate, tailor, clothesType]
            );
            
            if (existing.rows.length > 0) {
                const newTotal = Math.max(0, existing.rows[0].total_quantity - quantity);
                await pool.query(`
                    UPDATE daily_basket_summary 
                    SET total_quantity = $1, last_updated = NOW(), last_seq = $2
                    WHERE date = $3 AND tailor_name = $4 AND clothes_type_name = $5
                `, [newTotal, seq, prodDate, tailor, clothesType]);
            }
        } catch (err) {
            console.error(`   ⚠️ Projection line_deleted basket update failed: ${err.message}`);
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Main projection dispatcher
// ──────────────────────────────────────────────────────────────────────────
async function dispatchProjection(eventType, data, seq, eventUuid) {
    // Only run projections if DB is available
    if (!dbAvailable) return;
    
    try {
        switch (eventType) {
            case 'ficha_saved':
                await projectFichaSaved(data, seq, eventUuid);
                break;
            case 'ficha_deleted':
                await projectFichaDeleted(data, seq, eventUuid);
                break;
            case 'production_saved':
                await projectProductionSaved(data, seq, eventUuid);
                break;
            case 'production_line_edited':
                await projectProductionLineEdited(data, seq, eventUuid);
                break;
            case 'production_line_deleted':
                await projectProductionLineDeleted(data, seq, eventUuid);
                break;
            default:
                // For other event types, no projection needed
                break;
        }
    } catch (err) {
        console.error(`⚠️ Projection dispatch error for ${eventType}: ${err.message}`);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ V17-SYNC: addToBuffer — ON CONFLICT update ★★★
// ═══════════════════════════════════════════════════════════════════════════
async function addToBuffer(type, data, senderSocketId, clientUuid, clientHash) {
    const seq = await getNextSeq();
    const uuid = clientUuid || crypto.randomUUID();
    const serverHash = computeStableHash(data);

    const entry = {
        seq, uuid, type, data,
        hash: serverHash,
        client_hash: clientHash || null,
        timestamp: new Date().toISOString(),
        sender: senderSocketId,
        acked_by: [],
        ack_status: 'pending'
    };

    if (dbAvailable) {
        try {
            await pool.query(
                `INSERT INTO message_buffer (seq, uuid, type, data, hash, sender, timestamp, acked_by, ack_status)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (uuid) DO UPDATE SET
                    seq = EXCLUDED.seq,
                    data = EXCLUDED.data,
                    hash = EXCLUDED.hash,
                    sender = EXCLUDED.sender,
                    timestamp = EXCLUDED.timestamp,
                    type = EXCLUDED.type,
                    ack_status = 'pending'
                `,
                [seq, uuid, type, JSON.stringify(data), serverHash, senderSocketId, entry.timestamp, [], 'pending']
            );
        } catch (err) {
            console.error('⚠️ Buffer insert error:', err.message);
            if (err.message.includes('unique') || err.message.includes('duplicate')) {
                try {
                    const newSeq = await getNextSeq();
                    entry.seq = newSeq;
                    await pool.query(
                        `INSERT INTO message_buffer (seq, uuid, type, data, hash, sender, timestamp, acked_by, ack_status)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                         ON CONFLICT (uuid) DO UPDATE SET
                            seq = EXCLUDED.seq,
                            data = EXCLUDED.data,
                            hash = EXCLUDED.hash,
                            timestamp = EXCLUDED.timestamp,
                            type = EXCLUDED.type
                        `,
                        [newSeq, uuid, type, JSON.stringify(data), serverHash, senderSocketId, entry.timestamp, [], 'pending']
                    );
                    console.log(`   🔄 Retry insert succeeded with seq=${newSeq}`);
                } catch (retryErr) {
                    console.error('❌ Buffer retry insert failed:', retryErr.message);
                }
            }
        }
    }

    // ★ NEW: Dispatch projection AFTER storing in message_buffer
    await dispatchProjection(type, data, seq, uuid);

    return entry;
}

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ Buffer Query Functions (existing, no changes) ★★★
// ═══════════════════════════════════════════════════════════════════════════
async function getBufferAfterSeq(afterSeq, limit = 500) {
    if (!dbAvailable) return [];
    try {
        const result = await pool.query(
            `SELECT seq, uuid, type, data, hash, sender, timestamp, acked_by, ack_status
             FROM message_buffer WHERE seq > $1 ORDER BY seq ASC LIMIT $2`,
            [afterSeq, limit]
        );
        return result.rows.map(row => ({
            seq: row.seq,
            uuid: row.uuid,
            type: row.type,
            data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
            hash: row.hash,
            sender: row.sender,
            timestamp: row.timestamp,
            acked_by: row.acked_by || [],
            ack_status: row.ack_status || 'pending'
        }));
    } catch (err) {
        console.error('⚠️ getBufferAfterSeq error:', err.message);
        return [];
    }
}

async function getMessageByUuid(uuid) {
    if (!dbAvailable) return null;
    try {
        const result = await pool.query(
            'SELECT seq, uuid, type, data, hash, ack_status FROM message_buffer WHERE uuid=$1',
            [uuid]
        );
        if (result.rows.length > 0) {
            const row = result.rows[0];
            return {
                seq: row.seq, uuid: row.uuid, type: row.type,
                data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
                hash: row.hash, ack_status: row.ack_status
            };
        }
        return null;
    } catch (err) { return null; }
}

async function getBufferCount() {
    if (!dbAvailable) return 0;
    try {
        const result = await pool.query('SELECT COUNT(*) as count FROM message_buffer');
        return parseInt(result.rows[0].count);
    } catch (err) { return 0; }
}

async function getPendingCount() {
    if (!dbAvailable) return 0;
    try {
        const result = await pool.query("SELECT COUNT(*) as count FROM message_buffer WHERE ack_status='pending'");
        return parseInt(result.rows[0].count);
    } catch (err) { return 0; }
}

async function getBufferRange() {
    if (!dbAvailable) return { oldest: 0, newest: 0 };
    try {
        const result = await pool.query('SELECT MIN(seq) as oldest, MAX(seq) as newest FROM message_buffer');
        return { oldest: result.rows[0].oldest || 0, newest: result.rows[0].newest || 0 };
    } catch (err) { return { oldest: 0, newest: 0 }; }
}

async function markAcked(seq, socketId) {
    if (!dbAvailable) return;
    try {
        await pool.query(
            `UPDATE message_buffer SET acked_by = array_append(acked_by, $1)
             WHERE seq = $2 AND NOT ($1 = ANY(acked_by))`,
            [socketId, seq]
        );
    } catch (err) { /* ignore */ }
}

async function markMessageAckedByUuid(uuid, ackerSocketId) {
    if (!dbAvailable || !uuid) return false;
    try {
        const result = await pool.query(
            `UPDATE message_buffer
             SET ack_status = 'acked',
                 acked_by = array_append(acked_by, $1)
             WHERE uuid = $2 AND ack_status = 'pending'
             RETURNING seq, type`,
            [ackerSocketId, uuid]
        );
        if (result.rows.length > 0) {
            console.log(`   ✅ Message ACKed: uuid=${uuid.substring(0, 8)}... type=${result.rows[0].type}`);
            return true;
        }
        return false;
    } catch (err) {
        console.error(`⚠️ markMessageAckedByUuid error: ${err.message}`);
        return false;
    }
}

async function moveToDeadLetter(uuid, type, data, sender, reason, retryCount) {
    if (!dbAvailable) return;
    try {
        await pool.query(
            `INSERT INTO dead_letter_queue (uuid, type, data, sender, failure_reason, retry_count)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (uuid) DO UPDATE SET
                failure_reason = EXCLUDED.failure_reason,
                retry_count = EXCLUDED.retry_count`,
            [uuid, type, JSON.stringify(data), sender, reason, retryCount]
        );
        console.log(`💀 Dead letter: uuid=${uuid.substring(0, 8)}... type=${type} retries=${retryCount}`);
    } catch (err) {
        console.error(`⚠️ Dead letter insert error: ${err.message}`);
    }
}

async function updateMachineState(status, data) {
    machineStatus = status || machineStatus;
    if (data) {
        if (data.tailor) lastData.tailor = data.tailor;
        if (data.embroidery) lastData.embroidery = data.embroidery;
        if (data.color) lastData.color = data.color;
        if (data.ficha_id) lastData.ficha_id = data.ficha_id;
    }
    if (dbAvailable) {
        try {
            await pool.query(
                'UPDATE machine_state SET status=$1, last_data=$2, updated_at=NOW() WHERE id=1',
                [machineStatus, JSON.stringify(lastData)]
            );
        } catch (err) { /* ignore */ }
    }
}

async function cleanOldMessages() {
    if (!dbAvailable) return;
    try {
        const result1 = await pool.query(
            `DELETE FROM message_buffer WHERE timestamp < NOW() - INTERVAL '15 days'`
        );
        if (result1.rowCount > 0) {
            console.log(`🧹 Cleaned ${result1.rowCount} old messages (>15 days)`);
        }
        const countResult = await pool.query('SELECT COUNT(*) as count FROM message_buffer');
        const totalCount = parseInt(countResult.rows[0].count);
        if (totalCount > 50000) {
            const excess = totalCount - 40000;
            const result2 = await pool.query(
                `DELETE FROM message_buffer WHERE seq IN (
                    SELECT seq FROM message_buffer ORDER BY seq ASC LIMIT $1
                )`, [excess]
            );
            console.log(`🧹 Cleaned ${result2.rowCount} excess messages`);
        }
        await pool.query(`DELETE FROM dead_letter_queue WHERE created_at < NOW() - INTERVAL '30 days'`);
    } catch (err) {
        console.error('⚠️ Cleanup error:', err.message);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ V17-SYNC: broadcastExceptSender (no changes) ★★★
// ═══════════════════════════════════════════════════════════════════════════
function broadcastExceptSender(eventName, data, senderSocketId) {
    for (let [id, clientSocket] of io.sockets.sockets) {
        if (id !== senderSocketId) {
            clientSocket.emit(eventName, data);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ API endpoints ★★★
// ═══════════════════════════════════════════════════════════════════════════
app.use(express.json());

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/health', async (req, res) => {
    const bufferCount = await getBufferCount();
    const pendingCount = await getPendingCount();
    const range = await getBufferRange();
    const fichaCount = dbAvailable ? (await pool.query('SELECT COUNT(*) as count FROM ficha_state WHERE is_deleted=false')).rows[0].count : 0;
    const prodCount = dbAvailable ? (await pool.query('SELECT COUNT(*) as count FROM production_state WHERE is_deleted=false')).rows[0].count : 0;
    const basketCount = dbAvailable ? (await pool.query('SELECT COUNT(*) as count FROM daily_basket_summary')).rows[0].count : 0;
    
    res.json({
        status: 'ok', version: 'V18-STATE-SERVER',
        clients: io.engine.clientsCount,
        machineStatus, lastData,
        bufferSize: bufferCount, pendingAck: pendingCount,
        lastSeq: sequenceCounter,
        oldestSeq: range.oldest, newestSeq: range.newest,
        state: {
            active_fichas: parseInt(fichaCount),
            active_production_lines: parseInt(prodCount),
            daily_basket_entries: parseInt(basketCount)
        },
        dbAvailable, uptime: process.uptime()
    });
});

app.get('/api/sync', async (req, res) => {
    const afterSeq = parseInt(req.query.after_seq) || 0;
    const limit = Math.min(parseInt(req.query.limit) || 500, 2000);
    try {
        const messages = await getBufferAfterSeq(afterSeq, limit);
        res.json({
            status: 'ok', last_seq: sequenceCounter,
            count: messages.length, messages,
            machine_status: machineStatus, last_data: lastData,
            has_more: messages.length >= limit
        });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.get('/api/buffer/status', async (req, res) => {
    const bufferCount = await getBufferCount();
    const pendingCount = await getPendingCount();
    const range = await getBufferRange();
    res.json({
        total_messages: bufferCount, pending_ack: pendingCount,
        last_seq: sequenceCounter, machine_status: machineStatus,
        oldest_seq: range.oldest, newest_seq: range.newest,
        db_available: dbAvailable
    });
});

app.post('/api/ack', async (req, res) => {
    const { seq, client_id } = req.body;
    if (seq && client_id) {
        await markAcked(seq, client_id);
        res.json({ status: 'ok', seq });
    } else {
        res.status(400).json({ status: 'error', message: 'seq and client_id required' });
    }
});

app.get('/api/resend/:uuid', async (req, res) => {
    const msg = await getMessageByUuid(req.params.uuid);
    if (msg) { res.json({ status: 'ok', message: msg }); }
    else { res.status(404).json({ status: 'not_found' }); }
});

app.get('/api/messages', async (req, res) => {
    const afterSeq = parseInt(req.query.after_seq) || 0;
    const limit = Math.min(parseInt(req.query.limit) || 500, 2000);
    try {
        const messages = await getBufferAfterSeq(afterSeq, limit);
        res.json({ status: 'ok', count: messages.length, last_seq: sequenceCounter, messages });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.get('/api/stats', async (req, res) => {
    try {
        let stats = {
            total_messages: 0, pending_ack: 0, messages_today: 0,
            messages_by_type: {}, dead_letters: 0,
            last_seq: sequenceCounter, db_available: dbAvailable,
            state: { active_fichas: 0, active_production_lines: 0, daily_basket_entries: 0 }
        };
        if (dbAvailable) {
            const totalResult = await pool.query('SELECT COUNT(*) as count FROM message_buffer');
            stats.total_messages = parseInt(totalResult.rows[0].count);
            const pendingResult = await pool.query("SELECT COUNT(*) as count FROM message_buffer WHERE ack_status='pending'");
            stats.pending_ack = parseInt(pendingResult.rows[0].count);
            const todayResult = await pool.query("SELECT COUNT(*) as count FROM message_buffer WHERE timestamp >= CURRENT_DATE");
            stats.messages_today = parseInt(todayResult.rows[0].count);
            const typeResult = await pool.query('SELECT type, COUNT(*) as count FROM message_buffer GROUP BY type ORDER BY count DESC');
            typeResult.rows.forEach(row => { stats.messages_by_type[row.type] = parseInt(row.count); });
            const deadResult = await pool.query('SELECT COUNT(*) as count FROM dead_letter_queue');
            stats.dead_letters = parseInt(deadResult.rows[0].count);
            
            // State stats
            const fichaResult = await pool.query('SELECT COUNT(*) as count FROM ficha_state WHERE is_deleted=false');
            stats.state.active_fichas = parseInt(fichaResult.rows[0].count);
            const prodResult = await pool.query('SELECT COUNT(*) as count FROM production_state WHERE is_deleted=false');
            stats.state.active_production_lines = parseInt(prodResult.rows[0].count);
            const basketResult = await pool.query('SELECT COUNT(*) as count FROM daily_basket_summary');
            stats.state.daily_basket_entries = parseInt(basketResult.rows[0].count);
        }
        res.json(stats);
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.get('/api/dead-letters', async (req, res) => {
    if (!dbAvailable) return res.json({ status: 'ok', count: 0, messages: [] });
    try {
        const result = await pool.query('SELECT * FROM dead_letter_queue ORDER BY created_at DESC LIMIT 100');
        res.json({ status: 'ok', count: result.rows.length, messages: result.rows });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ NEW APIs for State Queries (RX can now read directly from server) ★★★
// ═══════════════════════════════════════════════════════════════════════════

// ──────────────────────────────────────────────────────────────────────────
// GET /api/daily-basket
// Returns complete daily basket summary from server's state tables
// Query params: date (optional), tailor (optional), clothes_type (optional)
// ──────────────────────────────────────────────────────────────────────────
app.get('/api/daily-basket', async (req, res) => {
    if (!dbAvailable) {
        return res.status(503).json({ status: 'error', message: 'Database not available' });
    }
    
    const { date, tailor, clothes_type } = req.query;
    let query = `
        SELECT date, tailor_name, clothes_type_name, total_quantity, 
               inspector_name, batch_quantity, defect_count, last_updated
        FROM daily_basket_summary
        WHERE 1=1
    `;
    const params = [];
    let paramIndex = 1;
    
    if (date) {
        query += ` AND date = $${paramIndex++}`;
        params.push(date);
    }
    if (tailor && tailor !== '-- الكل --' && tailor !== '') {
        query += ` AND tailor_name = $${paramIndex++}`;
        params.push(tailor);
    }
    if (clothes_type && clothes_type !== '-- الكل --' && clothes_type !== '') {
        query += ` AND clothes_type_name = $${paramIndex++}`;
        params.push(clothes_type);
    }
    
    query += ` ORDER BY date DESC, tailor_name, clothes_type_name`;
    
    try {
        const result = await pool.query(query, params);
        const rows = result.rows.map(row => ({
            date: row.date.toISOString().split('T')[0],
            tailor: row.tailor_name,
            clothes_type: row.clothes_type_name,
            total_quantity: parseInt(row.total_quantity),
            inspector: row.inspector_name || '',
            batch_qty: row.batch_quantity || '',
            defects: row.defect_count || '',
            last_updated: row.last_updated
        }));
        
        // Calculate grand total
        const totalResult = await pool.query('SELECT SUM(total_quantity) as grand_total FROM daily_basket_summary');
        const grandTotal = parseInt(totalResult.rows[0].grand_total) || 0;
        
        res.json({
            status: 'ok',
            count: rows.length,
            grand_total: grandTotal,
            data: rows,
            source: 'server_state',
            server_version: 'V18-STATE-SERVER'
        });
    } catch (err) {
        console.error('❌ /api/daily-basket error:', err.message);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ──────────────────────────────────────────────────────────────────────────
// GET /api/production
// Returns current production state from server
// Query params: ficha_id (optional), date (optional), include_deleted (optional)
// ──────────────────────────────────────────────────────────────────────────
app.get('/api/production', async (req, res) => {
    if (!dbAvailable) {
        return res.status(503).json({ status: 'error', message: 'Database not available' });
    }
    
    const { ficha_id, date, include_deleted } = req.query;
    let query = `
        SELECT p.ficha_id, p.size, p.color, p.quantity, p.notes, p.line_uuid, p.date,
               f.tailor_name, f.clothes_type_name, f.total_quantity as ficha_total_quantity
        FROM production_state p
        JOIN ficha_state f ON p.ficha_id = f.ficha_id
        WHERE 1=1
    `;
    const params = [];
    let paramIndex = 1;
    
    if (include_deleted !== 'true') {
        query += ` AND p.is_deleted = false`;
    }
    if (ficha_id) {
        query += ` AND p.ficha_id = $${paramIndex++}`;
        params.push(parseInt(ficha_id));
    }
    if (date) {
        query += ` AND p.date = $${paramIndex++}`;
        params.push(date);
    }
    
    query += ` ORDER BY p.ficha_id, p.date DESC, p.size, p.color`;
    
    try {
        const result = await pool.query(query, params);
        const rows = result.rows.map(row => ({
            ficha_id: row.ficha_id,
            tailor: row.tailor_name,
            clothes_type: row.clothes_type_name,
            size: row.size,
            color: row.color,
            quantity: parseInt(row.quantity),
            notes: row.notes || '',
            line_uuid: row.line_uuid,
            date: row.date.toISOString().split('T')[0],
            ficha_total_quantity: parseInt(row.ficha_total_quantity)
        }));
        
        res.json({
            status: 'ok',
            count: rows.length,
            data: rows,
            source: 'server_state',
            server_version: 'V18-STATE-SERVER'
        });
    } catch (err) {
        console.error('❌ /api/production error:', err.message);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ──────────────────────────────────────────────────────────────────────────
// GET /api/fichas
// Returns current ficha state from server
// ──────────────────────────────────────────────────────────────────────────
app.get('/api/fichas', async (req, res) => {
    if (!dbAvailable) {
        return res.status(503).json({ status: 'error', message: 'Database not available' });
    }
    
    const { include_deleted } = req.query;
    let query = `
        SELECT ficha_id, tailor_name, clothes_type_name, total_quantity, 
               created_date, last_updated, is_deleted
        FROM ficha_state
        WHERE 1=1
    `;
    const params = [];
    
    if (include_deleted !== 'true') {
        query += ` AND is_deleted = false`;
    }
    
    query += ` ORDER BY ficha_id DESC`;
    
    try {
        const result = await pool.query(query, params);
        const rows = result.rows.map(row => ({
            ficha_id: row.ficha_id,
            tailor: row.tailor_name,
            clothes_type: row.clothes_type_name,
            total_quantity: parseInt(row.total_quantity),
            created_date: row.created_date.toISOString().split('T')[0],
            is_deleted: row.is_deleted
        }));
        
        res.json({
            status: 'ok',
            count: rows.length,
            data: rows,
            source: 'server_state',
            server_version: 'V18-STATE-SERVER'
        });
    } catch (err) {
        console.error('❌ /api/fichas error:', err.message);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ──────────────────────────────────────────────────────────────────────────
// GET /api/production/summary
// Returns summary statistics from state tables
// ──────────────────────────────────────────────────────────────────────────
app.get('/api/production/summary', async (req, res) => {
    if (!dbAvailable) {
        return res.status(503).json({ status: 'error', message: 'Database not available' });
    }
    
    try {
        const totalProduction = await pool.query(
            'SELECT SUM(quantity) as total FROM production_state WHERE is_deleted = false'
        );
        const totalFichas = await pool.query(
            'SELECT COUNT(*) as count FROM ficha_state WHERE is_deleted = false'
        );
        const totalBasketEntries = await pool.query(
            'SELECT COUNT(*) as count, SUM(total_quantity) as total FROM daily_basket_summary'
        );
        
        res.json({
            status: 'ok',
            data: {
                total_production_quantity: parseInt(totalProduction.rows[0].total) || 0,
                active_fichas: parseInt(totalFichas.rows[0].count) || 0,
                daily_basket_entries: parseInt(totalBasketEntries.rows[0].count) || 0,
                daily_basket_total: parseInt(totalBasketEntries.rows[0].total) || 0
            },
            source: 'server_state'
        });
    } catch (err) {
        console.error('❌ /api/production/summary error:', err.message);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ──────────────────────────────────────────────────────────────────────────
// POST /api/rebuild-state (admin only - rebuild state from message_buffer)
// Useful for recovery if state tables get corrupted
// ──────────────────────────────────────────────────────────────────────────
app.post('/api/rebuild-state', async (req, res) => {
    if (!dbAvailable) {
        return res.status(503).json({ status: 'error', message: 'Database not available' });
    }
    
    const { secret } = req.body;
    const ADMIN_SECRET = process.env.ADMIN_SECRET || 'default-secret-change-me';
    
    if (secret !== ADMIN_SECRET) {
        return res.status(403).json({ status: 'error', message: 'Unauthorized' });
    }
    
    try {
        console.log('🔄 Starting state rebuild from message_buffer...');
        
        // Clear existing state tables
        await pool.query('TRUNCATE ficha_state CASCADE');
        await pool.query('TRUNCATE production_state CASCADE');
        await pool.query('TRUNCATE daily_basket_summary CASCADE');
        
        // Get all messages in order
        const messages = await pool.query(
            `SELECT seq, uuid, type, data FROM message_buffer ORDER BY seq ASC`
        );
        
        let processedCount = 0;
        for (const msg of messages.rows) {
            const data = typeof msg.data === 'string' ? JSON.parse(msg.data) : msg.data;
            await dispatchProjection(msg.type, data, msg.seq, msg.uuid);
            processedCount++;
            if (processedCount % 100 === 0) {
                console.log(`   Rebuilt ${processedCount}/${messages.rows.length} messages...`);
            }
        }
        
        console.log(`✅ State rebuild complete. Processed ${processedCount} messages.`);
        res.json({
            status: 'ok',
            message: `State rebuilt successfully from ${processedCount} messages`,
            processed_count: processedCount
        });
    } catch (err) {
        console.error('❌ State rebuild failed:', err.message);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ V17-SYNC: Socket.IO — with Projection Layer ★★★
// ═══════════════════════════════════════════════════════════════════════════
io.on('connection', async (socket) => {
    console.log(`✅ Connected: ${socket.id} | Total: ${io.engine.clientsCount}`);

    const bufferCount = await getBufferCount();
    const pendingCount = await getPendingCount();
    socket.emit('welcome', {
        status: machineStatus, details: lastData,
        last_seq: sequenceCounter, buffer_size: bufferCount,
        pending_ack: pendingCount,
        db_available: dbAvailable,
        server_version: 'V18-STATE-SERVER'  // Updated version
    });
    socket.emit('machine_status', { status: machineStatus });

    // ═══ get_missed_messages (no changes) ═══
    socket.on('get_missed_messages', async (payload) => {
        const afterSeq = payload.last_seq || payload.after_seq || 0;
        const limit = Math.min(payload.limit || 500, 2000);
        console.log(`🔄 [${socket.id}] get_missed_messages after seq=${afterSeq}`);
        try {
            const missed = await getBufferAfterSeq(afterSeq, limit);
            console.log(`   📦 Sending ${missed.length} missed messages`);
            socket.emit('missed_messages_response', {
                last_seq: sequenceCounter, count: missed.length, messages: missed,
                machine_status: machineStatus, last_data: lastData,
                has_more: missed.length >= limit
            });
        } catch (err) {
            console.error('❌ get_missed_messages error:', err.message);
            socket.emit('missed_messages_response', {
                last_seq: sequenceCounter, count: 0, messages: [],
                machine_status: machineStatus, error: err.message
            });
        }
    });

    // ═══ sync_request (no changes) ═══
    socket.on('sync_request', async (payload) => {
        const afterSeq = payload.after_seq || 0;
        const limit = Math.min(payload.limit || 500, 2000);
        console.log(`🔄 [${socket.id}] sync_request after seq=${afterSeq}`);
        try {
            const missed = await getBufferAfterSeq(afterSeq, limit);
            console.log(`   📦 Sending ${missed.length} messages`);
            socket.emit('sync_response', {
                last_seq: sequenceCounter, count: missed.length,
                messages: missed, machine_status: machineStatus, last_data: lastData,
                has_more: missed.length >= limit
            });
        } catch (err) {
            console.error('❌ sync error:', err.message);
            socket.emit('sync_response', {
                last_seq: sequenceCounter, count: 0, messages: [],
                machine_status: machineStatus, error: err.message
            });
        }
    });

    // ═══ ACK handling (no changes) ═══
    socket.on('ack', async (payload) => {
        const seq = payload.seq;
        if (seq) {
            await markAcked(seq, socket.id);
            if (dbAvailable) {
                try {
                    const result = await pool.query('SELECT sender FROM message_buffer WHERE seq=$1', [seq]);
                    if (result.rows.length > 0 && result.rows[0].sender) {
                        io.to(result.rows[0].sender).emit('delivery_confirmed', {
                            seq, acked_by: socket.id, timestamp: new Date().toISOString()
                        });
                    }
                } catch (err) { /* ignore */ }
            }
        }
    });

    socket.on('ack_batch', async (payload) => {
        const seqs = payload.seqs || [];
        for (const seq of seqs) { await markAcked(seq, socket.id); }
        console.log(`✅ [${socket.id}] ACK batch: ${seqs.length}`);
        if (dbAvailable) {
            try {
                const result = await pool.query(
                    'SELECT DISTINCT sender FROM message_buffer WHERE seq = ANY($1)', [seqs]
                );
                result.rows.forEach(row => {
                    if (row.sender) {
                        seqs.forEach(seq => {
                            io.to(row.sender).emit('delivery_confirmed', { seq, acked_by: socket.id });
                        });
                    }
                });
            } catch (err) { /* ignore */ }
        }
    });

    socket.on('ack_message', async (payload) => {
        const msgUuid = payload.msg_uuid || payload.uuid;
        if (!msgUuid) { return; }
        console.log(`📬 [${socket.id}] ack_message uuid=${msgUuid.substring(0, 8)}...`);
        const success = await markMessageAckedByUuid(msgUuid, socket.id);
        if (success && dbAvailable) {
            try {
                const result = await pool.query(
                    'SELECT sender, seq FROM message_buffer WHERE uuid=$1', [msgUuid]
                );
                if (result.rows.length > 0 && result.rows[0].sender) {
                    io.to(result.rows[0].sender).emit('delivery_confirmed', {
                        seq: result.rows[0].seq, uuid: msgUuid,
                        acked_by: socket.id, timestamp: new Date().toISOString()
                    });
                }
            } catch (err) { /* ignore */ }
        }
        const pendingCount = await getPendingCount();
        io.emit('pending_count_update', { pending: pendingCount });
    });

    // ═══ NACK / Resend (no changes) ═══
    socket.on('nack_resend', async (payload) => {
        const uuid = payload.uuid;
        const reason = payload.reason || 'unknown';
        console.log(`🔁 [${socket.id}] NACK uuid=${uuid} reason=${reason}`);
        if (uuid) {
            const msg = await getMessageByUuid(uuid);
            if (msg) {
                const serverHash = computeStableHash(msg.data);
                socket.emit('resend_message', {
                    seq: msg.seq, uuid: msg.uuid, type: msg.type,
                    data: msg.data, hash: serverHash, is_resend: true
                });
                console.log(`   ✅ Resent uuid=${uuid.substring(0, 8)}...`);
            } else {
                socket.emit('resend_failed', { uuid, reason: 'not_found' });
            }
        }
    });

    // ═══════════════════════════════════════════════════
    // ★★★ command handler (no changes) ★★★
    // ═══════════════════════════════════════════════════
    socket.on('command', async (payload) => {
        console.log(`📨 [${socket.id}] command:`, JSON.stringify(payload).substring(0, 200));
        const action = payload.action;
        const data = payload.data || payload.details || {};
        const clientUuid = payload.uuid;
        const clientHash = payload.hash;

        if (['START', 'STOP', 'PAUSE'].includes(action)) {
            await updateMachineState(action, data);
            const entry = await addToBuffer('machine_command', {
                action, speed: data.speed || null, timestamp: payload.timestamp
            }, socket.id, clientUuid, clientHash);

            console.log(`📡 Broadcasting machine_command [${action}] [seq=${entry.seq}]`);
            const machineData = {
                status: action, speed: data.speed || null,
                timestamp: payload.timestamp,
                seq: entry.seq, uuid: entry.uuid, hash: entry.hash,
                _sender: socket.id
            };
            broadcastExceptSender('machine_status', machineData, socket.id);
            broadcastExceptSender('update_ui', {
                status: machineStatus, details: lastData,
                seq: entry.seq, uuid: entry.uuid
            }, socket.id);
            socket.emit('server_ack', {
                seq: entry.seq, uuid: clientUuid || entry.uuid,
                server_uuid: entry.uuid,
                original_action: action, status: 'stored_in_db'
            });
        }

        if (action === 'data_change') {
            const changeType = payload.change_type;
            const details = payload.details || {};
            await updateMachineState(null, details);
            const entry = await addToBuffer(changeType, details, socket.id, clientUuid, clientHash);
            console.log(`📡 Broadcasting data_changed [${changeType}] [seq=${entry.seq}]`);
            broadcastExceptSender('data_changed', {
                type: changeType, change_type: changeType,
                details, timestamp: payload.timestamp,
                seq: entry.seq, uuid: entry.uuid, hash: entry.hash,
                _sender: socket.id
            }, socket.id);
            broadcastExceptSender('update_ui', {
                status: machineStatus, details: lastData,
                seq: entry.seq, uuid: entry.uuid
            }, socket.id);
            socket.emit('server_ack', {
                seq: entry.seq, uuid: clientUuid || entry.uuid,
                server_uuid: entry.uuid,
                original_action: changeType, status: 'stored_in_db'
            });
        }

        if (action === 'SYNC_REQUEST') {
            const afterSeq = data.after_seq || 0;
            const limit = Math.min(data.limit || 500, 2000);
            const missed = await getBufferAfterSeq(afterSeq, limit);
            socket.emit('sync_response', {
                last_seq: sequenceCounter, count: missed.length,
                messages: missed, machine_status: machineStatus, last_data: lastData,
                has_more: missed.length >= limit
            });
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // ★★★ V17-SYNC: handleDirectEvent — WITH PROJECTION ★★★
    // ═══════════════════════════════════════════════════════════════
    async function handleDirectEvent(eventName, data, socket) {
        const clientUuid = data._uuid || data.uuid;
        const clientHash = data._hash || data.hash;

        // Dedup check by UUID
        if (clientUuid && dbAvailable && eventName !== 'production_saved' && eventName !== 'production_line_deleted') {
            try {
                const existing = await pool.query(
                    'SELECT seq FROM message_buffer WHERE uuid=$1', [clientUuid]
                );
                if (existing.rows.length > 0) {
                    console.log(`   🔄 Duplicate UUID: ${clientUuid.substring(0, 8)}... — skip`);
                    socket.emit('server_ack', {
                        seq: existing.rows[0].seq,
                        uuid: clientUuid, server_uuid: clientUuid,
                        original_action: eventName, status: 'already_exists'
                    });
                    return;
                }
            } catch (err) { /* continue */ }
        }

        const cleanData = { ...data };
        delete cleanData._uuid; delete cleanData._hash;
        delete cleanData.uuid; delete cleanData.hash;

        const entry = await addToBuffer(eventName, cleanData, socket.id, clientUuid, clientHash);
        await updateMachineState(null, cleanData);

        const broadcastData = {
            ...cleanData,
            seq: entry.seq, uuid: entry.uuid, hash: entry.hash,
            _sender: socket.id
        };

        console.log(`📡 Broadcasting ${eventName} to others [seq=${entry.seq}, uuid=${entry.uuid.substring(0, 8)}...]`);
        broadcastExceptSender(eventName, broadcastData, socket.id);
        broadcastExceptSender('update_ui', {
            status: machineStatus, details: lastData,
            seq: entry.seq, uuid: entry.uuid
        }, socket.id);

        socket.emit('server_ack', {
            seq: entry.seq, uuid: clientUuid || entry.uuid,
            server_uuid: entry.uuid,
            original_action: eventName, status: 'stored_in_db'
        });

        console.log(`   ✅ ${eventName} [seq=${entry.seq}, uuid=${entry.uuid.substring(0, 8)}...] stored+broadcast+projected`);
    }

    // ═══ Dead letter (no changes) ═══
    socket.on('dead_letter', async (payload) => {
        const { uuid, type, data, reason, retry_count } = payload;
        console.log(`💀 [${socket.id}] dead_letter: uuid=${uuid?.substring(0, 8)}... type=${type} retries=${retry_count}`);
        if (uuid) {
            await moveToDeadLetter(uuid, type || 'unknown', data || {}, socket.id, reason || 'max_retries', retry_count || 0);
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // ★★★ V17-SYNC: Direct event handlers (same as before) ★★★
    // ═══════════════════════════════════════════════════════════════

    socket.on('ficha_saved', async (data) => {
        console.log(`📋 [${socket.id}] ficha_saved`);
        await handleDirectEvent('ficha_saved', data, socket);
    });

    socket.on('ficha_deleted', async (data) => {
        console.log(`🗑️ [${socket.id}] ficha_deleted`);
        await handleDirectEvent('ficha_deleted', data, socket);
    });

    socket.on('production_saved', async (data) => {
        console.log(`🧺 [${socket.id}] production_saved (edit-aware)`);
        await handleDirectEvent('production_saved', data, socket);
    });

    socket.on('production_deleted', async (data) => {
        console.log(`🗑️ [${socket.id}] production_deleted`);
        await handleDirectEvent('production_deleted', data, socket);
    });

    socket.on('production_line_deleted', async (data) => {
        console.log(`🗑️🔴 [${socket.id}] production_line_deleted uuid=${(data._uuid || data.uuid || data.line_uuid || '?').substring(0, 8)}...`);
        await handleDirectEvent('production_line_deleted', data, socket);
    });

    socket.on('production_line_edited', async (data) => {
        console.log(`✏️ [${socket.id}] production_line_edited uuid=${(data._uuid || data.uuid || data.line_uuid || '?').substring(0, 8)}...`);
        await handleDirectEvent('production_line_edited', data, socket);
    });

    socket.on('machine_status', async (data) => {
        console.log(`⚙️ [${socket.id}] machine_status`);
        if (data.status) await updateMachineState(data.status, null);
        await handleDirectEvent('machine_status', data, socket);
    });

    socket.on('speed_update', async (data) => {
        console.log(`⚡ [${socket.id}] speed_update`);
        const entry = await addToBuffer('speed_update', data, socket.id, data.uuid || data._uuid, data.hash || data._hash);
        const broadcastData = {
            speed: data.speed,
            seq: entry.seq, uuid: entry.uuid, hash: entry.hash,
            _sender: socket.id
        };
        console.log(`📡 Broadcasting speed_update [speed=${data.speed}] to others`);
        broadcastExceptSender('speed_update', broadcastData, socket.id);
    });

    socket.on('technical_data', async (data) => {
        console.log(`🔧 [${socket.id}] technical_data`);
        await handleDirectEvent('technical_data', data, socket);
    });

    socket.on('coordinates_batch', async (data) => {
        console.log(`📍 [${socket.id}] coordinates_batch`);
        await handleDirectEvent('coordinates_batch', data, socket);
    });

    socket.on('head_command', async (data) => {
        console.log(`🎯 [${socket.id}] head_command`);
        await handleDirectEvent('head_command', data, socket);
    });

    socket.on('disconnect', (reason) => {
        console.log(`❌ Disconnected: ${socket.id} reason=${reason} | Remaining: ${io.engine.clientsCount}`);
    });
});

// Periodic cleanup every hour
setInterval(cleanOldMessages, 60 * 60 * 1000);

// ═══════════════════════════════════════════════════════════════════════════
// ★★★ Start Server ★★★
// ═══════════════════════════════════════════════════════════════════════════
const PORT = process.env.PORT || 10000;

async function startServer() {
    await initDatabase();
    await loadStateFromDB();
    await cleanOldMessages();

    server.listen(PORT, () => {
        console.log('═'.repeat(70));
        console.log(`🚀 Server V18-STATE-SERVER ready on port ${PORT}`);
        console.log(`💾 PostgreSQL: ${dbAvailable ? '✅' : '❌'}`);
        console.log(`🔐 UUID + Server-side Hash`);
        console.log(`📡 broadcastExceptSender() — no echo back`);
        console.log(`🔁 NACK/Resend + Dead Letter Queue`);
        console.log(`📬 ON CONFLICT (uuid) DO UPDATE — edit support`);
        console.log(`✏️ production_line_edited event — sync edits`);
        console.log(`🗑️ production_line_deleted event — sync deletes`);
        console.log(`🔌 pingTimeout=120s, pingInterval=25s`);
        console.log(`🔄 get_missed_messages event`);
        console.log(`✅ ack_message → pending counter`);
        console.log(`💀 Dead letter queue for failed messages`);
        console.log(`═'.repeat(70)`);
        console.log(`╔════════════════════════════════════════════════════════════════╗`);
        console.log(`║  ★ V18-STATE-SERVER: New State Persistence Layer Active ★      ║`);
        console.log(`╠════════════════════════════════════════════════════════════════╣`);
        console.log(`║  Tables added:                                                ║`);
        console.log(`║    • ficha_state        — current fichas                      ║`);
        console.log(`║    • production_state   — current production lines            ║`);
        console.log(`║    • daily_basket_summary — aggregated daily basket           ║`);
        console.log(`╠════════════════════════════════════════════════════════════════╣`);
        console.log(`║  New APIs:                                                    ║`);
        console.log(`║    GET /api/daily-basket   — server-side daily basket         ║`);
        console.log(`║    GET /api/production     — current production state         ║`);
        console.log(`║    GET /api/fichas         — current fichas                   ║`);
        console.log(`║    GET /api/production/summary — statistics                   ║`);
        console.log(`║    POST /api/rebuild-state  — rebuild from message_buffer     ║`);
        console.log(`╠════════════════════════════════════════════════════════════════╣`);
        console.log(`║  Projection Layer:                                            ║`);
        console.log(`║    • ficha_saved → updates ficha_state                        ║`);
        console.log(`║    • ficha_deleted → soft delete ficha_state                  ║`);
        console.log(`║    • production_saved → updates production_state + basket     ║`);
        console.log(`║    • production_line_edited → updates quantity in both        ║`);
        console.log(`║    • production_line_deleted → soft delete + basket update    ║`);
        console.log(`╚════════════════════════════════════════════════════════════════╝`);
    });
}

startServer().catch(err => {
    console.error('❌ Startup failed:', err);
    process.exit(1);
});