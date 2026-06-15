const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);

// ═══════════════════════════════════════════════════════
// ★★★ V17-SYNC: Socket.IO with stable WebSocket ★★★
// ═══════════════════════════════════════════════════════
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

// ═══════════════════════════════════════════════════════
// ★★★ PostgreSQL Setup ★★★
// ═══════════════════════════════════════════════════════
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

// ═══════════════════════════════════════════════════════
// ★★★ V17-SYNC: Database Schema ★★★
// ═══════════════════════════════════════════════════════
async function initDatabase() {
    const client = await pool.connect();
    try {
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

        // ★★★ جدول السلة اليومية المُجمَّعة — مخزَّن في PostgreSQL ★★★
        // يُحدَّث تلقائياً عند كل production_saved / edited / deleted
        await client.query(`
            CREATE TABLE IF NOT EXISTS daily_basket_summary (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                tailor TEXT NOT NULL,
                clothes_type TEXT NOT NULL,
                inspector TEXT DEFAULT '',
                batch_qty TEXT DEFAULT '',
                defects TEXT DEFAULT '',
                total_quantity INTEGER NOT NULL DEFAULT 0,
                last_updated TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(date, tailor, clothes_type)
            )
        `);

        // Indexes
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_seq ON message_buffer(seq)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_uuid ON message_buffer(uuid)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_timestamp ON message_buffer(timestamp)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_type ON message_buffer(type)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_ack_status ON message_buffer(ack_status)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_dead_letter_uuid ON dead_letter_queue(uuid)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_date ON daily_basket_summary(date)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_basket_tailor ON daily_basket_summary(tailor)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_date_tailor ON daily_basket_summary(date, tailor)`);

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

        console.log('✅ Database initialized (V18-FULL: SEQ/UUID/Hash + Basket + Reconcile)');
        const countResult = await client.query('SELECT COUNT(*) as count FROM message_buffer');
        const seqResult = await client.query('SELECT current_seq FROM sequence_counter WHERE id=1');
        const pendingResult = await client.query("SELECT COUNT(*) as count FROM message_buffer WHERE ack_status='pending'");
        const basketResult = await client.query('SELECT COUNT(*) as count FROM daily_basket_summary');
        console.log(`   📦 Total messages: ${countResult.rows[0].count}`);
        console.log(`   🔢 Last SEQ: ${seqResult.rows[0]?.current_seq || 0}`);
        console.log(`   ⏳ Pending ACK: ${pendingResult.rows[0].count}`);
        console.log(`   🛒 Basket rows: ${basketResult.rows[0].count}`);
    } catch (err) {
        console.error('❌ DB init error:', err.message);
    } finally {
        client.release();
    }
}

// ═══════════════════════════════════════════════════════
// ★★★ State Management ★★★
// ═══════════════════════════════════════════════════════
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

// ═══════════════════════════════════════════════════════
// ★★★ V17-SYNC: addToBuffer — ON CONFLICT update ★★★
// ═══════════════════════════════════════════════════════
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

    return entry;
}

// ═══════════════════════════════════════════════════════
// ★★★ Buffer Query Functions ★★★
// ═══════════════════════════════════════════════════════
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

// ═══════════════════════════════════════════════════════
// ★★★ BASKET SUMMARY — دوال إدارة جدول daily_basket_summary ★★★
// ═══════════════════════════════════════════════════════

/**
 * يفكك حقل notes إلى: inspector / batch_qty / defects
 * الصيغة: "عامل الفحص: X | كمية الدفعة: Y | الأعطاب: Z"
 */
function parseNotes(notesStr) {
    let inspector = '', batch_qty = '', defects = '';
    if (!notesStr) return { inspector, batch_qty, defects };
    const parts = notesStr.split('|').map(p => p.trim());
    for (const part of parts) {
        if (part.startsWith('عامل الفحص:'))
            inspector = part.replace('عامل الفحص:', '').trim();
        else if (part.startsWith('كمية الدفعة:'))
            batch_qty = part.replace('كمية الدفعة:', '').trim();
        else if (part.startsWith('الأعطاب:'))
            defects = part.replace('الأعطاب:', '').trim();
    }
    // Regex fallback إذا لم تُطابق الصيغة العربية أعلاه
    if (!inspector) {
        const m = notesStr.match(/عامل الفحص:\s*([^|،\n]+)/);
        if (m) inspector = m[1].trim();
    }
    if (!batch_qty) {
        const m = notesStr.match(/كمية الدفعة:\s*([^|،\n]+)/);
        if (m) batch_qty = m[1].trim();
    }
    if (!defects) {
        const m = notesStr.match(/الأعطاب:\s*([^|،\n]+)/);
        if (m) defects = m[1].trim();
    }
    return { inspector, batch_qty, defects };
}

/**
 * يُحدّث أو يُنشئ سجل السلة في daily_basket_summary
 * quantityChange: موجب عند الإضافة، سالب عند الحذف
 */
async function updateBasketSummary(basketDate, tailor, clothesType, inspector, batchQty, defects, quantityChange) {
    if (!dbAvailable || !basketDate || !tailor || !clothesType) return;
    try {
        await pool.query(
            `INSERT INTO daily_basket_summary
                (date, tailor, clothes_type, inspector, batch_qty, defects, total_quantity, last_updated)
             VALUES ($1, $2, $3, $4, $5, $6, GREATEST(0, $7), NOW())
             ON CONFLICT (date, tailor, clothes_type) DO UPDATE SET
                total_quantity = GREATEST(0, daily_basket_summary.total_quantity + $7),
                inspector      = CASE WHEN daily_basket_summary.inspector = '' THEN EXCLUDED.inspector ELSE daily_basket_summary.inspector END,
                batch_qty      = CASE WHEN daily_basket_summary.batch_qty  = '' THEN EXCLUDED.batch_qty  ELSE daily_basket_summary.batch_qty  END,
                defects        = CASE WHEN daily_basket_summary.defects    = '' THEN EXCLUDED.defects    ELSE daily_basket_summary.defects    END,
                last_updated   = NOW()`,
            [basketDate, tailor, clothesType,
             inspector || '', batchQty || '', defects || '',
             quantityChange]
        );
    } catch (err) {
        console.error(`⚠️ updateBasketSummary error: ${err.message}`);
    }
}

/**
 * يعالج حدث production_saved ويُحدِّث السلة
 */
async function handleProductionSaved(data) {
    const tailor = data.tailor || '';
    const clothesType = data.clothes_type || '';
    const lines = data.lines || [];
    if (!tailor || !clothesType || !lines.length) return;
    for (const line of lines) {
        const lineDate = line.date || '';
        const qty = parseInt(line.quantity) || 0;
        if (!lineDate || qty <= 0) continue;
        const { inspector, batch_qty, defects } = parseNotes(line.notes || '');
        await updateBasketSummary(lineDate, tailor, clothesType, inspector, batch_qty, defects, qty);
    }
}

/**
 * ★★★ FIX-LIKE: يبحث عن سطر إنتاج أصلي عبر line_uuid باستخدام jsonb_path_exists
 * بدلاً من البحث النصي LIKE '%uuid%' الذي كان عرضة لتطابقات زائفة أو فشل
 * عند احتواء UUID على أحرف خاصة بصيغة JSON، أو عند تكرار جزء من الـ UUID
 * داخل حقول أخرى من نفس السجل.
 *
 * يعيد آخر سجل production_saved (الأحدث seq) يحتوي على هذا line_uuid،
 * مع بيانات الخياط/نوع الملابس والسطر نفسه.
 */
async function findOriginalLineByUuid(lineUuid) {
    if (!lineUuid || !dbAvailable) return null;
    try {
        const result = await pool.query(
            `SELECT data FROM message_buffer
             WHERE type = 'production_saved'
               AND jsonb_path_exists(
                     COALESCE(data->'lines', '[]'::jsonb),
                     '$[*] ? (@.line_uuid == $luuid)',
                     jsonb_build_object('luuid', $1::text)
                   )
             ORDER BY seq DESC
             LIMIT 1`,
            [lineUuid]
        );
        if (!result.rows.length) return null;

        const origData = typeof result.rows[0].data === 'string'
            ? JSON.parse(result.rows[0].data)
            : result.rows[0].data;

        const tailor      = origData.tailor || '';
        const clothesType = origData.clothes_type || '';
        const lines       = origData.lines || [];
        const origLine    = lines.find(l => l.line_uuid === lineUuid);
        if (!origLine || !tailor || !clothesType) return null;

        return { tailor, clothesType, origLine };
    } catch (err) {
        console.error(`⚠️ findOriginalLineByUuid error: ${err.message}`);
        return null;
    }
}

/**
 * يعالج حدث production_line_edited ويُحدِّث الفرق في السلة
 * يحتاج جلب البيانات القديمة من message_buffer
 */
async function handleProductionEdited(data) {
    const lineUuid = data.line_uuid;
    const newQty   = parseInt(data.new_quantity) || 0;
    if (!lineUuid || !dbAvailable) return;
    try {
        const found = await findOriginalLineByUuid(lineUuid);
        if (!found) return;
        const { tailor, clothesType, origLine } = found;

        const oldQty = parseInt(origLine.quantity) || 0;
        const diff   = newQty - oldQty;
        if (diff === 0) return;

        const { inspector, batch_qty, defects } = parseNotes(origLine.notes || '');
        await updateBasketSummary(origLine.date, tailor, clothesType, inspector, batch_qty, defects, diff);
        console.log(`   🛒 Basket updated (edit): ${tailor}/${clothesType} diff=${diff}`);
    } catch (err) {
        console.error(`⚠️ handleProductionEdited basket error: ${err.message}`);
    }
}

/**
 * يعالج حدث production_line_deleted ويطرح الكمية من السلة
 */
async function handleProductionLineDeleted(data) {
    const lineUuid = data.line_uuid;
    if (!lineUuid || !dbAvailable) return;
    try {
        const found = await findOriginalLineByUuid(lineUuid);
        if (!found) return;
        const { tailor, clothesType, origLine } = found;

        const qty = parseInt(origLine.quantity) || 0;
        const { inspector, batch_qty, defects } = parseNotes(origLine.notes || '');
        await updateBasketSummary(origLine.date, tailor, clothesType, inspector, batch_qty, defects, -qty);
        console.log(`   🛒 Basket updated (delete): ${tailor}/${clothesType} -${qty}`);
    } catch (err) {
        console.error(`⚠️ handleProductionLineDeleted basket error: ${err.message}`);
    }
}

/**
 * ★★★ NEW: يعالج حدث production_batch_deleted (حذف عدة أسطر دفعة واحدة)
 * يطرح كمية كل سطر من السلة اليومية، نفس منطق handleProductionLineDeleted
 * لكن لكل عنصر في data.lines.
 */
async function handleProductionBatchDeleted(data) {
    if (!dbAvailable) return;
    const lines = data.lines || [];
    for (const line of lines) {
        const lineUuid = line.line_uuid;
        if (!lineUuid) continue;
        try {
            const found = await findOriginalLineByUuid(lineUuid);
            if (!found) continue;
            const { tailor, clothesType, origLine } = found;
            const qty = parseInt(origLine.quantity) || 0;
            const { inspector, batch_qty, defects } = parseNotes(origLine.notes || '');
            await updateBasketSummary(origLine.date, tailor, clothesType, inspector, batch_qty, defects, -qty);
            console.log(`   🛒 Basket updated (batch-delete): ${tailor}/${clothesType} -${qty} (line=${lineUuid.substring(0, 8)}...)`);
        } catch (err) {
            console.error(`⚠️ handleProductionBatchDeleted line error: ${err.message}`);
        }
    }
}

/**
 * ★★★ NEW: يفحص إن كان سطر إنتاج (line_uuid) قد تم حذفه فعلاً
 * يُستخدم في reconcile_delete_request للتحقق من نجاح عمليات الحذف.
 * يبحث في:
 *   1) رسائل production_line_deleted المباشرة
 *   2) رسائل production_batch_deleted التي تحتوي هذا line_uuid ضمن lines[]
 */
async function lineWasDeleted(lineUuid) {
    if (!lineUuid || !dbAvailable) return false;
    try {
        const r1 = await pool.query(
            `SELECT 1 FROM message_buffer WHERE type='production_line_deleted' AND data->>'line_uuid' = $1 LIMIT 1`,
            [lineUuid]
        );
        if (r1.rows.length > 0) return true;

        const r2 = await pool.query(
            `SELECT 1 FROM message_buffer
             WHERE type = 'production_batch_deleted'
               AND jsonb_path_exists(
                     COALESCE(data->'lines', '[]'::jsonb),
                     '$[*] ? (@.line_uuid == $luuid)',
                     jsonb_build_object('luuid', $1::text)
                   )
             LIMIT 1`,
            [lineUuid]
        );
        return r2.rows.length > 0;
    } catch (err) {
        console.error(`⚠️ lineWasDeleted error: ${err.message}`);
        return false;
    }
}

/**
 * يعالج حدث production_deleted (حذف كل إنتاج فيشة)
 */
/**
 * ★★★ NEW (full_reconcile): يعيد مجموعة ficha_id الموجودة فعلاً في السيرفر
 * من بين القائمة المُرسَلة من TX (عبر رسائل ficha_saved في message_buffer)
 */
async function findExistingFichaIds(fichaIds) {
    if (!dbAvailable || !fichaIds || !fichaIds.length) return new Set();
    try {
        const result = await pool.query(
            `SELECT DISTINCT (data->>'ficha_id')::text AS fid
             FROM message_buffer
             WHERE type = 'ficha_saved' AND (data->>'ficha_id')::text = ANY($1::text[])`,
            [fichaIds.map(String)]
        );
        return new Set(result.rows.map(r => r.fid));
    } catch (err) {
        console.error(`⚠️ findExistingFichaIds error: ${err.message}`);
        return new Set();
    }
}

/**
 * ★★★ NEW (full_reconcile): يعيد مجموعة line_uuid الموجودة فعلاً في السيرفر
 * من بين القائمة المُرسَلة من TX، بالبحث داخل lines[] لرسائل production_saved
 */
async function findExistingProductionLineUuids(lineUuids) {
    if (!dbAvailable || !lineUuids || !lineUuids.length) return new Set();
    try {
        const result = await pool.query(
            `SELECT DISTINCT l->>'line_uuid' AS lu
             FROM message_buffer mb,
                  LATERAL jsonb_array_elements(COALESCE(mb.data->'lines', '[]'::jsonb)) AS l
             WHERE mb.type = 'production_saved'
               AND l->>'line_uuid' = ANY($1::text[])`,
            [lineUuids]
        );
        return new Set(result.rows.map(r => r.lu));
    } catch (err) {
        console.error(`⚠️ findExistingProductionLineUuids error: ${err.message}`);
        return new Set();
    }
}

/**
 * يعالج حدث production_deleted (حذف كل إنتاج فيشة)
 */
async function handleProductionDeleted(data) {
    const fichaId = data.ficha_id;
    if (!fichaId || !dbAvailable) return;
    try {
        // جلب كل رسائل production_saved لهذه الفيشة
        const result = await pool.query(
            `SELECT data FROM message_buffer
             WHERE type='production_saved' AND (data->>'ficha_id')::text = $1`,
            [String(fichaId)]
        );
        for (const row of result.rows) {
            const d = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
            await handleProductionSaved({ ...d, lines: (d.lines || []).map(l => ({ ...l, quantity: -(parseInt(l.quantity) || 0) })) });
        }
    } catch (err) {
        console.error(`⚠️ handleProductionDeleted basket error: ${err.message}`);
    }
}

// ═══════════════════════════════════════════════════════
// ★★★ Rebuild basket from scratch (utility) ★★★
// يُعيد بناء daily_basket_summary من message_buffer كاملاً
// ═══════════════════════════════════════════════════════
async function rebuildBasketSummary() {
    if (!dbAvailable) return;
    console.log('🔄 Rebuilding daily_basket_summary from message_buffer...');
    try {
        // امسح الجدول أولاً
        await pool.query('DELETE FROM daily_basket_summary');

        // اجلب كل production_saved
        const saved = await pool.query(
            `SELECT data FROM message_buffer WHERE type='production_saved' ORDER BY seq ASC`
        );
        for (const row of saved.rows) {
            const d = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
            await handleProductionSaved(d);
        }

        // طبّق التعديلات
        const edited = await pool.query(
            `SELECT data FROM message_buffer WHERE type='production_line_edited' ORDER BY seq ASC`
        );
        for (const row of edited.rows) {
            const d = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
            await handleProductionEdited(d);
        }

        // طبّق الحذفيات (سطر واحد)
        const deleted = await pool.query(
            `SELECT data FROM message_buffer WHERE type='production_line_deleted' ORDER BY seq ASC`
        );
        for (const row of deleted.rows) {
            const d = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
            await handleProductionLineDeleted(d);
        }

        // ★ طبّق الحذفيات الجماعية (production_batch_deleted)
        const batchDeleted = await pool.query(
            `SELECT data FROM message_buffer WHERE type='production_batch_deleted' ORDER BY seq ASC`
        );
        for (const row of batchDeleted.rows) {
            const d = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
            await handleProductionBatchDeleted(d);
        }

        const countRes = await pool.query('SELECT COUNT(*) as c FROM daily_basket_summary');
        console.log(`✅ Basket rebuild done — ${countRes.rows[0].c} rows`);
    } catch (err) {
        console.error('❌ Basket rebuild error:', err.message);
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

// ═══════════════════════════════════════════════════════
// ★★★ V17-SYNC: broadcastExceptSender ★★★
// ═══════════════════════════════════════════════════════
function broadcastExceptSender(eventName, data, senderSocketId) {
    for (let [id, clientSocket] of io.sockets.sockets) {
        if (id !== senderSocketId) {
            clientSocket.emit(eventName, data);
        }
    }
}

// ═══════════════════════════════════════════════════════
// ★ API endpoints
// ═══════════════════════════════════════════════════════
app.use(express.json());

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/health', async (req, res) => {
    const bufferCount = await getBufferCount();
    const pendingCount = await getPendingCount();
    const range = await getBufferRange();
    res.json({
        status: 'ok', version: 'V18-FULL',
        clients: io.engine.clientsCount,
        machineStatus, lastData,
        bufferSize: bufferCount, pendingAck: pendingCount,
        lastSeq: sequenceCounter,
        oldestSeq: range.oldest, newestSeq: range.newest,
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
            last_seq: sequenceCounter, db_available: dbAvailable
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
        }
        res.json(stats);
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ═══════════════════════════════════════════════════════
// ★★★ /api/daily-basket — السلة اليومية المُحسَّنة ★★★
// يقرأ من daily_basket_summary مباشرة (أسرع وأدق)
// مع fallback للبحث في message_buffer لو الجدول فارغ
// ═══════════════════════════════════════════════════════
app.get('/api/daily-basket', async (req, res) => {
    if (!dbAvailable) {
        return res.status(503).json({ data: [], error: 'DB unavailable' });
    }
    try {
        const { date, tailor, clothes_type } = req.query;

        // ── بناء الاستعلام مع فلاتر اختيارية ──
        let conditions = [];
        let params = [];
        let paramIdx = 1;

        if (date) {
            conditions.push(`date = $${paramIdx++}`);
            params.push(date);
        }
        if (tailor && tailor !== '-- الكل --') {
            conditions.push(`tailor = $${paramIdx++}`);
            params.push(tailor);
        }
        if (clothes_type && clothes_type !== '-- الكل --') {
            conditions.push(`clothes_type = $${paramIdx++}`);
            params.push(clothes_type);
        }

        const whereClause = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

        const result = await pool.query(
            `SELECT
                date::text AS date,
                tailor,
                clothes_type,
                inspector,
                batch_qty,
                defects,
                total_quantity,
                last_updated
             FROM daily_basket_summary
             ${whereClause}
             ORDER BY date DESC, tailor, clothes_type`,
            params
        );

        // إذا كان الجدول فارغاً للفلاتر المحددة، نرجع fallback من message_buffer
        if (result.rows.length === 0) {
            console.log('⚠️ daily_basket_summary empty for filter — falling back to message_buffer scan');
            const fallback = await buildBasketFromBuffer(date, tailor, clothes_type);
            return res.json({ data: fallback, source: 'buffer_fallback' });
        }

        res.json({ data: result.rows, source: 'basket_table' });

    } catch (err) {
        console.error('❌ /api/daily-basket error:', err.message);
        res.status(500).json({ data: [], error: err.message });
    }
});

/**
 * Fallback: يبني السلة من message_buffer عند غياب daily_basket_summary
 */
async function buildBasketFromBuffer(date, tailor, clothesType) {
    try {
        const result = await pool.query(
            `SELECT data FROM message_buffer WHERE type = 'production_saved' ORDER BY seq ASC`
        );
        const summary = {};
        for (const row of result.rows) {
            const d = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
            const t  = d.tailor || '';
            const ct = d.clothes_type || '';
            const lines = d.lines || [];
            for (const line of lines) {
                const lineDate = line.date || '';
                const qty = parseInt(line.quantity) || 0;
                if (!lineDate || qty <= 0) continue;
                if (date && lineDate !== date) continue;
                if (tailor && tailor !== '-- الكل --' && t !== tailor) continue;
                if (clothesType && clothesType !== '-- الكل --' && ct !== clothesType) continue;
                const key = `${lineDate}|${t}|${ct}`;
                if (!summary[key]) {
                    summary[key] = {
                        date: lineDate,
                        tailor: t,
                        clothes_type: ct,
                        total_quantity: 0,
                        inspector: '',
                        batch_qty: '',
                        defects: ''
                    };
                }
                summary[key].total_quantity += qty;
                const { inspector, batch_qty, defects } = parseNotes(line.notes || '');
                if (inspector && !summary[key].inspector) summary[key].inspector = inspector;
                if (batch_qty  && !summary[key].batch_qty)  summary[key].batch_qty  = batch_qty;
                if (defects    && !summary[key].defects)    summary[key].defects    = defects;
            }
        }

        // أيضاً طبّق التعديلات والحذفيات من buffer
        const editedRows = await pool.query(
            `SELECT data FROM message_buffer WHERE type='production_line_edited' ORDER BY seq ASC`
        );
        // (تطبيق مبسط — الـ rebuild الكامل يتم عبر /api/basket/rebuild)

        return Object.values(summary);
    } catch (err) {
        console.error('⚠️ buildBasketFromBuffer error:', err.message);
        return [];
    }
}

// ★ جلب قائمة الخياطين من السلة (للـ combobox في RX)
app.get('/api/daily-basket/tailors', async (req, res) => {
    if (!dbAvailable) return res.json({ tailors: [] });
    try {
        const result = await pool.query(
            `SELECT DISTINCT tailor FROM daily_basket_summary ORDER BY tailor`
        );
        res.json({ tailors: result.rows.map(r => r.tailor) });
    } catch (err) {
        res.status(500).json({ tailors: [], error: err.message });
    }
});

// ★ جلب قائمة أنواع الملابس من السلة (للـ combobox في RX)
app.get('/api/daily-basket/clothes-types', async (req, res) => {
    if (!dbAvailable) return res.json({ clothes_types: [] });
    try {
        const result = await pool.query(
            `SELECT DISTINCT clothes_type FROM daily_basket_summary ORDER BY clothes_type`
        );
        res.json({ clothes_types: result.rows.map(r => r.clothes_type) });
    } catch (err) {
        res.status(500).json({ clothes_types: [], error: err.message });
    }
});

// ★ إعادة بناء السلة من الصفر (admin endpoint)
app.post('/api/basket/rebuild', async (req, res) => {
    if (!dbAvailable) return res.status(503).json({ error: 'DB unavailable' });
    try {
        await rebuildBasketSummary();
        const countRes = await pool.query('SELECT COUNT(*) as c FROM daily_basket_summary');
        res.json({ status: 'ok', rows: parseInt(countRes.rows[0].c) });
    } catch (err) {
        res.status(500).json({ error: err.message });
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

// ★ NEW: إعادة محاولة رسالة من dead_letter_queue — تُعاد إلى message_buffer
app.post('/api/dead-letters/:uuid/retry', async (req, res) => {
    if (!dbAvailable) return res.status(503).json({ status: 'error', message: 'DB unavailable' });
    try {
        const { uuid } = req.params;
        const dl = await pool.query('SELECT * FROM dead_letter_queue WHERE uuid=$1', [uuid]);
        if (!dl.rows.length) return res.status(404).json({ status: 'error', message: 'not_found' });

        const row = dl.rows[0];
        const data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
        const entry = await addToBuffer(row.type, data, row.sender, uuid, null);
        await pool.query('DELETE FROM dead_letter_queue WHERE uuid=$1', [uuid]);

        // بث الرسالة من جديد لكل العملاء المتصلين
        broadcastExceptSender(row.type, {
            ...data, seq: entry.seq, uuid: entry.uuid, hash: entry.hash, _sender: 'server_retry'
        }, null);

        res.json({ status: 'ok', requeued_seq: entry.seq, uuid: entry.uuid });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ★ NEW: حذف رسالة نهائياً من dead_letter_queue
app.delete('/api/dead-letters/:uuid', async (req, res) => {
    if (!dbAvailable) return res.status(503).json({ status: 'error', message: 'DB unavailable' });
    try {
        const result = await pool.query('DELETE FROM dead_letter_queue WHERE uuid=$1', [req.params.uuid]);
        res.json({ status: 'ok', deleted: result.rowCount > 0 });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ★ NEW: فحص حالة سطر إنتاج بالـ line_uuid — موجود؟ محذوف؟
// مفيد للتشخيص اليدوي لمشاكل المطابقة (reconcile)
app.get('/api/production/line/:line_uuid', async (req, res) => {
    if (!dbAvailable) return res.status(503).json({ status: 'error', message: 'DB unavailable' });
    try {
        const lineUuid = req.params.line_uuid;
        const found   = await findOriginalLineByUuid(lineUuid);
        const deleted = await lineWasDeleted(lineUuid);
        res.json({
            status: 'ok',
            line_uuid: lineUuid,
            exists_in_production_saved: !!found,
            deleted,
            data: found ? { tailor: found.tailor, clothes_type: found.clothesType, line: found.origLine } : null
        });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ═══════════════════════════════════════════════════════════════════
// ★★★ V17-SYNC: Socket.IO — Edit/Delete Production Support ★★★
// ═══════════════════════════════════════════════════════════════════
io.on('connection', async (socket) => {
    console.log(`✅ Connected: ${socket.id} | Total: ${io.engine.clientsCount}`);

    const bufferCount = await getBufferCount();
    const pendingCount = await getPendingCount();
    socket.emit('welcome', {
        status: machineStatus, details: lastData,
        last_seq: sequenceCounter, buffer_size: bufferCount,
        pending_ack: pendingCount,
        db_available: dbAvailable,
        server_version: 'V18-FULL'
    });
    socket.emit('machine_status', { status: machineStatus });

    // ═══ get_missed_messages ═══
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

    // ═══ sync_request ═══
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

    // ═══ ACK handling ═══
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

    // ═══ NACK / Resend ═══
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
    // ★★★ command handler ★★★
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
    // ★★★ NEW: handleBatchDelete — production_batch_deleted ★★★
    // معالجة حذف عدة أسطر دفعة واحدة (delete_production_by_date في TX)
    // ═══════════════════════════════════════════════════════════════
    async function handleBatchDelete(data, socket) {
        const clientUuid = data._uuid || data.uuid;
        const clientHash = data._hash || data.hash;
        const batchUuid = data.batch_uuid || clientUuid;

        // Dedup: إن كان هذا UUID قد عُولج من قبل، فقط أعد إرسال الإقرارات
        if (clientUuid && dbAvailable) {
            try {
                const existing = await pool.query('SELECT seq FROM message_buffer WHERE uuid=$1', [clientUuid]);
                if (existing.rows.length > 0) {
                    console.log(`   🔄 Duplicate batch-delete UUID: ${clientUuid.substring(0, 8)}... — re-ack`);
                    socket.emit('server_ack', {
                        seq: existing.rows[0].seq, uuid: clientUuid, server_uuid: clientUuid,
                        original_action: 'production_batch_deleted', status: 'already_exists'
                    });
                    socket.emit('batch_delete_ack', {
                        batch_uuid: batchUuid, seq: existing.rows[0].seq, status: 'already_exists'
                    });
                    return;
                }
            } catch (err) { /* continue */ }
        }

        const cleanData = { ...data };
        delete cleanData._uuid; delete cleanData._hash;
        delete cleanData.uuid; delete cleanData.hash;

        const entry = await addToBuffer('production_batch_deleted', cleanData, socket.id, clientUuid, clientHash);

        // ★★★ تحديث جدول السلة اليومية لكل سطر في الدفعة ★★★
        try {
            await handleProductionBatchDeleted(cleanData);
            console.log(`   🛒 Basket updated (batch-delete): ${(cleanData.lines || []).length} line(s)`);
        } catch (basketErr) {
            console.error(`⚠️ Basket update error for production_batch_deleted: ${basketErr.message}`);
        }

        const broadcastData = {
            ...cleanData,
            seq: entry.seq, uuid: entry.uuid, hash: entry.hash,
            _sender: socket.id
        };

        console.log(`📡 Broadcasting production_batch_deleted to others [seq=${entry.seq}, batch=${(batchUuid || '').substring(0, 8)}...]`);
        broadcastExceptSender('production_batch_deleted', broadcastData, socket.id);
        broadcastExceptSender('update_ui', {
            status: machineStatus, details: lastData,
            seq: entry.seq, uuid: entry.uuid
        }, socket.id);

        // إقرار عام
        socket.emit('server_ack', {
            seq: entry.seq, uuid: clientUuid || entry.uuid,
            server_uuid: entry.uuid,
            original_action: 'production_batch_deleted', status: 'stored_in_db'
        });

        // ★ إقرار مخصص ينتظره TX (delete_log.server_confirmed)
        socket.emit('batch_delete_ack', {
            batch_uuid: batchUuid, seq: entry.seq, uuid: entry.uuid, status: 'stored_in_db'
        });

        console.log(`   ✅ production_batch_deleted [seq=${entry.seq}, batch=${(batchUuid || '').substring(0, 8)}...] stored+broadcast+ack`);
    }

    // ═══════════════════════════════════════════════════════════════
    // ★★★ V17-SYNC: handleDirectEvent — generic handler ★★★
    // ═══════════════════════════════════════════════════════════════
    async function handleDirectEvent(eventName, data, socket) {
        const clientUuid = data._uuid || data.uuid;
        const clientHash = data._hash || data.hash;

        // Dedup check by UUID — but for production_saved we ALLOW re-insert (edit)
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

        // ★★★ تحديث جدول السلة اليومية ★★★
        try {
            if (eventName === 'production_saved') {
                await handleProductionSaved(cleanData);
                console.log(`   🛒 Basket updated (saved): ${cleanData.tailor}/${cleanData.clothes_type}`);
            } else if (eventName === 'production_line_edited') {
                await handleProductionEdited(cleanData);
            } else if (eventName === 'production_line_deleted') {
                await handleProductionLineDeleted(cleanData);
            } else if (eventName === 'production_deleted') {
                await handleProductionDeleted(cleanData);
            }
        } catch (basketErr) {
            console.error(`⚠️ Basket update error for ${eventName}: ${basketErr.message}`);
        }

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

        console.log(`   ✅ ${eventName} [seq=${entry.seq}, uuid=${entry.uuid.substring(0, 8)}...] stored+broadcast`);
    }

    // ═══ Dead letter ═══
    socket.on('dead_letter', async (payload) => {
        const { uuid, type, data, reason, retry_count } = payload;
        console.log(`💀 [${socket.id}] dead_letter: uuid=${uuid?.substring(0, 8)}... type=${type} retries=${retry_count}`);
        if (uuid) {
            await moveToDeadLetter(uuid, type || 'unknown', data || {}, socket.id, reason || 'max_retries', retry_count || 0);
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // ★★★ V17-SYNC: Direct event handlers ★★★
    // ═══════════════════════════════════════════════════════════════

    socket.on('ficha_saved', async (data) => {
        console.log(`📋 [${socket.id}] ficha_saved`);
        await handleDirectEvent('ficha_saved', data, socket);
    });

    socket.on('ficha_deleted', async (data) => {
        console.log(`🗑️ [${socket.id}] ficha_deleted`);
        await handleDirectEvent('ficha_deleted', data, socket);
    });

    // ★★★ V17-SYNC: production_saved — allows UUID re-use for edits ★★★
    socket.on('production_saved', async (data) => {
        console.log(`🧺 [${socket.id}] production_saved (edit-aware)`);
        await handleDirectEvent('production_saved', data, socket);
    });

    socket.on('production_deleted', async (data) => {
        console.log(`🗑️ [${socket.id}] production_deleted`);
        await handleDirectEvent('production_deleted', data, socket);
    });

    // ★★★ V17-SYNC: NEW EVENT — production_line_deleted ★★★
    socket.on('production_line_deleted', async (data) => {
        console.log(`🗑️🔴 [${socket.id}] production_line_deleted uuid=${(data._uuid || data.uuid || data.line_uuid || '?').substring(0, 8)}...`);
        await handleDirectEvent('production_line_deleted', data, socket);
    });

    // ★★★ V17-SYNC: NEW EVENT — production_line_edited ★★★
    socket.on('production_line_edited', async (data) => {
        console.log(`✏️ [${socket.id}] production_line_edited uuid=${(data._uuid || data.uuid || data.line_uuid || '?').substring(0, 8)}...`);
        await handleDirectEvent('production_line_edited', data, socket);
    });

    // ═══════════════════════════════════════════════════════════════
    // ★★★ NEW: production_batch_deleted — حذف دفعة أسطر (حذف بالتاريخ) ★★★
    // ═══════════════════════════════════════════════════════════════
    socket.on('production_batch_deleted', async (data) => {
        console.log(`🗑️📦 [${socket.id}] production_batch_deleted batch=${(data.batch_uuid || data._uuid || data.uuid || '?').substring(0, 8)}... lines=${(data.lines || []).length}`);
        await handleBatchDelete(data, socket);
    });

    // ═══════════════════════════════════════════════════════════════
    // ★★★ NEW: rx_delete_confirmed — RX يبلغ السيرفر باستلام الحذف ★★★
    // السيرفر يُعيد توجيه التأكيد إلى TX (المُرسل الأصلي للدفعة)
    // ═══════════════════════════════════════════════════════════════
    socket.on('rx_delete_confirmed', async (payload) => {
        const batchUuid = payload && payload.batch_uuid;
        console.log(`✅📥 [${socket.id}] rx_delete_confirmed batch=${(batchUuid || '?').substring(0, 8)}...`);
        if (!batchUuid || !dbAvailable) return;
        try {
            const result = await pool.query(
                `SELECT sender FROM message_buffer WHERE type='production_batch_deleted' AND data->>'batch_uuid'=$1 LIMIT 1`,
                [batchUuid]
            );
            if (result.rows.length > 0 && result.rows[0].sender && result.rows[0].sender !== socket.id) {
                io.to(result.rows[0].sender).emit('rx_delete_confirmed', { batch_uuid: batchUuid, confirmed: true });
                console.log(`   📤 Forwarded rx_delete_confirmed → sender=${result.rows[0].sender}`);
            }
        } catch (err) {
            console.error(`⚠️ rx_delete_confirmed error: ${err.message}`);
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // ★★★ NEW: reconcile_delete_request — مطابقة حذف دفعة معينة ★★★
    // TX يسأل: هل هذه الأسطر (line_uuids) محذوفة فعلاً عند السيرفر؟
    // ═══════════════════════════════════════════════════════════════
    socket.on('reconcile_delete_request', async (payload) => {
        const batchUuid  = payload && payload.batch_uuid;
        const fichaId    = payload && payload.ficha_id;
        const lineUuids  = (payload && Array.isArray(payload.line_uuids)) ? payload.line_uuids : [];
        console.log(`🩺 [${socket.id}] reconcile_delete_request batch=${(batchUuid || '?').substring(0, 8)}... lines=${lineUuids.length}`);

        const stillExisting = [];
        if (dbAvailable) {
            for (const lu of lineUuids) {
                try {
                    const deleted = await lineWasDeleted(lu);
                    if (!deleted) stillExisting.push(lu);
                } catch (err) { /* تجاهل وحاول التالي */ }
            }
        }

        socket.emit('reconcile_delete_response', {
            batch_uuid: batchUuid,
            ficha_id: fichaId,
            still_existing: stillExisting,
            all_deleted: stillExisting.length === 0
        });
        console.log(`   🩺 reconcile_delete_response → still_existing=${stillExisting.length}/${lineUuids.length}`);
    });

    // ═══════════════════════════════════════════════════════════════
    // ★★★ NEW: full_reconcile_request (RX⟷TX عبر السيرفر) ★★★
    // TX يرسل قوائم محلية (ficha_ids, production_uuids) على دفعات،
    // والسيرفر يردّ بما هو "غائب" عن السيرفر ليُعاد إرساله.
    // ═══════════════════════════════════════════════════════════════
    socket.on('full_reconcile_request', async (payload) => {
        const requestId       = payload && payload.request_id;
        const chunkIndex      = (payload && payload.chunk_index) || 0;
        const chunkCount      = (payload && payload.chunk_count) || 1;
        const fichaIds        = (payload && Array.isArray(payload.ficha_ids)) ? payload.ficha_ids : [];
        const productionUuids = (payload && Array.isArray(payload.production_uuids)) ? payload.production_uuids : [];

        console.log(`🩺 [${socket.id}] full_reconcile_request ${chunkIndex + 1}/${chunkCount} fichas=${fichaIds.length} prod=${productionUuids.length} req=${requestId}`);

        let missingFichas = [];
        let missingProduction = [];
        if (dbAvailable) {
            const existingFichas = await findExistingFichaIds(fichaIds);
            missingFichas = fichaIds.filter(fid => !existingFichas.has(String(fid)));

            const existingProd = await findExistingProductionLineUuids(productionUuids);
            missingProduction = productionUuids.filter(lu => !existingProd.has(lu));
        }

        socket.emit('full_reconcile_response', {
            request_id: requestId,
            chunk_index: chunkIndex,
            chunk_count: chunkCount,
            missing_fichas: missingFichas,
            missing_production: missingProduction
        });
        console.log(`   🩺 full_reconcile_response → missing_fichas=${missingFichas.length}, missing_production=${missingProduction.length}`);
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

// ═══════════════════════════════════════════════════════
// ★★★ Start Server ★★★
// ═══════════════════════════════════════════════════════
const PORT = process.env.PORT || 10000;

async function startServer() {
    await initDatabase();
    await loadStateFromDB();
    await cleanOldMessages();

    // بناء السلة من message_buffer إذا كان daily_basket_summary فارغاً
    if (dbAvailable) {
        try {
            const basketCount = await pool.query('SELECT COUNT(*) as c FROM daily_basket_summary');
            if (parseInt(basketCount.rows[0].c) === 0) {
                console.log('🔄 daily_basket_summary is empty — rebuilding from message_buffer...');
                await rebuildBasketSummary();
            }
        } catch (err) {
            console.error('⚠️ Basket init check failed:', err.message);
        }
    }

    server.listen(PORT, () => {
        console.log('═'.repeat(65));
        console.log(`🚀 Server V18-FULL ready on port ${PORT}`);
        console.log(`💾 PostgreSQL: ${dbAvailable ? '✅' : '❌'}`);
        console.log(`🔐 UUID + Server-side Hash + SEQ`);
        console.log(`📡 broadcastExceptSender() — no echo back`);
        console.log(`🔁 NACK/Resend + Dead Letter Queue (+ retry/delete API)`);
        console.log(`📬 ON CONFLICT (uuid) DO UPDATE — edit support`);
        console.log(`✏️ production_line_edited event — sync edits`);
        console.log(`🗑️ production_line_deleted event — sync deletes`);
        console.log(`🗑️📦 production_batch_deleted + batch_delete_ack — حذف بالتاريخ`);
        console.log(`✅📥 rx_delete_confirmed — تأكيد RX يُعاد توجيهه إلى TX`);
        console.log(`🩺 reconcile_delete_request/response — مطابقة دفعة حذف`);
        console.log(`🩺 full_reconcile_request/response — مطابقة شاملة RX⟷TX`);
        console.log(`🔧 FIX: jsonb_path_exists بدلاً من LIKE '%uuid%' (basket lookups)`);
        console.log(`🛒 /api/daily-basket — PostgreSQL basket table`);
        console.log(`🛒 /api/daily-basket/tailors — tailors list`);
        console.log(`🛒 /api/daily-basket/clothes-types — clothes list`);
        console.log(`🔄 /api/basket/rebuild — force rebuild`);
        console.log(`🔍 /api/production/line/:line_uuid — debug lookup`);
        console.log(`💀 /api/dead-letters [+ /:uuid/retry, DELETE /:uuid]`);
        console.log(`🔌 pingTimeout=120s, pingInterval=25s`);
        console.log('═'.repeat(65));
    });
}

startServer().catch(err => {
    console.error('❌ Startup failed:', err);
    process.exit(1);
});