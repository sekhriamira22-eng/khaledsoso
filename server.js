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

        // Indexes
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

        console.log('✅ Database initialized (V17-SYNC)');
        const countResult = await client.query('SELECT COUNT(*) as count FROM message_buffer');
        const seqResult = await client.query('SELECT current_seq FROM sequence_counter WHERE id=1');
        const pendingResult = await client.query("SELECT COUNT(*) as count FROM message_buffer WHERE ack_status='pending'");
        console.log(`   📦 Total messages: ${countResult.rows[0].count}`);
        console.log(`   🔢 Last SEQ: ${seqResult.rows[0]?.current_seq || 0}`);
        console.log(`   ⏳ Pending ACK: ${pendingResult.rows[0].count}`);
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
        status: 'ok', version: 'V17-SYNC',
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

app.get('/api/dead-letters', async (req, res) => {
    if (!dbAvailable) return res.json({ status: 'ok', count: 0, messages: [] });
    try {
        const result = await pool.query('SELECT * FROM dead_letter_queue ORDER BY created_at DESC LIMIT 100');
        res.json({ status: 'ok', count: result.rows.length, messages: result.rows });
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
        server_version: 'V17-SYNC'
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
    // ★★★ V17-SYNC: handleDirectEvent — generic handler ★★★
    // ═══════════════════════════════════════════════════════════════
    async function handleDirectEvent(eventName, data, socket) {
        const clientUuid = data._uuid || data.uuid;
        const clientHash = data._hash || data.hash;

        // Dedup check by UUID — but for production_saved we ALLOW re-insert (edit)
        // Only skip if it's NOT an edit-capable event
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
    // Broadcasts deletion of a single production line by msg_uuid
    socket.on('production_line_deleted', async (data) => {
        console.log(`🗑️🔴 [${socket.id}] production_line_deleted uuid=${(data._uuid || data.uuid || data.line_uuid || '?').substring(0, 8)}...`);
        await handleDirectEvent('production_line_deleted', data, socket);
    });

    // ★★★ V17-SYNC: NEW EVENT — production_line_edited ★★★
    // Broadcasts edit of a single production line — same UUID, new quantity
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

// ═══════════════════════════════════════════════════════
// ★★★ Start Server ★★★
// ═══════════════════════════════════════════════════════
const PORT = process.env.PORT || 10000;

async function startServer() {
    await initDatabase();
    await loadStateFromDB();
    await cleanOldMessages();

    server.listen(PORT, () => {
        console.log('═'.repeat(65));
        console.log(`🚀 Server V17-SYNC ready on port ${PORT}`);
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
        console.log('═'.repeat(65));
    });
}

startServer().catch(err => {
    console.error('❌ Startup failed:', err);
    process.exit(1);
});