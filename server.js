'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({
  server,
  perMessageDeflate: false,
  maxPayload: 1024 * 1024
});

const rooms = new Map();
const startedAt = Date.now();
let totalConnections = 0;
let failedMessages = 0;
let lastConnectionAt = null;

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

function open(ws) {
  return ws && ws.readyState === WebSocket.OPEN;
}
function send(ws, data) {
  if (open(ws)) ws.send(JSON.stringify(data));
}
function makeCode() {
  let code;
  do { code = String(100000 + Math.floor(Math.random() * 900000)); }
  while (rooms.has(code));
  return code;
}
function getRoomPlayers(room) {
  return [...room.players.values()]
    .sort((a, b) => a.seat - b.seat)
    .map(p => ({ id: p.id, name: p.name, seat: p.seat, connected: open(p.ws) }));
}
function roomUpdate(code) {
  const room = rooms.get(code);
  if (!room) return;
  const players = getRoomPlayers(room);
  const message = {
    type: 'roomUpdate',
    room: code,
    players: players.length,
    playerList: players,
    format: room.format
  };
  for (const p of room.players.values()) send(p.ws, message);
}
function detach(ws) {
  if (!ws.room) return;
  const code = ws.room;
  const room = rooms.get(code);
  ws.room = null;
  if (!room) return;
  const player = room.players.get(ws.clientId);
  if (player && player.ws === ws) player.ws = null;
  // Keep disconnected seats for 2 minutes so mobile backgrounding can reconnect.
  player && (player.disconnectedAt = Date.now());
  roomUpdate(code);
  if (![...room.players.values()].some(p => p.ws)) {
    room.emptySince = Date.now();
  }
}
function attach(ws, code, player) {
  const room = rooms.get(code);
  if (!room) return false;
  if (player.ws && player.ws !== ws) {
    try { player.ws.close(4001, 'Reconnected on another connection'); } catch (_) {}
  }
  player.ws = ws;
  player.disconnectedAt = null;
  ws.room = code;
  ws.clientId = player.id;
  ws.seat = player.seat;
  return true;
}
function sendJoined(ws, type, code, player, room) {
  send(ws, {
    type, room: code, seat: player.seat,
    players: room.players.size, playerList: getRoomPlayers(room),
    format: room.format, state: room.state || null
  });
}
function cleanRoom(code) {
  const room = rooms.get(code);
  if (!room) return;
  const now = Date.now();
  for (const [id, p] of room.players) {
    if (!p.ws && p.disconnectedAt && now - p.disconnectedAt > 120000) {
      room.players.delete(id);
    }
  }
  if (!room.players.size || (room.emptySince && now - room.emptySince > 120000)) {
    rooms.delete(code);
  }
}

app.get('/', (_req, res) => res.send('POWERPLAY X multiplayer server is online'));
app.get('/health', (_req, res) => {
  let connections = 0;
  for (const ws of wss.clients) if (open(ws)) connections++;
  res.json({
    ok: true,
    status: 'online',
    version: 'powerplayx-stable-3.0',
    rooms: rooms.size,
    connections,
    totalConnections,
    failedMessages,
    uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
    startedAt: new Date(startedAt).toISOString(),
    lastConnectionAt: lastConnectionAt ? new Date(lastConnectionAt).toISOString() : null,
    checkedAt: new Date().toISOString()
  });
});

wss.on('connection', ws => {
  ws.isAlive = true;
  totalConnections++;
  lastConnectionAt = Date.now();
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch (_) { failedMessages++; return send(ws, { type: 'error', message: 'Invalid JSON' }); }
    if (!msg || typeof msg.type !== 'string') return;

    const clientId = String(msg.clientId || ws.clientId || '').slice(0, 80);
    const name = String(msg.name || 'Player').trim().slice(0, 18) || 'Player';

    if (msg.type === 'create') {
      const id = clientId || crypto.randomUUID();
      // Idempotency: if a create acknowledgement was lost, reuse the same room.
      for (const [existingCode, existingRoom] of rooms) {
        const existingPlayer = existingRoom.players.get(id);
        if (existingPlayer) {
          attach(ws, existingCode, existingPlayer);
          sendJoined(ws, 'rejoined', existingCode, existingPlayer, existingRoom);
          roomUpdate(existingCode);
          if (existingRoom.state) send(ws, { type: 'state', state: existingRoom.state });
          return;
        }
      }
      const code = makeCode();
      const player = { id, name, seat: 0, ws, disconnectedAt: null };
      const room = {
        players: new Map([[id, player]]),
        format: typeof msg.format === 'string' ? msg.format : '1v1',
        state: null,
        createdAt: Date.now(),
        emptySince: null
      };
      rooms.set(code, room);
      ws.room = code; ws.clientId = id; ws.seat = 0;
      return sendJoined(ws, 'created', code, player, room);
    }

    if (msg.type === 'join' || msg.type === 'rejoin') {
      const code = String(msg.room || '').trim();
      const room = rooms.get(code);
      if (!/^\d{6}$/.test(code) || !room) {
        return send(ws, { type: 'error', message: 'Room not found. Check the 6-digit code.' });
      }
      const id = clientId || crypto.randomUUID();
      let player = room.players.get(id);

      if (!player && msg.type === 'rejoin') {
        return send(ws, { type: 'error', message: 'Your previous seat expired. Join the room again.' });
      }
      if (!player) {
        if (room.players.size >= 10) {
          return send(ws, { type: 'error', message: 'Room is full (maximum 10 players).' });
        }
        const used = new Set([...room.players.values()].map(p => p.seat));
        let seat = 0;
        while (used.has(seat)) seat++;
        player = { id, name, seat, ws, disconnectedAt: null };
        room.players.set(id, player);
      } else {
        player.name = name;
      }

      attach(ws, code, player);
      sendJoined(ws, msg.type === 'rejoin' ? 'rejoined' : 'joined', code, player, room);
      roomUpdate(code);
      if (room.state) send(ws, { type: 'state', state: room.state });
      return;
    }

    const code = ws.room;
    const room = code && rooms.get(code);
    if (!room) return send(ws, { type: 'error', message: 'Create or join a room first.' });

    if (msg.type === 'format') {
      if (ws.seat !== 0) return send(ws, { type: 'error', message: 'Only the host can change format.' });
      if (typeof msg.format === 'string') room.format = msg.format.slice(0, 8);
      roomUpdate(code);
      return;
    }
    if (msg.type === 'state') {
      if (ws.seat !== 0) return send(ws, { type: 'error', message: 'Only the host can publish game state.' });
      room.state = msg.state || null;
      for (const p of room.players.values()) if (p.ws !== ws) send(p.ws, { type: 'state', state: room.state });
      return;
    }
    if (msg.type === 'bidRequest' || msg.type === 'passRequest') {
      const host = [...room.players.values()].find(p => p.seat === 0);
      if (host) send(host.ws, { type: msg.type, seat: ws.seat, step: Number(msg.step) || 1, name });
      return;
    }
    if (msg.type === 'chat') {
      const text = String(msg.text || '').slice(0, 240);
      for (const p of room.players.values()) {
        if (p.ws !== ws) send(p.ws, { type: 'chat', name, text });
      }
      return;
    }
    send(ws, { type: 'error', message: 'Unsupported message type.' });
  });

  ws.on('close', () => detach(ws));
  ws.on('error', () => detach(ws));
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) { ws.terminate(); }
  }
  for (const code of rooms.keys()) cleanRoom(code);
}, 20000);

server.on('close', () => clearInterval(heartbeat));
const port = process.env.PORT || 10000;
server.listen(port, '0.0.0.0', () => {
  console.log(`POWERPLAY X stable multiplayer server listening on ${port}`);
});
