
'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const rooms = new Map();
const startedAt = Date.now();
let lastRequestAt = Date.now();
let lastConnectionAt = null;
let totalConnections = 0;
let failedMessages = 0;

const VERSION = 'powerplayx-room-server-2.0';

app.use((req, res, next) => {
  lastRequestAt = Date.now();
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

function countConnections() {
  let count = 0;
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) count++;
  }
  return count;
}

app.get('/', (_req, res) => {
  res.send('POWERPLAY X server is online');
});

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    status: 'online',
    version: VERSION,
    rooms: rooms.size,
    connections: countConnections(),
    totalConnections,
    failedMessages,
    uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
    startedAt: new Date(startedAt).toISOString(),
    lastRequestAt: new Date(lastRequestAt).toISOString(),
    lastConnectionAt: lastConnectionAt
      ? new Date(lastConnectionAt).toISOString()
      : null,
    checkedAt: new Date().toISOString()
  });
});

function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function roomInfo(room) {
  const record = rooms.get(room);
  return {
    type: 'roomUpdate',
    room,
    players: record ? record.players.size : 0,
    format: record ? record.format : '1v1'
  };
}

function broadcast(room, payload, exclude = null) {
  const record = rooms.get(room);
  if (!record) return;

  for (const peer of record.players) {
    if (peer !== exclude) send(peer, payload);
  }
}

function detach(ws) {
  if (!ws.room) return;

  const room = ws.room;
  ws.room = null;

  const record = rooms.get(room);
  if (!record) return;

  record.players.delete(ws);

  if (record.players.size === 0) {
    rooms.delete(room);
  } else {
    broadcast(room, roomInfo(room));
  }
}

function makeCode() {
  let code;

  do {
    code = String(Math.floor(100000 + Math.random() * 900000));
  } while (rooms.has(code));

  return code;
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  lastConnectionAt = Date.now();
  totalConnections++;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      failedMessages++;
      return send(ws, {
        type: 'error',
        message: 'Invalid JSON'
      });
    }

    if (!msg || typeof msg.type !== 'string') {
      failedMessages++;
      return;
    }

    if (msg.type === 'create') {
      detach(ws);

      const room = makeCode();
      const record = {
        players: new Set([ws]),
        format: typeof msg.format === 'string' ? msg.format : '1v1',
        state: null,
        createdAt: Date.now()
      };

      rooms.set(room, record);
      ws.room = room;
      ws.seat = 0;

      return send(ws, {
        type: 'created',
        room,
        seat: 0,
        players: 1,
        format: record.format
      });
    }

    if (msg.type === 'join') {
      const room = String(msg.room || '').trim();
      const record = rooms.get(room);

      if (!/^\d{6}$/.test(room) || !record) {
        return send(ws, {
          type: 'error',
          message: 'Room not found. Check the 6-digit code and try again.'
        });
      }

      if (record.players.size >= 10) {
        return send(ws, {
          type: 'error',
          message: 'Room is full (maximum 10 players).'
        });
      }

      detach(ws);

      ws.room = room;
      ws.seat = record.players.size;
      record.players.add(ws);

      send(ws, {
        type: 'joined',
        room,
        seat: ws.seat,
        players: record.players.size,
        format: record.format
      });

      broadcast(room, roomInfo(room), ws);

      if (record.state) {
        send(ws, { type: 'start', state: record.state });
      }

      return;
    }

    const room = ws.room;
    const record = room && rooms.get(room);

    if (!record) {
      return send(ws, {
        type: 'error',
        message: 'Create or join a room first.'
      });
    }

    if (msg.type === 'format') {
      if (ws.seat !== 0) {
        return send(ws, {
          type: 'error',
          message: 'Only the host can change format.'
        });
      }

      if (typeof msg.format === 'string') {
        record.format = msg.format;
      }

      broadcast(room, {
        type: 'format',
        format: record.format
      });

      broadcast(room, roomInfo(room));
      return;
    }

    if (msg.type === 'state') {
      if (ws.seat !== 0) {
        return send(ws, {
          type: 'error',
          message: 'Only the host can publish game state.'
        });
      }

      record.state = msg.state || null;

      broadcast(room, {
        type: 'state',
        state: record.state
      }, ws);

      return;
    }

    if (msg.type === 'bidRequest' || msg.type === 'passRequest') {
      const host = [...record.players].find(
        (player) => player.seat === 0
      );

      if (host) {
        send(host, {
          type: msg.type,
          seat: ws.seat,
          step: Number(msg.step) || 1
        });
      }

      return;
    }

    if (msg.type === 'chat') {
      broadcast(room, {
        type: 'chat',
        name: String(msg.name || 'Player').slice(0, 20),
        text: String(msg.text || '').slice(0, 300)
      }, ws);

      return;
    }

    send(ws, {
      type: 'error',
      message: 'Unsupported message type: ' + msg.type
    });
  });

  ws.on('close', () => detach(ws));
  ws.on('error', () => detach(ws));
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }

    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

wss.on('close', () => clearInterval(heartbeat));

const port = process.env.PORT || 10000;

server.listen(port, '0.0.0.0', () => {
  console.log(`POWERPLAY X server listening on ${port}`);
});
          
