
const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const rooms = new Map();

app.get("/", (req, res) => {
  res.send("POWERPLAY X server is online");
});

app.get("/health", (req, res) => {
  res.json({ ok: true, rooms: rooms.size });
});

wss.on("connection", (ws) => {
  ws.on("message", (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === "join") {
      const room = String(msg.room || "").trim();

      if (!/^[A-Za-z0-9-]{4,12}$/.test(room)) {
        ws.send(JSON.stringify({
          type: "error",
          message: "Invalid room code"
        }));
        return;
      }

      ws.room = room;

      if (!rooms.has(room)) rooms.set(room, new Set());
      rooms.get(room).add(ws);

      ws.send(JSON.stringify({
        type: "joined",
        room,
        players: rooms.get(room).size
      }));

      broadcast(room, {
        type: "players",
        count: rooms.get(room).size
      }, ws);

      return;
    }

    if (!ws.room || !rooms.has(ws.room)) return;

    const allowed = [
      "ready", "teamSelect", "bid",
      "gameState", "startAuction", "chat"
    ];

    if (!allowed.includes(msg.type)) return;

    broadcast(ws.room, {
      type: msg.type,
      data: msg.data
    }, ws);
  });

  ws.on("close", () => {
    if (!ws.room || !rooms.has(ws.room)) return;

    const room = ws.room;
    const players = rooms.get(room);

    players.delete(ws);

    if (players.size === 0) {
      rooms.delete(room);
    } else {
      broadcast(room, {
        type: "players",
        count: players.size
      });
    }
  });
});

function broadcast(room, message, exclude = null) {
  const players = rooms.get(room);
  if (!players) return;

  const data = JSON.stringify(message);

  for (const player of players) {
    if (player !== exclude &&
        player.readyState === WebSocket.OPEN) {
      player.send(data);
    }
  }
}

const PORT = process.env.PORT || 10000;

server.listen(PORT, "0.0.0.0", () => {
  console.log("POWERPLAY X server running on port " + PORT);
});
