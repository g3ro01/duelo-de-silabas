// Relevo en tiempo real para jugar en línea: conecta a los niños que escriben el mismo código.
// No guarda nada: cada sala vive en memoria mientras haya alguien conectado.
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const ALLOWED = (process.env.ALLOWED_ORIGINS || "https://duelo-de-silabas.onrender.com").split(",").map(s => s.trim());
const LOCAL = /^(https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?|file:\/\/|null)$/;
const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const MAX_PEERS = 8, MAX_ROOMS = 500, MAX_PRESENCE = 4096, MAX_MSG_PER_SEC = 40;

const rooms = new Map(); // nombre → Set de sockets
let nextId = 1;

const server = http.createServer((req, res) => {
  if (req.url === "/health") { res.writeHead(200, { "content-type": "text/plain", "access-control-allow-origin": "*" }); return res.end("ok"); }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({
  server, maxPayload: 8192,
  verifyClient: ({ origin }) => ALLOWED.includes(origin) || (process.env.ALLOW_LOCAL === "1" && LOCAL.test(origin || "null"))
});

const send = (ws, msg) => { if (ws.readyState === 1) ws.send(JSON.stringify(msg)); };
function broadcast(name){
  const set = rooms.get(name); if (!set) return;
  const peers = [...set].map(s => ({ peer: s.peer, presence: s.presence }));
  set.forEach(s => send(s, { t: "peers", room: name, peers }));
}
function leave(ws){
  if (!ws.room) return;
  const name = ws.room, set = rooms.get(name); ws.room = null;
  if (set) { set.delete(ws); if (!set.size) rooms.delete(name); else broadcast(name); }
}

wss.on("connection", ws => {
  ws.peer = "p" + (nextId++).toString(36) + Math.random().toString(36).slice(2, 8);
  ws.presence = {}; ws.room = null; ws.alive = true; ws.budget = { t: Date.now(), n: 0 };
  send(ws, { t: "hello", peer: ws.peer });
  ws.on("pong", () => { ws.alive = true; });
  ws.on("message", raw => {
    const now = Date.now();
    if (now - ws.budget.t > 1000) ws.budget = { t: now, n: 0 };
    if (++ws.budget.n > MAX_MSG_PER_SEC) return; // demasiados mensajes: se descartan
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== "object") return;
    if (m.t === "join") {
      if (typeof m.room !== "string" || !ROOM_RE.test(m.room)) return send(ws, { t: "error", code: "invalid_argument" });
      leave(ws);
      if (!rooms.has(m.room) && rooms.size >= MAX_ROOMS) return send(ws, { t: "error", code: "limit_reached" });
      const set = rooms.get(m.room) || new Set();
      if (set.size >= MAX_PEERS) return send(ws, { t: "error", code: "limit_reached" });
      set.add(ws); rooms.set(m.room, set); ws.room = m.room;
      send(ws, { t: "joined", room: m.room }); broadcast(m.room);
    } else if (m.t === "presence") {
      if (!m.patch || typeof m.patch !== "object" || Array.isArray(m.patch)) return;
      const merged = { ...ws.presence };
      for (const [k, v] of Object.entries(m.patch)) { if (!/^[A-Za-z_][A-Za-z0-9_-]{0,63}$/.test(k)) continue; if (v === null) delete merged[k]; else merged[k] = v; }
      if (JSON.stringify(merged).length > MAX_PRESENCE) return send(ws, { t: "error", code: "invalid_argument" });
      ws.presence = merged; if (ws.room) broadcast(ws.room);
    } else if (m.t === "leave") leave(ws);
  });
  ws.on("close", () => leave(ws));
});

// Corta conexiones muertas cada 25 s
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 25000).unref();
setInterval(() => console.log(`salas=${rooms.size} conexiones=${wss.clients.size}`), 600000).unref();

server.listen(PORT, () => console.log("Relevo de Duelo de Sílabas escuchando en el puerto " + PORT));
