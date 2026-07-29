// server.js
// Phase 1: room-code plumbing only. No gameplay/sync logic lives here yet --
// that's Phase 2+. This just proves rooms can be created, joined, and that
// both clients see a live player list.

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// ---------- In-memory room store ----------
// { roomCode: { hostId, players: [{id, name}], seed, level, createdAt } }
const rooms = new Map();

// Render's free tier spins the process down when idle -- that's fine here
// since rooms are meant to be short-lived (one match), but it does mean
// this store is NOT durable. Don't rely on it surviving a restart.

function makeRoomCode() {
  // 4 letters, unambiguous-ish alphabet (no O/0, I/1 confusion avoided by
  // just sticking to letters). Retry on collision.
  const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; // no I, O
  let code;
  do {
    code = Array.from({ length: 4 }, () => ALPHABET[Math.floor(Math.random() * ALPHABET.length)]).join('');
  } while (rooms.has(code));
  return code;
}

function makeClientId() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

// How far in the future match_starting's startAt is set. Clients count
// down to this shared instant; keep it long enough for the broadcast to
// reach every client with room to spare, short enough that starting a
// match doesn't feel laggy.
const START_COUNTDOWN_MS = 3000;

// ---------- Room reaping ----------
// Rooms should live only as long as a lobby+match reasonably takes. Anything
// left over (host closed the tab without a clean disconnect, browser crash,
// etc.) should eventually get swept so `rooms` doesn't grow forever.
const ROOM_MAX_AGE_MS = 2 * 60 * 60 * 1000; // 2 hours
const ROOM_SWEEP_INTERVAL_MS = 10 * 60 * 1000; // check every 10 min

function sweepStaleRooms() {
  const now = Date.now();
  for (const [roomCode, room] of rooms) {
    const stale = (now - room.createdAt) > ROOM_MAX_AGE_MS;
    // Also reap rooms whose sockets have all silently died (readyState !== OPEN)
    // even if a `close` event never fired for some reason.
    const allDead = room.players.length > 0 && room.players.every(p => !p.ws || p.ws.readyState !== p.ws.OPEN);
    if (stale || allDead) {
      for (const p of room.players) {
        if (p.ws && p.ws.readyState === p.ws.OPEN) {
          try { p.ws.close(); } catch (e) { /* ignore */ }
        }
      }
      rooms.delete(roomCode);
    }
  }
}

setInterval(sweepStaleRooms, ROOM_SWEEP_INTERVAL_MS).unref();

// ---------- Per-connection rate limiting ----------
// Cheap defense against a buggy or malicious client flooding the room with
// messages (e.g. peer_pos spam). Not meant to be bulletproof -- just enough
// that one bad client can't hammer the event loop or other players' sockets.
const RATE_LIMIT_WINDOW_MS = 1000;
const RATE_LIMIT_MAX_MESSAGES = 40; // generous headroom above the ~12.5/s peer_pos cadence
const MAX_MESSAGE_BYTES = 4 * 1024; // 4KB is far more than any message here needs

function makeSeed() {
  // Seed lives entirely server-side now (Phase 1 had the client generate
  // it). Doesn't need to be cryptographically strong -- just unique enough
  // that two rooms never collide.
  return Date.now().toString(36) + '-' + Math.floor(Math.random() * 1e9).toString(36);
}

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function broadcastRoom(roomCode, msg) {
  const room = rooms.get(roomCode);
  if (!room) return;
  for (const p of room.players) {
    if (p.ws) send(p.ws, msg);
  }
}

// Phase 4: live position relay. Fire-and-forget, no server-side storage --
// each update just gets forwarded to everyone else currently in the room.
function broadcastRoomExcept(roomCode, exceptClientId, msg) {
  const room = rooms.get(roomCode);
  if (!room) return;
  for (const p of room.players) {
    if (p.id === exceptClientId) continue;
    if (p.ws) send(p.ws, msg);
  }
}

function roomUpdatePayload(room, roomCode) {
  return {
    type: 'room_update',
    roomCode,
    hostId: room.hostId,
    seed: room.seed,
    level: room.level,
    players: room.players.map(p => ({ id: p.id, name: p.name })),
  };
}

function removeFromRoom(ws) {
  if (!ws.roomCode) return;
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  room.players = room.players.filter(p => p.id !== ws.clientId);

  if (room.players.length === 0) {
    rooms.delete(ws.roomCode);
    return;
  }
  // If the host left, promote the next player.
  if (room.hostId === ws.clientId) {
    room.hostId = room.players[0].id;
  }
  broadcastRoom(ws.roomCode, roomUpdatePayload(room, ws.roomCode));
  ws.roomCode = null;
}

// ---------- Phase 3: round-end sync ----------
// A round is "done" once every still-active player in the room has sent
// round_done for that roundIndex. "Active" shrinks as players finish their
// own match (lives hit 0) or disconnect, so the room doesn't wait forever
// on someone who's no longer playing.
function recordRoundReport(room, roomCode, clientId, roundIndex, finished) {
  let roundSet = room.roundReports.get(roundIndex);
  if (!roundSet) {
    roundSet = new Set();
    room.roundReports.set(roundIndex, roundSet);
  }
  roundSet.add(clientId);

  // Total expected for this round is however many players were still
  // active at the moment this report came in -- capture it before removing
  // a finishing player, since they DO count as having reported for the
  // round they just finished.
  const total = room.activePlayers.size;
  if (finished) room.activePlayers.delete(clientId);

  broadcastRoom(roomCode, { type: 'progress_update', roundIndex, done: roundSet.size, total });

  if (total > 0 && roundSet.size >= total) {
    room.roundReports.delete(roundIndex);
    broadcastRoom(roomCode, { type: 'advance_round', roundIndex });
  }
}

// After a player drops out mid-match (disconnect), any round already
// waiting on them may now be complete -- or may need its progress count
// corrected downward. Re-evaluate every pending round.
function recheckPendingRounds(room, roomCode) {
  if (!room || !room.roundReports) return;
  const total = room.activePlayers.size;
  for (const [roundIndex, roundSet] of Array.from(room.roundReports.entries())) {
    broadcastRoom(roomCode, { type: 'progress_update', roundIndex, done: roundSet.size, total });
    if (total > 0 && roundSet.size >= total) {
      room.roundReports.delete(roundIndex);
      broadcastRoom(roomCode, { type: 'advance_round', roundIndex });
    }
  }
}

const server = http.createServer((req, res) => {
  // Simple health check for Render.
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('ok');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.clientId = makeClientId();
  ws.roomCode = null;
  ws._msgCount = 0;
  ws._msgWindowStart = Date.now();

  ws.on('message', (raw) => {
    // Payload size cap -- ignore anything oversized rather than letting a
    // malformed/huge message through to JSON.parse or a broadcast.
    if (raw.length > MAX_MESSAGE_BYTES) return;

    // Sliding-window rate limit. Cheap and approximate on purpose.
    const now = Date.now();
    if (now - ws._msgWindowStart > RATE_LIMIT_WINDOW_MS) {
      ws._msgWindowStart = now;
      ws._msgCount = 0;
    }
    ws._msgCount++;
    if (ws._msgCount > RATE_LIMIT_MAX_MESSAGES) return; // silently drop, don't disconnect for one burst

    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      return; // ignore malformed messages
    }

    switch (msg.type) {
      case 'create_room': {
        // Leave any existing room first (defensive -- shouldn't normally happen).
        removeFromRoom(ws);

        const roomCode = makeRoomCode();
        const name = (msg.name || 'Host').toString().slice(0, 20);
        const room = {
          hostId: ws.clientId,
          players: [{ id: ws.clientId, ws, name }],
          seed: null,
          level: msg.level || 1,
          createdAt: Date.now(),
        };
        rooms.set(roomCode, room);
        ws.roomCode = roomCode;

        send(ws, { type: 'room_created', roomCode, clientId: ws.clientId });
        broadcastRoom(roomCode, roomUpdatePayload(room, roomCode));
        break;
      }

      case 'join_room': {
        const roomCode = (msg.roomCode || '').toString().trim().toUpperCase();
        const room = rooms.get(roomCode);
        if (!room) {
          send(ws, { type: 'join_error', reason: 'not_found', roomCode });
          return;
        }

        removeFromRoom(ws);

        const name = (msg.name || 'Player').toString().slice(0, 20);
        room.players.push({ id: ws.clientId, ws, name });
        ws.roomCode = roomCode;

        send(ws, { type: 'room_joined', roomCode, clientId: ws.clientId });
        broadcastRoom(roomCode, roomUpdatePayload(room, roomCode));
        break;
      }

      case 'leave_room': {
        removeFromRoom(ws);
        break;
      }

      case 'start_match': {
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        if (room.hostId !== ws.clientId) return; // only the host can start
        if (room.matchStarting) return; // already starting, ignore repeats

        room.matchStarting = true;
        room.seed = makeSeed();
        // Round-sync state for Phase 3: which players are still playing this
        // match, and who has reported in for each round index.
        room.activePlayers = new Set(room.players.map(p => p.id));
        room.roundReports = new Map(); // roundIndex -> Set(clientId)
        const startAt = Date.now() + START_COUNTDOWN_MS;

        broadcastRoom(ws.roomCode, {
          type: 'match_starting',
          seed: room.seed,
          level: room.level,
          startAt,
          serverTime: Date.now(),
        });
        break;
      }

      case 'round_done': {
        const room = rooms.get(ws.roomCode);
        if (!room || !room.activePlayers) return;
        if (!room.activePlayers.has(ws.clientId)) return; // already finished or not in a match

        const roundIndex = msg.roundIndex;
        if (typeof roundIndex !== 'number') return;

        recordRoundReport(room, ws.roomCode, ws.clientId, roundIndex, !!msg.finished);

        // Let everyone else's opponents panel show this player's current
        // lives/round. Purely cosmetic, so no validation beyond basic
        // typeof checks -- worst case a bad value just displays oddly for
        // one player, it never touches actual round-advance logic above.
        broadcastRoomExcept(ws.roomCode, ws.clientId, {
          type: 'peer_progress',
          clientId: ws.clientId,
          lives: typeof msg.lives === 'number' ? msg.lives : null,
          round: typeof msg.round === 'number' ? msg.round : null,
          finished: !!msg.finished,
        });
        break;
      }

      case 'peer_pos': {
        // Phase 4: live minigame visibility. Client streams its own
        // position/state during an eligible minigame; just relay it to
        // everyone else in the room. No validation of x/y ranges -- these
        // are cosmetic ghost markers, not authoritative game state, so a
        // malformed or out-of-range value only makes someone's own ghost
        // render oddly, never affects anyone's actual round outcome.
        if (!ws.roomCode) return;
        broadcastRoomExcept(ws.roomCode, ws.clientId, {
          type: 'peer_pos',
          clientId: ws.clientId,
          roundIndex: msg.roundIndex,
          x: msg.x,
          y: msg.y,
          state: msg.state,
        });
        break;
      }

      default:
        // Unknown message type -- ignore.
        break;
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.roomCode);
    const roomCode = ws.roomCode;
    removeFromRoom(ws);
    // If they disconnected mid-match, dropping out shouldn't leave everyone
    // else waiting forever on a report that will never come.
    if (room && room.activePlayers && room.activePlayers.has(ws.clientId)) {
      room.activePlayers.delete(ws.clientId);
      recheckPendingRounds(room, roomCode);
    }
  });
});

server.listen(PORT, () => {
  console.log(`Room server listening on :${PORT}`);
});
