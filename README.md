# Phase 1 — Room server

## Run locally
```
cd server
npm install
npm start
```
Server listens on `:3000` (or `$PORT`). `GET /` returns `ok` for health checks.

## Deploy to Render
1. Push `server.js` + `package.json` to a repo (or a `server/` subfolder of your existing repo).
2. New Render service → Web Service → point at the repo.
   - Build command: `npm install`
   - Start command: `npm start`
   - Root directory: `server` (if it's a subfolder)
3. Once deployed, Render gives you a URL like `https://your-app.onrender.com`.
   The WebSocket URL is the same host with `wss://` instead of `https://`.

## Wire up the client
In `index.html`, set:
```js
const ROOM_SERVER_URL = 'wss://your-app.onrender.com';
```

## Protocol
Client → server:
- `{ type: 'create_room', level, name }`
- `{ type: 'join_room', roomCode, name }`
- `{ type: 'leave_room' }`
- `{ type: 'start_match' }` — host only; server ignores if sender isn't the current host

Server → client:
- `{ type: 'room_created', roomCode, clientId }` (to the creator)
- `{ type: 'room_joined', roomCode, clientId }` (to the joiner)
- `{ type: 'room_update', roomCode, hostId, seed, level, players: [{id, name}] }` (broadcast to everyone in the room whenever membership changes)
- `{ type: 'join_error', reason: 'not_found', roomCode }`
- `{ type: 'match_starting', seed, level, startAt, serverTime }` (broadcast once the host starts the match; `startAt` is an absolute server timestamp ~3s out, `serverTime` is the server's clock at broadcast time so clients can correct for skew)
- `{ type: 'progress_update', roundIndex, done, total }` (broadcast as each active player reports in for a round)
- `{ type: 'advance_round', roundIndex }` (broadcast once every active player has reported for `roundIndex`; clients waiting on that round call their next-round logic on receipt)

Client → server (continued):
- `{ type: 'round_done', roundIndex, success, finished }` — sent the instant a client resolves a round; `finished: true` means that client is out of lives and won't report for future rounds (server drops them from the room's active-player count from that point on)
- `{ type: 'peer_pos', roundIndex, x, y, state }` — Phase 4, sent only during minigames flagged `livePositions` (currently dodge and spot); `x`/`y` are 0–1 fractions of the arena so they're resolution-independent. Server relays this verbatim to every other player in the room as `{ type: 'peer_pos', clientId, roundIndex, x, y, state }` (broadcast, no storage — a client that joins mid-stream just doesn't see ghosts until the next update arrives). Not gated by round validity server-side; the client discards anything for a round index it's no longer on.

The seed is generated server-side at `start_match` time. Each client corrects `startAt` for clock skew (`offset = serverTime - Date.now()` at receipt), then runs its own local 3-2-1 countdown ending at the corrected instant before calling into the existing round-start logic. The old "wait 2 minutes then auto-start" buffer and the manual "-15s" sync button are gone -- the server handshake replaces both.

Round advancement is also live now: rounds no longer wait out a fixed worst-case slot. A client reports `round_done` the moment its round resolves, sees "Waiting for other players... X of Y done" (from `progress_update`) if others haven't finished yet, and moves to the next round the instant the server's `advance_round` arrives -- which happens as soon as every still-active player (not yet out of lives, not disconnected) has reported. A player going out of lives or disconnecting mid-round is excluded from that round's expected count so nobody's stuck waiting on someone no longer playing.

