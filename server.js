/* ============================================================
   실시간 테트리스 배틀 - 서버 (외부 라이브러리 0개)
   실행:  node server.js
   포트 바꾸기:  PORT=4000 node server.js
   ============================================================ */

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const os = require('os');
const EventEmitter = require('events');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');

/* ---------- 1) 정적 파일 서버 ---------- */
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.ico': 'image/x-icon',
};

const server = http.createServer((req, res) => {
  let urlPath = req.url.split('?')[0];

  // 서버의 랜(LAN) IP 주소를 알려주는 엔드포인트 (상황판에서 학생 접속 주소 표시용)
  if (urlPath === '/info') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ port: PORT, ips: lanIPs() }));
    return;
  }

  if (urlPath === '/') urlPath = '/index.html';
  const filePath = path.join(PUBLIC, path.normalize(urlPath));
  if (!filePath.startsWith(PUBLIC)) { res.writeHead(403); res.end('forbidden'); return; }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
    res.end(data);
  });
});

function lanIPs() {
  const out = [];
  const ifs = os.networkInterfaces();
  for (const name of Object.keys(ifs)) {
    for (const ni of ifs[name] || []) {
      if (ni.family === 'IPv4' && !ni.internal) out.push(ni.address);
    }
  }
  return out;
}

/* ---------- 2) WebSocket (RFC6455 최소 구현) ---------- */
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const clients = new Set();

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );
  const ws = new WSConn(socket);
  clients.add(ws);
  ws.on('message', (msg) => handleMessage(ws, msg));
  ws.on('close', () => { clients.delete(ws); });
});

class WSConn extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.closed = false;
    this.meta = {}; // 앱에서 쓰는 상태(역할, 이름, 점수 등)
    socket.setNoDelay(true);
    socket.on('data', (d) => { this.buffer = Buffer.concat([this.buffer, d]); this._parse(); });
    socket.on('close', () => this._onClose());
    socket.on('error', () => this._onClose());
  }
  _onClose() {
    if (this.closed) return;
    this.closed = true;
    this.emit('close');
  }
  _parse() {
    while (true) {
      const buf = this.buffer;
      if (buf.length < 2) return;
      const b0 = buf[0], b1 = buf[1];
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let off = 2;
      if (len === 126) {
        if (buf.length < off + 2) return;
        len = buf.readUInt16BE(off); off += 2;
      } else if (len === 127) {
        if (buf.length < off + 8) return;
        const hi = buf.readUInt32BE(off), lo = buf.readUInt32BE(off + 4);
        len = hi * 4294967296 + lo; off += 8;
      }
      let maskKey;
      if (masked) {
        if (buf.length < off + 4) return;
        maskKey = buf.slice(off, off + 4); off += 4;
      }
      if (buf.length < off + len) return; // 아직 페이로드가 다 안 옴
      let payload = buf.slice(off, off + len);
      if (masked) {
        const out = Buffer.allocUnsafe(len);
        for (let i = 0; i < len; i++) out[i] = payload[i] ^ maskKey[i & 3];
        payload = out;
      }
      this.buffer = buf.slice(off + len);

      if (opcode === 0x8) { this.close(); return; }      // close
      else if (opcode === 0x9) { this._send(0xA, payload); } // ping -> pong
      else if (opcode === 0x1) { this.emit('message', payload.toString('utf8')); } // text
      // (작은 JSON만 주고받으므로 프레임 분할은 처리하지 않음)
    }
  }
  _send(opcode, data) {
    if (this.closed) return;
    const len = data.length;
    let header;
    if (len < 126) {
      header = Buffer.allocUnsafe(2);
      header[0] = 0x80 | opcode; header[1] = len;
    } else if (len < 65536) {
      header = Buffer.allocUnsafe(4);
      header[0] = 0x80 | opcode; header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.allocUnsafe(10);
      header[0] = 0x80 | opcode; header[1] = 127;
      header.writeUInt32BE(Math.floor(len / 4294967296), 2);
      header.writeUInt32BE(len >>> 0, 6);
    }
    try { this.socket.write(Buffer.concat([header, data])); } catch (e) {}
  }
  send(str) { this._send(0x1, Buffer.from(str, 'utf8')); }
  close() {
    if (this.closed) return;
    try { this._send(0x8, Buffer.alloc(0)); } catch (e) {}
    try { this.socket.end(); } catch (e) {}
    this._onClose();
  }
}


/* ---------- 3) 게임 로직 (여러 방 지원) ---------- */
let nextId = 1;
let nextRoom = 1;
const MAX_ROOMS = 15;
const rooms = new Map(); // roomId -> { id, name, mode, phase, activeTeams, keywords, ending }

const players = () => [...clients].filter(c => c.meta.role === 'player');
const hosts = () => [...clients].filter(c => c.meta.role === 'host');
const playersIn = (rid) => players().filter(p => p.meta.room === rid);
const waitingPlayers = () => players().filter(p => !p.meta.room);
const isHost = (ws) => ws.meta.role === 'host';
const roomById = (id) => rooms.get(id) || null;

// 방장 결정: auto=먼저 들어온 학생, fixed=교사 지정 학생, none=방장 없음(교사만 운영)
function roomLeaderId(rid) {
  const room = rooms.get(rid); if (!room) return 0;
  const ps = playersIn(rid);
  if (!ps.length) return 0;
  if (room.leaderMode === 'none') return 0;
  if (room.leaderMode === 'fixed' && ps.some(p => p.meta.id === room.leaderId)) return room.leaderId;
  return ps.reduce((min, p) => (p.meta.id < min ? p.meta.id : min), Infinity); // auto (또는 지정자가 나간 경우)
}
function isLeader(ws) {
  return ws.meta.role === 'player' && !!ws.meta.room && roomLeaderId(ws.meta.room) === ws.meta.id;
}
// 교사면 m.room, 방장이면 자기 방을 대상으로 — 권한 있는 방을 돌려줌(없으면 null)
function targetRoom(ws, m) {
  if (isHost(ws)) return roomById(m.room);
  if (isLeader(ws)) return roomById(ws.meta.room);
  return null;
}

function makeRoom(name, mode) {
  if (rooms.size >= MAX_ROOMS) return null;
  const id = 'r' + (nextRoom++);
  const room = {
    id,
    name: (name || ('방 ' + id.slice(1))).toString().slice(0, 16),
    mode: (mode === 'team' ? 'team' : 'solo'),
    phase: 'lobby', activeTeams: [], keywords: [], ending: false,
    leaderMode: 'auto', leaderId: 0,   // auto=먼저 들어온 학생 / fixed=지정 / none=교사만
  };
  rooms.set(id, room);
  return room;
}

function handleMessage(ws, raw) {
  let m; try { m = JSON.parse(raw); } catch { return; }
  switch (m.type) {
    case 'join':
      ws.meta.role = (m.role === 'host') ? 'host' : 'player';
      ws.meta.id = nextId++;
      ws.meta.name = (m.name || '학생').toString().slice(0, 14);
      ws.meta.team = '';
      ws.meta.room = '';   // 처음엔 대기실
      ws.meta.score = 0; ws.meta.lines = 0; ws.meta.alive = false; ws.meta.board = null;
      ws.meta.atk = 0; ws.meta.maxCombo = 0; ws.meta.wbonus = 0; ws.meta.cbonus = 0;
      ws.send(JSON.stringify({ type: 'welcome', id: ws.meta.id, phase: 'lobby' }));
      if (isHost(ws)) { ws.meta.viewRoom = ''; sendRoomsList(ws); }
      else { ws.send(JSON.stringify({ type: 'waiting' })); }   // 대기실 안내
      break;

    case 'state':
      ws.meta.score = m.score | 0; ws.meta.lines = m.lines | 0; ws.meta.board = m.board || null;
      ws.meta.atk = m.atk | 0; ws.meta.maxCombo = m.combo | 0; ws.meta.wbonus = m.wbonus | 0; ws.meta.cbonus = m.cbonus | 0;
      break;
    case 'topout':
      ws.meta.alive = false; ws.meta.score = m.score | 0; ws.meta.lines = m.lines | 0;
      ws.meta.atk = m.atk | 0; ws.meta.maxCombo = m.combo | 0; ws.meta.wbonus = m.wbonus | 0; ws.meta.cbonus = m.cbonus | 0;
      break;
    case 'attack':
      sendGarbage(ws, m.amount | 0);
      break;

    /* ----- 교사(host) 전용 ----- */
    case 'createroom':
      if (isHost(ws)) { const r = makeRoom(m.name, m.mode); if (r) ws.meta.viewRoom = r.id; broadcastRooms(); }
      break;
    case 'deleteroom':
      if (isHost(ws)) {
        const r = roomById(m.room);
        if (r) {
          for (const p of playersIn(r.id)) { p.meta.room = ''; p.meta.team = ''; p.meta.alive = false; p.send(JSON.stringify({ type: 'waiting' })); }
          rooms.delete(r.id);
          for (const h of hosts()) if (h.meta.viewRoom === r.id) h.meta.viewRoom = '';
        }
        broadcastRooms();
      }
      break;
    case 'setleader':        // 교사: 특정 학생을 방장으로 지정
      if (isHost(ws)) { const r = roomById(m.room); if (r) { r.leaderMode = 'fixed'; r.leaderId = (m.id | 0); broadcastRooms(); } }
      break;
    case 'clearleader':      // 교사: 방장 없앰 (이 방은 교사만 운영)
      if (isHost(ws)) { const r = roomById(m.room); if (r) { r.leaderMode = 'none'; r.leaderId = 0; broadcastRooms(); } }
      break;
    case 'autoleader':       // 교사: 방장 자동(먼저 들어온 학생)으로 되돌림
      if (isHost(ws)) { const r = roomById(m.room); if (r) { r.leaderMode = 'auto'; r.leaderId = 0; broadcastRooms(); } }
      break;
    case 'setmode':
      {
        const r = targetRoom(ws, m);
        if (r && r.phase === 'lobby') {
          r.mode = (m.mode === 'team' ? 'team' : 'solo');
          if (r.mode === 'solo') { r.activeTeams = []; for (const p of playersIn(r.id)) { p.meta.team = ''; notifyTeam(p); } }
        }
        broadcastRooms();
      }
      break;
    case 'viewroom':
      if (isHost(ws)) { ws.meta.viewRoom = roomById(m.room) ? m.room : ''; sendRoomsList(ws); }
      break;
    case 'assignroom':       // 학생을 특정 방으로 (room 비우면 대기실)
      if (isHost(ws)) {
        const p = players().find(x => x.meta.id === (m.id | 0));
        if (p) {
          const r = roomById(m.room);
          p.meta.room = r ? r.id : '';
          p.meta.team = ''; p.meta.alive = false; p.meta.score = 0; p.meta.lines = 0; p.meta.board = null;
          if (r) {
            p.send(JSON.stringify({ type: 'roomset', room: r.id, name: r.name, mode: r.mode }));
            if (r.keywords.length) p.send(JSON.stringify({ type: 'keywords', words: r.keywords }));
            if (r.mode === 'team' && r.activeTeams.length) { p.meta.team = smallestActiveTeam(r, p); notifyTeam(p); }
            if (r.phase === 'playing') { p.meta.alive = true; p.send(JSON.stringify({ type: 'start' })); }
            else p.send(JSON.stringify({ type: 'reset' }));
          } else {
            p.send(JSON.stringify({ type: 'waiting' }));
          }
          console.log('[room] ' + p.meta.name + ' -> ' + (r ? r.name : '(대기실)'));
        }
        broadcastRooms();
      }
      break;
    case 'start':
      { const r = targetRoom(ws, m); if (r) startGame(r); }
      break;
    case 'reset':
      { const r = targetRoom(ws, m); if (r) resetGame(r); }
      break;
    case 'finish':           // 방 강제 종료 (5초 카운트다운 후 결과)
      {
        const r = targetRoom(ws, m);
        if (r && r.phase === 'playing' && !r.ending) {
          r.ending = true;
          const cd = JSON.stringify({ type: 'endcountdown', sec: 10 });
          for (const p of playersIn(r.id)) p.send(cd);
          for (const h of hosts()) if (h.meta.viewRoom === r.id) h.send(cd);
          setTimeout(() => { r.ending = false; finishGame(r); }, 10000);
        }
      }
      break;
    case 'assign':           // 학생을 팀으로 (교사=아무 방, 방장=자기 방)
      {
        const p = players().find(x => x.meta.id === (m.id | 0));
        if (p && p.meta.room) {
          const allowed = isHost(ws) || (isLeader(ws) && ws.meta.room === p.meta.room);
          if (allowed) {
            const r = roomById(p.meta.room);
            const t = (m.team || '').toString().slice(0, 16);
            p.meta.team = t;
            if (r && t && !r.activeTeams.includes(t)) r.activeTeams.push(t);
            notifyTeam(p);
            console.log('[assign] ' + p.meta.name + ' -> ' + (t || '(미배정)'));
          }
        }
      }
      break;
    case 'clearteams':
      { const r = targetRoom(ws, m); if (r) { r.activeTeams = []; for (const p of playersIn(r.id)) { p.meta.team = ''; notifyTeam(p); } } }
      break;
    case 'autoassign':
      { const r = targetRoom(ws, m); if (r) autoAssign(r, Array.isArray(m.teams) ? m.teams : []); }
      break;
    case 'setkeyword':
      {
        const r = targetRoom(ws, m);
        if (r) {
          let arr = Array.isArray(m.words) ? m.words : String(m.word || '').split(/[\n,]/);
          r.keywords = arr.map(w => String(w).replace(/\s+/g, '').slice(0, 12)).filter(w => w.length > 0).slice(0, 20);
          const kw = JSON.stringify({ type: 'keywords', words: r.keywords });
          for (const p of playersIn(r.id)) p.send(kw);
        }
      }
      break;
  }
}

function startGame(room) {
  room.phase = 'playing'; room.ending = false;
  for (const p of playersIn(room.id)) {
    p.meta.alive = true; p.meta.score = 0; p.meta.lines = 0; p.meta.board = null;
    p.meta.atk = 0; p.meta.maxCombo = 0; p.meta.wbonus = 0; p.meta.cbonus = 0;
    p.send(JSON.stringify({ type: 'start' }));
  }
  const st = JSON.stringify({ type: 'start', room: room.id });
  for (const h of hosts()) if (h.meta.viewRoom === room.id) h.send(st);   // 보고 있는 교사 화면에도 카운트
  broadcastRooms();
}
function resetGame(room) {
  room.phase = 'lobby'; room.ending = false;
  for (const p of playersIn(room.id)) { p.meta.alive = false; p.meta.score = 0; p.meta.lines = 0; p.meta.board = null; p.send(JSON.stringify({ type: 'reset' })); }
  broadcastRooms();
}
function sendGarbage(from, amount) {
  const room = roomById(from.meta.room);
  if (!room || room.phase !== 'playing' || amount <= 0) return;
  const inRoom = playersIn(room.id);
  // 팀전이면 '다른 팀'에게만, 개인전이면 같은 방 아무에게나
  const targets = from.meta.team
    ? inRoom.filter(p => p !== from && p.meta.alive && p.meta.team !== from.meta.team)
    : inRoom.filter(p => p !== from && p.meta.alive);
  if (!targets.length) return;
  const t = targets[(Math.random() * targets.length) | 0];
  t.send(JSON.stringify({ type: 'garbage', amount, from: from.meta.name }));
  from.send(JSON.stringify({ type: 'attacked', amount, to: t.meta.name }));
  const fx = JSON.stringify({ type: 'attackfx', fromId: from.meta.id, toId: t.meta.id, amount, room: room.id });
  for (const h of hosts()) if (h.meta.viewRoom === room.id) h.send(fx);
}

/* ---------- 팀 배정 (교사 권한, 방 단위) ---------- */
function notifyTeam(p) { p.send(JSON.stringify({ type: 'team', team: p.meta.team })); }
function smallestActiveTeam(room, except) {
  const counts = {}; for (const t of room.activeTeams) counts[t] = 0;
  for (const p of playersIn(room.id)) { if (p !== except && counts[p.meta.team] != null) counts[p.meta.team]++; }
  let best = room.activeTeams[0], bestN = Infinity;
  for (const t of room.activeTeams) { if (counts[t] < bestN) { bestN = counts[t]; best = t; } }
  return best;
}
function autoAssign(room, teamIds) {
  room.activeTeams = teamIds.filter(x => typeof x === 'string' && x).slice(0, 8);
  if (room.activeTeams.length) room.mode = 'team';
  const ps = playersIn(room.id);
  for (let i = ps.length - 1; i > 0; i--) { const j = (Math.random() * (i + 1)) | 0;[ps[i], ps[j]] = [ps[j], ps[i]]; }
  ps.forEach((p, i) => { p.meta.team = room.activeTeams.length ? room.activeTeams[i % room.activeTeams.length] : ''; notifyTeam(p); });
  broadcastRooms();
}

/* ---------- 4) 주기적 상태 전송 (300ms마다, 방별) ---------- */
function teamSummary(ps) {
  const map = new Map();
  for (const p of ps) {
    const t = p.meta.team || '';
    if (!map.has(t)) map.set(t, { team: t, score: 0, lines: 0, alive: 0, players: 0 });
    const e = map.get(t);
    e.score += p.meta.score; e.lines += p.meta.lines; e.players++;
    if (p.meta.alive) e.alive++;
  }
  const arr = [...map.values()];
  arr.forEach(e => e.avg = e.players ? Math.round(e.score / e.players) : 0);
  arr.sort((a, b) => (b.avg - a.avg) || (b.score - a.score));
  return arr;
}
function roomWorld(room) {
  const ps = playersIn(room.id);
  const rank = ps.map(p => ({
    id: p.meta.id, name: p.meta.name, team: p.meta.team, score: p.meta.score, lines: p.meta.lines, alive: p.meta.alive, board: p.meta.board,
    atk: p.meta.atk || 0, combo: p.meta.maxCombo || 0, wbonus: p.meta.wbonus || 0, cbonus: p.meta.cbonus || 0,
  })).sort((a, b) => (b.alive - a.alive) || (b.score - a.score));
  const teams = teamSummary(ps);
  return { type: 'world', room: room.id, name: room.name, mode: room.mode, phase: room.phase, leaderId: roomLeaderId(room.id), count: ps.length, teams, players: rank };
}
function roomsListPayload() {
  const list = [...rooms.values()].map(r => {
    const ps = playersIn(r.id);
    return { id: r.id, name: r.name, mode: r.mode, phase: r.phase, count: ps.length, alive: ps.filter(p => p.meta.alive).length };
  });
  const waiting = waitingPlayers().map(p => ({ id: p.meta.id, name: p.meta.name }));
  return { type: 'rooms', rooms: list, waiting };
}
function sendRoomsList(ws) {
  ws.send(JSON.stringify(roomsListPayload()));
  if (ws.meta.viewRoom) { const r = roomById(ws.meta.viewRoom); if (r) ws.send(JSON.stringify(roomWorld(r))); }
}
function broadcastRooms() { for (const h of hosts()) sendRoomsList(h); }

setInterval(() => {
  const worlds = new Map();
  for (const r of rooms.values()) worlds.set(r.id, roomWorld(r));

  // 학생: 자기 방 월드 전송 (대기실이면 보내지 않음 — waiting 상태 유지)
  for (const p of players()) {
    if (p.meta.room && worlds.has(p.meta.room)) p.send(JSON.stringify(worlds.get(p.meta.room)));
  }
  // 교사: 방 목록 + 보고 있는 방 월드
  const listStr = JSON.stringify(roomsListPayload());
  for (const h of hosts()) {
    h.send(listStr);
    if (h.meta.viewRoom && worlds.has(h.meta.viewRoom)) h.send(JSON.stringify(worlds.get(h.meta.viewRoom)));
  }
  // 방별 승부 판정
  for (const r of rooms.values()) {
    if (r.phase !== 'playing') continue;
    const ps = playersIn(r.id);
    if (ps.length === 0) continue;
    const alive = ps.filter(p => p.meta.alive);
    let ended = false;
    if (r.mode === 'team') {
      const aliveTeams = new Set(alive.map(p => p.meta.team));
      const allTeams = new Set(ps.map(p => p.meta.team));
      if (alive.length === 0 || (allTeams.size > 1 && aliveTeams.size === 1)) ended = true;
    } else {
      const lastStanding = ps.length > 1 && alive.length === 1;
      if (lastStanding || alive.length === 0) ended = true;
    }
    if (ended) finishGame(r);
  }
}, 300);

// 방 종료 결과 산출 + 그 방에만 전송
function finishGame(room) {
  if (room.phase !== 'playing') return;
  const ps = playersIn(room.id);
  const teamMode = room.mode === 'team';
  const rank = ps.map(p => ({
    id: p.meta.id, name: p.meta.name, team: p.meta.team, score: p.meta.score, lines: p.meta.lines, alive: p.meta.alive,
    atk: p.meta.atk || 0, combo: p.meta.maxCombo || 0, wbonus: p.meta.wbonus || 0, cbonus: p.meta.cbonus || 0,
  }));
  const teams = teamSummary(ps);
  room.phase = 'ended';
  const result = rank.slice().sort((a, b) => b.score - a.score);
  let winnerTeam = null, winnerTeams = [], winnerNames = [];
  if (teamMode && teams[0]) {
    const top = teams[0].avg;
    winnerTeams = teams.filter(t => t.team && t.avg === top).map(t => t.team);
    winnerTeam = winnerTeams[0];
  } else if (!teamMode && result[0]) {
    const top = result[0].score;
    winnerNames = result.filter(p => p.score === top).map(p => p.name);
  }
  const payload = JSON.stringify({ type: 'gameover', room: room.id, players: result, teams, teamMode, winnerTeam, winnerTeams, winnerNames });
  for (const p of playersIn(room.id)) p.send(payload);
  for (const h of hosts()) if (h.meta.viewRoom === room.id) h.send(payload);
  broadcastRooms();
}

/* ---------- 5) 시작 ---------- */
server.listen(PORT, '0.0.0.0', () => {
  const ips = lanIPs();
  console.log('\n=== 실시간 테트리스 배틀 서버 시작 ===');
  console.log('상황판(교사 화면) 열기:');
  console.log('   http://localhost:' + PORT + '/?host=1');
  console.log('\n학생 접속 주소 (같은 와이파이):');
  if (ips.length) ips.forEach(ip => console.log('   http://' + ip + ':' + PORT));
  else console.log('   (네트워크 주소를 찾지 못했어요)');
  console.log('\n종료: Ctrl + C\n');
});
