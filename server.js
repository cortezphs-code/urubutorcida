const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');

let WebSocketServer;
let LiveConnectionClass;

try {
  WebSocketServer = require('ws').WebSocketServer;
} catch (e) {
  console.warn('⚠️ Pacote "ws" não instalado. Rode: npm install');
}

try {
  const pkg = require('tiktok-live-connector');
  LiveConnectionClass = pkg.TikTokLiveConnection || pkg.WebcastPushConnection;
} catch (err) {
  console.warn('⚠️ Pacote "tiktok-live-connector" não instalado. Rode: npm install');
}

const PORT = process.env.PORT || 3000;
let currentTikTokUser = (process.env.TIKTOK_USER || '').replace(/^@/, '').trim();
let tiktokConnection = null;
let currentViewerCount = 0;

const sseClients = new Set();
let wss = null;

function broadcast(eventObj) {
  const jsonStr = JSON.stringify(eventObj);

  // 1. Envia via WebSocket
  if (wss) {
    wss.clients.forEach((client) => {
      if (client.readyState === 1) {
        try {
          client.send(jsonStr);
        } catch (_) {}
      }
    });
  }

  // 2. Envia também via SSE (/events) como fallback
  const ssePayload = `data: ${jsonStr}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(ssePayload);
    } catch (_) {
      sseClients.delete(res);
    }
  }
}

function normalizeText(raw) {
  return String(raw || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim();
}

function detectTeam(text) {
  const clean = normalizeText(text);
  if (!clean) return null;

  // 1. Flamengo (Urubu)
  if (
    clean.includes('urubu') ||
    clean.includes('flamengo') ||
    clean.includes('mengao') ||
    clean.includes('mengo') ||
    /\bfla\b/.test(clean) ||
    clean === '1'
  ) {
    return 'fla';
  }

  // 2. Palmeiras (Porco)
  if (
    clean.includes('porco') ||
    clean.includes('palmeiras') ||
    clean.includes('verdao') ||
    /\bpal\b/.test(clean) ||
    clean === '2'
  ) {
    return 'pal';
  }

  // 3. Corinthians (Gavião)
  if (
    clean.includes('gaviao') ||
    clean.includes('corinthians') ||
    clean.includes('timao') ||
    clean.includes('fiel') ||
    /\bcor\b/.test(clean) ||
    clean === '3'
  ) {
    return 'cor';
  }

  // 4. Vasco (Almirante)
  if (
    clean.includes('vasco') ||
    clean.includes('almirante') ||
    clean.includes('gigante') ||
    clean.includes('vascao') ||
    /\bvas\b/.test(clean) ||
    clean === '4'
  ) {
    return 'vas';
  }

  return null;
}

function extractUsername(data) {
  if (!data) return 'Torcedor';
  if (data.user) {
    return (
      data.user.uniqueId ||
      data.user.displayId ||
      data.user.nickname ||
      'Torcedor'
    );
  }
  return data.uniqueId || data.displayId || data.nickname || 'Torcedor';
}

function disconnectFromTikTok() {
  const oldUser = currentTikTokUser;
  if (tiktokConnection) {
    try {
      tiktokConnection.disconnect();
    } catch (_) {}
    tiktokConnection = null;
  }
  currentTikTokUser = '';
  currentViewerCount = 0;
  console.log(`🔌 Desconectado da Live${oldUser ? ` de @${oldUser}` : ''}.`);
  broadcast({
    type: 'status',
    connected: false,
    connecting: false,
    username: '',
    viewerCount: 0,
    message: '🔌 Live desconectada (Modo Simulação / Bots).'
  });
}

function connectToTikTok(username) {
  if (!LiveConnectionClass) {
    broadcast({
      type: 'status',
      connected: false,
      message: 'Instale as dependências com "npm install" para conectar ao TikTok.'
    });
    return;
  }

  const cleanUser = (username || '').replace(/^@/, '').trim();
  if (!cleanUser) {
    broadcast({
      type: 'status',
      connected: false,
      message: 'Digite o @usuario da Live para conectar'
    });
    return;
  }

  if (tiktokConnection) {
    try {
      tiktokConnection.disconnect();
    } catch (_) {}
    tiktokConnection = null;
  }

  currentTikTokUser = cleanUser;
  currentViewerCount = 0;
  console.log(`🔌 Conectando à Live de @${cleanUser}...`);

  tiktokConnection = new LiveConnectionClass(cleanUser, {
    processInitialData: true,
    enableExtendedGiftInfo: false
  });

  // Evita crash do Node se o TikTok emitir aviso secundário
  tiktokConnection.on('error', (err) => {
    console.error('⚠️ Aviso TikTok Live:', err && err.message ? err.message : err);
  });

  broadcast({
    type: 'status',
    connected: false,
    connecting: true,
    username: cleanUser,
    viewerCount: 0,
    message: `Conectando à live de @${cleanUser}...`
  });

  tiktokConnection
    .connect()
    .then((state) => {
      const initialViewers = Number(
        (state && state.roomInfo && state.roomInfo.data && state.roomInfo.data.user_count) ||
        (state && state.roomInfo && state.roomInfo.user_count) ||
        (state && state.viewerCount) ||
        currentViewerCount ||
        0
      );
      currentViewerCount = initialViewers;
      console.log(`✅ Conectado à Live de @${cleanUser} (Espectadores: ${currentViewerCount})`);
      broadcast({
        type: 'status',
        connected: true,
        username: cleanUser,
        roomId: state && state.roomId,
        viewerCount: currentViewerCount,
        message: `✅ LIVE CONECTADA: @${cleanUser}`
      });
    })
    .catch((err) => {
      const errMsg = (err && err.message) || 'Verifique se a conta está AO VIVO agora';
      console.error(`❌ Erro ao conectar em @${cleanUser}:`, errMsg);
      broadcast({
        type: 'status',
        connected: false,
        username: cleanUser,
        viewerCount: 0,
        error: errMsg,
        message: `Erro ao conectar em @${cleanUser}: ${errMsg}`
      });
    });

  // Chat messages (suporta v2 data.content e v1 data.comment)
  tiktokConnection.on('chat', (data) => {
    const rawComment = (data && (data.content || data.comment)) || '';
    const comment = String(rawComment).trim();
    const nickname = extractUsername(data);
    const userId = (data && data.user && data.user.uniqueId) || data.uniqueId || nickname;

    console.log(`💬 [${nickname}]: ${comment}`);
    if (!comment) return;

    const team = detectTeam(comment);
    broadcast({
      type: 'chat',
      user: nickname,
      userId,
      nickname,
      comment,
      team
    });
  });

  // Presentes (Gifts)
  tiktokConnection.on('gift', (data) => {
    if (data && data.giftType === 1 && !data.repeatEnd) return;
    const nickname = extractUsername(data);
    const userId = (data && data.user && data.user.uniqueId) || data.uniqueId || nickname;
    const giftName =
      (data && data.giftDetails && data.giftDetails.giftName) ||
      (data && data.extendedGiftInfo && data.extendedGiftInfo.name) ||
      (data && data.giftName) ||
      'Presente';
    const repeatCount = Number((data && data.repeatCount) || 1);
    const baseDiamonds = Number(
      (data && data.giftDetails && data.giftDetails.diamondCount) ||
      (data && data.extendedGiftInfo && data.extendedGiftInfo.diamond_count) ||
      (data && data.fanTicketCount) ||
      (data && data.diamondCount) ||
      1
    );
    const diamondCount = baseDiamonds * repeatCount;

    console.log(`🎁 [${nickname}] enviou ${giftName} (${diamondCount} moedas)`);
    broadcast({
      type: 'gift',
      user: nickname,
      userId,
      nickname,
      giftName,
      diamondCount,
      repeatCount
    });
  });

  // Curtidas (Likes)
  tiktokConnection.on('like', (data) => {
    const nickname = extractUsername(data);
    const userId = (data && data.user && data.user.uniqueId) || data.uniqueId || nickname;
    const likeCount = Number((data && (data.count || data.likeCount)) || 1);
    broadcast({
      type: 'like',
      user: nickname,
      userId,
      nickname,
      likeCount
    });
  });

  // Entrada de espectadores
  tiktokConnection.on('member', (data) => {
    const count = Number((data && (data.memberCount ?? data.viewerCount)) || 0);
    if (count > 0) {
      currentViewerCount = count;
      broadcast({
        type: 'roomUser',
        viewerCount: currentViewerCount
      });
    }
  });

  // Atualização periódica da sala
  tiktokConnection.on('roomUser', (data) => {
    const count = Number((data && (data.total ?? data.viewerCount ?? data.userCount)) ?? -1);
    if (count >= 0) {
      currentViewerCount = count;
      broadcast({
        type: 'roomUser',
        viewerCount: currentViewerCount
      });
    }
  });

  // Reconecta automaticamente se o WebSocket do TikTok cair durante a Live
  tiktokConnection.on('disconnected', () => {
    console.warn(`⚠️ WebSocket do TikTok desconectou de @${cleanUser}. Reconectando em 3s...`);
    if (currentTikTokUser === cleanUser) {
      setTimeout(() => {
        if (currentTikTokUser === cleanUser) {
          connectToTikTok(cleanUser);
        }
      }, 3000);
    }
  });

  tiktokConnection.on('streamEnd', () => {
    currentViewerCount = 0;
    tiktokConnection = null;
    currentTikTokUser = '';
    broadcast({
      type: 'status',
      connected: false,
      username: '',
      viewerCount: 0,
      error: 'Live encerrada',
      message: 'Live encerrada'
    });
  });
}

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon'
};

const server = http.createServer((req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (parsedUrl.pathname === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
      'Access-Control-Allow-Origin': '*'
    });
    res.write('\n');
    sseClients.add(res);

    res.write(
      `data: ${JSON.stringify({
        type: 'status',
        connected: !!(tiktokConnection && currentTikTokUser),
        username: currentTikTokUser,
        viewerCount: currentViewerCount,
        message: currentTikTokUser
          ? `✅ LIVE CONECTADA: @${currentTikTokUser}`
          : 'Clique em 📡 LIVE no topo para conectar seu @usuario'
      })}\n\n`
    );

    req.on('close', () => {
      sseClients.delete(res);
    });
    return;
  }

  if (parsedUrl.pathname === '/connect') {
    const userParam = parsedUrl.searchParams.get('user') || '';
    connectToTikTok(userParam);
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: true, username: userParam.replace(/^@/, '').trim() }));
    return;
  }

  if (parsedUrl.pathname === '/disconnect') {
    disconnectFromTikTok();
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: true, connected: false }));
    return;
  }

  let filePath = parsedUrl.pathname === '/' ? '/index.html' : parsedUrl.pathname;
  const safePath = path.normalize(filePath).replace(/^(\.\.[\/\\])+/, '');
  const fullPath = path.join(__dirname, safePath);

  fs.readFile(fullPath, (err, content) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Arquivo não encontrado');
      return;
    }
    const ext = path.extname(fullPath).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME_TYPES[ext] || 'application/octet-stream',
      'Cache-Control': 'no-store, no-cache, must-revalidate'
    });
    res.end(content);
  });
});

if (WebSocketServer) {
  wss = new WebSocketServer({ server });

  // Keep-alive ping a cada 20s para o proxy do Render nunca fechar o WebSocket
  setInterval(() => {
    wss.clients.forEach((client) => {
      if (client.readyState === 1) {
        try {
          client.ping();
        } catch (_) {}
      }
    });
  }, 20000);

  wss.on('connection', (ws) => {
    const isConn = !!(tiktokConnection && tiktokConnection.isConnected && currentTikTokUser);
    ws.send(
      JSON.stringify({
        type: 'status',
        connected: isConn,
        username: currentTikTokUser,
        viewerCount: currentViewerCount,
        message: isConn
          ? `✅ LIVE CONECTADA: @${currentTikTokUser}`
          : 'Servidor pronto! Digite seu @usuario para conectar.'
      })
    );

    ws.on('message', (msg) => {
      try {
        const data = JSON.parse(msg.toString());
        if ((data.type === 'connect_tiktok' || data.type === 'connect') && data.username) {
          connectToTikTok(data.username);
        } else if (data.type === 'disconnect_tiktok' || data.type === 'disconnect') {
          disconnectFromTikTok();
        }
      } catch (_) {}
    });
  });
}

function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return 'localhost';
}

server.listen(PORT, '0.0.0.0', () => {
  const ip = getLocalIP();
  console.log('\n======================================================');
  console.log('⚔️ DUELO DAS 4 TORCIDAS LIVE — SERVIDOR ATIVO');
  console.log('======================================================');
  console.log(`💻 Abra no Mac / Live Studio: http://localhost:${PORT}`);
  console.log(`📱 Abra no Celular:           http://${ip}:${PORT}`);
  console.log('======================================================\n');
  if (currentTikTokUser) {
    connectToTikTok(currentTikTokUser);
  }
});
