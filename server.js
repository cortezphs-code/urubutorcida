const http = require('http');
const fs = require('fs');
const path = require('path');

let TikTokLiveConnection;
try {
  const pkg = require('tiktok-live-connector');
  TikTokLiveConnection = pkg.TikTokLiveConnection || pkg.WebcastPushConnection;
} catch (err) {
  console.warn('⚠️ tiktok-live-connector não instalado ainda. Rode: npm install');
}

const PORT = process.env.PORT || 3000;
let currentTikTokUser = (process.env.TIKTOK_USER || '').replace(/^@/, '').trim();
let tiktokConnection = null;
let currentViewerCount = 0;

const clients = new Set();

function broadcast(eventObj) {
  const payload = `data: ${JSON.stringify(eventObj)}\n\n`;
  for (const res of clients) {
    try {
      res.write(payload);
    } catch (_) {
      clients.delete(res);
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
    clean.includes('rubro') ||
    clean.includes('crf') ||
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
    clean.includes('alviverde') ||
    clean.includes('sep') ||
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
    clean.includes('mosqueteiro') ||
    clean.includes('fiel') ||
    clean.includes('sccp') ||
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
    clean.includes('bacalhau') ||
    clean.includes('crvg') ||
    /\bvas\b/.test(clean) ||
    clean === '4'
  ) {
    return 'vas';
  }

  return null;
}

function connectToTikTok(username) {
  if (!TikTokLiveConnection) {
    broadcast({ type: 'status', connected: false, message: 'tiktok-live-connector ausente' });
    return;
  }

  const cleanUser = (username || '').replace(/^@/, '').trim();
  if (!cleanUser) {
    broadcast({ type: 'status', connected: false, message: 'Aguardando @usuario da Live' });
    return;
  }

  if (tiktokConnection) {
    try {
      tiktokConnection.disconnect();
    } catch (_) {}
    tiktokConnection = null;
  }

  currentTikTokUser = cleanUser;
  console.log(`🔌 Conectando à Live de @${cleanUser}...`);
  broadcast({ type: 'status', connected: false, connecting: true, username: cleanUser });

  tiktokConnection = new TikTokLiveConnection(cleanUser, {
    processInitialData: true,
    enableExtendedGiftInfo: true,
    requestPollingIntervalMs: 1000
  });

  tiktokConnection
    .connect()
    .then((state) => {
      const initialViewers =
        state?.roomInfo?.data?.user_count ||
        state?.roomInfo?.user_count ||
        state?.viewerCount ||
        0;
      if (initialViewers > 0) {
        currentViewerCount = initialViewers;
      }
      console.log(`✅ Conectado à Live de @${cleanUser} (Room ID: ${state.roomId}, Viewers: ${currentViewerCount})`);
      broadcast({
        type: 'status',
        connected: true,
        username: cleanUser,
        roomId: state.roomId,
        viewerCount: currentViewerCount
      });
    })
    .catch((err) => {
      console.error(`❌ Erro ao conectar em @${cleanUser}:`, err.message || err);
      broadcast({
        type: 'status',
        connected: false,
        username: cleanUser,
        error: err.message || 'Falha ao conectar na Live (verifique se está AO VIVO)'
      });
    });

  // Chat messages (v2 uses data.content, v1 uses data.comment)
  tiktokConnection.on('chat', (data) => {
    const rawComment = data?.content || data?.comment || '';
    const nickname = data?.user?.nickname || data?.nickname || data?.user?.uniqueId || data?.uniqueId || 'Torcedor';
    const userId = data?.user?.uniqueId || data?.uniqueId || nickname;
    const avatar =
      data?.user?.profilePicture?.url?.[0] ||
      data?.profilePictureUrl ||
      '';

    const team = detectTeam(rawComment);
    broadcast({
      type: 'chat',
      userId,
      nickname,
      avatar,
      comment: rawComment,
      team
    });
  });

  // Gifts
  tiktokConnection.on('gift', (data) => {
    if (data.giftType === 1 && !data.repeatEnd) return;
    const nickname = data?.user?.nickname || data?.nickname || data?.user?.uniqueId || data?.uniqueId || 'Torcedor VIP';
    const userId = data?.user?.uniqueId || data?.uniqueId || nickname;
    const giftName = data?.giftName || data?.extendedGiftInfo?.name || 'Presente';
    const repeatCount = data?.repeatCount || 1;
    const diamondCount = (data?.diamondCount || data?.extendedGiftInfo?.diamond_count || 1) * repeatCount;

    broadcast({
      type: 'gift',
      userId,
      nickname,
      giftName,
      diamondCount,
      repeatCount
    });
  });

  // Likes
  tiktokConnection.on('like', (data) => {
    const nickname = data?.user?.nickname || data?.nickname || data?.user?.uniqueId || data?.uniqueId || 'Torcedor';
    const userId = data?.user?.uniqueId || data?.uniqueId || nickname;
    const likeCount = data?.likeCount || 1;
    broadcast({
      type: 'like',
      userId,
      nickname,
      likeCount
    });
  });

  // Viewer count updates
  tiktokConnection.on('roomUser', (data) => {
    const count = data?.viewerCount ?? data?.total ?? data?.userCount ?? 0;
    if (typeof count === 'number' && count >= 0) {
      currentViewerCount = count;
      broadcast({ type: 'roomUser', viewerCount: currentViewerCount });
    }
  });

  tiktokConnection.on('member', (data) => {
    const count = data?.memberCount ?? data?.viewerCount ?? 0;
    if (typeof count === 'number' && count > 0) {
      currentViewerCount = count;
      broadcast({ type: 'roomUser', viewerCount: currentViewerCount });
    }
  });

  tiktokConnection.on('streamEnd', () => {
    currentViewerCount = 0;
    broadcast({ type: 'status', connected: false, username: cleanUser, error: 'Live encerrada' });
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
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);

  if (parsedUrl.pathname === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*'
    });
    res.write('\n');
    clients.add(res);

    res.write(
      `data: ${JSON.stringify({
        type: 'status',
        connected: !!(tiktokConnection && tiktokConnection.isConnected),
        username: currentTikTokUser,
        viewerCount: currentViewerCount
      })}\n\n`
    );

    req.on('close', () => {
      clients.delete(res);
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
    res.writeHead(200, { 'Content-Type': MIME_TYPES[ext] || 'application/octet-stream' });
    res.end(content);
  });
});

server.listen(PORT, () => {
  console.log(`🔥 Duelo das 4 Torcidas Live rodando em http://localhost:${PORT}`);
  if (currentTikTokUser) {
    connectToTikTok(currentTikTokUser);
  }
});
