'use strict';
const http = require('http');
const https = require('https');
const { WebSocketServer, WebSocket } = require('ws');
const PORT = process.env.PORT || 8080;

// ── Виртуальный движок (mode=virtual) ──
const virtualEngine = {
  balance: 10000.00,
  equity: 10000.00,
  positions: [],
  trades: 0,
  pnl_today: 0,
  total_pnl: 0,
};

// ── История сделок (последние 200) ──
const tradeHistory = [];

function broadcastToAll(msg) {
  const data = JSON.stringify(msg);
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(data); });
}

// ── Парсинг тела POST запроса ──
function readBody(req) {
  return new Promise((resolve) => {
    let b = '';
    req.on('data', d => { b += d; });
    req.on('end', () => { try { resolve(JSON.parse(b)); } catch { resolve({}); } });
  });
}

const httpServer = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Timestamp, X-Signature');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  function json(code, data) {
    const body = JSON.stringify(data);
    res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(body);
  }

  // ── Health ──
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK'); return;
  }

  // ── REST прокси для фронтенда ──
  if (req.url.startsWith('/api?url=')) {
    const targetUrl = decodeURIComponent(req.url.slice(9));
    if (!targetUrl.startsWith('https://open-api.bingx.com') &&
        !targetUrl.startsWith('https://open-api-swap.bingx.com')) {
      res.writeHead(403); res.end('Forbidden'); return;
    }
    const proxyReq = https.get(targetUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Origin': 'https://bingx.com',
        'Referer': 'https://bingx.com/',
      }
    }, (proxyRes) => {
      res.writeHead(proxyRes.statusCode, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      });
      proxyRes.pipe(res);
    });
    proxyReq.on('error', () => { res.writeHead(502); res.end('{}'); });
    proxyReq.setTimeout(8000, () => { proxyReq.destroy(); res.writeHead(504); res.end('{}'); });
    return;
  }

  // ── Broadcast сигнал от Brain/PowerShell ──
  if (req.method === 'POST' && req.url === '/api/order') {
    const m = await readBody(req);
    m.event = m.event || 'trade.signal';
    m.timestamp = m.timestamp || Date.now();

    // Сохраняем trade.closed в историю
    if (m.event === 'trade.closed') {
      tradeHistory.unshift(m);
      if (tradeHistory.length > 200) tradeHistory.pop();
      // Обновляем виртуальную статистику
      const pnl = +(m.pnl || m.realized_pnl || 0);
      virtualEngine.pnl_today += pnl;
      virtualEngine.total_pnl += pnl;
      virtualEngine.balance += pnl;
      virtualEngine.equity += pnl;
      virtualEngine.trades += 1;
    }

    let n = 0;
    wss.clients.forEach(c => { if (c.readyState === 1) { c.send(JSON.stringify(m)); n++; } });
    json(200, { ok: true, clients: n });
    return;
  }

  // ══════════════════════════════════════════
  // TERMINAL API — для executor.py
  // ══════════════════════════════════════════

  // ── GET /api/balance ──
  if (req.method === 'GET' && req.url === '/api/balance') {
    json(200, {
      code: 0,
      data: {
        balance: {
          balance: virtualEngine.balance.toFixed(2),
          equity: virtualEngine.equity.toFixed(2),
          availableMargin: (virtualEngine.balance * 0.9).toFixed(2),
          unrealizedProfit: '0.00',
        }
      },
      bot: { trades: virtualEngine.trades }
    });
    return;
  }

  // ── GET /api/positions ──
  if (req.method === 'GET' && req.url === '/api/positions') {
    json(200, { code: 0, positions: virtualEngine.positions });
    return;
  }

  // ── GET /api/history ──
  if (req.method === 'GET' && req.url === '/api/history') {
    json(200, { code: 0, trades: tradeHistory });
    return;
  }

  // ── POST /api/limit_batch — виртуальное размещение ордеров ──
  if (req.method === 'POST' && req.url === '/api/limit_batch') {
    const body = await readBody(req);
    const orders = body.orders || [];
    console.log(`[Terminal] limit_batch: ${orders.length} ордеров`);

    // В виртуальном режиме — симулируем исполнение
    const results = orders.map((o, i) => ({
      orderId: `VIRTUAL_${Date.now()}_${i}`,
      symbol: o.symbol,
      side: o.side,
      price: o.price,
      quantity: o.quantity,
      status: 'FILLED',
    }));

    // Добавляем виртуальную позицию
    if (orders.length > 0) {
      const o = orders[0];
      const existing = virtualEngine.positions.findIndex(
        p => p.symbol === o.symbol && p.positionSide === o.positionSide
      );
      if (existing >= 0) {
        virtualEngine.positions[existing].size += orders.length;
      } else {
        virtualEngine.positions.push({
          symbol: o.symbol,
          positionSide: o.positionSide || 'LONG',
          avgPrice: o.price,
          markPrice: o.price,
          leverage: '50',
          unrealizedProfit: '0.00',
          size: orders.length,
        });
      }
    }

    json(200, { code: 0, msg: 'virtual ok', orders: results });
    return;
  }

  // ── POST /api/close — закрыть позицию ──
  if (req.method === 'POST' && req.url === '/api/close') {
    const body = await readBody(req);
    const { symbol, positionSide } = body;

    const idx = virtualEngine.positions.findIndex(
      p => p.symbol === symbol && p.positionSide === positionSide
    );

    let pnl = (Math.random() - 0.4) * 150; // симуляция PnL
    if (idx >= 0) virtualEngine.positions.splice(idx, 1);

    // Обновляем баланс и рассылаем broadcast
    virtualEngine.balance += pnl;
    virtualEngine.equity += pnl;
    virtualEngine.pnl_today += pnl;
    virtualEngine.total_pnl += pnl;
    virtualEngine.trades += 1;

    const tradeMsg = {
      event: 'trade.closed',
      symbol: symbol,
      side: positionSide === 'LONG' ? 'BUY' : 'SELL',
      pnl: +pnl.toFixed(2),
      realized_pnl: +pnl.toFixed(2),
      timestamp: Date.now(),
      source: 'brain',
      mode: 'virtual',
    };
    tradeHistory.unshift(tradeMsg);
    if (tradeHistory.length > 200) tradeHistory.pop();
    broadcastToAll(tradeMsg);

    // Обновляем статистику в терминале
    broadcastToAll({
      event: 'equity.update',
      total_balance: +virtualEngine.balance.toFixed(2),
      pnl_today: +virtualEngine.pnl_today.toFixed(2),
      total_pnl: +virtualEngine.total_pnl.toFixed(2),
    });

    json(200, { code: 0, msg: 'closed', pnl: +pnl.toFixed(2) });
    return;
  }

  // ── POST /api/cancel_batch ──
  if (req.method === 'POST' && req.url === '/api/cancel_batch') {
    const body = await readBody(req);
    console.log(`[Terminal] cancel_batch: ${(body.orderIds || []).length} ордеров`);
    json(200, { code: 0, msg: 'cancelled' });
    return;
  }

  // ── GET /api/order_status/:id ──
  if (req.method === 'GET' && req.url.startsWith('/api/order_status/')) {
    const orderId = req.url.split('/').pop();
    json(200, {
      code: 0,
      data: {
        orderId,
        status: 'FILLED',
        executedQty: '1.0000',
      }
    });
    return;
  }

  // ── GET /status (для проверки executor) ──
  if (req.method === 'GET' && req.url === '/status') {
    json(200, { code: 0, status: 'online', mode: 'virtual', balance: virtualEngine.balance });
    return;
  }

  res.writeHead(404); res.end('Not found');
});

// ── WebSocket сервер ──
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (clientWs, req) => {
  const url = new URL(req.url, 'http://localhost');
  const market = url.searchParams.get('market') || 'swap';

  // /ws/stream — подписка на broadcast (AXONIZE CHANNEL)
  if (req.url === '/ws/stream') {
    console.log('[Broadcast] новый клиент подключён');

    // Отправляем историю сделок сразу при подключении
    if (tradeHistory.length > 0) {
      clientWs.send(JSON.stringify({
        event: 'history',
        trades: tradeHistory,
      }));
    }

    clientWs.on('close', () => console.log('[Broadcast] клиент отключился'));
    return;
  }

  // Обычный WS прокси → BingX
  const bingxUrl = market === 'spot'
    ? 'wss://open-api-ws.bingx.com/market'
    : 'wss://open-api-swap.bingx.com/swap-market';

  let bingxWs = null;
  let reconnectTimer = null;
  let dead = false;

  function connect() {
    if (dead) return;
    bingxWs = new WebSocket(bingxUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Origin': 'https://bingx.com',
        'Host': market === 'spot' ? 'open-api-ws.bingx.com' : 'open-api-swap.bingx.com',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
      }
    });
    bingxWs.on('open', () => console.log(`[Proxy] подключён (${market})`));
    bingxWs.on('message', (data) => { if (clientWs.readyState === 1) clientWs.send(data); });
    bingxWs.on('close', (code) => {
      console.log(`[Proxy] BingX закрыл: ${code} — переподключение через 2с`);
      if (!dead) reconnectTimer = setTimeout(connect, 2000);
    });
    bingxWs.on('error', (err) => {
      console.error(`[Proxy] ошибка: ${err.message}`);
      bingxWs.terminate();
      if (!dead) reconnectTimer = setTimeout(connect, 2000);
    });
  }

  connect();

  clientWs.on('message', (data) => { if (bingxWs && bingxWs.readyState === 1) bingxWs.send(data); });
  clientWs.on('close', () => {
    dead = true;
    clearTimeout(reconnectTimer);
    if (bingxWs) bingxWs.terminate();
  });
  clientWs.on('error', () => {
    dead = true;
    clearTimeout(reconnectTimer);
    if (bingxWs) bingxWs.terminate();
  });
});

httpServer.listen(PORT, () => console.log(`[Proxy] ✅ порт ${PORT}`));
