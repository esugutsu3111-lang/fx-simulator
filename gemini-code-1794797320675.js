// ==========================================
// 1. Zero-Dependency MQTT over WebSocket Client (with RETAIN)
// ==========================================
class MiniMQTT {
  constructor(urls, clientId, onMessage, onStatus) {
    this.urls = urls;
    this.urlIdx = 0;
    this.clientId = clientId;
    this.onMessage = onMessage;
    this.onStatus = onStatus;
    this.ws = null;
    this.connected = false;
    this.buf = new Uint8Array(0);
    this.pingTimer = null;
    this.reconnectTimer = null;
    this.subs = [];
    this.closedByUser = false;
  }

  connect() {
    this.closedByUser = false;
    if (this.ws) {
      try { this.ws.close(); } catch (e) {}
    }
    const url = this.urls[this.urlIdx % this.urls.length];
    this.onStatus('CONNECTING');
    try {
      this.ws = new WebSocket(url, 'mqtt');
      this.ws.binaryType = 'arraybuffer';
    } catch (e) {
      this.scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      this.buf = new Uint8Array(0);
      this.sendConnect();
    };

    this.ws.onmessage = (ev) => {
      const chunk = new Uint8Array(ev.data);
      const merged = new Uint8Array(this.buf.length + chunk.length);
      merged.set(this.buf);
      merged.set(chunk, this.buf.length);
      this.buf = merged;
      this.processBuffer();
    };

    this.ws.onerror = () => {};
    this.ws.onclose = () => {
      this.connected = false;
      clearInterval(this.pingTimer);
      this.onStatus('OFFLINE');
      if (!this.closedByUser) {
        this.urlIdx++;
        this.scheduleReconnect();
      }
    };
  }

  scheduleReconnect() {
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => this.connect(), 2500);
  }

  encodeStr(str) {
    const bytes = new TextEncoder().encode(str);
    const out = new Uint8Array(2 + bytes.length);
    out[0] = (bytes.length >> 8) & 0xff;
    out[1] = bytes.length & 0xff;
    out.set(bytes, 2);
    return out;
  }

  encodeVarLen(len) {
    const bytes = [];
    do {
      let digit = len % 128;
      len = Math.floor(len / 128);
      if (len > 0) digit |= 0x80;
      bytes.push(digit);
    } while (len > 0);
    return new Uint8Array(bytes);
  }

  sendPacket(headerByte, payloadParts) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    let totalLen = 0;
    for (const p of payloadParts) totalLen += p.length;
    const varLen = this.encodeVarLen(totalLen);
    const pkt = new Uint8Array(1 + varLen.length + totalLen);
    pkt[0] = headerByte;
    pkt.set(varLen, 1);
    let offset = 1 + varLen.length;
    for (const p of payloadParts) {
      pkt.set(p, offset);
      offset += p.length;
    }
    this.ws.send(pkt);
  }

  sendConnect() {
    const proto = new Uint8Array([0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x04, 0x02, 0x00, 0x1e]);
    const cid = this.encodeStr(this.clientId);
    this.sendPacket(0x10, [proto, cid]);
  }

  subscribe(topic) {
    if (!this.subs.includes(topic)) this.subs.push(topic);
    if (!this.connected) return;
    const pktId = new Uint8Array([0x00, 0x01]);
    const tBytes = this.encodeStr(topic);
    const qos = new Uint8Array([0x00]);
    this.sendPacket(0x82, [pktId, tBytes, qos]);
  }

  publish(topic, obj, retain = false) {
    if (!this.connected) return;
    const tBytes = this.encodeStr(topic);
    const pBytes = new TextEncoder().encode(JSON.stringify(obj));
    this.sendPacket(retain ? 0x31 : 0x30, [tBytes, pBytes]);
  }

  processBuffer() {
    while (this.buf.length >= 2) {
      let multiplier = 1;
      let remLen = 0;
      let idx = 1;
      let encodedByte = 0;
      let completeHeader = false;
      while (idx < this.buf.length) {
        encodedByte = this.buf[idx];
        remLen += (encodedByte & 127) * multiplier;
        multiplier *= 128;
        idx++;
        if ((encodedByte & 128) === 0) {
          completeHeader = true;
          break;
        }
      }
      if (!completeHeader) return;
      const totalPktLen = idx + remLen;
      if (this.buf.length < totalPktLen) return;

      const firstByte = this.buf[0];
      const pktType = firstByte >> 4;
      const body = this.buf.slice(idx, totalPktLen);
      this.buf = this.buf.slice(totalPktLen);

      if (pktType === 2) {
        this.connected = true;
        this.onStatus('ONLINE');
        for (const t of this.subs) this.subscribe(t);
        clearInterval(this.pingTimer);
        this.pingTimer = setInterval(() => {
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(new Uint8Array([0xc0, 0x00]));
          }
        }, 15000);
      } else if (pktType === 3) {
        const qos = (firstByte & 0x06) >> 1;
        if (body.length >= 2) {
          const tLen = (body[0] << 8) | body[1];
          const topic = new TextDecoder().decode(body.slice(2, 2 + tLen));
          const payloadStart = 2 + tLen + (qos > 0 ? 2 : 0);
          const payloadStr = new TextDecoder().decode(body.slice(payloadStart));
          try {
            const data = JSON.parse(payloadStr);
            this.onMessage(topic, data);
          } catch (e) {}
        }
      }
    }
  }
}

// ==========================================
// 2. Constants, State & Persistence
// ==========================================
const TEACHER_CODE = '1234';
const INITIAL_CASH = 1000000;

const PAIR_DEFS = {
  USD: { code: 'USD', name: '🇺🇸 米ドル / 円 (USD/JPY)', base: 150.00, desc: '基準: 150.00円 ｜ 買いはASK（高値）、売りはBID（安値）で約定します' },
  SKR: { code: 'SKR', name: '🌸 サクラコイン (SKR/JPY)', base: 100.00, desc: '基準: 100.00円 ｜ 安定経済国の通貨。値動きが比較的穏やか' },
  TCD: { code: 'TCD', name: '💻 テックドル (TCD/JPY)', base: 150.00, desc: '基準: 150.00円 ｜ IT・半導体成長国の通貨。トレンドが出やすい' },
  RSR: { code: 'RSR', name: '🛢️ リソースレアル (RSR/JPY)', base: 80.00, desc: '基準: 80.00円 ｜ 資源輸出国の通貨。ニュースに大きく反応する' },
  MMP: { code: 'MMP', name: '🚀 ミームペソ (MMP/JPY)', base: 50.00, desc: '基準: 50.00円 ｜ SNS人気だけで動く超投機通貨。急騰と暴落に注意！' }
};

const VOL_CONFIG = {
  TINY:   { noise: 0.0005, impact: 0.0025, aiBias: 0.15 },
  NORMAL: { noise: 0.0015, impact: 0.0055, aiBias: 0.25 },
  HIGH:   { noise: 0.0035, impact: 0.0110, aiBias: 0.40 },
  CRAZY:  { noise: 0.0075, impact: 0.0220, aiBias: 0.60 }
};

let myId = localStorage.getItem('fx_user_id');
if (!myId) {
  myId = 'u_' + Math.random().toString(36).slice(2, 10);
  localStorage.setItem('fx_user_id', myId);
}
const joinedAt = Date.now();
let bootGraceUntil = Date.now() + 2400;

let roomId = new URLSearchParams(location.search).get('room') || localStorage.getItem('fx_last_room') || 'CLASS-1';
document.getElementById('roomInput').value = roomId;

let isTeacher = false;
let selectedPair = 'USD';
let selectedAmt = 100000;
let selectedLev = 1;

let myName = localStorage.getItem('fx_player_name') || ('生徒_' + myId.slice(2, 6).toUpperCase());
document.getElementById('playerNameInput').value = myName;

let cash = INITIAL_CASH;
let positions = [];
let pendingOrders = [];
let bankruptCount = 0;
let lastResetEpoch = 1;
let stats = { trades: 0, wins: 0, spreadCost: 0, maxProfit: 0, maxLoss: 0 };
let playersMap = {};

function createInitialMarket() {
  const rates = {};
  const history = {};
  const demand = {};
  for (const k of Object.keys(PAIR_DEFS)) {
    const b = PAIR_DEFS[k].base;
    rates[k] = b;
    history[k] = Array(32).fill(b);
    demand[k] = 50;
  }
  return {
    mode: 'USD',
    marketOpen: true,
    maxLev: 1,
    vol: 'TINY',
    spread: 0.10,
    aiCount: 15,
    rates,
    history,
    demand,
    orderBook: { bids: [], asks: [] },
    defense: { active: 'NONE', price: 155.00, budget: -1, remaining: -1 },
    countdown: { active: false, title: '', targetTs: 0, shockBias: 0, shockText: '' },
    newsText: '現在、市場は穏やかに推移しています。売値(BID)と買値(ASK)の差（スプレッド）を意識して取引しましょう。',
    newsCalm: true,
    newsBias: 0,
    newsExpiry: 0,
    epoch: 1,
    updatedAt: 0
  };
}

function saveMarketLocal() {
  try { localStorage.setItem('fx_market_v2_' + roomId, JSON.stringify(market)); } catch (e) {}
}

function loadMarketLocal(rId) {
  try {
    const raw = localStorage.getItem('fx_market_v2_' + rId);
    if (raw) {
      const p = JSON.parse(raw);
      if (p && p.rates && p.rates.USD) {
        if (typeof p.spread !== 'number') p.spread = 0.10;
        if (!p.orderBook) p.orderBook = { bids: [], asks: [] };
        if (!p.defense) p.defense = { active: 'NONE', price: 155.00, budget: -1, remaining: -1 };
        if (!p.countdown) p.countdown = { active: false, title: '', targetTs: 0, shockBias: 0, shockText: '' };
        return p;
      }
    }
  } catch (e) {}
  return createInitialMarket();
}

function savePortfolioLocal() {
  try {
    localStorage.setItem('fx_portfolio_v2_' + roomId, JSON.stringify({
      cash, positions, pendingOrders, bankruptCount, lastResetEpoch, stats
    }));
  } catch (e) {}
}

function loadPortfolioLocal(rId) {
  try {
    const raw = localStorage.getItem('fx_portfolio_v2_' + rId);
    if (raw) {
      const p = JSON.parse(raw);
      if (typeof p.cash === 'number' && Array.isArray(p.positions)) {
        cash = p.cash;
        positions = p.positions;
        pendingOrders = Array.isArray(p.pendingOrders) ? p.pendingOrders : [];
        bankruptCount = p.bankruptCount || 0;
        lastResetEpoch = p.lastResetEpoch || 1;
        stats = p.stats || { trades: 0, wins: 0, spreadCost: 0, maxProfit: 0, maxLoss: 0 };
        return;
      }
    }
  } catch (e) {}
  cash = INITIAL_CASH;
  positions = [];
  pendingOrders = [];
  bankruptCount = 0;
  lastResetEpoch = 1;
  stats = { trades: 0, wins: 0, spreadCost: 0, maxProfit: 0, maxLoss: 0 };
}

let market = loadMarketLocal(roomId);
loadPortfolioLocal(roomId);
if (market.epoch && market.epoch > lastResetEpoch) {
  lastResetEpoch = market.epoch;
  cash = INITIAL_CASH;
  positions = [];
  pendingOrders = [];
  bankruptCount = 0;
  stats = { trades: 0, wins: 0, spreadCost: 0, maxProfit: 0, maxLoss: 0 };
  savePortfolioLocal();
}
selectedPair = market.mode === 'FOUR' ? 'SKR' : 'USD';
document.getElementById('limitPriceInput').value = market.rates.USD.toFixed(2);

let knownHosts = {};

// ==========================================
// 3. BID / ASK Quote Helpers
// ==========================================
function getQuotes(pair) {
  const mid = market.rates[pair] || PAIR_DEFS[pair].base;
  const sp = Math.max(0, Number(market.spread) || 0);
  const half = sp / 2;
  const bid = Math.max(0.01, +(mid - half).toFixed(2));
  const ask = +(mid + half).toFixed(2);
  return { mid, bid, ask, spread: +sp.toFixed(2) };
}

// ==========================================
// 4. MQTT Sync Setup
// ==========================================
const mqtt = new MiniMQTT(
  ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt'],
  'fx_' + myId + '_' + Math.random().toString(36).slice(2, 5),
  handleIncomingMessage,
  (st) => {
    const dot = document.getElementById('connDot');
    const txt = document.getElementById('connText');
    dot.className = 'dot ' + (st === 'ONLINE' ? 'online' : (st === 'OFFLINE' ? 'offline' : ''));
    txt.textContent = st === 'ONLINE' ? '同期中' : (st === 'OFFLINE' ? '再接続中' : '接続中');
    if (st === 'ONLINE') broadcastPresence();
  }
);

function subTopics() {
  mqtt.subs = [];
  mqtt.subscribe(`saitama_fx_pro/${roomId}/state`);
  mqtt.subscribe(`saitama_fx_pro/${roomId}/order`);
  mqtt.subscribe(`saitama_fx_pro/${roomId}/fills`);
  mqtt.subscribe(`saitama_fx_pro/${roomId}/player`);
  mqtt.subscribe(`saitama_fx_pro/${roomId}/host`);
}

subTopics();
mqtt.connect();

function changeRoom() {
  const val = document.getElementById('roomInput').value.trim().toUpperCase();
  if (!val) return;
  roomId = val;
  localStorage.setItem('fx_last_room', roomId);
  playersMap = {};
  knownHosts = {};
  bootGraceUntil = Date.now() + 2400;
  market = loadMarketLocal(roomId);
  loadPortfolioLocal(roomId);
  selectedPair = market.mode === 'FOUR' ? 'SKR' : 'USD';
  subTopics();
  if (mqtt.connected) {
    mqtt.subscribe(`saitama_fx_pro/${roomId}/state`);
    mqtt.subscribe(`saitama_fx_pro/${roomId}/order`);
    mqtt.subscribe(`saitama_fx_pro/${roomId}/fills`);
    mqtt.subscribe(`saitama_fx_pro/${roomId}/player`);
    mqtt.subscribe(`saitama_fx_pro/${roomId}/host`);
  }
  showToast(`合言葉「${roomId}」に入室しました`);
  updateAllUI();
}

function handleIncomingMessage(topic, data) {
  const kind = topic.split('/').pop();

  if (kind === 'state') {
    if (!data || !data.rates) return;
    const incEpoch = data.epoch || 1;
    const curEpoch = market.epoch || 1;
    const incTs = data.updatedAt || 0;
    const curTs = market.updatedAt || 0;

    if (incEpoch < curEpoch) return;
    if (incEpoch === curEpoch && incTs < curTs) return;

    if (incEpoch > lastResetEpoch) {
      lastResetEpoch = incEpoch;
      cash = INITIAL_CASH;
      positions = [];
      pendingOrders = [];
      bankruptCount = 0;
      stats = { trades: 0, wins: 0, spreadCost: 0, maxProfit: 0, maxLoss: 0 };
      savePortfolioLocal();
      showToast('🔄 先生により相場と資産がリセットされました');
    }

    const prevNews = market.newsText;
    const prevMode = market.mode;
    market = data;
    saveMarketLocal();

    if (market.newsText !== prevNews && !market.newsCalm) {
      playBeep(880, 1174);
      showToast('📢 緊急ニュース速報が入りました！');
    }
    if (market.mode !== prevMode) {
      selectedPair = market.mode === 'FOUR' ? 'SKR' : 'USD';
      document.getElementById('limitPriceInput').value = market.rates.USD.toFixed(2);
    } else if (market.mode !== 'FOUR' && selectedPair !== 'USD') {
      selectedPair = 'USD';
    } else if (market.mode === 'FOUR' && selectedPair === 'USD') {
      selectedPair = 'SKR';
    }

    checkLiquidation();
    updateAllUI();
  } else if (kind === 'order') {
    if (!amIMaster() || !data) return;
    if (data.type === 'MARKET_IMPACT' && data.sender !== myId) {
      applyOrderImpactToMarket(data.pair, data.dir, data.volume);
    } else if (data.type === 'BOOK_ORDER' && data.sender !== myId) {
      processBookOrderOnMaster(data);
    } else if (data.type === 'CANCEL_BOOK' && data.sender !== myId) {
      cancelBookOrderOnMaster(data.orderId);
    }
  } else if (kind === 'fills') {
    if (!data) return;
    handleFillNotification(data);
  } else if (kind === 'player') {
    if (data && data.id) {
      playersMap[data.id] = {
        id: data.id,
        name: data.name || '名無し',
        equity: data.equity,
        pnl: data.pnl,
        bankruptCount: data.bankruptCount || 0,
        ts: Date.now()
      };
      renderLeaderboard();
    }
  } else if (kind === 'host') {
    if (data && data.id) {
      knownHosts[data.id] = {
        id: data.id,
        isTeacher: !!data.isTeacher,
        joinedAt: data.joinedAt || Date.now(),
        ts: Date.now()
      };
    }
  }
}

function amIMaster() {
  if (isTeacher) return true;
  const now = Date.now();
  for (const k of Object.keys(knownHosts)) {
    if (now - knownHosts[k].ts > 6000) delete knownHosts[k];
  }
  const active = Object.values(knownHosts);
  if (active.some(h => h.isTeacher)) return false;
  if (active.length === 0) return true;
  active.sort((a, b) => a.joinedAt - b.joinedAt);
  return active[0].id === myId;
}

// ==========================================
// 5. Market Engine & Central Bank Defense
// ==========================================
function getActivePairs() {
  return market.mode === 'FOUR' ? ['SKR', 'TCD', 'RSR', 'MMP'] : ['USD'];
}

function applyCentralBankDefense(pair, candidateRate) {
  const def = market.defense;
  if (!def || def.active === 'NONE') return candidateRate;
  if (market.mode === 'FOUR' && pair !== 'SKR') return candidateRate;
  if (market.mode !== 'FOUR' && pair !== 'USD') return candidateRate;

  const limitP = Number(def.price) || 155;
  if (def.active === 'CEILING' && candidateRate > limitP) {
    const excessYen = Math.round((candidateRate - limitP) * 2000000);
    if (def.budget < 0) {
      return limitP;
    } else if (def.remaining > excessYen) {
      def.remaining -= excessYen;
      return limitP;
    } else {
      def.active = 'NONE';
      def.remaining = 0;
      market.newsText = `💥【速報】中央銀行の防衛ライン（${limitP.toFixed(2)}円）が突破されました！防衛資金が枯渇！`;
      market.newsCalm = false;
      market.newsBias = 0.5;
      market.newsExpiry = Date.now() + 20000;
      return candidateRate;
    }
  } else if (def.active === 'FLOOR' && candidateRate < limitP) {
    const excessYen = Math.round((limitP - candidateRate) * 2000000);
    if (def.budget < 0) {
      return limitP;
    } else if (def.remaining > excessYen) {
      def.remaining -= excessYen;
      return limitP;
    } else {
      def.active = 'NONE';
      def.remaining = 0;
      market.newsText = `💥【速報】中央銀行の下限防衛ライン（${limitP.toFixed(2)}円）が突破されました！`;
      market.newsCalm = false;
      market.newsBias = -0.5;
      market.newsExpiry = Date.now() + 20000;
      return candidateRate;
    }
  }
  return candidateRate;
}

function applyOrderImpactToMarket(pair, dir, volumeYen) {
  if (!market.rates[pair]) return;
  const vCfg = VOL_CONFIG[market.vol] || VOL_CONFIG.TINY;
  const units = Math.min(20, Math.max(0.2, volumeYen / 100000));
  const sign = dir === 'BUY' ? 1 : -1;
  const pctChange = sign * vCfg.impact * Math.sqrt(units);
  let newRate = Math.max(1.0, +(market.rates[pair] * (1 + pctChange)).toFixed(2));
  newRate = applyCentralBankDefense(pair, newRate);
  market.rates[pair] = newRate;

  const shift = sign * Math.min(18, 4 * Math.sqrt(units));
  market.demand[pair] = Math.max(8, Math.min(92, Math.round(market.demand[pair] + shift)));
}

function pushHistoryPoint() {
  for (const k of Object.keys(PAIR_DEFS)) {
    const arr = market.history[k] || [];
    arr.push(market.rates[k]);
    if (arr.length > 35) arr.shift();
    market.history[k] = arr;
  }
}

// ==========================================
// 6. Mode C: Real Order Book Matching Engine
// ==========================================
function seedAiOrderBook() {
  const aiN = Number(market.aiCount) || 0;
  const ob = market.orderBook;
  ob.asks = ob.asks.filter(o => !o.isAi);
  ob.bids = ob.bids.filter(o => !o.isAi);

  if (aiN <= 0) return;

  const { bid, ask } = getQuotes('USD');
  const biasShift = (market.newsBias || 0) * 0.4;
  const levels = Math.min(8, Math.max(2, Math.ceil(aiN / 6)));
  const perLevelYen = Math.max(100000, Math.round((aiN / levels) * 150000));

  for (let i = 0; i < levels; i++) {
    const step = (i * 0.08) + (Math.random() * 0.05) + biasShift;
    const askP = Math.max(1.0, +(ask + step).toFixed(2));
    const bidP = Math.max(1.0, +(bid - (i * 0.08) - (Math.random() * 0.05) + biasShift).toFixed(2));

    if (!(market.defense.active === 'FLOOR' && askP < market.defense.price)) {
      ob.asks.push({
        id: 'ai_a_' + i,
        ownerId: 'AI',
        ownerName: `AIトレーダー#${i + 1}`,
        price: askP,
        remainingYen: perLevelYen,
        lev: 1,
        isAi: true,
        ts: Date.now()
      });
    }
    if (!(market.defense.active === 'CEILING' && bidP > market.defense.price)) {
      ob.bids.push({
        id: 'ai_b_' + i,
        ownerId: 'AI',
        ownerName: `AIトレーダー#${i + 10}`,
        price: bidP,
        remainingYen: perLevelYen,
        lev: 1,
        isAi: true,
        ts: Date.now()
      });
    }
  }

  ob.asks.sort((a, b) => a.price - b.price || a.ts - b.ts);
  ob.bids.sort((a, b) => b.price - a.price || a.ts - b.ts);
}

function processBookOrderOnMaster(ord) {
  const ob = market.orderBook;
  let remYen = ord.tradeYen;
  const fills = [];

  if (ord.side === 'BUY') {
    ob.asks.sort((a, b) => a.price - b.price || a.ts - b.ts);
    while (remYen >= 1000 && ob.asks.length > 0 && ob.asks[0].price <= ord.price) {
      const best = ob.asks[0];
      const matchYen = Math.min(remYen, best.remainingYen);
      const execPrice = best.price;
      market.rates.USD = applyCentralBankDefense('USD', execPrice);

      fills.push({
        targetOwner: ord.sender,
        orderId: ord.orderId,
        side: 'BUY',
        execPrice,
        matchYen,
        isSettlement: !!ord.isSettlement,
        posId: ord.posId || null
      });
      if (!best.isAi) {
        fills.push({
          targetOwner: best.ownerId,
          orderId: best.id,
          side: 'SELL',
          execPrice,
          matchYen,
          isSettlement: false,
          posId: null
        });
      }

      remYen -= matchYen;
      best.remainingYen -= matchYen;
      if (best.remainingYen < 1000) ob.asks.shift();
    }

    if (remYen >= 1000 && !ord.isSettlement) {
      ob.bids.push({
        id: ord.orderId,
        ownerId: ord.sender,
        ownerName: ord.senderName,
        price: ord.price,
        remainingYen: remYen,
        lev: ord.lev,
        isAi: false,
        ts: Date.now()
      });
      ob.bids.sort((a, b) => b.price - a.price || a.ts - b.ts);
      if (ob.bids.length > 20) ob.bids.length = 20;
    }
  } else {
    ob.bids.sort((a, b) => b.price - a.price || a.ts - b.ts);
    while (remYen >= 1000 && ob.bids.length > 0 && ob.bids[0].price >= ord.price) {
      const best = ob.bids[0];
      const matchYen = Math.min(remYen, best.remainingYen);
      const execPrice = best.price;
      market.rates.USD = applyCentralBankDefense('USD', execPrice);

      fills.push({
        targetOwner: ord.sender,
        orderId: ord.orderId,
        side: 'SELL',
        execPrice,
        matchYen,
        isSettlement: !!ord.isSettlement,
        posId: ord.posId || null
      });
      if (!best.isAi) {
        fills.push({
          targetOwner: best.ownerId,
          orderId: best.id,
          side: 'BUY',
          execPrice,
          matchYen,
          isSettlement: false,
          posId: null
        });
      }

      remYen -= matchYen;
      best.remainingYen -= matchYen;
      if (best.remainingYen < 1000) ob.bids.shift();
    }

    if (remYen >= 1000 && !ord.isSettlement) {
      ob.asks.push({
        id: ord.orderId,
        ownerId: ord.sender,
        ownerName: ord.senderName,
        price: ord.price,
        remainingYen: remYen,
        lev: ord.lev,
        isAi: false,
        ts: Date.now()
      });
      ob.asks.sort((a, b) => a.price - b.price || a.ts - b.ts);
      if (ob.asks.length > 20) ob.asks.length = 20;
    }
  }

  const totalBid = ob.bids.reduce((s, o) => s + o.remainingYen, 0);
  const totalAsk = ob.asks.reduce((s, o) => s + o.remainingYen, 0);
  if (totalBid + totalAsk > 0) {
    market.demand.USD = Math.max(5, Math.min(95, Math.round((totalBid / (totalBid + totalAsk)) * 100)));
  }

  const payload = {
    sender: ord.sender,
    orderId: ord.orderId,
    fills,
    unfilledYen: remYen,
    isSettlement: !!ord.isSettlement,
    posId: ord.posId || null
  };

  handleFillNotification(payload);
  mqtt.publish(`saitama_fx_pro/${roomId}/fills`, payload, false);
  broadcastMarketState();
}

function cancelBookOrderOnMaster(orderId) {
  const ob = market.orderBook;
  ob.bids = ob.bids.filter(o => o.id !== orderId);
  ob.asks = ob.asks.filter(o => o.id !== orderId);
  broadcastMarketState();
}

function handleFillNotification(payload) {
  const myFills = (payload.fills || []).filter(f => f.targetOwner === myId);

  for (const f of myFills) {
    if (f.isSettlement && f.posId) {
      const pIdx = positions.findIndex(p => p.id === f.posId);
      if (pIdx !== -1) {
        const pos = positions[pIdx];
        const diff = pos.dir === 'BUY' ? (f.execPrice - pos.entryRate) : (pos.entryRate - f.execPrice);
        const pnl = Math.round(diff * pos.qty);
        const payout = Math.max(0, pos.margin + pnl);
        cash += payout;
        positions.splice(pIdx, 1);
        recordClosedTrade(pnl);
        const sign = pnl >= 0 ? '+' : '';
        showToast(`🤝 板約定で決済完了！(${f.execPrice.toFixed(2)}円) 損益: ${sign}${pnl.toLocaleString()}円`);
      }
    } else {
      const pendIdx = pendingOrders.findIndex(o => o.orderId === f.orderId);
      let lev = 1;
      let usedMargin = 0;
      if (pendIdx !== -1) {
        const pend = pendingOrders[pendIdx];
        lev = pend.lev;
        usedMargin = Math.min(pend.remainingMargin, Math.round(f.matchYen / lev));
        pend.remainingMargin -= usedMargin;
        if (pend.remainingMargin < 500) {
          pendingOrders.splice(pendIdx, 1);
        }
      } else {
        usedMargin = Math.round(f.matchYen);
      }

      if (usedMargin > 0) {
        const qty = (usedMargin * lev) / f.execPrice;
        const spCost = Math.round((market.spread / 2) * qty);
        stats.spreadCost += spCost;

        positions.push({
          id: 'pos_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5),
          pair: 'USD',
          dir: f.side,
          entryRate: f.execPrice,
          margin: usedMargin,
          lev,
          qty
        });
        showToast(`🤝 売買成立！ ${f.execPrice.toFixed(2)}円で${f.side === 'BUY' ? '買い' : '売り'}約定しました`);
      }
    }
  }

  if (payload.sender === myId && payload.unfilledYen >= 1000) {
    if (payload.isSettlement) {
      showToast('⚠️ 買い手(売り手)が板にいないため決済できません！誰かの注文を待ってください');
    } else if (myFills.length === 0) {
      showToast('📋 条件に合う相手がいないため即約定せず、板に並べて待機中！');
    } else {
      showToast('📋 一部約定し、残りは板に並べて待機中です');
    }
  }

  savePortfolioLocal();
  broadcastPresence();
  updateAllUI();
}

// ==========================================
// 7. Master Simulation Loop (1.2s Tick)
// ==========================================
setInterval(() => {
  mqtt.publish(`saitama_fx_pro/${roomId}/host`, {
    id: myId, isTeacher, joinedAt
  });

  const master = amIMaster();
  const roleLbl = document.getElementById('masterRoleLabel');
  if (roleLbl) roleLbl.textContent = master ? '状態: ホスト維持中（リロード耐性ON）' : '状態: 受信同期中';

  updateCountdownBanner();

  if (!master) return;
  if (Date.now() < bootGraceUntil) return;

  const now = Date.now();

  if (market.countdown && market.countdown.active && now >= market.countdown.targetTs) {
    const cd = market.countdown;
    cd.active = false;
    market.newsText = cd.shockText || `【指標発表】${cd.title} が発表され、市場に大きな衝撃が走りました！`;
    market.newsCalm = false;
    market.newsBias = cd.shockBias || 0.7;
    market.newsExpiry = now + 22000;
    const targetP = market.mode === 'FOUR' ? 'SKR' : 'USD';
    const shockPct = (cd.shockBias || 0.6) * 0.025;
    let shocked = +(market.rates[targetP] * (1 + shockPct)).toFixed(2);
    market.rates[targetP] = applyCentralBankDefense(targetP, shocked);
  }

  if (market.newsExpiry && now > market.newsExpiry) {
    market.newsBias = 0;
    market.newsCalm = true;
    market.newsText = '市場は落ち着きを取り戻しています。売値(BID)と買値(ASK)の差（スプレッド）を意識して取引しましょう。';
    market.newsExpiry = 0;
  }

  if (market.marketOpen) {
    if (market.mode === 'BOOK') {
      seedAiOrderBook();
      pushHistoryPoint();
    } else {
      const vCfg = VOL_CONFIG[market.vol] || VOL_CONFIG.TINY;
      const pairs = getActivePairs();

      for (const p of pairs) {
        const rnd = (Math.random() - 0.5) * 2 * vCfg.noise;
        let netPct = rnd;

        const aiN = Number(market.aiCount) || 0;
        if (aiN > 0) {
          const hist = market.history[p];
          const prev = hist.length >= 2 ? hist[hist.length - 2] : market.rates[p];
          const trend = (market.rates[p] - prev) / prev;
          const aiSentiment = (trend * 8) + (market.newsBias * vCfg.aiBias) + ((Math.random() - 0.5) * 0.6);
          const activeAi = Math.ceil(aiN * 0.25);
          const aiOrderSign = aiSentiment >= 0 ? 1 : -1;
          const aiStrength = Math.min(1.5, Math.abs(aiSentiment)) * (activeAi / 15);
          netPct += aiOrderSign * vCfg.impact * 0.45 * aiStrength;

          const targetDemand = Math.max(15, Math.min(85, 50 + aiSentiment * 25));
          market.demand[p] = Math.round(market.demand[p] * 0.8 + targetDemand * 0.2);
        } else {
          market.demand[p] = Math.round(market.demand[p] * 0.85 + 50 * 0.15);
        }

        if (p === 'MMP') netPct *= 1.6;
        let nextR = Math.max(1.0, +(market.rates[p] * (1 + netPct)).toFixed(2));
        nextR = applyCentralBankDefense(p, nextR);
        market.rates[p] = nextR;
      }
      pushHistoryPoint();
    }
  }

  broadcastMarketState();
  checkLiquidation();
  updateAllUI();
}, 1200);

function broadcastMarketState() {
  market.updatedAt = Date.now();
  saveMarketLocal();
  mqtt.publish(`saitama_fx_pro/${roomId}/state`, market, true);
}

function broadcastPresence() {
  const { equity, totalPnl } = calcPortfolio();
  playersMap[myId] = {
    id: myId,
    name: myName,
    equity,
    pnl: totalPnl,
    bankruptCount,
    ts: Date.now()
  };
  mqtt.publish(`saitama_fx_pro/${roomId}/player`, playersMap[myId], false);
  renderLeaderboard();
}
setInterval(broadcastPresence, 2000);

// ==========================================
// 8. Portfolio, BID/ASK PnL, Liquidation & Revival
// ==========================================
function calcPosPnl(pos) {
  const { bid, ask } = getQuotes(pos.pair);
  const closeRate = pos.dir === 'BUY' ? bid : ask;
  const diff = pos.dir === 'BUY' ? (closeRate - pos.entryRate) : (pos.entryRate - closeRate);
  return Math.round(diff * pos.qty);
}

function calcPortfolio() {
  let lockedMargin = 0;
  let unrealizedPnl = 0;
  for (const pos of positions) {
    lockedMargin += pos.margin;
    unrealizedPnl += calcPosPnl(pos);
  }
  let escrowedMargin = 0;
  for (const po of pendingOrders) {
    escrowedMargin += po.remainingMargin;
  }
  const equity = Math.round(cash + lockedMargin + escrowedMargin + unrealizedPnl);
  const totalPnl = equity - INITIAL_CASH;
  return { equity, totalPnl, lockedMargin, unrealizedPnl };
}

function recordClosedTrade(pnl) {
  stats.trades += 1;
  if (pnl > 0) stats.wins += 1;
  if (pnl > stats.maxProfit) stats.maxProfit = pnl;
  if (pnl < stats.maxLoss) stats.maxLoss = pnl;
}

function checkLiquidation() {
  if (positions.length === 0) return;
  const survived = [];
  let liquidatedCount = 0;

  for (const pos of positions) {
    const pnl = calcPosPnl(pos);
    if (pnl < 0 && (pos.margin + pnl) <= pos.margin * 0.20) {
      const returned = Math.max(0, pos.margin + pnl);
      cash += returned;
      recordClosedTrade(pnl);
      liquidatedCount++;
    } else {
      survived.push(pos);
    }
  }

  if (liquidatedCount > 0) {
    positions = survived;
    savePortfolioLocal();
    showToast(`⚠️ 損失拡大により ${liquidatedCount}件 が強制ロスカットされました！`);
    broadcastPresence();
  }
}

function reviveFromBankruptcy() {
  if (positions.length > 0 || pendingOrders.length > 0 || cash >= 10000) return;
  cash = 50000;
  bankruptCount += 1;
  savePortfolioLocal();
  broadcastPresence();
  updateAllUI();
  showToast('💸 50,000円の救済資金で再スタートしました！');
}

function placeOrder(dir) {
  if (!market.marketOpen) {
    showToast('⏸ 現在、先生により取引が一時停止されています');
    return;
  }
  const lev = Math.min(selectedLev, market.maxLev);
  let margin = selectedAmt === 'ALL' ? Math.floor(cash) : Math.min(selectedAmt, Math.floor(cash));
  if (margin < 10000) {
    showToast('余力（現金）が不足しています（最低1万円）');
    return;
  }

  const tradeVolumeYen = margin * lev;

  if (market.mode === 'BOOK') {
    const limitP = parseFloat(document.getElementById('limitPriceInput').value);
    if (isNaN(limitP) || limitP <= 0) {
      showToast('有効な指値価格を入力してください');
      return;
    }
    const orderId = 'ord_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5);
    cash -= margin;
    pendingOrders.push({
      orderId,
      pair: 'USD',
      dir,
      limitPrice: +limitP.toFixed(2),
      totalMargin: margin,
      remainingMargin: margin,
      lev
    });
    savePortfolioLocal();

    const ordPayload = {
      type: 'BOOK_ORDER',
      sender: myId,
      senderName: myName,
      orderId,
      side: dir,
      price: +limitP.toFixed(2),
      tradeYen: tradeVolumeYen,
      margin,
      lev,
      isSettlement: false
    };

    if (amIMaster()) {
      processBookOrderOnMaster(ordPayload);
    } else {
      mqtt.publish(`saitama_fx_pro/${roomId}/order`, ordPayload, false);
    }
    updateAllUI();
    return;
  }

  const { bid, ask, spread } = getQuotes(selectedPair);
  const execRate = dir === 'BUY' ? ask : bid;
  const qty = tradeVolumeYen / execRate;
  const spCost = Math.round((spread / 2) * qty);
  stats.spreadCost += spCost;

  cash -= margin;
  positions.push({
    id: 'p_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5),
    pair: selectedPair,
    dir,
    entryRate: execRate,
    margin,
    lev,
    qty
  });
  savePortfolioLocal();

  if (amIMaster()) {
    applyOrderImpactToMarket(selectedPair, dir, tradeVolumeYen);
    broadcastMarketState();
  } else {
    mqtt.publish(`saitama_fx_pro/${roomId}/order`, {
      type: 'MARKET_IMPACT',
      sender: myId,
      pair: selectedPair,
      dir,
      volume: tradeVolumeYen
    }, false);
    applyOrderImpactToMarket(selectedPair, dir, tradeVolumeYen * 0.5);
  }

  showToast(`${PAIR_DEFS[selectedPair].code} ${dir === 'BUY' ? '買(ASK)' : '売(BID)'} ${execRate.toFixed(2)}円で約定！`);
  broadcastPresence();
  updateAllUI();
}

function cancelPendingOrder(orderId) {
  const idx = pendingOrders.findIndex(o => o.orderId === orderId);
  if (idx === -1) return;
  const ord = pendingOrders[idx];
  cash += ord.remainingMargin;
  pendingOrders.splice(idx, 1);
  savePortfolioLocal();

  if (amIMaster()) {
    cancelBookOrderOnMaster(orderId);
  } else {
    mqtt.publish(`saitama_fx_pro/${roomId}/order`, {
      type: 'CANCEL_BOOK',
      sender: myId,
      orderId
    }, false);
  }
  showToast('待機中の注文を取り消し、証拠金を返還しました');
  broadcastPresence();
  updateAllUI();
}

function settlePosition(posId) {
  const idx = positions.findIndex(p => p.id === posId);
  if (idx === -1) return;
  const pos = positions[idx];

  if (market.mode === 'BOOK') {
    const closeSide = pos.dir === 'BUY' ? 'SELL' : 'BUY';
    const ordPayload = {
      type: 'BOOK_ORDER',
      sender: myId,
      senderName: myName,
      orderId: 'settle_' + Date.now(),
      side: closeSide,
      price: closeSide === 'SELL' ? 1.0 : 99999.0,
      tradeYen: Math.round(pos.qty * market.rates.USD),
      margin: pos.margin,
      lev: pos.lev,
      isSettlement: true,
      posId: pos.id
    };
    if (amIMaster()) {
      processBookOrderOnMaster(ordPayload);
    } else {
      mqtt.publish(`saitama_fx_pro/${roomId}/order`, ordPayload, false);
    }
    return;
  }

  const pnl = calcPosPnl(pos);
  const payout = Math.max(0, pos.margin + pnl);
  cash += payout;
  positions.splice(idx, 1);
  recordClosedTrade(pnl);
  savePortfolioLocal();

  const counterDir = pos.dir === 'BUY' ? 'SELL' : 'BUY';
  const vol = pos.margin * pos.lev;
  if (amIMaster()) {
    applyOrderImpactToMarket(pos.pair, counterDir, vol * 0.7);
    broadcastMarketState();
  } else {
    mqtt.publish(`saitama_fx_pro/${roomId}/order`, {
      type: 'MARKET_IMPACT',
      sender: myId,
      pair: pos.pair,
      dir: counterDir,
      volume: vol * 0.7
    }, false);
  }

  const sign = pnl >= 0 ? '+' : '';
  showToast(`決済完了！ 確定損益: ${sign}${pnl.toLocaleString()}円`);
  broadcastPresence();
  updateAllUI();
}

function settleAllPositions() {
  if (positions.length === 0) return;

  if (market.mode === 'BOOK') {
    const copy = [...positions];
    for (const p of copy) settlePosition(p.id);
    return;
  }

  let totalPnl = 0;
  let totalPayout = 0;

  for (const pos of positions) {
    const pnl = calcPosPnl(pos);
    totalPnl += pnl;
    totalPayout += Math.max(0, pos.margin + pnl);
    recordClosedTrade(pnl);

    const counterDir = pos.dir === 'BUY' ? 'SELL' : 'BUY';
    const vol = pos.margin * pos.lev;
    if (amIMaster()) {
      applyOrderImpactToMarket(pos.pair, counterDir, vol * 0.6);
    } else {
      mqtt.publish(`saitama_fx_pro/${roomId}/order`, {
        type: 'MARKET_IMPACT',
        sender: myId,
        pair: pos.pair,
        dir: counterDir,
        volume: vol * 0.6
      }, false);
    }
  }

  const count = positions.length;
  cash += totalPayout;
  positions = [];
  savePortfolioLocal();
  if (amIMaster()) broadcastMarketState();

  const sign = totalPnl >= 0 ? '+' : '';
  showToast(`⚡ ${count}件を一斉決済しました！（合計損益: ${sign}${totalPnl.toLocaleString()}円）`);
  broadcastPresence();
  updateAllUI();
}

// ==========================================
// 9. UI Rendering
// ==========================================
function formatFxDigits(val) {
  const s = val.toFixed(2);
  const parts = s.split('.');
  return `${parts[0]}.<span class="big-dec">${parts[1]}</span>`;
}

function savePlayerName() {
  const v = document.getElementById('playerNameInput').value.trim();
  if (v) {
    myName = v;
    localStorage.setItem('fx_player_name', myName);
    broadcastPresence();
    showToast(`名前を「${myName}」に変更しました`);
  }
}

function selectPair(code) {
  selectedPair = code;
  updateAllUI();
}

function selectAmt(amt) {
  selectedAmt = amt;
  document.querySelectorAll('#amtBtns .sel-btn').forEach(b => {
    b.classList.toggle('active', String(b.dataset.amt) === String(amt));
  });
  updateTradePowerLabel();
}

function selectLev(lev) {
  if (lev > market.maxLev) return;
  selectedLev = lev;
  document.querySelectorAll('#levBtns .sel-btn').forEach(b => {
    b.classList.toggle('active', Number(b.dataset.lev) === lev);
  });
  updateTradePowerLabel();
}

function updateTradePowerLabel() {
  const m = selectedAmt === 'ALL' ? Math.floor(cash) : selectedAmt;
  const power = Math.round((m * selectedLev) / 10000);
  document.getElementById('tradePowerNote').textContent = `取引規模: ${power.toLocaleString()}万円分`;
}

function stepLimitPrice(delta) {
  const inp = document.getElementById('limitPriceInput');
  const cur = parseFloat(inp.value) || market.rates.USD;
  inp.value = Math.max(1.0, +(cur + delta).toFixed(2)).toFixed(2);
}

function syncLimitToMarket() {
  document.getElementById('limitPriceInput').value = market.rates.USD.toFixed(2);
}

function updateCountdownBanner() {
  const banner = document.getElementById('countdownBanner');
  const cd = market.countdown;
  if (cd && cd.active && cd.targetTs > Date.now()) {
    const secLeft = Math.ceil((cd.targetTs - Date.now()) / 1000);
    banner.classList.add('show');
    document.getElementById('cdEventTitle').textContent = `${cd.title} 発表まで`;
    document.getElementById('cdTimerVal').textContent = `あと ${secLeft} 秒`;
  } else {
    banner.classList.remove('show');
  }
}

function updateAllUI() {
  const modeNames = {
    USD: 'モードA：ドル円集中',
    FOUR: 'モードB：4通貨マーケット',
    BOOK: 'モードC：リアル板マッチング'
  };
  const modeBadge = document.getElementById('headerModeBadge');
  modeBadge.textContent = modeNames[market.mode] || 'ドル円集中モード';
  modeBadge.className = 'mode-pill' + (market.mode === 'BOOK' ? ' real-book' : '');

  const newsBar = document.getElementById('newsBar');
  newsBar.className = 'news-bar' + (market.newsCalm ? ' calm' : '');
  document.getElementById('newsTag').textContent = market.newsCalm ? 'MARKET' : '緊急速報';
  document.getElementById('newsMsg').textContent = market.newsText;

  const { equity, totalPnl } = calcPortfolio();
  document.getElementById('totalEquityVal').textContent = equity.toLocaleString() + ' 円';
  document.getElementById('cashVal').textContent = Math.floor(cash).toLocaleString() + ' 円';
  const pnlEl = document.getElementById('totalPnlVal');
  pnlEl.textContent = (totalPnl >= 0 ? '+' : '') + totalPnl.toLocaleString() + ' 円';
  pnlEl.className = 's-val num ' + (totalPnl > 0 ? 'up' : (totalPnl < 0 ? 'down' : 'flat'));

  updateTradePowerLabel();

  const isBankrupt = (positions.length === 0 && pendingOrders.length === 0 && cash < 10000);
  document.getElementById('bankruptBox').classList.toggle('show', isBankrupt);

  const activePairs = getActivePairs();
  const tabsEl = document.getElementById('pairTabs');
  tabsEl.className = 'pair-tabs' + (activePairs.length === 1 ? ' single' : '');
  tabsEl.innerHTML = activePairs.map(code => {
    const def = PAIR_DEFS[code];
    const r = market.rates[code];
    const diff = +(r - def.base).toFixed(2);
    const cls = diff > 0 ? 'up' : (diff < 0 ? 'down' : 'flat');
    const sign = diff > 0 ? '+' : '';
    return `
      <div class="pair-tab ${code === selectedPair ? 'active' : ''}" onclick="selectPair('${code}')">
        <div class="pt-code">${code}/JPY</div>
        <div class="pt-rate num">${r.toFixed(2)}</div>
        <div class="pt-diff num ${cls}">${sign}${diff.toFixed(2)}</div>
      </div>
    `;
  }).join('');

  const curDef = PAIR_DEFS[selectedPair];
  const { mid, bid, ask, spread } = getQuotes(selectedPair);
  const baseDiff = +(mid - curDef.base).toFixed(2);

  document.getElementById('heroTitle').textContent = curDef.name;
  document.getElementById('heroDesc').textContent = curDef.desc;
  document.getElementById('bidPriceDisp').innerHTML = formatFxDigits(bid);
  document.getElementById('askPriceDisp').innerHTML = formatFxDigits(ask);
  document.getElementById('spreadDisp').textContent = spread.toFixed(2);
  document.getElementById('spreadSenDisp').textContent = `(${Math.round(spread * 100)}銭)`;

  document.getElementById('btnSellSub').textContent = `${bid.toFixed(2)} 円`;
  document.getElementById('btnBuySub').textContent = `${ask.toFixed(2)} 円`;

  const yenBadge = document.getElementById('yenBadge');
  if (baseDiff > 0) {
    yenBadge.className = 'yen-pill yen-weak';
    yenBadge.textContent = `基準比 +${baseDiff.toFixed(2)}円 【 円安・${curDef.code}高 】`;
  } else if (baseDiff < 0) {
    yenBadge.className = 'yen-pill yen-strong';
    yenBadge.textContent = `基準比 ${baseDiff.toFixed(2)}円 【 円高・${curDef.code}安 】`;
  } else {
    yenBadge.className = 'yen-pill yen-even';
    yenBadge.textContent = '基準レートと同値（±0.00円）';
  }

  const defBanner = document.getElementById('defenseBanner');
  if (market.defense && market.defense.active !== 'NONE') {
    defBanner.classList.add('show');
    const dirTxt = market.defense.active === 'CEILING' ? '上限防衛（円安阻止）' : '下限防衛（円高阻止）';
    document.getElementById('defenseText').textContent = `🏦 中央銀行 ${dirTxt}: ${Number(market.defense.price).toFixed(2)}円`;
    document.getElementById('defenseBudgetText').textContent =
      market.defense.budget < 0 ? '防衛資金: 無制限' : `残り防衛資金: ${(market.defense.remaining / 10000).toLocaleString()}万円`;
  } else {
    defBanner.classList.remove('show');
  }

  const buyPct = market.demand[selectedPair] || 50;
  document.getElementById('buyRatioLabel').textContent = `買い需要 ${buyPct}%`;
  document.getElementById('sellRatioLabel').textContent = `売り供給 ${100 - buyPct}%`;
  document.getElementById('sdBuyBar').style.width = buyPct + '%';

  const mBadge = document.getElementById('marketStatusBadge');
  mBadge.className = 'm-status ' + (market.marketOpen ? 'm-open' : 'm-closed');
  mBadge.textContent = market.marketOpen ? '取引受付中 (OPEN)' : '一時停止中 (STOP)';
  document.getElementById('btnBuy').disabled = !market.marketOpen || isBankrupt;
  document.getElementById('btnSell').disabled = !market.marketOpen || isBankrupt;

  document.getElementById('levStatusNote').textContent = `現在 ${market.maxLev}倍 まで許可`;
  document.getElementById('lev5Btn').disabled = market.maxLev < 5;
  document.getElementById('lev10Btn').disabled = market.maxLev < 10;
  document.getElementById('lev25Btn').disabled = market.maxLev < 25;
  if (selectedLev > market.maxLev) selectLev(1);

  const isBook = market.mode === 'BOOK';
  document.getElementById('orderBookCard').style.display = isBook ? 'block' : 'none';
  document.getElementById('limitOrderBox').classList.toggle('show', isBook);
  document.getElementById('pendingWrap').style.display = (isBook && pendingOrders.length > 0) ? 'block' : 'none';

  if (isBook) {
    renderOrderBook();
    renderPendingOrders();
  }

  document.getElementById('posCount').textContent = positions.length;
  document.getElementById('btnSettleAll').disabled = positions.length === 0;
  const posListEl = document.getElementById('posList');
  if (positions.length === 0) {
    posListEl.innerHTML = '<div style="font-size:0.74rem; color:var(--muted); text-align:center; padding:10px;">保有中のポジションはありません</div>';
  } else {
    posListEl.innerHTML = positions.map(pos => {
      const pnl = calcPosPnl(pos);
      const cls = pnl > 0 ? 'up' : (pnl < 0 ? 'down' : 'flat');
      const sign = pnl > 0 ? '+' : '';
      return `
        <div class="pos-card">
          <div>
            <div>
              <span class="badge-dir ${pos.dir === 'BUY' ? 'b-buy' : 'b-sell'}">${pos.dir === 'BUY' ? '買' : '売'}</span>
              <strong>${pos.pair}/JPY</strong> (${pos.lev}倍 / ${(pos.margin/10000).toFixed(1).replace(/\.0$/, '')}万円)
            </div>
            <div style="color:var(--muted); font-size:0.68rem; margin-top:2px;">
              約定: ${pos.entryRate.toFixed(2)}円 → 損益: <strong class="num ${cls}">${sign}${pnl.toLocaleString()}円</strong>
            </div>
          </div>
          <button class="btn-close-one" onclick="settlePosition('${pos.id}')">決済</button>
        </div>
      `;
    }).join('');
  }

  const winRate = stats.trades > 0 ? Math.round((stats.wins / stats.trades) * 100) : 0;
  document.getElementById('kWinRate').textContent = `${stats.trades}回 ／ ${winRate}%`;
  document.getElementById('kSpreadCost').textContent = `${stats.spreadCost.toLocaleString()} 円`;
  document.getElementById('kMaxProfit').textContent = `+${stats.maxProfit.toLocaleString()} 円`;
  document.getElementById('kMaxLoss').textContent = `${stats.maxLoss.toLocaleString()} 円`;

  if (isTeacher && document.getElementById('btnModeUSD')) {
    syncTeacherPanelUI();
  }

  drawChart();
  renderLeaderboard();
}

function renderOrderBook() {
  const ob = market.orderBook || { asks: [], bids: [] };
  const asksEl = document.getElementById('bookAsksList');
  const bidsEl = document.getElementById('bookBidsList');

  if (ob.asks.length === 0) {
    asksEl.innerHTML = '<div style="color:var(--muted); text-align:center; padding:8px; font-size:0.68rem;">売り注文なし</div>';
  } else {
    asksEl.innerHTML = ob.asks.slice(0, 8).map(o => `
      <div class="book-row ${o.ownerId === myId ? 'mine' : ''}">
        <span class="up num"><strong>${o.price.toFixed(2)}円</strong></span>
        <span>${escapeHtml(o.ownerName)}</span>
        <span class="num">${Math.round(o.remainingYen / 10000)}万</span>
      </div>
    `).join('');
  }

  if (ob.bids.length === 0) {
    bidsEl.innerHTML = '<div style="color:var(--muted); text-align:center; padding:8px; font-size:0.68rem;">買い注文なし</div>';
  } else {
    bidsEl.innerHTML = ob.bids.slice(0, 8).map(o => `
      <div class="book-row ${o.ownerId === myId ? 'mine' : ''}">
        <span class="down num"><strong>${o.price.toFixed(2)}円</strong></span>
        <span>${escapeHtml(o.ownerName)}</span>
        <span class="num">${Math.round(o.remainingYen / 10000)}万</span>
      </div>
    `).join('');
  }
}

function renderPendingOrders() {
  document.getElementById('pendingCount').textContent = pendingOrders.length;
  const el = document.getElementById('pendingList');
  el.innerHTML = pendingOrders.map(o => `
    <div class="pos-card">
      <div>
        <span class="badge-dir b-wait">待機中</span>
        <strong>${o.dir === 'BUY' ? '買い指値' : '売り指値'} ${o.limitPrice.toFixed(2)}円</strong>
        <div style="color:var(--muted); font-size:0.68rem;">残り証拠金: ${o.remainingMargin.toLocaleString()}円 (${o.lev}倍)</div>
      </div>
      <button class="btn-close-one" onclick="cancelPendingOrder('${o.orderId}')">取消</button>
    </div>
  `).join('');
}

function renderLeaderboard() {
  const now = Date.now();
  const { equity, totalPnl } = calcPortfolio();
  playersMap[myId] = { id: myId, name: myName, equity, pnl: totalPnl, bankruptCount, ts: now };

  const list = Object.values(playersMap).filter(p => now - p.ts < 35000);
  list.sort((a, b) => b.equity - a.equity);

  document.getElementById('onlineCount').textContent = list.length;
  const myIndex = list.findIndex(p => p.id === myId);
  document.getElementById('myRankVal').textContent =
    myIndex >= 0 ? `${myIndex + 1} 位 / ${list.length} 人` : `- 位 / ${list.length} 人`;

  const tbody = document.getElementById('leaderboardBody');
  tbody.innerHTML = list.map((p, i) => {
    const rank = i + 1;
    const medal = rank === 1 ? '🥇 1位' : (rank === 2 ? '🥈 2位' : (rank === 3 ? '🥉 3位' : `${rank}位`));
    const cls = p.pnl > 0 ? 'up' : (p.pnl < 0 ? 'down' : 'flat');
    const sign = p.pnl > 0 ? '+' : '';
    const isMe = p.id === myId;
    const rev = p.bankruptCount > 0 ? ` <span style="font-size:0.64rem; color:#f87171;">(復活${p.bankruptCount})</span>` : '';
    return `
      <tr class="${isMe ? 'me-row' : ''}">
        <td>${medal}</td>
        <td>${escapeHtml(p.name)}${rev}${isMe ? ' <span style="color:#fbbf24;">(あなた)</span>' : ''}</td>
        <td class="num"><strong>${p.equity.toLocaleString()}円</strong></td>
        <td class="num ${cls}">${sign}${p.pnl.toLocaleString()}円</td>
      </tr>
    `;
  }).join('');
}

function drawChart() {
  const canvas = document.getElementById('rateChart');
  if (!canvas) return;
  const rect = canvas.parentElement.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const w = rect.width;
  const h = rect.height;
  ctx.clearRect(0, 0, w, h);

  const data = market.history[selectedPair] || [PAIR_DEFS[selectedPair].base];
  const baseRate = PAIR_DEFS[selectedPair].base;
  const allVals = [...data, baseRate];
  const hasDefense = market.defense && market.defense.active !== 'NONE' &&
    ((market.mode === 'FOUR' && selectedPair === 'SKR') || (market.mode !== 'FOUR' && selectedPair === 'USD'));
  if (hasDefense) allVals.push(Number(market.defense.price));

  let minV = Math.min(...allVals);
  let maxV = Math.max(...allVals);
  const pad = Math.max((maxV - minV) * 0.25, baseRate * 0.004);
  minV -= pad;
  maxV += pad;

  const leftPad = 48;
  const rightPad = 12;
  const topPad = 12;
  const botPad = 16;
  const plotW = w - leftPad - rightPad;
  const plotH = h - topPad - botPad;

  const getY = (val) => topPad + plotH - ((val - minV) / (maxV - minV)) * plotH;

  ctx.strokeStyle = '#1e222d';
  ctx.lineWidth = 1;
  ctx.fillStyle = '#868993';
  ctx.font = '10px sans-serif';
  ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) {
    const v = minV + ((maxV - minV) * i) / 4;
    const y = getY(v);
    ctx.beginPath();
    ctx.moveTo(leftPad, y);
    ctx.lineTo(w - rightPad, y);
    ctx.stroke();
    ctx.fillText(v.toFixed(2), leftPad - 4, y + 3);
  }

  const baseY = getY(baseRate);
  ctx.save();
  ctx.strokeStyle = '#475569';
  ctx.setLineDash([4, 4]);
  ctx.beginPath();
  ctx.moveTo(leftPad, baseY);
  ctx.lineTo(w - rightPad, baseY);
  ctx.stroke();
  ctx.restore();

  if (hasDefense) {
    const defY = getY(Number(market.defense.price));
    ctx.save();
    ctx.strokeStyle = '#a8