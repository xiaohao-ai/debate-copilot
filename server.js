/**
 * 反方辩论作战台 · 本地代理服务（零第三方依赖，Node 18+）
 * ------------------------------------------------------------------
 * 作用：
 *   1) 在 127.0.0.1 上托管 index.html（http://127.0.0.1:8787 是安全上下文，麦克风可用）
 *   2) 接收浏览器采集的 PCM 音频（本地 WebSocket /asr）
 *   3) 用火山引擎「大模型流式语音识别」二进制协议转发给豆包语音 ASR
 *   4) 把识别结果归一化成 {t:'asr', confirmed, interim} 回传浏览器
 *
 * 密钥只从本机 config.json 读取，绝不写进前端页面，也不会上传到任何其他地方。
 * 启动：node server.js    停止：Ctrl + C
 */

'use strict';
const http = require('http');
const https = require('https');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const ROOT = __dirname;
const CONFIG_PATH = path.join(ROOT, 'config.json');
const EXAMPLE_PATH = path.join(ROOT, 'config.example.json');
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const UPSTREAM_HOST = 'openspeech.bytedance.com';

/* ---------------- 配置 ---------------- */
function ensureConfig() {
  if (!fs.existsSync(CONFIG_PATH)) {
    try {
      fs.copyFileSync(EXAMPLE_PATH, CONFIG_PATH);
      console.log('\n[初始化] 已为你生成 config.json，请填入火山引擎 X-Api-Key 后重新运行本服务。\n');
    } catch (e) { /* 没有示例文件就跳过 */ }
  }
}
function readConfig() {
  ensureConfig();
  let cfg = {};
  try { cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')); } catch (e) { cfg = {}; }
  const endpointMap = {
    bigmodel_async: '/api/v3/sauc/bigmodel_async',
    bigmodel: '/api/v3/sauc/bigmodel',
    bigmodel_nostream: '/api/v3/sauc/bigmodel_nostream'
  };
  const resourceMap = {
    seedasr2: 'volc.seedasr.sauc.duration',
    bigasr1: 'volc.bigasr.sauc.duration'
  };
  return {
    port: Number(cfg.port) || 8787,
    apiKey: (cfg.apiKey || '').trim(),
    appKey: (cfg.appKey || '').trim(),
    accessKey: (cfg.accessKey || '').trim(),
    arkApiKey: (cfg.arkApiKey || '').trim(),
    arkModel: (cfg.arkModel || 'doubao-seed-2-1-pro-260628').trim(),
    resourceId: cfg.resourceId || resourceMap[cfg.engine] || resourceMap.seedasr2,
    upstreamPath: endpointMap[cfg.endpoint] || endpointMap.bigmodel_async,
    hotwords: Array.isArray(cfg.hotwords) ? cfg.hotwords : []
  };
}

/* ---------------- WebSocket 帧编解码 ---------------- */
// 编码「客户端帧」（发往火山上游，必须带 mask）
function wsClientFrame(payload, opcode) {
  opcode = opcode == null ? 2 : opcode; // 2=binary,1=text
  const len = payload.length;
  let header;
  if (len < 126) { header = Buffer.alloc(2); header[1] = 0x80 | len; }
  else if (len < 65536) { header = Buffer.alloc(4); header[1] = 0x80 | 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[1] = 0x80 | 127; header.writeBigUInt64BE(BigInt(len), 2); }
  header[0] = 0x80 | opcode; // FIN
  const mask = crypto.randomBytes(4);
  const masked = Buffer.alloc(len);
  for (let i = 0; i < len; i++) masked[i] = payload[i] ^ mask[i % 4];
  return Buffer.concat([header, mask, masked]);
}
// 编码「服务端帧」（发往浏览器，不带 mask）
function wsServerFrame(payload, opcode) {
  opcode = opcode == null ? 1 : opcode;
  const len = payload.length;
  let header;
  if (len < 126) { header = Buffer.alloc(2); header[1] = len; }
  else if (len < 65536) { header = Buffer.alloc(4); header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  header[0] = 0x80 | opcode;
  return Buffer.concat([header, payload]);
}
// 通用帧解析器（自动处理 mask / 分片 / ping / close）
function makeFrameParser(onFrame) {
  let buf = Buffer.alloc(0);
  return function onChunk(chunk) {
    buf = Buffer.concat([buf, chunk]);
    while (true) {
      if (buf.length < 2) return;
      const fin = buf[0] & 0x80;
      const opcode = buf[0] & 0x0f;
      const masked = (buf[1] & 0x80) === 0x80;
      let len = buf[1] & 0x7f;
      let off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      let maskKey = null;
      if (masked) { if (buf.length < off + 4) return; maskKey = buf.subarray(off, off + 4); off += 4; }
      if (buf.length < off + len) return;
      let payload = buf.subarray(off, off + len);
      if (masked) { const out = Buffer.alloc(len); for (let i = 0; i < len; i++) out[i] = payload[i] ^ maskKey[i % 4]; payload = out; }
      buf = buf.subarray(off + len);
      if (!fin) { /* 本工具不产生分片，忽略理论上的分片场景 */ }
      onFrame(opcode, payload);
    }
  };
}

/* ---------------- 火山二进制协议组帧 ---------------- */
// header: version(4bit)=1 | headerSize(4bit)=1 ; msgType(4)|flags(4) ; serialization(4)|compression(4) ; reserved
function volcFrame(msgType, flags, serialization, payload) {
  const header = Buffer.alloc(4);
  header[0] = 0x11;
  header[1] = (msgType << 4) | flags;
  header[2] = (serialization << 4); // 不压缩
  header[3] = 0x00;
  const size = Buffer.alloc(4);
  size.writeUInt32BE(payload.length, 0);
  return Buffer.concat([header, size, payload]);
}
const VOLC = { FULL_CLIENT: 1, AUDIO: 2, SERVER_RESP: 9, ERROR: 15 };
const SER = { NONE: 0, JSON: 1 };
const FLAG = { NORMAL: 0, LAST: 2 };

/* ---------------- 一条浏览器连接 = 一次识别会话 ---------------- */
function createSession(browserSocket) {
  const session = { up: null, upReady: false, closed: false, hotwords: [] };

  function sendBrowser(obj) {
    if (browserSocket.destroyed) return;
    try { browserSocket.write(wsServerFrame(Buffer.from(JSON.stringify(obj), 'utf8'), 1)); } catch (e) {}
  }

  function openUpstream() {
    const cfg = readConfig();
    if (!cfg.apiKey && !(cfg.appKey && cfg.accessKey)) {
      sendBrowser({ t: 'error', stage: 'config', message: 'config.json 里还没有填写火山引擎密钥（X-Api-Key），请按使用说明填写后重启服务。' });
      return;
    }
    const authHeaders = cfg.apiKey
      ? { 'X-Api-Key': cfg.apiKey }
      : { 'X-Api-App-Key': cfg.appKey, 'X-Api-Access-Key': cfg.accessKey };
    const headers = Object.assign({
      'Upgrade': 'websocket',
      'Connection': 'Upgrade',
      'Sec-WebSocket-Key': crypto.randomBytes(16).toString('base64'),
      'Sec-WebSocket-Version': '13',
      'X-Api-Resource-Id': cfg.resourceId,
      'X-Api-Request-Id': crypto.randomUUID(),
      'X-Api-Sequence': '-1',
      'X-Api-Connect-Id': crypto.randomUUID()
    }, authHeaders);

    const req = https.request({ host: UPSTREAM_HOST, port: 443, path: cfg.upstreamPath, method: 'GET', headers });
    req.on('upgrade', function (res, upSocket) {
      session.up = upSocket; session.upReady = true;
      upSocket.setKeepAlive(true);
      // full client request（识别参数）
      const requestCfg = {
        user: { uid: 'debate-copilot' },
        audio: { format: 'pcm', rate: 16000, bits: 16, channel: 1 },
        request: {
          model_name: 'bigmodel',
          enable_itn: true,
          enable_punc: true,
          result_type: 'full',
          show_utterances: true
        }
      };
      const words = (session.hotwords && session.hotwords.length ? session.hotwords : cfg.hotwords) || [];
      if (words.length) requestCfg.request.context = JSON.stringify({ hotwords: words.map(function (w) { return { word: w }; }) });
      const payload = Buffer.from(JSON.stringify(requestCfg), 'utf8');
      upSocket.write(wsClientFrame(volcFrame(VOLC.FULL_CLIENT, FLAG.NORMAL, SER.JSON, payload), 2));
      sendBrowser({ t: 'ready' });

      const parse = makeFrameParser(function (opcode, p) {
        if (opcode === 0x8) { // 上游关闭帧：记录原因便于排查
          var code = p.length >= 2 ? p.readUInt16BE(0) : 0;
          console.log('[上游关闭] code=' + code + ' reason=' + p.subarray(2).toString('utf8').slice(0, 200));
          return;
        }
        if (opcode === 0x9) { try { upSocket.write(wsClientFrame(Buffer.alloc(0), 0xA)); } catch (e) {} return; }
        if (opcode !== 2 && opcode !== 1) return;
        handleUpstreamPayload(p);
      });
      upSocket.on('data', parse);
      upSocket.on('close', function (hadErr) { console.log('[上游连接结束] hadError=' + !!hadErr); sendBrowser({ t: 'upstream-closed' }); });
      upSocket.on('error', function (err) { sendBrowser({ t: 'error', stage: 'upstream', message: '与火山服务连接异常：' + err.message }); });
    });
    req.on('error', function (err) {
      sendBrowser({ t: 'error', stage: 'network', message: '无法连接火山语音服务（检查网络）：' + err.message });
    });
    req.end();
  }

  function handleUpstreamPayload(p) {
    if (p.length < 8) return;
    const msgType = (p[1] >> 4) & 0x0f;
    const flags = p[1] & 0x0f;
    const compression = p[2] & 0x0f;
    // 协议：4字节头；当 flags 最低位=1 时，头部后先有 4 字节大端序列号，再是 4 字节 payload 长度，最后是 payload
    let off = 4;
    if (flags & 0x01) off += 4;
    if (p.length < off + 4) return;
    off += 4;
    let body = p.subarray(off);
    if (compression === 1) { try { body = zlib.gunzipSync(body); } catch (e) { return; } }
    let msg;
    try { msg = JSON.parse(body.toString('utf8')); } catch (e) { return; }

    if (msgType === VOLC.ERROR) {
      sendBrowser({ t: 'error', stage: 'asr', message: '火山识别返回错误：' + (body.toString('utf8').slice(0, 300)) });
      return;
    }
    if (msgType !== VOLC.SERVER_RESP) return;
    const r = msg.result || {};
    let confirmed = '', interim = '';
    if (Array.isArray(r.utterances) && r.utterances.length) {
      confirmed = r.utterances.filter(function (u) { return u.definite; }).map(function (u) { return u.text || ''; }).join('');
      interim = r.utterances.filter(function (u) { return !u.definite; }).map(function (u) { return u.text || ''; }).join('');
    } else if (typeof r.text === 'string') {
      interim = r.text;
    }
    if (confirmed || interim) sendBrowser({ t: 'asr', confirmed: confirmed, interim: interim });
  }

  function sendAudio(pcmBuffer) {
    if (!session.upReady || !session.up || session.up.destroyed) return;
    try { session.up.write(wsClientFrame(volcFrame(VOLC.AUDIO, FLAG.NORMAL, SER.NONE, pcmBuffer), 2)); } catch (e) {}
  }
  function finish() {
    try {
      if (session.upReady && session.up && !session.up.destroyed) {
        session.up.write(wsClientFrame(volcFrame(VOLC.AUDIO, FLAG.LAST, SER.NONE, Buffer.alloc(0)), 2));
        setTimeout(function () { try { session.up.end(); } catch (e) {} }, 400);
      }
    } catch (e) {}
    session.upReady = false;
  }

  // 解析浏览器发来的消息
  const browserParse = makeFrameParser(function (opcode, payload) {
    if (opcode === 0x8) { try { browserSocket.end(); } catch (e) {} return; }
    if (opcode === 0x9) { try { browserSocket.write(wsServerFrame(Buffer.alloc(0), 0xA)); } catch (e) {} return; }
    if (opcode === 1) { // 控制指令
      let cmd = {};
      try { cmd = JSON.parse(payload.toString('utf8')); } catch (e) { return; }
      if (cmd.cmd === 'start') { session.hotwords = Array.isArray(cmd.hotwords) ? cmd.hotwords : []; openUpstream(); }
      else if (cmd.cmd === 'stop') { finish(); }
    } else if (opcode === 2) { // PCM 音频
      sendAudio(payload);
    }
  });
  browserSocket.on('data', browserParse);
  browserSocket.on('close', function () { session.closed = true; finish(); });
  browserSocket.on('error', function () {});
}

/* ---------------- 豆包大模型：反方教练提示词（与前端共用 shared/debate-llm.js） ---------------- */
const ARK_HOST = 'ark.cn-beijing.volces.com';
const DebateLLM = require(path.join(ROOT, 'shared', 'debate-llm.js'));
const POSITIONS = DebateLLM.POSITIONS;
function buildLLMMessages(p) { return DebateLLM.buildLLMMessages(p); }

/* ---------------- 静态文件服务 ---------------- */
const MIME = { '.html': 'text/html;charset=utf-8', '.js': 'text/javascript;charset=utf-8', '.json': 'application/json;charset=utf-8', '.md': 'text/markdown;charset=utf-8' };
const server = http.createServer(function (req, res) {
  // 允许 GitHub Pages 等静态页跨源连接自建代理（隧道/局域网部署）
  res.setHeader('Access-Control-Allow-Origin', req.headers.origin || '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  const urlPath = req.url.split('?')[0];
  if (urlPath === '/config-status') {
    const cfg = readConfig();
    res.writeHead(200, { 'Content-Type': 'application/json;charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify({ configured: !!(cfg.apiKey || (cfg.appKey && cfg.accessKey)), llmConfigured: !!cfg.arkApiKey, arkModel: cfg.arkModel, resourceId: cfg.resourceId, endpoint: cfg.upstreamPath, hotwordCount: cfg.hotwords.length }));
    return;
  }
  // 豆包大模型流式生成（SSE）
  if (urlPath === '/llm' && req.method === 'POST') {
    let raw = '';
    req.on('data', function (c) { raw += c; if (raw.length > 300000) req.destroy(); });
    req.on('end', function () {
      let payload = {};
      try { payload = JSON.parse(raw || '{}'); } catch (e) { res.writeHead(400, { 'Content-Type': 'application/json;charset=utf-8' }); res.end(JSON.stringify({ error: 'bad-json' })); return; }
      const cfg = readConfig();
      if (!cfg.arkApiKey) { res.writeHead(400, { 'Content-Type': 'application/json;charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify({ error: 'no-ark-key' })); return; }
      const maxTokens = DebateLLM.maxTokensFor(payload.mode, payload.pos);
      const arkBody = JSON.stringify({ model: payload.model || cfg.arkModel, stream: true, temperature: 0.72, max_tokens: maxTokens, thinking: { type: 'disabled' }, messages: buildLLMMessages(payload) });
      res.writeHead(200, { 'Content-Type': 'text/event-stream;charset=utf-8', 'Cache-Control': 'no-store', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
      const up = https.request({
        host: ARK_HOST, port: 443, path: '/api/v3/chat/completions', method: 'POST', family: 4,
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg.arkApiKey, 'Content-Length': Buffer.byteLength(arkBody) }
      });
      let closed = false;
      function sendEvt(obj) { if (closed) return; try { res.write('data: ' + JSON.stringify(obj) + '\n\n'); } catch (e) {} }
      res.on('close', function () { closed = true; try { up.destroy(); } catch (e) {} });
      up.on('response', function (ur) {
        if (ur.statusCode !== 200) {
          let eb = '';
          ur.on('data', function (d) { eb += d; });
          ur.on('end', function () { sendEvt({ e: 'ark-' + ur.statusCode, detail: eb.slice(0, 600) }); try { res.end(); } catch (e) {} });
          return;
        }
        ur.setEncoding('utf8');
        let tail = '';
        ur.on('data', function (chunk) {
          tail += chunk;
          let idx;
          while ((idx = tail.indexOf('\n')) >= 0) {
            const line = tail.slice(0, idx).trim();
            tail = tail.slice(idx + 1);
            if (!line.startsWith('data:')) continue;
            const data = line.slice(5).trim();
            if (data === '[DONE]') continue;
            try {
              const j = JSON.parse(data);
              const delta = j.choices && j.choices[0] && j.choices[0].delta && j.choices[0].delta.content;
              if (delta) sendEvt({ d: delta });
            } catch (e) {}
          }
        });
        ur.on('end', function () { sendEvt({ done: true }); try { res.end(); } catch (e) {} });
        ur.on('error', function (e) { sendEvt({ e: 'upstream', detail: e.message }); try { res.end(); } catch (e2) {} });
      });
      up.on('error', function (e) { sendEvt({ e: 'network', detail: e.message }); try { res.end(); } catch (e2) {} });
      up.write(arkBody); up.end();
    });
    return;
  }
  let file = urlPath === '/' ? '/index.html' : urlPath;
  const filePath = path.join(ROOT, path.normalize(file).replace(/^(\.\.[/\\])+/, ''));
  if (!filePath.startsWith(ROOT)) { res.writeHead(403); res.end('forbidden'); return; }
  fs.readFile(filePath, function (err, data) {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain;charset=utf-8' }); res.end('未找到文件：' + file); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream', 'Cache-Control': 'no-store' });
    res.end(data);
  });
});

server.on('upgrade', function (req, socket) {
  if ((req.url || '').split('?')[0] !== '/asr') { socket.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  createSession(socket);
});

ensureConfig();
const cfg0 = readConfig();
// 绑定 0.0.0.0：本机走 127.0.0.1（安全上下文，麦克风可用）；同一局域网可经内网 IP 访问
server.listen(cfg0.port, '0.0.0.0', function () {
  let lanIp = '';
  try {
    const os = require('os');
    const ifs = os.networkInterfaces();
    Object.keys(ifs).forEach(function (name) {
      ifs[name].forEach(function (ni) { if (ni.family === 'IPv4' && !ni.internal && !lanIp) lanIp = ni.address; });
    });
  } catch (e) {}
  console.log('========================================================');
  console.log(' 反方辩论作战台 · 本地代理已启动');
  console.log(' 本机访问： http://127.0.0.1:' + cfg0.port + '（推荐，麦克风可用）');
  if (lanIp) console.log(' 同局域网访问： http://' + lanIp + ':' + cfg0.port + '（注意：非 localhost 的 http 页面浏览器会禁用麦克风，外网分享请用 HTTPS 隧道或 GitHub Pages）');
  console.log(' 密钥状态：' + (cfg0.apiKey || (cfg0.appKey && cfg0.accessKey) ? '已配置' : '未配置（请编辑 config.json 后重启）'));
  console.log(' 识别线路：' + cfg0.resourceId + '  ' + cfg0.upstreamPath);
  console.log(' 豆包大模型(AI生成)：' + (cfg0.arkApiKey ? '已配置 · ' + cfg0.arkModel : '未配置（config.json 填 arkApiKey 后启用 AI 生成）'));
  console.log(' 停止服务：在本窗口按 Ctrl + C');
  console.log('========================================================');
});
