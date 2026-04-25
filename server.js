const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const { WebSocketServer } = require('ws');
const http = require('http');
const { nanoid } = require('nanoid');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(cors());
app.use(express.json({ limit: '5mb' }));
app.use(express.static('public'));

// ─── MongoDB Schemas ───────────────────────────────────────────
mongoose.connect(process.env.MONGODB_URI);

const PageSchema = new mongoose.Schema({
  projectId: { type: String, required: true, index: true },
  pageId:    { type: String, required: true },
  name:      { type: String, default: 'Untitled Page' },
  code:      { type: String, default: '' },
  order:     { type: Number, default: 0 },
  updatedAt: { type: Date, default: Date.now },
});

const ProjectSchema = new mongoose.Schema({
  projectId:   { type: String, unique: true, default: () => nanoid(10) },
  name:        { type: String, default: 'My Project' },
  shareToken:  { type: String, unique: true, sparse: true },
  deployedUrl: { type: String },
  createdAt:   { type: Date, default: Date.now },
  updatedAt:   { type: Date, default: Date.now },
});

const Page    = mongoose.model('Page', PageSchema);
const Project = mongoose.model('Project', ProjectSchema);

// ─── WebSocket: room-based live sync ──────────────────────────
const rooms = {}; // projectId → Set of ws clients

wss.on('connection', (ws) => {
  let room = null;

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);

      if (msg.type === 'join') {
        room = msg.projectId;
        if (!rooms[room]) rooms[room] = new Set();
        rooms[room].add(ws);
      }

      // Live preview broadcast (liveCode — not saved, just preview)
      if (msg.type === 'liveCode' && room) {
        broadcast(room, ws, { type: 'liveCode', pageId: msg.pageId, code: msg.code });
      }
    } catch {}
  });

  ws.on('close', () => {
    if (room && rooms[room]) rooms[room].delete(ws);
  });
});

function broadcast(room, sender, data) {
  if (!rooms[room]) return;
  const payload = JSON.stringify(data);
  rooms[room].forEach(client => {
    if (client !== sender && client.readyState === 1) client.send(payload);
  });
}

// ─── API: Projects ─────────────────────────────────────────────
// Create or get project
app.post('/api/project', async (req, res) => {
  try {
    const project = await Project.create({ name: req.body.name || 'My Project' });
    // Create first blank page
    await Page.create({
      projectId: project.projectId,
      pageId: nanoid(8),
      name: 'Page 1',
      code: '',
      order: 0,
    });
    res.json({ success: true, project });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/project/:projectId', async (req, res) => {
  try {
    const project = await Project.findOne({ projectId: req.params.projectId });
    if (!project) return res.status(404).json({ error: 'Project not found' });
    const pages = await Page.find({ projectId: req.params.projectId }).sort('order');
    res.json({ project, pages });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── API: Pages ────────────────────────────────────────────────
app.post('/api/project/:projectId/page', async (req, res) => {
  try {
    const count = await Page.countDocuments({ projectId: req.params.projectId });
    const page = await Page.create({
      projectId: req.params.projectId,
      pageId: nanoid(8),
      name: req.body.name || `Page ${count + 1}`,
      code: '',
      order: count,
    });
    // Notify all clients in room
    const room = req.params.projectId;
    if (rooms[room]) {
      const payload = JSON.stringify({ type: 'pageAdded', page });
      rooms[room].forEach(c => c.readyState === 1 && c.send(payload));
    }
    res.json({ success: true, page });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/project/:projectId/page/:pageId', async (req, res) => {
  try {
    const update = { updatedAt: Date.now() };
    if (req.body.code  !== undefined) update.code = req.body.code;
    if (req.body.name  !== undefined) update.name = req.body.name;
    if (req.body.order !== undefined) update.order = req.body.order;

    const page = await Page.findOneAndUpdate(
      { projectId: req.params.projectId, pageId: req.params.pageId },
      update,
      { new: true }
    );
    await Project.updateOne({ projectId: req.params.projectId }, { updatedAt: Date.now() });

    // Broadcast saved code to all clients (including viewers)
    const room = req.params.projectId;
    if (rooms[room]) {
      const payload = JSON.stringify({ type: 'pageSaved', pageId: req.params.pageId, code: page.code, name: page.name });
      rooms[room].forEach(c => c.readyState === 1 && c.send(payload));
    }
    res.json({ success: true, page });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/project/:projectId/page/:pageId', async (req, res) => {
  try {
    await Page.deleteOne({ projectId: req.params.projectId, pageId: req.params.pageId });
    const room = req.params.projectId;
    if (rooms[room]) {
      const payload = JSON.stringify({ type: 'pageDeleted', pageId: req.params.pageId });
      rooms[room].forEach(c => c.readyState === 1 && c.send(payload));
    }
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── API: Deploy / Share ────────────────────────────────────────
app.post('/api/project/:projectId/deploy', async (req, res) => {
  try {
    const { projectId } = req.params;
    const project = await Project.findOne({ projectId });
    if (!project) return res.status(404).json({ error: 'Not found' });

    let shareToken = project.shareToken;
    if (!shareToken) {
      shareToken = nanoid(12);
      await Project.updateOne({ projectId }, { shareToken });
    }

    const baseUrl = process.env.BASE_URL || `http://localhost:${PORT}`;
    const url = `${baseUrl}/view/${shareToken}`;
    await Project.updateOne({ projectId }, { deployedUrl: url });

    res.json({ success: true, url, shareToken });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Public viewer route ────────────────────────────────────────
app.get('/view/:shareToken', async (req, res) => {
  try {
    const project = await Project.findOne({ shareToken: req.params.shareToken });
    if (!project) return res.status(404).send('<h2>Project not found</h2>');
    const pages = await Page.find({ projectId: project.projectId }).sort('order');

    // Build multi-page viewer HTML
    const pagesJson = JSON.stringify(pages.map(p => ({ pageId: p.pageId, name: p.name, code: p.code })));
    const wsUrl = (process.env.BASE_URL || `ws://localhost:${PORT}`).replace('http', 'ws').replace('https', 'wss');

    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>${project.name} — Live Preview</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:system-ui,sans-serif;background:#0f0f17;display:flex;flex-direction:column;align-items:center;min-height:100vh;padding:20px;}
.top{display:flex;align-items:center;gap:12px;margin-bottom:20px;width:100%;max-width:420px;}
.proj-name{color:white;font-size:16px;font-weight:700;flex:1;}
.live-dot{width:8px;height:8px;border-radius:50%;background:#10B981;animation:pulse 1.5s infinite;}
@keyframes pulse{0%,100%{opacity:1;}50%{opacity:0.4;}}
.nav{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:20px;width:100%;max-width:420px;}
.nav-btn{background:#1e1e2e;color:#888;border:none;padding:6px 14px;border-radius:20px;font-size:12px;cursor:pointer;transition:all 0.2s;}
.nav-btn.active{background:#2A6DF4;color:white;}
.phone{background:#111118;border-radius:40px;padding:10px 6px;box-shadow:0 24px 60px rgba(0,0,0,0.6),0 0 0 8px #1e1e2e;width:380px;max-width:100%;}
.screen{width:100%;min-height:600px;background:#F8F9FC;border-radius:30px;overflow:hidden;}
iframe{width:100%;height:680px;border:none;border-radius:30px;}
</style>
</head>
<body>
<div class="top">
  <div class="proj-name">📱 ${project.name}</div>
  <div class="live-dot" title="Live updates on"></div>
</div>
<div class="nav" id="nav"></div>
<div class="phone"><div class="screen"><iframe id="frame" sandbox="allow-scripts allow-same-origin"></iframe></div></div>

<script>
let pages = ${pagesJson};
let activeIdx = 0;
const nav = document.getElementById('nav');
const frame = document.getElementById('frame');

function renderNav(){
  nav.innerHTML='';
  pages.forEach((p,i)=>{
    const b=document.createElement('button');
    b.className='nav-btn'+(i===activeIdx?' active':'');
    b.textContent=p.name;
    b.onclick=()=>{ activeIdx=i; renderNav(); showPage(); };
    nav.appendChild(b);
  });
}

function showPage(){
  const p=pages[activeIdx];
  const doc=frame.contentDocument||frame.contentWindow.document;
  doc.open();
  doc.write(p.code || '<div style="padding:40px;text-align:center;color:#9CA3AF;font-family:system-ui;">Empty page</div>');
  doc.close();
}

renderNav(); showPage();

// WebSocket live updates
const ws=new WebSocket('${wsUrl}');
ws.onopen=()=>ws.send(JSON.stringify({type:'join',projectId:'${project.projectId}'}));
ws.onmessage=(e)=>{
  const msg=JSON.parse(e.data);
  if(msg.type==='liveCode'||msg.type==='pageSaved'){
    const p=pages.find(x=>x.pageId===msg.pageId);
    if(p){ p.code=msg.code; if(pages.indexOf(p)===activeIdx) showPage(); }
  }
  if(msg.type==='pageAdded'){
    pages.push(msg.page); renderNav();
  }
  if(msg.type==='pageDeleted'){
    pages=pages.filter(x=>x.pageId!==msg.pageId);
    if(activeIdx>=pages.length) activeIdx=Math.max(0,pages.length-1);
    renderNav(); showPage();
  }
  if(msg.type==='pageSaved'&&msg.name){
    const p=pages.find(x=>x.pageId===msg.pageId);
    if(p){ p.name=msg.name; renderNav(); }
  }
};
<\/script>
</body>
</html>`);
  } catch (err) {
    res.status(500).send('<h2>Server error</h2>');
  }
});

// ─── Start ─────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Smart Transit Editor running on port ${PORT}`));