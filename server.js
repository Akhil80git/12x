// ════════════════════════════════════════════════════════════════
//  PageCraft — server.js
//  Auto-detects: localhost OR Render.com — koi extra config nahi
//  .env mein sirf MONGODB_URI chahiye, baki sab automatic!
// ════════════════════════════════════════════════════════════════

const express    = require('express');
const mongoose   = require('mongoose');
const cors       = require('cors');
const { WebSocketServer } = require('ws');
const http       = require('http');
const { nanoid } = require('nanoid');
require('dotenv').config();

// ── Environment auto-detect ──────────────────────────────────────
//  Render pe RENDER_EXTERNAL_HOSTNAME auto-milta hai e.g. "app.onrender.com"
//  .env mein RENDER ya BASE_URL likhne ki zaroorat NAHI — server khud detect karta hai
const RENDER_HOST = process.env.RENDER_EXTERNAL_HOSTNAME || null;
const IS_RENDER   = !!RENDER_HOST;
const PORT        = process.env.PORT || 3000;

// Request se base URL dynamically nikalo
function getBaseUrl(req) {
  if (IS_RENDER) {
    return 'https://' + RENDER_HOST;
  }
  return 'http://localhost:' + PORT;
}

function getWsUrl(req) {
  return getBaseUrl(req)
    .replace('https://', 'wss://')
    .replace('http://',  'ws://');
}

// ── Express + HTTP + WebSocket ───────────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

app.use(cors());
app.use(express.json({ limit: '5mb' }));
app.use(express.static('public'));

// ── MongoDB ──────────────────────────────────────────────────────
mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('✅ MongoDB connected'))
  .catch(err => console.error('❌ MongoDB error:', err.message));

const PageSchema = new mongoose.Schema({
  projectId: { type: String, required: true, index: true },
  pageId:    { type: String, required: true },
  name:      { type: String, default: 'Untitled Page' },
  code:      { type: String, default: '' },
  order:     { type: Number, default: 0 },
  updatedAt: { type: Date,   default: Date.now },
});

const ProjectSchema = new mongoose.Schema({
  projectId:  { type: String, unique: true, default: () => nanoid(10) },
  name:       { type: String, default: 'My Project' },
  shareToken: { type: String, unique: true, sparse: true },
  createdAt:  { type: Date, default: Date.now },
  updatedAt:  { type: Date, default: Date.now },
});

const Page    = mongoose.model('Page',    PageSchema);
const Project = mongoose.model('Project', ProjectSchema);

// ── WebSocket rooms ──────────────────────────────────────────────
const rooms = {}; // projectId -> Set<ws>

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
  rooms[room].forEach(c => {
    if (c !== sender && c.readyState === 1) c.send(payload);
  });
}

function broadcastAll(room, data) {
  if (!rooms[room]) return;
  const payload = JSON.stringify(data);
  rooms[room].forEach(c => {
    if (c.readyState === 1) c.send(payload);
  });
}

// ════════════════════════════════════════════════════════════════
//  API — Projects
// ════════════════════════════════════════════════════════════════

app.post('/api/project', async (req, res) => {
  try {
    const project = await Project.create({ name: req.body.name || 'My Project' });
    await Page.create({
      projectId: project.projectId,
      pageId:    nanoid(8),
      name:      'Page 1',
      code:      '',
      order:     0,
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

// ════════════════════════════════════════════════════════════════
//  API — Pages
// ════════════════════════════════════════════════════════════════

app.post('/api/project/:projectId/page', async (req, res) => {
  try {
    const count = await Page.countDocuments({ projectId: req.params.projectId });
    const page  = await Page.create({
      projectId: req.params.projectId,
      pageId:    nanoid(8),
      name:      req.body.name || `Page ${count + 1}`,
      code:      '',
      order:     count,
    });
    broadcastAll(req.params.projectId, { type: 'pageAdded', page });
    res.json({ success: true, page });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/project/:projectId/page/:pageId', async (req, res) => {
  try {
    const update = { updatedAt: Date.now() };
    if (req.body.code  !== undefined) update.code  = req.body.code;
    if (req.body.name  !== undefined) update.name  = req.body.name;
    if (req.body.order !== undefined) update.order = req.body.order;

    const page = await Page.findOneAndUpdate(
      { projectId: req.params.projectId, pageId: req.params.pageId },
      update,
      { new: true }
    );
    await Project.updateOne(
      { projectId: req.params.projectId },
      { updatedAt: Date.now() }
    );

    broadcastAll(req.params.projectId, {
      type:   'pageSaved',
      pageId: req.params.pageId,
      code:   page.code,
      name:   page.name,
    });

    res.json({ success: true, page });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/project/:projectId/page/:pageId', async (req, res) => {
  try {
    await Page.deleteOne({
      projectId: req.params.projectId,
      pageId:    req.params.pageId,
    });
    broadcastAll(req.params.projectId, {
      type:   'pageDeleted',
      pageId: req.params.pageId,
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ════════════════════════════════════════════════════════════════
//  API — Deploy & Share
//  URL auto-detect hoti hai — localhost ya Render dono pe sahi
// ════════════════════════════════════════════════════════════════

app.post('/api/project/:projectId/deploy', async (req, res) => {
  try {
    const project = await Project.findOne({ projectId: req.params.projectId });
    if (!project) return res.status(404).json({ error: 'Not found' });

    let { shareToken } = project;
    if (!shareToken) {
      shareToken = nanoid(12);
      await Project.updateOne({ projectId: req.params.projectId }, { shareToken });
    }

    // Dynamically build URL — localhost pe localhost, Render pe Render
    const url = `${getBaseUrl(req)}/view/${shareToken}`;
    res.json({ success: true, url, shareToken });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ════════════════════════════════════════════════════════════════
//  PUBLIC VIEW
//  /view/:shareToken          => pehla page (default)
//  /view/:shareToken/:pageId  => specific page
//  Sirf raw HTML serve hoga — koi UI wrapper nahi
// ════════════════════════════════════════════════════════════════

app.get(['/view/:shareToken', '/view/:shareToken/:pageId'], async (req, res) => {
  try {
    const project = await Project.findOne({ shareToken: req.params.shareToken });
    if (!project) return res.status(404).send(errPage('Project not found'));

    const pages = await Page.find({ projectId: project.projectId }).sort('order');
    if (!pages.length) return res.send(errPage('No pages added yet'));

    const page = req.params.pageId
      ? (pages.find(p => p.pageId === req.params.pageId) || pages[0])
      : pages[0];

    const wsUrl = getWsUrl(req);

    // Live-reload script — save hone par automatically update hoga
    const liveScript = `
<script>
(function(){
  var PROJ='${project.projectId}';
  var PAGE='${page.pageId}';
  function connect(){
    var ws=new WebSocket('${wsUrl}');
    ws.onopen=function(){
      ws.send(JSON.stringify({type:'join',projectId:PROJ}));
    };
    ws.onmessage=function(e){
      try{
        var msg=JSON.parse(e.data);
        if(msg.type==='pageSaved' && msg.pageId===PAGE){
          var parser=new DOMParser();
          var newDoc=parser.parseFromString(msg.code,'text/html');
          // body update
          document.body.innerHTML=newDoc.body.innerHTML;
          // styles update
          document.querySelectorAll('style[data-live]').forEach(function(s){s.remove();});
          newDoc.querySelectorAll('style').forEach(function(s){
            var clone=s.cloneNode(true);
            clone.setAttribute('data-live','1');
            document.head.appendChild(clone);
          });
        }
      }catch(err){}
    };
    ws.onclose=function(){
      // Auto-reconnect after 2 sec
      setTimeout(connect,2000);
    };
  }
  connect();
})();
<\/script>`;

    let html = page.code || '<html><body></body></html>';

    // Inject before </body>
    if (/<\/body>/i.test(html)) {
      html = html.replace(/<\/body>/i, liveScript + '\n</body>');
    } else {
      html = html + liveScript;
    }

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache');
    res.send(html);
  } catch (err) {
    res.status(500).send(errPage('Server error: ' + err.message));
  }
});

function errPage(msg) {
  return `<!DOCTYPE html>
<html>
<head><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:system-ui;display:flex;align-items:center;justify-content:center;
             height:100vh;margin:0;background:#f8fafc;">
  <div style="text-align:center;color:#64748b;">
    <div style="font-size:48px;margin-bottom:16px;">📭</div>
    <h2 style="font-weight:600;">${msg}</h2>
  </div>
</body>
</html>`;
}

// ════════════════════════════════════════════════════════════════
//  START
// ════════════════════════════════════════════════════════════════
server.listen(PORT, () => {
  console.log('\n╔════════════════════════════════╗');
  console.log('║      PageCraft  🚀 Started     ║');
  console.log('╚════════════════════════════════╝');
  if (IS_RENDER) {
    console.log(`🌐 Environment  : Render.com`);
    console.log(`🔗 App URL      : https://${process.env.RENDER_EXTERNAL_HOSTNAME}`);
  } else {
    console.log(`💻 Environment  : Localhost`);
    console.log(`🔗 Editor URL   : http://localhost:${PORT}`);
    console.log(`🔗 View URL     : http://localhost:${PORT}/view/<shareToken>`);
  }
  console.log(`📦 Port         : ${PORT}`);
  console.log('');
});