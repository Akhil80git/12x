const express    = require('express');
const mongoose   = require('mongoose');
const cors       = require('cors');
const { WebSocketServer } = require('ws');
const http       = require('http');
const { nanoid } = require('nanoid');
require('dotenv').config();

const RENDER_HOST = process.env.RENDER_EXTERNAL_HOSTNAME || '';
const IS_RENDER   = RENDER_HOST.length > 0;
const PORT        = process.env.PORT || 3000;

function getBaseUrl(req) {
  if (IS_RENDER) return 'https://' + RENDER_HOST;
  return 'http://' + req.headers.host;
}
function getWsUrl(req) {
  return getBaseUrl(req).replace('https://', 'wss://').replace('http://', 'ws://');
}

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

app.use(cors());
app.use(express.json({ limit: '5mb' }));
app.use(express.static('public'));

mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('MongoDB connected'))
  .catch(err => console.error('MongoDB error:', err.message));

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

// ── WebSocket ────────────────────────────────────────────────────
const rooms = {};

wss.on('connection', (ws) => {
  let myRoom = null;

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type === 'join' && msg.projectId) {
        myRoom = msg.projectId;
        if (!rooms[myRoom]) rooms[myRoom] = new Set();
        rooms[myRoom].add(ws);
        ws.send(JSON.stringify({ type: 'joined', projectId: myRoom }));
      }

      if (msg.type === 'liveCode' && myRoom) {
        broadcastOthers(myRoom, ws, {
          type:   'liveCode',
          pageId: msg.pageId,
          code:   msg.code,
        });
      }
    } catch (e) {}
  });

  ws.on('close', () => {
    if (myRoom && rooms[myRoom]) rooms[myRoom].delete(ws);
  });

  ws.on('error', () => {});
});

function broadcastOthers(room, sender, data) {
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

// ── API: Projects ────────────────────────────────────────────────
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

// ── API: Pages ───────────────────────────────────────────────────
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

// ── SAVE PAGE ─────────────────────────────────────────────────────
// Jab editor save karta hai:
// 1. DB mein code update hota hai
// 2. broadcastAll se viewer ko naya code milta hai
// 3. Viewer iframe.srcdoc = naya code → poora page fresh load
app.put('/api/project/:projectId/page/:pageId', async (req, res) => {
  try {
    const { projectId, pageId } = req.params;
    const update = { updatedAt: Date.now() };
    if (req.body.code  !== undefined) update.code  = req.body.code;
    if (req.body.name  !== undefined) update.name  = req.body.name;
    if (req.body.order !== undefined) update.order = req.body.order;

    const page = await Page.findOneAndUpdate(
      { projectId, pageId },
      update,
      { returnDocument: 'after' }   // mongoose deprecation warning bhi fix
    );

    if (!page) return res.status(404).json({ error: 'Page not found' });

    await Project.updateOne({ projectId }, { updatedAt: Date.now() });

    broadcastAll(projectId, {
      type:   'pageSaved',
      pageId: pageId,
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
    await Page.deleteOne({ projectId: req.params.projectId, pageId: req.params.pageId });
    broadcastAll(req.params.projectId, { type: 'pageDeleted', pageId: req.params.pageId });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Deploy ───────────────────────────────────────────────────────
app.post('/api/project/:projectId/deploy', async (req, res) => {
  try {
    const project = await Project.findOne({ projectId: req.params.projectId });
    if (!project) return res.status(404).json({ error: 'Not found' });

    let { shareToken } = project;
    if (!shareToken) {
      shareToken = nanoid(12);
      await Project.updateOne({ projectId: req.params.projectId }, { shareToken });
    }

    const url = getBaseUrl(req) + '/view/' + shareToken;
    res.json({ success: true, url, shareToken });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── VIEW (shared link) ───────────────────────────────────────────
// Fix: viewer ek wrapper HTML hai jisme:
//   - <iframe id="f"> = user ka actual HTML (srcdoc se load)
//   - WS script iframe ke BAHAR hai
// Jab pageSaved aata hai → f.srcdoc = msg.code
// Poora HTML replace hota hai (head+body+styles+scripts sab)
// WS connection alive rehti hai kyunki wo iframe ke bahar hai
app.get(['/view/:shareToken', '/view/:shareToken/:pageId'], async (req, res) => {
  try {
    const project = await Project.findOne({ shareToken: req.params.shareToken });
    if (!project) return res.status(404).send(errPage('Project not found'));

    const pages = await Page.find({ projectId: project.projectId }).sort('order');
    if (!pages.length) return res.send(errPage('No pages yet'));

    const page = req.params.pageId
      ? (pages.find(p => p.pageId === req.params.pageId) || pages[0])
      : pages[0];

    const wsUrl     = getWsUrl(req);
    const projectId = project.projectId;
    const pageId    = page.pageId;
    const initCode  = JSON.stringify(page.code || '');

    const html = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${esc(page.name)}</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
html,body{width:100%;height:100%;overflow:hidden;background:#fff}
#f{width:100%;height:100%;border:none;display:block}
#badge{
  position:fixed;bottom:12px;right:12px;z-index:99999;
  background:rgba(0,0,0,0.6);color:#22c55e;
  font:600 11px/1 system-ui,sans-serif;
  padding:5px 10px;border-radius:20px;
  display:flex;align-items:center;gap:5px;
  pointer-events:none;transition:color .3s;
}
#badge .d{width:6px;height:6px;border-radius:50%;background:#22c55e;animation:p 2s infinite}
@keyframes p{0%,100%{opacity:1}50%{opacity:.2}}
#badge.upd{color:#f59e0b}
#badge.upd .d{background:#f59e0b;animation:none}
</style>
</head>
<body>
<iframe id="f" sandbox="allow-scripts allow-same-origin allow-forms allow-popups"></iframe>
<div id="badge"><span class="d"></span><span id="bt">Live</span></div>
<script>
(function(){
  var ws,retries=0;
  var f=document.getElementById('f');
  var badge=document.getElementById('badge');
  var bt=document.getElementById('bt');
  var WSURL=${JSON.stringify(wsUrl)};
  var PROJ=${JSON.stringify(projectId)};
  var PAGE=${JSON.stringify(pageId)};

  function load(code){
    f.srcdoc=code||'<body style="font:14px system-ui;display:flex;align-items:center;justify-content:center;height:100vh;color:#94a3b8">Empty page</body>';
  }

  load(${initCode});

  function connect(){
    ws=new WebSocket(WSURL);
    ws.onopen=function(){
      ws.send(JSON.stringify({type:'join',projectId:PROJ}));
      retries=0;
      badge.classList.remove('upd');
      bt.textContent='Live';
    };
    ws.onmessage=function(e){
      try{
        var msg=JSON.parse(e.data);
        if(msg.type==='pageSaved'&&msg.pageId===PAGE){
          badge.classList.add('upd');
          bt.textContent='Updating...';
          load(msg.code);
          setTimeout(function(){
            badge.classList.remove('upd');
            bt.textContent='Updated \u2713';
            setTimeout(function(){bt.textContent='Live';},2000);
          },400);
        }
      }catch(err){}
    };
    ws.onclose=function(){
      retries++;
      bt.textContent='Reconnecting...';
      setTimeout(connect,Math.min(retries*1000,8000));
    };
    ws.onerror=function(){ws.close();};
  }

  connect();
})();
</script>
</body>
</html>`;

    res.setHeader('Content-Type','text/html;charset=utf-8');
    res.setHeader('Cache-Control','no-store');
    res.send(html);
  } catch(err){
    res.status(500).send(errPage('Server error'));
  }
});

function esc(s){
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function errPage(msg){
  return `<!DOCTYPE html><html><body style="font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;color:#64748b"><div style="text-align:center"><div style="font-size:48px">📭</div><h2>${msg}</h2></div></body></html>`;
}

server.listen(PORT,()=>{
  console.log('PageCraft started on port '+PORT);
});