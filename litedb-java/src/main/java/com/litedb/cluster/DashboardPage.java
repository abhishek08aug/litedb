package com.litedb.cluster;

/** The dashboard's HTML/CSS/JS, kept apart from {@link Dashboard} for readability. Mirrors the
 * Python dashboard UI. %NODES% and %SHARDS% are substituted at serve time. */
final class DashboardPage {
    private DashboardPage() {}

    static final String HTML = """
<!doctype html>
<html><head><meta charset="utf-8"><title>litedb — distributed cluster</title>
<style>
  :root{--bg:#0d1117;--panel:#161b22;--line:#30363d;--dim:#8b949e;--fg:#c9d1d9;--ok:#3fb950;--bad:#f85149;--warn:#d29922}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace}
  header{padding:10px 16px;border-bottom:1px solid var(--line);display:flex;gap:12px;align-items:center;flex-wrap:wrap;position:sticky;top:0;background:var(--bg);z-index:5}
  h1{font-size:15px;margin:0;font-weight:600}
  .chip{font-size:11px;padding:3px 8px;border-radius:5px;border:1px solid var(--line);color:var(--dim)}
  .chip b{color:var(--fg)}
  .badge{font-weight:600;padding:3px 9px;border-radius:5px}
  .badge.ok{background:#132e1a;color:var(--ok);border:1px solid #238636}
  .badge.bad{background:#3a1416;color:var(--bad);border:1px solid #da3633}
  .controls{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-left:auto}
  input{background:#0d1117;border:1px solid var(--line);color:var(--fg);padding:5px 7px;border-radius:5px;font:inherit;width:96px}
  button{background:#21262d;border:1px solid var(--line);color:var(--fg);padding:5px 10px;border-radius:5px;cursor:pointer;font:inherit}
  button:hover{background:#30363d}
  button.primary{background:#1f6feb;border-color:#1f6feb;color:#fff}
  button.danger{background:#da3633;border-color:#da3633;color:#fff}
  .wrap{padding:12px;display:flex;flex-direction:column;gap:12px}
  .section-title{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:var(--dim);margin:4px 2px}
  .cards{display:grid;grid-template-columns:240px 300px 1fr;gap:12px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:10px 12px}
  .card h3{margin:0 0 8px;font-size:12px;color:var(--dim);font-weight:600;text-transform:uppercase;letter-spacing:.05em}
  .kv{display:flex;justify-content:space-between;padding:2px 0;border-bottom:1px solid #1c2128}
  .kv span:last-child{color:var(--fg)}
  table{border-collapse:collapse;width:100%;font-size:12px}
  th,td{padding:4px 7px;text-align:center;border:1px solid var(--line)}
  th{color:var(--dim);font-weight:600}
  td.shard{text-align:left;color:var(--dim)}
  .cellL{background:#132e1a;color:var(--ok);font-weight:700}
  .cellF{background:#161b22;color:var(--dim)}
  .cellNo{background:#0d1117;color:#30363d}
  .star{color:var(--warn)}
  .nodes{display:grid;gap:12px}
  .node{background:var(--panel);border:1px solid var(--line);border-radius:8px;display:flex;flex-direction:column;min-height:54vh}
  .node.down{opacity:.5}
  .nhead{padding:9px 12px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:8px}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--ok)}
  .down .dot{background:var(--bad)}
  .nname{font-weight:600}.nmeta{color:var(--dim);font-size:11px;margin-left:auto;text-align:right}
  .shards{display:flex;flex-wrap:wrap;gap:4px;padding:7px 12px;border-bottom:1px solid var(--line)}
  .sc{font-size:10px;padding:2px 6px;border-radius:4px;border:1px solid var(--line);color:var(--dim)}
  .sc.lead{background:#132e1a;border-color:#238636;color:var(--ok)}
  .feed{flex:1;overflow-y:auto;padding:6px 10px;display:flex;flex-direction:column-reverse;max-height:42vh}
  .ev{padding:3px 0;border-bottom:1px solid #1c2128}
  .ev .cat{display:inline-block;min-width:74px;font-size:10px;text-transform:uppercase}
  .ev .node{color:var(--dim);font-size:10px}
  .election{color:#d29922}.leader{color:#3fb950}.vote{color:#39c5cf}.replication{color:#58a6ff}
  .apply{color:#e6edf3}.routing{color:#bc8cff}.txn{color:#f0883e}.config{color:#79c0ff}
  .nfoot{padding:6px 12px;border-top:1px solid var(--line)}
  .nfoot button{font-size:11px;padding:3px 8px;width:100%}
  .legend{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px;font-size:11px}
  .legend span{display:flex;align-items:center;gap:4px}
  .sw{width:10px;height:10px;border-radius:2px;display:inline-block}
  #stream{max-height:30vh;overflow-y:auto;display:flex;flex-direction:column-reverse}
</style></head>
<body>
<header>
  <h1>litedb · distributed cluster</h1>
  <span class="chip">instances <b id="h-up">–</b></span>
  <span class="chip">shards <b id="h-shards">–</b></span>
  <span class="chip">RF <b id="h-rf">–</b></span>
  <span id="h-badge" class="badge ok">…</span>
  <div class="controls">
    <input id="k" placeholder="key" value="user:42">
    <input id="v" placeholder="value" value="hello">
    <button class="primary" onclick="put()">PUT</button>
    <button onclick="get()">GET</button>
    <button onclick="txn()">Cross-shard txn (2PC)</button>
    <button id="btn-add" onclick="control('add_node')">+ Add node</button>
    <button id="btn-rm" onclick="control('remove_node')">- Remove node</button>
  </div>
</header>
<div class="wrap">
  <div class="section-title">Cluster overview</div>
  <div class="cards">
    <div class="card"><h3>Configuration</h3><div id="config"></div></div>
    <div class="card"><h3>Consistent-hash ring</h3><div id="ring"></div></div>
    <div class="card"><h3>Shard placement (live)</h3><div id="placement"></div></div>
  </div>
  <div class="section-title">Instances — each panel narrates its own reasoning</div>
  <div class="nodes" id="grid"></div>
  <div class="section-title">Control plane - rebalancing log (add / remove node)</div>
  <div class="card"><div id="ctrl"></div></div>
  <div class="section-title">System event stream (all instances, newest first)</div>
  <div class="card"><div id="stream"></div></div>
</div>
<script>
let ORDER = %NODES%;
const SHARDS = %SHARDS%;
const COLORS = ["#58a6ff","#3fb950","#f0883e","#bc8cff","#39c5cf","#d29922","#db61a2","#a5d6ff"];
const colorOf = s => COLORS[SHARDS.indexOf(s) % COLORS.length];
const cursors = {}; const feeds = {}; let stream = [];
ORDER.forEach(n => { cursors[n]=0; feeds[n]=[]; });

function el(t,c,h){const e=document.createElement(t); if(c)e.className=c; if(h!=null)e.innerHTML=h; return e;}
function esc(s){return s.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}

async function poll(){
  let ov;
  try { ov = await (await fetch('/api/overview')).json(); } catch(e){ return; }
  const active = ov.config.active;
  active.forEach(n => { if(!(n in cursors)){ cursors[n]=0; feeds[n]=[]; } });
  const changed = JSON.stringify(active) !== JSON.stringify(ORDER);
  ORDER = active;
  renderHeader(ov); renderConfig(ov); renderRing(ov); renderPlacement(ov); renderCtrl(ov);
  const grid = document.getElementById('grid');
  grid.style.gridTemplateColumns = `repeat(${ORDER.length},1fr)`;
  if(changed || grid.children.length !== ORDER.length){ grid.innerHTML=''; ORDER.forEach(n=>grid.appendChild(buildNode(n))); }
  for(const nid of ORDER){ renderHead(nid, ov.live[nid]||{alive:false}); await pullEvents(nid); }
  renderStream();
  document.getElementById('btn-add').disabled = !ov.config.can_add;
  document.getElementById('btn-rm').disabled = !ov.config.can_remove;
}
function renderCtrl(ov){
  const box=document.getElementById('ctrl'); box.innerHTML='';
  (ov.control_log||[]).slice().reverse().forEach(m=> box.appendChild(el('div','ev',`<span class="cat config">control</span> ${esc(m)}`)));
  if(!(ov.control_log||[]).length) box.innerHTML='<div style="color:var(--dim)">use the Add / Remove node buttons to rebalance the cluster</div>';
}

function renderHeader(ov){
  const h=ov.health;
  document.getElementById('h-up').textContent = h.up+'/'+h.total;
  document.getElementById('h-shards').textContent = h.total_shards;
  document.getElementById('h-rf').textContent = ov.config.rf;
  const b=document.getElementById('h-badge');
  const healthy = h.up===h.total && h.with_leader===h.total_shards && h.under_replicated.length===0;
  b.className='badge '+(healthy?'ok':'bad');
  b.textContent = healthy ? 'HEALTHY' : 'DEGRADED';
}
function renderConfig(ov){
  const c=ov.config, h=ov.health;
  let html='';
  html+=row('instances up', h.up+' / '+h.total);
  html+=row('shards', c.shards.length);
  html+=row('replication factor', c.rf);
  html+=row('shards with leader', h.with_leader+' / '+h.total_shards);
  html+=row('under-replicated', h.under_replicated.length? h.under_replicated.join(', ') : 'none');
  c.nodes.forEach(n=>{ const up=(ov.live[n.id]||{}).alive; html+=row(n.id+' @ '+n.host+':'+n.port, up?'UP':'DOWN'); });
  document.getElementById('config').innerHTML = html;
}
function row(k,v){return `<div class="kv"><span>${k}</span><span>${v}</span></div>`;}

function renderRing(ov){
  const cx=95, cy=95, r=64, sw=22;
  let paths='';
  ov.ring.forEach(a=>{
    const a0=a.start/ov.config.ring_size*2*Math.PI - Math.PI/2;
    const a1=a.end/ov.config.ring_size*2*Math.PI - Math.PI/2;
    const x0=cx+r*Math.cos(a0), y0=cy+r*Math.sin(a0), x1=cx+r*Math.cos(a1), y1=cy+r*Math.sin(a1);
    const large=(a1-a0)>Math.PI?1:0;
    paths+=`<path d="M ${x0} ${y0} A ${r} ${r} 0 ${large} 1 ${x1} ${y1}" stroke="${colorOf(a.shard)}" stroke-width="${sw}" fill="none"/>`;
  });
  let legend='<div class="legend">'+SHARDS.map(s=>`<span><i class="sw" style="background:${colorOf(s)}"></i>${s.replace('shard-','S')}</span>`).join('')+'</div>';
  document.getElementById('ring').innerHTML =
    `<svg width="190" height="190" viewBox="0 0 190 190">${paths}</svg>`+legend
    +`<div style="color:var(--dim);font-size:11px;margin-top:4px">keys hash onto the ring → the arc's shard owns them</div>`;
}

function renderPlacement(ov){
  let html='<table><tr><th class="shard">shard</th>'+ORDER.map(n=>`<th>${n.replace('node-','n')}</th>`).join('')+'</tr>';
  ov.placement.forEach(p=>{
    html+=`<tr><td class="shard"><i class="sw" style="background:${colorOf(p.shard)}"></i> ${p.shard.replace('shard-','S')}</td>`;
    ORDER.forEach(n=>{
      const role=p.hosts[n];
      const isRep=p.replicas.includes(n);
      const pref=p.preferred===n?'<span class="star">★</span>':'';
      if(!isRep) html+=`<td class="cellNo">·</td>`;
      else if(role==='leader') html+=`<td class="cellL">L${pref}</td>`;
      else if(role==='follower') html+=`<td class="cellF">F${pref}</td>`;
      else html+=`<td class="cellF">${pref||'–'}</td>`;
    });
    html+='</tr>';
  });
  html+='</table><div style="color:var(--dim);font-size:11px;margin-top:6px">L=leader · F=follower · ·=not hosted · ★=preferred</div>';
  document.getElementById('placement').innerHTML = html;
}

function buildNode(nid){
  const n=el('div','node'); n.id='node-'+nid;
  n.appendChild(el('div','nhead',`<span class="dot"></span><span class="nname">${nid}</span><span class="nmeta"></span>`));
  n.appendChild(el('div','shards'));
  n.appendChild(el('div','feed'));
  const foot=el('div','nfoot'); foot.appendChild(el('div','','')); n.appendChild(foot);
  return n;
}
function renderHead(nid, st){
  const node=document.getElementById('node-'+nid);
  node.classList.toggle('down', !st.alive);
  const shards=st.shards||[];
  const leads=shards.filter(s=>s.role==='leader').length;
  node.querySelector('.nmeta').innerHTML = st.alive ? `leader of ${leads} · hosts ${shards.length}` : 'DOWN';
  const sh=node.querySelector('.shards'); sh.innerHTML='';
  shards.slice().sort((a,b)=>a.group<b.group?-1:1).forEach(s=>{
    sh.appendChild(el('span','sc '+(s.role==='leader'?'lead':''),
      `${s.group.replace('shard-','S')} ${s.role==='leader'?'L':'f'} t${s.term} #${s.log_len}`));
  });
  const foot=node.querySelector('.nfoot div'); foot.innerHTML='';
  const b=el('button', st.alive?'danger':'', st.alive?('Kill '+nid):('Restart '+nid));
  b.onclick=()=>control(st.alive?'kill':'start', nid); foot.appendChild(b);
}

async function pullEvents(nid){
  let ev;
  try { ev = await (await fetch('/api/events?node='+nid+'&after='+cursors[nid])).json(); } catch(e){ return; }
  if(ev.events && ev.events.length){
    ev.events.forEach(e=>{ e.node=nid; stream.push(e); });
    feeds[nid] = ev.events.slice().reverse().concat(feeds[nid]).slice(0,150);
    cursors[nid]=ev.next;
    const feed=document.querySelector('#node-'+nid+' .feed'); feed.innerHTML='';
    feeds[nid].forEach(e=> feed.appendChild(el('div','ev',`<span class="cat ${e.cat}">${e.cat}</span> ${esc(e.msg)}`)));
  }
}
function renderStream(){
  stream = stream.slice(-400);
  const sorted = stream.slice().sort((a,b)=>a.t-b.t).slice(-120);
  const box=document.getElementById('stream'); box.innerHTML='';
  sorted.forEach(e=> box.appendChild(el('div','ev',`<span class="node">${e.node}</span> <span class="cat ${e.cat}">${e.cat}</span> ${esc(e.msg)}`)));
}

async function post(p,b){return (await fetch(p,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b||{})})).json();}
async function put(){ await post('/api/put',{key:k.value,value:v.value}); }
async function get(){ const r=await post('/api/get',{key:k.value}); alert(k.value+' = '+JSON.stringify(r.value)); }
async function txn(){ await post('/api/txn',{}); }
async function control(a,n){ await post('/api/control',{action:a,node:n}); }

setInterval(poll, 500); poll();
</script>
</body></html>""";
}
