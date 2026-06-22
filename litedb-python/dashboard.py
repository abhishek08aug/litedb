"""
dashboard.py — launcher + live web dashboard for the distributed database.

Spawns the cluster (one OS process per node), then serves a web UI that shows, side by side, ONE
PANEL PER INSTANCE. Each panel streams that instance's own reasoning — election timeouts, accepting
a leader, routing a write by consistent hashing, replicating to peers, receiving replicated entries,
applying committed entries, running 2PC — so you can watch the distributed logic unfold and see WHY
each node does what it does. Controls let you write keys, run a cross-shard transaction, and kill or
restart a node to watch failover live.

Run:  python dashboard.py      then open  http://127.0.0.1:7080
"""

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import _loader  # noqa: F401
from cluster_client import ClusterClient
from cluster_config import DASHBOARD_PORT, DATA_ROOT, NODES, make_partitioner
from rpc import RPCClient

HERE = os.path.dirname(os.path.abspath(__file__))


class Launcher:
    def __init__(self) -> None:
        self.procs: dict[str, subprocess.Popen] = {}
        self.client = ClusterClient()
        self.rpc = RPCClient(timeout=1.5)
        self.partitioner = make_partitioner()

    def start_all(self) -> None:
        shutil.rmtree(DATA_ROOT, ignore_errors=True)  # fresh demo
        for nid in NODES:
            self.start_node(nid)

    def start_node(self, nid: str) -> None:
        if nid in self.procs and self.procs[nid].poll() is None:
            return  # already running
        self.procs[nid] = subprocess.Popen(
            [sys.executable, "node.py", nid], cwd=HERE,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def kill_node(self, nid: str) -> None:
        p = self.procs.get(nid)
        if p and p.poll() is None:
            p.kill()
            p.wait()

    def stop_all(self) -> None:
        for nid in self.procs:
            self.kill_node(nid)

    def node_events(self, nid: str, after: int) -> dict:
        host, port = NODES[nid]
        resp = self.rpc.call(host, port, "events", {"after": after})
        if resp.get("ok"):
            return resp["result"]
        return {"events": [], "next": after, "down": True}

    def cross_shard_pair(self) -> list[str]:
        """Two keys that hash to different shards — so a transfer between them needs 2PC."""
        base = "acct:alice"
        s0 = self.partitioner.shard_for(base)
        for i in range(200):
            cand = f"acct:user{i}"
            if self.partitioner.shard_for(cand) != s0:
                return [base, cand]
        return [base, "acct:bob"]


LAUNCHER: Launcher  # set in main()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence default logging
        pass

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, text):
        body = text.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            self._html(PAGE)
        elif u.path == "/api/cluster":
            self._json({"nodes": LAUNCHER.client.status(), "order": list(NODES)})
        elif u.path == "/api/events":
            q = parse_qs(u.query)
            nid = q.get("node", [""])[0]
            after = int(q.get("after", ["0"])[0])
            self._json(LAUNCHER.node_events(nid, after))
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        u = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or "{}") if length else {}
        if u.path == "/api/put":
            self._json(LAUNCHER.client.put(body["key"], body["value"]))
        elif u.path == "/api/get":
            self._json(LAUNCHER.client.get_full(body["key"]))
        elif u.path == "/api/txn":
            a, b = LAUNCHER.cross_shard_pair()
            self._json(LAUNCHER.client.txn({a: "balance=900", b: "balance=1100"}))
        elif u.path == "/api/control":
            action, nid = body.get("action"), body.get("node")
            if action == "kill":
                LAUNCHER.kill_node(nid)
            elif action == "start":
                LAUNCHER.start_node(nid)
            self._json({"ok": True})
        else:
            self._json({"error": "not found"}, 404)


PAGE = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>litedb — distributed cluster</title>
<style>
  :root{--bg:#0d1117;--panel:#161b22;--line:#30363d;--dim:#8b949e;--fg:#c9d1d9}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace}
  header{padding:10px 16px;border-bottom:1px solid var(--line);display:flex;gap:14px;align-items:center;flex-wrap:wrap}
  h1{font-size:15px;margin:0;font-weight:600}
  .sub{color:var(--dim);font-size:12px}
  .controls{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-left:auto}
  input{background:#0d1117;border:1px solid var(--line);color:var(--fg);padding:5px 7px;border-radius:5px;font:inherit;width:110px}
  button{background:#21262d;border:1px solid var(--line);color:var(--fg);padding:5px 10px;border-radius:5px;cursor:pointer;font:inherit}
  button:hover{background:#30363d}
  button.primary{background:#1f6feb;border-color:#1f6feb;color:#fff}
  button.danger{background:#da3633;border-color:#da3633;color:#fff}
  #grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;padding:12px}
  .node{background:var(--panel);border:1px solid var(--line);border-radius:8px;display:flex;flex-direction:column;min-height:78vh}
  .node.down{opacity:.5}
  .nhead{padding:10px 12px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:8px}
  .dot{width:9px;height:9px;border-radius:50%;background:#3fb950}
  .down .dot{background:#f85149}
  .nname{font-weight:600}
  .nmeta{color:var(--dim);font-size:11px;margin-left:auto;text-align:right}
  .shards{display:flex;flex-wrap:wrap;gap:4px;padding:8px 12px;border-bottom:1px solid var(--line)}
  .chip{font-size:10px;padding:2px 6px;border-radius:4px;border:1px solid var(--line);color:var(--dim)}
  .chip.lead{background:#132e1a;border-color:#238636;color:#3fb950}
  .chip.foll{background:#161b22}
  .feed{flex:1;overflow-y:auto;padding:6px 10px;display:flex;flex-direction:column-reverse}
  .ev{padding:3px 0;border-bottom:1px solid #1c2128}
  .ev .cat{display:inline-block;min-width:78px;font-size:10px;text-transform:uppercase;letter-spacing:.04em}
  .election{color:#d29922}.leader{color:#3fb950}.vote{color:#39c5cf}.replication{color:#58a6ff}
  .apply{color:#e6edf3}.routing{color:#bc8cff}.txn{color:#f0883e}
  .nfoot{padding:6px 12px;border-top:1px solid var(--line)}
  .nfoot button{font-size:11px;padding:3px 8px;width:100%}
</style></head>
<body>
<header>
  <h1>litedb · distributed cluster</h1>
  <span class="sub">6 shards · multi-raft · RF 3 · one panel per instance — watch the reasoning</span>
  <div class="controls">
    <input id="k" placeholder="key" value="user:42">
    <input id="v" placeholder="value" value="hello">
    <button class="primary" onclick="put()">PUT</button>
    <button onclick="get()">GET</button>
    <button onclick="txn()">Cross-shard txn (2PC)</button>
  </div>
</header>
<div id="grid"></div>
<script>
const ORDER = %NODES%;
const cursors = {}; const feeds = {};
ORDER.forEach(n => { cursors[n]=0; feeds[n]=[]; });

function el(tag, cls, html){const e=document.createElement(tag); if(cls)e.className=cls; if(html!=null)e.innerHTML=html; return e;}

async function poll(){
  let cluster;
  try { cluster = await (await fetch('/api/cluster')).json(); } catch(e){ return; }
  const byId = {}; cluster.nodes.forEach(n => byId[n.node||n.name]=n);
  const grid = document.getElementById('grid');
  if(grid.children.length !== ORDER.length){ grid.innerHTML=''; ORDER.forEach(n=>grid.appendChild(buildNode(n))); }
  for(const nid of ORDER){
    const st = byId[nid] || {alive:false};
    renderHead(nid, st);
    let ev;
    try { ev = await (await fetch('/api/events?node='+nid+'&after='+cursors[nid])).json(); } catch(e){ continue; }
    if(ev.events && ev.events.length){
      feeds[nid] = ev.events.concat(feeds[nid]).slice(0,200);
      cursors[nid] = ev.next;
      renderFeed(nid);
    }
  }
}

function buildNode(nid){
  const n = el('div','node'); n.id='node-'+nid;
  n.appendChild(el('div','nhead','<span class="dot"></span><span class="nname">'+nid+'</span><span class="nmeta"></span>'));
  n.appendChild(el('div','shards'));
  n.appendChild(el('div','feed'));
  const foot = el('div','nfoot'); foot.appendChild(el('div','',''));
  n.appendChild(foot);
  return n;
}

function renderHead(nid, st){
  const node = document.getElementById('node-'+nid);
  node.classList.toggle('down', !st.alive);
  const leads = (st.shards||[]).filter(s=>s.role==='leader').length;
  node.querySelector('.nmeta').innerHTML = st.alive
     ? ('leader of '+leads+' shard'+(leads===1?'':'s'))
     : 'DOWN';
  const sh = node.querySelector('.shards'); sh.innerHTML='';
  (st.shards||[]).sort((a,b)=>a.group<b.group?-1:1).forEach(s=>{
    const lead = s.role==='leader';
    sh.appendChild(el('span','chip '+(lead?'lead':'foll'),
       s.group.replace('shard-','S')+' '+(lead?'L':'f')+' t'+s.term+' #'+s.log_len));
  });
  const foot = node.querySelector('.nfoot div');
  foot.innerHTML='';
  const b = el('button', st.alive?'danger':'', st.alive?('Kill '+nid):('Restart '+nid));
  b.onclick = ()=> control(st.alive?'kill':'start', nid);
  foot.appendChild(b);
}

function renderFeed(nid){
  const feed = document.querySelector('#node-'+nid+' .feed'); feed.innerHTML='';
  feeds[nid].forEach(e=>{
    feed.appendChild(el('div','ev','<span class="cat '+e.cat+'">'+e.cat+'</span> '+escapeHtml(e.msg)));
  });
}
function escapeHtml(s){return s.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}

async function post(path, body){ return (await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})})).json(); }
async function put(){ await post('/api/put',{key:k.value,value:v.value}); }
async function get(){ const r=await post('/api/get',{key:k.value}); alert(k.value+' = '+JSON.stringify(r.value)); }
async function txn(){ await post('/api/txn',{}); }
async function control(action,nid){ await post('/api/control',{action,node:nid}); }

setInterval(poll, 400); poll();
</script>
</body></html>"""


def main() -> None:
    global LAUNCHER
    LAUNCHER = Launcher()
    LAUNCHER.start_all()

    page_nodes = json.dumps(list(NODES))
    globals()["PAGE"] = PAGE.replace("%NODES%", page_nodes)

    httpd = ThreadingHTTPServer(("127.0.0.1", DASHBOARD_PORT), Handler)
    print(f"\n  litedb cluster up — {len(NODES)} instances, {len(LAUNCHER.partitioner.shard_ids)} shards")
    print(f"  dashboard:  http://127.0.0.1:{DASHBOARD_PORT}\n")
    print("  (Ctrl-C to shut the whole cluster down)\n")

    def serve():
        httpd.serve_forever()
    threading.Thread(target=serve, daemon=True).start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n  shutting down cluster...")
        LAUNCHER.stop_all()
        httpd.shutdown()


if __name__ == "__main__":
    main()
