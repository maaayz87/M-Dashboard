import asyncio
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import aiosqlite
import psutil
import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import llm_monitor
from collector import collector_loop, DB_PATH, init_db
from term import handle_ws as term_handle_ws
from pirate import (
    CACHE as PIRATE_CACHE,
    KEYWORDS as PIRATE_KEYWORDS,
    REFRESH_STATE as PIRATE_REFRESH_STATE,
    init_db as pirate_init_db,
    load_keywords as pirate_load_keywords,
    save_keyword as pirate_save_keyword,
    delete_keyword as pirate_delete_keyword,
    refresh_all as pirate_refresh_all,
    refresh_loop as pirate_refresh_loop,
    search_keyword as pirate_search_keyword,
)

DOCKER_SOCKET = "/var/run/docker.sock"
DEFAULT_PORTS = {
    "jellyfin": 8096,
    "qbittorrent": 8090,
    "resilio-sync": 8888,
    "filebrowser": 8081,
    "alist": 5244,
    "speedtest": 8989,
    "trendradar-mcp": 3333,
}

WEB_PORT_PREFERENCES = {
    "qbittorrent": 8090,
    "jellyfin": 8096,
    "resilio-sync": 8888,
    "filebrowser": 8081,
    "alist": 5244,
    "speedtest": 8989,
}

app = FastAPI(title="Service Hub")

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


async def docker_request(method: str, endpoint: str):
    import http.client as httplib

    conn = httplib.HTTPConnection("localhost", 80, timeout=5)
    try:
        conn.sock = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _connect_unix(DOCKER_SOCKET),
        )
        conn.request(method, endpoint, headers={"Host": "localhost"})
        resp = conn.getresponse()
        return json.loads(resp.read())
    finally:
        conn.close()


def _connect_unix(socket_path):
    import socket

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(socket_path)
    return sock


def extract_web_port(container):
    name = container.get("Names", [""])[0].lstrip("/")
    ports = container.get("Ports", []) or []

    # 优先匹配预期的 WebUI 端口
    preferred = WEB_PORT_PREFERENCES.get(name)
    if preferred:
        for p in ports:
            pub_port = p.get("PublicPort")
            typ = p.get("Type", "")
            if pub_port and pub_port == preferred and typ == "tcp":
                return pub_port

    # 其次取第一个映射到 host 的 TCP 端口
    for p in ports:
        pub_port = p.get("PublicPort")
        typ = p.get("Type", "")
        if pub_port and typ == "tcp":
            return pub_port

    # 没有端口映射，用默认配置
    return DEFAULT_PORTS.get(name, None)


async def get_containers():
    data = await docker_request("GET", "/containers/json?all=false")
    result = []

    for c in data:
        name = c.get("Names", [""])[0].lstrip("/")
        if name == "service-hub":
            continue

        state = c.get("State", "unknown")
        if state != "running":
            continue

        web_port = extract_web_port(c)
        created = c.get("Created", 0)
        uptime = ""
        if created:
            created_int = int(created)
            if created_int > 1e12:
                created_int = created_int // 1_000_000_000
            uptime_secs = int(time.time()) - created_int
            if uptime_secs < 0:
                uptime_secs = 0
            uptime = format_uptime(uptime_secs)

        entry = {
            "name": name,
            "state": state,
            "web_port": web_port,
            "uptime": uptime,
            "image": c.get("Image", ""),
        }
        result.append(entry)

    return result


_DOCKER_API_URL = None

_last_net = None
_last_net_time = 0
_net_speed = {"sent": 0.0, "recv": 0.0}
_proc_samples = []


def _host_net_bytes():
    try:
        path = "/host/net/dev"
        with open(path) as f:
            lines = f.readlines()[2:]
        sent = 0
        recv = 0
        for line in lines:
            parts = line.strip().split()
            if len(parts) < 10:
                continue
            iface = parts[0].rstrip(":")
            if iface != "enp1s0":
                continue
            recv = int(parts[1])
            sent = int(parts[9])
            break
        return sent, recv
    except Exception:
        return 0, 0


def _sample_net_speed():
    global _last_net, _last_net_time, _net_speed
    now = time.time()
    sent, recv = _host_net_bytes()
    if _last_net is not None:
        elapsed = now - _last_net_time
        if elapsed > 0:
            _net_speed["sent"] = (sent - _last_net[0]) / elapsed
            _net_speed["recv"] = (recv - _last_net[1]) / elapsed
    _last_net = (sent, recv)
    _last_net_time = now


def get_disks():
    try:
        disks = []
        output = os.popen("df -B1 --type=ext4 --type=xfs --type=btrfs /host/root /host/root/media/mayizhe/2t 2>/dev/null").read()
        for line in output.strip().split("\n"):
            if not line or line.startswith("Filesystem") or line.startswith("df:"):
                continue
            parts = line.strip().split()
            if len(parts) < 6:
                continue
            mount = parts[5]
            mount = mount.replace("/host/root", "")
            if not mount:
                mount = "/"
            if mount.startswith("/snap") or mount.startswith("/boot"):
                continue
            total = int(parts[1])
            used = int(parts[2])
            pct = round(used / total * 100, 1) if total > 0 else 0
            disks.append({
                "mount": mount,
                "percent": pct,
                "used_gb": round(used / 1024**3, 1),
                "total_gb": round(total / 1024**3, 1),
            })
        return disks
    except Exception:
        return []

def _proc_cmdline(p) -> str:
    """Return a human-readable, trimmed command line for a process."""
    try:
        parts = p.cmdline() or []
    except (psutil.NoSuchProcess, psutil.AccessDenied, Exception):
        parts = []
    if not parts:
        return ""
    # 把每个 token 里的绝对路径裁成 basename，便于阅读
    cleaned = []
    for tok in parts:
        if tok.startswith("/") and " " not in tok:
            cleaned.append(os.path.basename(tok) or tok)
        else:
            cleaned.append(tok)
    cmd = " ".join(cleaned).strip()
    if len(cmd) > 90:
        cmd = cmd[:87] + "..."
    return cmd


def _sample_procs():
    try:
        samples = []
        for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent"]):
            try:
                info = p.info
                cpu = info["cpu_percent"] or 0
                cmd = _proc_cmdline(p)
                name = (info["name"] or "")[:24]
                if cmd == name or cmd == "":
                    desc = ""
                else:
                    desc = cmd
                samples.append({
                    "pid": info["pid"],
                    "name": name,
                    "cpu_percent": round(cpu, 1),
                    "memory_percent": round(info["memory_percent"] or 0, 1),
                    "cmdline": desc,
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        samples.sort(key=lambda x: x["cpu_percent"], reverse=True)
        return samples[:10]
    except Exception:
        return []

def get_top_processes_avg(n=5):
    if not _proc_samples:
        return []
    merged = {}
    for sample in _proc_samples:
        for p in sample:
            key = p["pid"]
            if key not in merged:
                merged[key] = {
                    "pid": key,
                    "name": p["name"],
                    "cmdline": p.get("cmdline", ""),
                    "cpu_sum": 0.0,
                    "mem_sum": 0.0,
                    "count": 0,
                }
            else:
                # 总是用最近一次的 cmdline（更可能是最新状态）
                if p.get("cmdline"):
                    merged[key]["cmdline"] = p["cmdline"]
            merged[key]["cpu_sum"] += p["cpu_percent"]
            merged[key]["mem_sum"] += p["memory_percent"]
            merged[key]["count"] += 1
    result = []
    for p in merged.values():
        result.append({
            "pid": p["pid"],
            "name": p["name"],
            "cmdline": p["cmdline"],
            "cpu_percent": round(p["cpu_sum"] / p["count"], 1),
            "memory_percent": round(p["mem_sum"] / p["count"], 1),
        })
    result.sort(key=lambda x: x["cpu_percent"], reverse=True)
    return result[:n]


@app.on_event("startup")
async def startup():
    pirate_init_db()
    pirate_load_keywords()
    llm_monitor._init_state()
    asyncio.create_task(collector_loop())
    asyncio.create_task(_background_sampler_loop())
    asyncio.create_task(pirate_refresh_loop())
    asyncio.create_task(asyncio.to_thread(llm_monitor.startup_probe))
    asyncio.create_task(asyncio.to_thread(llm_monitor.probe_loop))


async def _background_sampler_loop():
    while True:
        _sample_net_speed()
        _proc_samples.append(_sample_procs())
        if len(_proc_samples) > 5:
            _proc_samples.pop(0)
        await asyncio.sleep(1.0)


@app.get("/api/containers")
async def api_containers():
    try:
        containers = await get_containers()
        return {"containers": containers}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to fetch containers: {str(e)}"},
        )


@app.get("/api/stats/current")
async def api_stats_current():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT cpu_percent, disk_percent, disk_used, disk_total, memory_percent FROM metrics ORDER BY timestamp DESC LIMIT 1"
        )
        row = cursor.fetchone()
        conn.close()
        if row:
            return {
                "cpu_percent": row[0],
                "disk_percent": row[1],
                "disk_used": row[2],
                "disk_total": row[3],
                "memory_percent": row[4],
            }
        return {"cpu_percent": 0, "disk_percent": 0, "disk_used": 0, "disk_total": 0, "memory_percent": 0}
    except Exception:
        return {"cpu_percent": 0, "disk_percent": 0, "disk_used": 0, "disk_total": 0, "memory_percent": 0}


@app.get("/api/stats/realtime")
async def api_stats_realtime():
    try:
        cpu = psutil.cpu_percent(interval=0)
        mem = psutil.virtual_memory()
        top = get_top_processes_avg(5)
        disks = get_disks()
        return {
            "cpu_percent": cpu,
            "memory_percent": mem.percent,
            "disks": disks,
            "net_sent": _net_speed["sent"],
            "net_recv": _net_speed["recv"],
            "top_processes": top,
        }
    except Exception as e:
        return {
            "cpu_percent": 0,
            "memory_percent": 0,
            "disks": [],
            "net_sent": 0,
            "net_recv": 0,
            "top_processes": [],
        }


@app.get("/api/stats/history")
async def api_stats_history(hours: int = 24):
    cutoff = int((datetime.now() - timedelta(hours=hours)).timestamp())
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT timestamp, cpu_percent, disk_percent, memory_percent FROM metrics WHERE timestamp >= ? ORDER BY timestamp",
            (cutoff,),
        )
        rows = cursor.fetchall()
        conn.close()
        return {
            "timestamps": [r[0] for r in rows],
            "cpu": [r[1] for r in rows],
            "disk": [r[2] for r in rows],
            "memory": [r[3] for r in rows],
        }
    except Exception:
        return {"timestamps": [], "cpu": [], "disk": [], "memory": []}


@app.get("/api/llm/status")
async def api_llm_status():
    return llm_monitor.get_status()


@app.post("/api/llm/check")
async def api_llm_check(req: Request):
    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    started = llm_monitor.trigger_check(body.get("provider"), body.get("model"))
    return {"ok": True, "started": started}


@app.post("/api/llm/settings")
async def api_llm_settings(req: Request):
    try:
        body = await req.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    auto = llm_monitor.set_auto(body.get("enabled"), body.get("interval"))
    return {"ok": True, "auto": auto}


@app.get("/", response_class=HTMLResponse)
async def index():
    html = (Path(__file__).parent / "templates" / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.get("/api/pirate/keywords")
async def pirate_get_keywords():
    return {"keywords": PIRATE_KEYWORDS}


@app.post("/api/pirate/keyword")
async def pirate_add_keyword(req: Request):
    body = await req.json()
    kw = body.get("keyword", "").strip()
    if not kw:
        raise HTTPException(400, "keyword required")
    if kw not in PIRATE_KEYWORDS:
        PIRATE_KEYWORDS.append(kw)
        pirate_save_keyword(kw)
        asyncio.create_task(_search_one(kw))
    return {"ok": True, "keywords": PIRATE_KEYWORDS}


async def _search_one(kw: str):
    try:
        results = await pirate_search_keyword(kw)
        PIRATE_CACHE[kw] = results
    except Exception:
        PIRATE_CACHE[kw] = []


@app.delete("/api/pirate/keyword/{keyword:path}")
async def pirate_del_keyword(keyword: str):
    if keyword in PIRATE_KEYWORDS:
        PIRATE_KEYWORDS.remove(keyword)
        PIRATE_CACHE.pop(keyword, None)
        pirate_delete_keyword(keyword)
    return {"ok": True, "keywords": PIRATE_KEYWORDS}


@app.get("/api/pirate/results")
async def pirate_results():
    return {"results": {kw: PIRATE_CACHE.get(kw, []) for kw in PIRATE_KEYWORDS}}


@app.post("/api/pirate/refresh")
async def pirate_refresh():
    if PIRATE_REFRESH_STATE["running"]:
        return {"ok": True, "message": "refresh already running", "status": PIRATE_REFRESH_STATE}
    asyncio.create_task(pirate_refresh_all())
    return {"ok": True, "message": "refresh started"}


@app.get("/api/pirate/status")
async def pirate_status():
    return PIRATE_REFRESH_STATE


@app.websocket("/ws/term")
async def ws_term(websocket: WebSocket):
    await websocket.accept()
    await term_handle_ws(websocket)


def format_uptime(seconds: int) -> str:
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    mins = (seconds % 3600) // 60
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    parts.append(f"{mins}m")
    return " ".join(parts)


if __name__ == "__main__":
    init_db()
    uvicorn.run("main:app", host="0.0.0.0", port=3000, reload=False)
