import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import requests


def _load_dotenv(path=".env"):
    """Load KEY=VALUE pairs from a .env file (no external dependency).

    Existing environment variables take precedence, so values injected by
    docker-compose / the host are never overwritten.
    """
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except FileNotFoundError:
        pass


_load_dotenv(os.environ.get("DOTENV_PATH", ".env"))

DEFAULT_INTERVAL = int(os.environ.get("LLM_PROBE_INTERVAL", "900"))
ALLOWED_INTERVALS = (900, 3600, 10800)
SETTINGS_PATH = os.environ.get("LLM_SETTINGS_PATH", "/app/data/llm_settings.json")
PROBE_TIMEOUT = (10, 45)
HISTORY_LIMIT = 40
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

PROVIDERS = {
    "csu": {
        "label": "CSU Chat",
        "base_url": "https://api.chat.csu.edu.cn/v1",
        "api_key": os.environ.get("CSU_API_KEY", ""),
        "models": ["DeepSeek", "GLM", "Qwen"],
    },
    "xtoken": {
        "label": "XToken GPT Mirror",
        "base_url": "https://api.xtokenmirror.com/v1",
        "api_key": os.environ.get("XTOKEN_API_KEY", ""),
        "models": [
            "gpt-6-astra",
            "gpt-5.6-sol",
            "gpt-5.6",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
        ],
    },
}

_lock = threading.Lock()
_state = {}
_inflight = set()
_auto = {
    "enabled": True,
    "interval": DEFAULT_INTERVAL,
    "next_at": time.time() + DEFAULT_INTERVAL,
    "last_auto_probe": None,
}


def _load_settings():
    try:
        with open(SETTINGS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        with _lock:
            _auto["enabled"] = bool(data.get("enabled", True))
            interval = int(data.get("interval", DEFAULT_INTERVAL))
            if interval in ALLOWED_INTERVALS:
                _auto["interval"] = interval
    except Exception:
        pass


def _save_settings():
    try:
        Path(SETTINGS_PATH).parent.mkdir(parents=True, exist_ok=True)
        tmp = SETTINGS_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"enabled": _auto["enabled"], "interval": _auto["interval"]}, f)
        os.replace(tmp, SETTINGS_PATH)
    except Exception:
        pass


def get_auto():
    with _lock:
        return {
            "enabled": _auto["enabled"],
            "interval": _auto["interval"],
            "next_in": max(0, int(_auto["next_at"] - time.time())) if _auto["enabled"] else None,
            "last_ago": int(time.time() - _auto["last_auto_probe"]) if _auto["last_auto_probe"] else None,
        }


def set_auto(enabled=None, interval=None):
    with _lock:
        if enabled is not None:
            _auto["enabled"] = bool(enabled)
        if interval is not None:
            try:
                interval = int(interval)
            except (TypeError, ValueError):
                interval = None
            if interval in ALLOWED_INTERVALS:
                _auto["interval"] = interval
        # 开启（或修改间隔）后立即补一轮探测
        if _auto["enabled"]:
            _auto["next_at"] = time.time()
    _save_settings()
    return get_auto()
_inflight = set()


def _key(provider, model):
    return f"{provider}/{model}"


def _init_state():
    with _lock:
        for provider, cfg in PROVIDERS.items():
            for model in cfg["models"]:
                _state.setdefault(_key(provider, model), {
                    "provider": provider,
                    "provider_label": cfg["label"],
                    "model": model,
                    "status": "pending",
                    "http_status": None,
                    "latency_ms": None,
                    "error": None,
                    "checked_at": None,
                    "history": [],
                })


def _short_error(http_status, body):
    if not body:
        return f"HTTP {http_status}" if http_status else "no response"
    text = body.strip()
    if text.startswith("<"):
        return f"HTTP {http_status} gateway error" if http_status else "gateway error"
    return text[:160]


def _probe_model(provider, model):
    cfg = PROVIDERS[provider]
    url = cfg["base_url"].rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {cfg['api_key']}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    content = "Reply with the single word: pong"

    def _post(payload):
        t0 = time.time()
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=PROBE_TIMEOUT)
            return resp, int((time.time() - t0) * 1000), None
        except requests.Timeout:
            return None, int((time.time() - t0) * 1000), "timeout, no response"
        except requests.RequestException as exc:
            return None, int((time.time() - t0) * 1000), f"{type(exc).__name__}: {exc}"[:160]

    def _attempt(payload):
        resp, latency, err = _post(payload)
        ok = False
        error = None
        if err:
            error = err
        else:
            try:
                data = resp.json()
            except ValueError:
                data = None
            if resp.status_code == 200 and isinstance(data, dict) and isinstance(data.get("choices"), list) and data["choices"]:
                ok = True
            elif isinstance(data, dict) and data.get("error"):
                err_obj = data["error"]
                error = err_obj.get("message") if isinstance(err_obj, dict) else str(err_obj)
            else:
                error = _short_error(resp.status_code, resp.text)
        return ok, resp, latency, error

    small = {"model": model, "messages": [{"role": "user", "content": content}], "max_tokens": 16, "stream": False}
    ok, resp, latency, error = _attempt(small)

    # 小请求失败时兜底：部分模型只认 max_completion_tokens，部分令牌通道要求请求体 >= 2000 token
    if not ok:
        padded = {
            "model": model,
            "messages": [{"role": "user", "content": "System calibration preamble. " * 120 + "\nIgnore the text above. " + content}],
            "max_completion_tokens": 16,
            "stream": False,
        }
        ok2, resp2, latency2, error2 = _attempt(padded)
        if ok2 or (resp is not None and resp.status_code == 400):
            ok, resp, latency, error = ok2, resp2, latency2, error2

    entry = {"t": int(time.time()), "ok": ok, "ms": latency, "code": resp.status_code if resp is not None else 0}
    with _lock:
        state = _state.get(_key(provider, model))
        if state is None:
            return
        state["status"] = "up" if ok else "down"
        state["http_status"] = entry["code"]
        state["latency_ms"] = latency
        state["error"] = None if ok else (error or f"HTTP {entry['code']}")
        state["checked_at"] = datetime.now().isoformat(timespec="seconds")
        state["history"].append(entry)
        if len(state["history"]) > HISTORY_LIMIT:
            del state["history"][:-HISTORY_LIMIT]


def probe_all():
    with _lock:
        targets = [key for key in _state if key not in _inflight]
        for key in targets:
            _inflight.add(key)
    if not targets:
        return 0

    def _run(key):
        try:
            with _lock:
                state = _state.get(key)
            if state is None:
                return
            _probe_model(state["provider"], state["model"])
        except Exception:
            pass
        finally:
            with _lock:
                _inflight.discard(key)

    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        list(pool.map(_run, targets))
    return len(targets)


def startup_probe():
    """启动后立即探测一轮，并把这一轮记为自动探测。"""
    _load_settings()
    try:
        probe_all()
    except Exception:
        pass
    with _lock:
        _auto["last_auto_probe"] = time.time()
        _auto["next_at"] = time.time() + _auto["interval"]


def trigger_check(provider=None, model=None):
    with _lock:
        wanted = []
        for key, state in _state.items():
            if provider and state["provider"] != provider:
                continue
            if model and state["model"] != model:
                continue
            if key in _inflight:
                continue
            _inflight.add(key)
            state["status"] = "checking"
            wanted.append(key)
    if not wanted:
        return 0

    def _probe_key(key):
        try:
            with _lock:
                state = _state.get(key)
            if state is None:
                return
            _probe_model(state["provider"], state["model"])
        except Exception:
            pass
        finally:
            with _lock:
                _inflight.discard(key)

    def _worker():
        with ThreadPoolExecutor(max_workers=len(wanted)) as pool:
            list(pool.map(_probe_key, wanted))

    threading.Thread(target=_worker, daemon=True).start()
    return len(wanted)


def probe_loop():
    _load_settings()
    while True:
        try:
            with _lock:
                enabled = _auto["enabled"]
                due = time.time() >= _auto["next_at"]
            if enabled and due:
                probe_all()
                with _lock:
                    _auto["last_auto_probe"] = time.time()
                    _auto["next_at"] = time.time() + _auto["interval"]
        except Exception:
            pass
        time.sleep(5)


def startup_probe():
    probe_all()
    with _lock:
        _auto["last_auto_probe"] = time.time()
        _auto["next_at"] = time.time() + _auto["interval"]


def get_status():
    with _lock:
        models = [json.loads(json.dumps(state)) for state in _state.values()]
    for m in models:
        history = m.get("history") or []
        m["uptime"] = round(sum(1 for h in history if h["ok"]) / len(history) * 100, 1) if history else None
    return {
        "interval": _auto["interval"],
        "auto": get_auto(),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "models": models,
    }
