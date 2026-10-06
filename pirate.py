import asyncio
import logging
import os
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import quote

import requests

API_BASE = "https://apibay.org"
CACHE: dict[str, list[dict]] = {}
KEYWORDS: list[str] = []
REFRESH_INTERVAL = 3600
REFRESH_STATE: dict = {
    "running": False,
    "done": 0,
    "total": 0,
    "started_at": 0,
    "current": "",
}

DB_PATH = os.environ.get("DB_PATH", "/app/data/metrics.db")

logger = logging.getLogger("pirate")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pirate_keywords (
            keyword TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def load_keywords():
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT keyword FROM pirate_keywords ORDER BY created_at").fetchall()
        conn.close()
        KEYWORDS.clear()
        KEYWORDS.extend(r[0] for r in rows)
    except Exception:
        KEYWORDS.clear()


def save_keyword(kw: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT OR IGNORE INTO pirate_keywords (keyword, created_at) VALUES (?, ?)", (kw, int(time.time())))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"save keyword failed: {e}")


def delete_keyword(kw: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM pirate_keywords WHERE keyword = ?", (kw,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"delete keyword failed: {e}")


def format_size(size_bytes: int) -> str:
    if size_bytes >= 1_000_000_000:
        return f"{size_bytes / 1_000_000_000:.1f} GB"
    if size_bytes >= 1_000_000:
        return f"{size_bytes / 1_000_000:.1f} MB"
    return f"{size_bytes / 1_000:.0f} KB"


_FIELD_PATTERNS = {
    "video": re.compile(r"(?i)^Video\s*[:：]\s*(.+)"),
    "audio": re.compile(r"(?i)^Audio\s*[:：]\s*(.+)"),
    "subtitles": re.compile(r"(?i)^Sub(?:s|titles?|title\(s\))\s*[:：]\s*(.+)"),
    "source": re.compile(r"(?i)^Source\s*[:：]\s*(.+)"),
    "imdb": re.compile(r"(?i)^IMDb?\s*(?:Information)?\.*\s*[:：]*\s*(https?://.+)"),
}
# 探测"另一个字段开始"用,排除把别的 key 当成续行
_GENERIC_FIELD_HEAD = re.compile(r"^[A-Za-z][A-Za-z _-]{1,18}\s*[:：]")

_CHINESE_RE = re.compile(
    r"(?i)\b(?:chinese|mandarin|cantonese|"
    r"simplified[\s_-]*chinese|traditional[\s_-]*chinese|"
    r"zh[\s_-]?(?:cn|tw|hk|hans|hant)|chs|cht)\b"
    r"|中文|中字|简体|繁体|国语|粤语|汉语|普通话"
)


def extract_media_info(descr: str) -> dict:
    info: dict = {}
    if not descr:
        return info
    current_key = None
    for raw in descr.split("\n"):
        stripped = raw.strip()
        if not stripped:
            current_key = None
            continue
        matched = None
        for key, pat in _FIELD_PATTERNS.items():
            m = pat.match(stripped)
            if m:
                info[key] = m.group(1).strip().rstrip(",")
                matched = key
                break
        if matched:
            current_key = matched
            continue
        # 续行:原行以空白开头 && 不是另一个独立字段 → 追加到上一个 key
        if current_key and (raw.startswith(" ") or raw.startswith("\t")) and not _GENERIC_FIELD_HEAD.match(stripped):
            info[current_key] = (info[current_key] + " " + stripped).rstrip(",")
        else:
            current_key = None
    return info


def has_chinese_subs(detail: dict) -> bool:
    subs = (detail or {}).get("subtitles", "")
    if not subs:
        return False
    return bool(_CHINESE_RE.search(subs))


def _fetch_json(url: str, timeout: int = 12):
    try:
        proxy = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
        proxies = {"http": proxy, "https": proxy} if proxy else None
        r = requests.get(url, timeout=timeout, proxies=proxies)
        return r.json()
    except Exception as e:
        logger.warning(f"fetch failed: {url[:60]} {e}")
        return None


def _to_int(v) -> int:
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0


def search_keyword_sync(keyword: str) -> list[dict]:
    url = f"{API_BASE}/q.php?q={quote(keyword)}&cat=0"
    data = _fetch_json(url)
    if not isinstance(data, list):
        return []
    results = []
    for item in data:
        if item.get("id") == "0":
            continue
        info_hash = item.get("info_hash", "")
        name = item.get("name", "")
        size = _to_int(item.get("size"))
        magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={quote(name)}"
        results.append({
            "id": item["id"],
            "name": name,
            "size": size,
            "size_fmt": format_size(size),
            "seeders": _to_int(item.get("seeders")),
            "leechers": _to_int(item.get("leechers")),
            "added_unix": _to_int(item.get("added")),
            "info_hash": info_hash,
            "magnet": magnet,
            "detail": {},
        })
    results.sort(key=lambda x: x["seeders"], reverse=True)
    top = results[:10]
    # 只为 top-N 拉详情,避免 N+1 问题
    for r in top:
        try:
            d2 = _fetch_json(f"{API_BASE}/t.php?id={r['id']}", timeout=8)
            if d2:
                r["detail"] = extract_media_info(d2.get("descr", ""))
        except Exception:
            pass
        r["has_cn_subs"] = has_chinese_subs(r["detail"])
    # 展示顺序:日期新 → 旧
    top.sort(key=lambda x: x["added_unix"], reverse=True)
    return top


async def search_keyword(keyword: str) -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, search_keyword_sync, keyword)


def torrent_detail_sync(tid: str) -> dict | None:
    d = _fetch_json(f"{API_BASE}/t.php?id={tid}", timeout=10)
    return d


async def torrent_detail(tid: str) -> dict | None:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, torrent_detail_sync, tid)


async def refresh_all():
    if not KEYWORDS:
        return
    if REFRESH_STATE["running"]:
        logger.info("refresh already running, skip")
        return
    kws = list(KEYWORDS)  # snapshot,避免迭代期间被修改
    REFRESH_STATE.update({
        "running": True,
        "done": 0,
        "total": len(kws),
        "started_at": int(time.time()),
        "current": "",
    })
    logger.info(f"refreshing {len(kws)} keywords...")

    async def _one(kw: str):
        REFRESH_STATE["current"] = kw
        try:
            CACHE[kw] = await search_keyword(kw)
        except Exception as e:
            logger.warning(f"search {kw} failed: {e}")
            CACHE[kw] = []
        finally:
            REFRESH_STATE["done"] += 1

    try:
        await asyncio.gather(*[_one(kw) for kw in kws])
        logger.info(f"refresh done, {sum(len(v) for v in CACHE.values())} total results")
    finally:
        REFRESH_STATE["running"] = False
        REFRESH_STATE["current"] = ""


async def refresh_loop():
    await asyncio.sleep(5)
    while True:
        try:
            await refresh_all()
        except Exception as e:
            logger.error(f"refresh_loop error: {e}")
        await asyncio.sleep(REFRESH_INTERVAL)
