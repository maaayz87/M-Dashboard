import asyncio
import sqlite3
import os
import time
from datetime import datetime, timedelta
import psutil

DB_PATH = os.environ.get("DB_PATH", "/app/data/metrics.db")
INTERVAL = 30  # seconds
RETENTION_HOURS = 24


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            timestamp INTEGER PRIMARY KEY,
            cpu_percent REAL,
            disk_percent REAL,
            disk_used REAL,
            disk_total REAL,
            memory_percent REAL
        )
    """)
    conn.commit()
    conn.close()


def collect():
    cpu = psutil.cpu_percent(interval=1)
    disk = psutil.disk_usage("/")
    mem = psutil.virtual_memory()
    now = int(time.time())
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO metrics (timestamp, cpu_percent, disk_percent, disk_used, disk_total, memory_percent) VALUES (?, ?, ?, ?, ?, ?)",
        (now, cpu, disk.percent, disk.used, disk.total, mem.percent),
    )
    cutoff = int((datetime.now() - timedelta(hours=RETENTION_HOURS)).timestamp())
    conn.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff,))
    conn.commit()
    conn.close()


async def collector_loop():
    init_db()
    while True:
        try:
            collect()
        except Exception as e:
            print(f"[collector] error: {e}")
        await asyncio.sleep(INTERVAL)


if __name__ == "__main__":
    asyncio.run(collector_loop())
