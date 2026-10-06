import asyncio
import logging
import os
import signal
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect
from ptyprocess import PtyProcessUnicode

logger = logging.getLogger("term")

# nsenter 进入 host PID 1 的所有 namespace,再 su 切到目标用户跑 zsh login shell
SPAWN_CMD = [
    "nsenter", "--all", "--target", "1",
    "--",
    "/usr/bin/su", "-", "mayizhe", "-s", "/usr/bin/zsh",
]


def _spawn(rows: int, cols: int) -> PtyProcessUnicode:
    env = os.environ.copy()
    env["TERM"] = "xterm-256color"
    env.setdefault("LANG", "en_US.UTF-8")
    return PtyProcessUnicode.spawn(SPAWN_CMD, dimensions=(rows, cols), env=env)


async def handle_ws(ws: WebSocket):
    loop = asyncio.get_event_loop()
    pty: Optional[PtyProcessUnicode] = None
    reader: Optional[asyncio.Task] = None

    async def reader_loop(p: PtyProcessUnicode):
        try:
            while True:
                chunk = await loop.run_in_executor(None, p.read, 4096)
                if not chunk:
                    break
                await ws.send_text(chunk)
        except (EOFError, OSError):
            pass
        except Exception as e:
            logger.warning(f"reader: {e}")
        finally:
            try:
                await ws.close()
            except Exception:
                pass

    try:
        first = await ws.receive_json()
        rows, cols = 24, 80
        pending_data = ""
        if isinstance(first, dict):
            t = first.get("type")
            if t == "resize":
                rows = int(first.get("rows") or 24) or 24
                cols = int(first.get("cols") or 80) or 80
            elif t == "data":
                pending_data = first.get("data", "")

        pty = await loop.run_in_executor(None, lambda: _spawn(rows, cols))
        reader = asyncio.create_task(reader_loop(pty))

        if pending_data:
            pty.write(pending_data)

        while True:
            msg = await ws.receive_json()
            if not isinstance(msg, dict):
                continue
            t = msg.get("type")
            if t == "data":
                pty.write(msg.get("data", ""))
            elif t == "resize":
                try:
                    pty.setwinsize(int(msg["rows"]), int(msg["cols"]))
                except Exception as e:
                    logger.debug(f"resize ignored: {e}")
            elif t == "signal":
                name = msg.get("signal", "SIGINT")
                try:
                    pty.kill(getattr(signal, name))
                except Exception:
                    pass
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning(f"ws term error: {e}")
    finally:
        if reader and not reader.done():
            reader.cancel()
        if pty:
            try:
                pty.kill(signal.SIGHUP)
            except Exception:
                pass
            try:
                pty.close(force=True)
            except Exception:
                pass
