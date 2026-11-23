"""LordTradingPlatform live relay (FastAPI) - streams OANDA practice prices to your site.

Free deploy on Render: put server.py + requirements.txt in a GitHub repo, then
Render > New > Web Service > that repo (Free instance).
  Build command: pip install -r requirements.txt
  Start command: uvicorn server:app --host 0.0.0.0 --port $PORT
Environment variables (set them in Render, never commit them):
  LTP_TOKEN         long random password your site sends to connect
  OANDA_API_KEY     API token from your free OANDA practice account
  OANDA_ACCOUNT_ID  practice account id, like 101-001-1234567-001
  ALLOWED_ORIGINS   your site, like https://lordtradingplatform.pages.dev
"""
import asyncio
import hmac
import json
import os
import time

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

TOKEN = os.environ["LTP_TOKEN"].encode()
ACC = os.environ["OANDA_ACCOUNT_ID"]
ORIGINS = {o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()}
HDR = {"Authorization": "Bearer " + os.environ["OANDA_API_KEY"], "Accept-Datetime-Format": "UNIX"}
API = "https://api-fxpractice.oanda.com"
STREAM = "https://stream-fxpractice.oanda.com"

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
FAILS: dict = {}
ACTIVE: set = set()


def client_ip(ws: WebSocket) -> str:
    fwd = ws.headers.get("x-forwarded-for", "")
    return (fwd or (ws.client.host if ws.client else "")).split(",")[0].strip()


@app.get("/")
def health():
    return {"ok": True}


async def pump(ws: WebSocket, sym: str):
    inst = sym[:3] + "_" + sym[3:]
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=None), headers=HDR) as c:
        r = await c.get(f"{API}/v3/instruments/{inst}/candles",
                        params={"granularity": "M1", "count": 1500, "price": "M"})
        if r.status_code != 200:
            await ws.send_json({"err": f"history {r.status_code}"})
            return
        await ws.send_json({"hist": [
            [int(float(k["time"]) * 1000)] + [float(k["mid"][x]) for x in "ohlc"] + [k["volume"]]
            for k in r.json()["candles"]]})
        async with c.stream("GET", f"{STREAM}/v3/accounts/{ACC}/pricing/stream",
                            params={"instruments": inst}) as s:
            if s.status_code != 200:
                await ws.send_json({"err": f"stream {s.status_code}"})
                return
            async for line in s.aiter_lines():
                m = json.loads(line) if line else {}
                if m.get("type") == "PRICE" and m.get("bids") and m.get("asks"):
                    await ws.send_json({"b": float(m["bids"][0]["price"]),
                                        "a": float(m["asks"][0]["price"]),
                                        "t": int(float(m["time"]) * 1000)})


@app.websocket("/ws")
async def feed(ws: WebSocket):
    if ORIGINS and ws.headers.get("origin") not in ORIGINS:
        await ws.close(code=1008)
        return
    ip = client_ip(ws)
    n, ts = FAILS.get(ip, (0, 0.0))
    if (n >= 8 and time.time() - ts < 600) or len(ACTIVE) >= 3:
        await ws.close(code=1008)
        return
    await ws.accept()
    ACTIVE.add(ws)
    task = None
    try:
        msg = await asyncio.wait_for(ws.receive_json(), 10)
        if not hmac.compare_digest(str(msg.get("auth", "")).encode(), TOKEN):
            FAILS[ip] = (n + 1, time.time())
            await asyncio.sleep(2)
            await ws.close(code=1008)
            return
        FAILS.pop(ip, None)
        while True:
            sym = str(msg.get("sym", "")).upper()
            if len(sym) == 6 and sym.isalpha():
                if task:
                    task.cancel()
                task = asyncio.create_task(pump(ws, sym))
            msg = await ws.receive_json()
    except (WebSocketDisconnect, asyncio.TimeoutError, json.JSONDecodeError, RuntimeError):
        pass
    finally:
        ACTIVE.discard(ws)
        if task:
            task.cancel()
