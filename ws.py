import asyncio, json, aiohttp
out = []
def note(tag, obj):
    out.append(f"{tag} " + (obj if isinstance(obj, str) else json.dumps(obj, ensure_ascii=False)))
async def try_url(s, url, acc):
    try:
        async with s.ws_connect(url, max_msg_size=0) as ws:
            note("WS_OK", url)
            await ws.send_json({"type": "subscribe", "channel": "market_stats/all"})
            await ws.send_json({"type": "subscribe", "channel": f"account_all_positions/{acc}"})
            await ws.send_json({"type": "subscribe", "channel": f"account_all_orders/{acc}"})
            await ws.send_json({"type": "subscribe", "channel": f"account_all_orders/{acc}", "auth": "1793289617:3:4:" + "ab" * 40})
            await ws.send_json({"type": "ping"})
            ms = pos = other = 0
            types = []
            for _ in range(80):
                try:
                    m = await ws.receive(timeout=8)
                except asyncio.TimeoutError:
                    break
                if m.type != aiohttp.WSMsgType.TEXT:
                    note("WS_CLOSE", str(m.type) + " " + str(m.data)[:200]); break
                d = json.loads(m.data); t = str(d.get("type")); types.append(t)
                if t.endswith("market_stats") and ms < 1:
                    ms += 1; x = d.get("market_stats") or {}
                    keyed = "market_id" not in x
                    first = next(iter(x.values()), {}) if keyed else x
                    note("MS", {"type": t, "keyed_by_id": keyed, "n": len(x) if keyed else 1,
                                "first_key": next(iter(x.keys()), None) if keyed else None,
                                "fields": {k: first.get(k) for k in ("market_id", "symbol", "best_bid_price", "best_ask_price", "mark_price", "mid_price")}})
                elif "positions" in t and pos < 2:
                    pos += 1; p = d.get("positions")
                    note("POS", {"type": t, "keys": sorted(d.keys()), "ptype": type(p).__name__,
                                 "first": next(iter(p.items())) if isinstance(p, dict) and p else None})
                elif not t.endswith("market_stats") and other < 6:
                    other += 1; note("OTHER", json.dumps(d)[:500])
            note("TYPES", {"url": url, "types": sorted(set(types))})
    except Exception as e:
        note("WS_FAIL", f"{url} {type(e).__name__}: {e}")
async def main():
    async with aiohttp.ClientSession() as s:
        for url in ("wss://api.rh.lighter.xyz/stream?readonly=true", "wss://api.rh.lighter.xyz/stream",
                    "wss://mainnet.zklighter.elliot.ai/stream", "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"):
            await try_url(s, url, 3)
try:
    asyncio.run(main())
except Exception:
    import traceback; out.append("CRASH " + traceback.format_exc()[-1200:])
text = " || ".join(out).replace("\n", " | ")
print(text)
for i in range(0, min(len(text), 3800 * 8), 3800):
    print("::notice::" + text[i:i + 3800])
