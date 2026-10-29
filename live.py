"""Живая проверка API Lighter RH для стража (только чтение)."""
import asyncio, json, sys
import aiohttp

RH = "https://api.rh.lighter.xyz"
WS = "wss://api.rh.lighter.xyz/stream"
out = []


def note(tag, obj):
    t = obj if isinstance(obj, str) else json.dumps(obj, ensure_ascii=False)
    out.append(f"{tag} {t}")


async def get(s, path, headers=None, **params):
    async with s.get(f"{RH}/api/v1/{path}", params=params, headers=headers or {}) as r:
        txt = await r.text()
        try:
            return r.status, json.loads(txt)
        except Exception:
            return r.status, txt[:300]


async def main():
    async with aiohttp.ClientSession() as s:
        st, d = await get(s, "orderBookDetails", filter="perp")
        lst = d.get("order_book_details") if isinstance(d, dict) else None
        note("OBD_FILTER", {"status": st, "n": len(lst or []), "keys": sorted((lst or [{}])[0].keys())[:40],
                             "spot_key": isinstance(d, dict) and "spot_order_book_details" in d,
                             "sample": {k: (lst or [{}])[0].get(k) for k in ("market_id", "symbol", "size_decimals", "price_decimals", "status")}})
        st2, d2 = await get(s, "orderBookDetails")
        note("OBD_NOFILTER", {"status": st2, "n": len((d2 or {}).get("order_book_details") or [])})
        mid = next((m["market_id"] for m in lst if m["symbol"] == "NVDA"), lst[0]["market_id"])
        st, d = await get(s, "orderBookOrders", market_id=mid, limit=1)
        note("OBO", {"status": st, "data": d})
        # активные ордера: без market_id, с market_id, с мусорным токеном
        for label, params, hdr in [("noauth_nomarket", {"account_index": 1}, None),
                                   ("noauth_market", {"account_index": 1, "market_id": mid}, None),
                                   ("badtoken_market", {"account_index": 1, "market_id": mid}, {"authorization": "123:1:4:abcd"}),
                                   ("badtoken_nomarket", {"account_index": 1}, {"authorization": "123:1:4:abcd"})]:
            st, d = await get(s, "accountActiveOrders", headers=hdr, **params)
            note("AAO_" + label, {"status": st, "data": d})
        # аккаунт с позициями
        found = None
        for idx in list(range(1, 40)) + [100, 200, 281474976710654]:
            st, d = await get(s, "account", by="index", value=str(idx))
            accs = d.get("accounts") if isinstance(d, dict) else None
            if not accs:
                continue
            pos = [p for p in accs[0].get("positions") or [] if float(p.get("position") or 0) != 0]
            if pos:
                found = idx
                note("ACCOUNT", {"index": idx, "n_positions_total": len(accs[0].get("positions") or []),
                                 "nonzero": len(pos), "sample": pos[0], "acc_keys": sorted(accs[0].keys())})
                break
        if not found:
            note("ACCOUNT", "no account with positions in scanned range")
            found = 1
        open("found.txt", "w").write(str(found))
        st, d = await get(s, "nextNonce", account_index=found, api_key_index=4)
        note("NONCE", {"status": st, "data": d})
        # WebSocket
        async with s.ws_connect(WS, max_msg_size=0) as ws:
            await ws.send_json({"type": "subscribe", "channel": "market_stats/all"})
            await ws.send_json({"type": "subscribe", "channel": f"account_all_positions/{found}"})
            await ws.send_json({"type": "subscribe", "channel": f"account_all_orders/{found}"})
            await ws.send_json({"type": "subscribe", "channel": f"account_all_orders/{found}", "auth": "123:1:4:abcd"})
            await ws.send_json({"type": "ping"})
            types = []
            got_ms = got_pos = 0
            for _ in range(60):
                try:
                    m = await ws.receive(timeout=10)
                except asyncio.TimeoutError:
                    break
                if m.type != aiohttp.WSMsgType.TEXT:
                    note("WS_CLOSE", str(m.type))
                    break
                d = json.loads(m.data)
                t = d.get("type")
                types.append(t)
                if str(t).endswith("market_stats") and got_ms < 2:
                    got_ms += 1
                    ms = d.get("market_stats") or {}
                    first = ms if "market_id" in ms else next(iter(ms.values()), {})
                    note("WS_MS", {"type": t, "keyed_by_id": "market_id" not in ms, "n": len(ms) if "market_id" not in ms else 1,
                                   "sample": {k: first.get(k) for k in ("market_id", "symbol", "best_bid_price", "best_ask_price", "mark_price", "mid_price")}})
                elif "positions" in str(t) and got_pos < 2:
                    got_pos += 1
                    p = d.get("positions") or {}
                    note("WS_POS", {"type": t, "keys": sorted(d.keys()), "positions_type": type(p).__name__,
                                    "first": (next(iter(p.items())) if isinstance(p, dict) and p else p[:1] if isinstance(p, list) else None)})
                elif "error" in json.dumps(d)[:200].lower() or "orders" in str(t):
                    note("WS_OTHER", json.dumps(d)[:600])
            note("WS_TYPES", sorted(set(map(str, types))))


try:
    asyncio.run(main())
except Exception as e:
    import traceback
    out.append("CRASH " + traceback.format_exc()[-1500:])
text = " || ".join(out).replace("\n", " | ")
print(text)
for i in range(0, min(len(text), 3800 * 8), 3800):
    print("::notice::" + text[i:i + 3800])
