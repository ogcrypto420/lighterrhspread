"""Живой прогон исправленного стража на Lighter RH (только чтение, случайный ключ)."""
import asyncio, json, os, subprocess, sys, time
from pathlib import Path
G = Path(sys.argv[1]); ACC = int(sys.argv[2])
sys.path.insert(0, str(G)); os.chdir(G)
out = []
def note(tag, obj): out.append(f"{tag} " + (obj if isinstance(obj, str) else json.dumps(obj, ensure_ascii=False)))

async def stream_check():
    import aiohttp, guardian as Gd, lighter_signer as LS
    from security import SafeSession
    priv, _ = LS.generate_api_key()
    async with aiohttp.ClientSession(trust_env=False) as raw:
        http = SafeSession(raw, Gd.ALLOWED_HOSTS)
        ex = Gd.LighterExchange(http, "robinhood", ACC, 4, priv, wallet=1, wallet_name="Проба")
        await ex.load_markets()
        ex.signer = LS.Signer(ACC, 4, priv, 466324)
        g = Gd.Guardian(ex, Gd.Config(), lambda t: asyncio.sleep(0))
        ex.guardian = g
        await ex.refresh_positions()
        rest_pos = {ex.markets[m].symbol: p for m, p in g.positions.items() if p}
        task = asyncio.create_task(ex.stream())
        await asyncio.sleep(20)
        task.cancel()
        nvda = next(m for m, x in ex.markets.items() if x.symbol == "NVDA")
        q = g.quotes.get(nvda)
        bp_bid = await ex.best_price(nvda, is_ask=True)
        ao = await ex.fetch_active_orders(nvda)
        note("STREAM", {"markets": len(ex.markets), "rest_positions": rest_pos,
                        "ws_positions": {ex.markets[m].symbol: p for m, p in g.positions.items() if p and m in ex.markets},
                        "quotes": len(g.quotes), "nvda_quote": q and [q.bid, q.ask, q.mark, round(time.time() - q.ts, 1)],
                        "best_bid_rest": bp_bid, "last_msg_age": round(time.time() - ex.last_msg, 1),
                        "sub_error": ex.sub_error, "active_orders_with_bad_key": ao})

try:
    asyncio.run(stream_check())
except Exception:
    import traceback; note("CRASH", traceback.format_exc()[-1500:])

# весь страж: неверный ключ → понятное сообщение и повторы в фоне, процесс не падает
import vault, lighter_signer
priv, _ = lighter_signer.generate_api_key()
v = vault.Vault.create("probe password 123", G / "guardian.vault")
v.data.update({"W1_RH_API_PRIVATE_KEY": priv, "W1_RH_API_KEY_INDEX": "4", "W1_RH_ACCOUNT_INDEX": str(ACC), "W1_NAME": "Проба"})
v.save()
pw = G / "pw.txt"; pw.write_text("probe password 123\n"); os.chmod(pw, 0o600)
(G / ".env").write_text("DRY_RUN=true\n")
try:
    r = subprocess.run([sys.executable, "guardian.py"], env=dict(os.environ, GUARDIAN_PASSWORD_FILE=str(pw)),
                       capture_output=True, text=True, timeout=45)
    note("E2E_EXITED", {"rc": r.returncode, "tail": (r.stdout + r.stderr)[-1200:]})
except subprocess.TimeoutExpired as e:
    txt = (e.stdout or b"").decode() + (e.stderr or b"").decode() if isinstance(e.stdout, bytes) else (e.stdout or "") + (e.stderr or "")
    note("E2E_ALIVE_AFTER_45s", {"tail": txt[-1200:], "leaked_key": priv[2:20] in txt})
text = " || ".join(out).replace("\n", " | ")
print(text)
for i in range(0, min(len(text), 3800 * 8), 3800):
    print("::notice::" + text[i:i + 3800])
