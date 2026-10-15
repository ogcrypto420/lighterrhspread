#!/usr/bin/env python3
"""
Lighter RH (Robinhood Chain) — алерты о узком спреде и плотном стакане по акциям.

Как работает:
  1. Раз в N минут берёт список рынков с https://api.rh.lighter.xyz/api/v1/orderBookDetails
     и сам отбирает рынки акций (новые листинги подхватываются автоматически).
  2. Держит живые стаканы через WebSocket (канал order_book/{id}).
  3. Раз в секунду считает по каждой паре: спред, глубину около цены и
     проскальзывание для рыночной заявки твоего размера.
  4. Если пара проходит все фильтры и держится так HOLD_SECONDS секунд —
     шлёт сообщение в Telegram. Когда условия пропадают — (опционально) шлёт «пара ушла».

Зависимость одна: aiohttp  (pip install aiohttp)
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import math
import os
import re
import time
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path

import aiohttp

try:  # картинка для /status (если Pillow нет — бот пришлёт текст)
    import status_image
except Exception:  # noqa: BLE001
    status_image = None

HERE = Path(__file__).resolve().parent
log = logging.getLogger("lighter-bot")


# ───────────────────────────── конфиг ─────────────────────────────

def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = re.split(r"\s+#", v, maxsplit=1)[0]  # отрезать комментарий в конце строки
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_dotenv(HERE / ".env")


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v.replace(",", ".")) if v not in (None, "") else default


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v in (None, "") else v.strip().lower() in ("1", "true", "yes", "on", "да")


def env_set(name: str) -> set[str]:
    return {s.strip().upper() for s in os.getenv(name, "").split(",") if s.strip()}


TG_TOKEN = env_str("TG_TOKEN")
TG_CHAT_IDS = [c.strip() for c in env_str("TG_CHAT_ID").split(",") if c.strip()]
API_URL = env_str("API_URL", "https://api.rh.lighter.xyz").rstrip("/")
WS_URL = env_str("WS_URL", "wss://api.rh.lighter.xyz/stream?readonly=true")
# Основной Lighter («core») — отдельная биржа, мониторится дополнительно
ENABLE_CORE = env_bool("ENABLE_CORE", True)
CORE_API_URL = env_str("CORE_API_URL", "https://mainnet.zklighter.elliot.ai").rstrip("/")
CORE_WS_URL = env_str("CORE_WS_URL", "wss://mainnet.zklighter.elliot.ai/stream?readonly=true")
# код биржи → (название в сообщениях, REST, WebSocket)
EXCHANGES: dict[str, tuple[str, str, str]] = {"rh": ("RH", API_URL, WS_URL)}
if ENABLE_CORE:
    EXCHANGES["core"] = ("CORE", CORE_API_URL, CORE_WS_URL)

INCLUDE_ETF = env_bool("INCLUDE_ETF", False)              # SPY, QQQ, SOXL, USO, SLV, SGOV
INCLUDE_PREIPO = env_bool("INCLUDE_PREIPO", True)        # OPENAI, ANTHROPIC, SHEIN …
INCLUDE_COMMODITIES = env_bool("INCLUDE_COMMODITIES", False)  # XAU, XAG
INCLUDE_SPOT = env_bool("INCLUDE_SPOT", False)            # спот-пары вида NVDA/USDG
ONLY_SYMBOLS = env_set("SYMBOLS")                         # если задано — только эти тикеры
EXCLUDE_SYMBOLS = env_set("EXCLUDE")

MARKETS_REFRESH_SEC = env_float("MARKETS_REFRESH_MIN", 10) * 60
MARKETS_PER_WS = int(env_float("MARKETS_PER_WS", 20))
EVAL_INTERVAL_SEC = env_float("EVAL_INTERVAL_SEC", 1.0)
DATA_DIR = Path(env_str("DATA_DIR", str(HERE)))
DATA_DIR.mkdir(parents=True, exist_ok=True)
SETTINGS_FILE = DATA_DIR / "settings.json"
STATE_FILE = DATA_DIR / "state.json"   # чтобы после перезапуска не дублировать алерты
# Через сколько минут бот сам корректно завершится (0 — работать бесконечно).
# Нужно для GitHub Actions, где один запуск живёт максимум 6 часов.
MAX_RUNTIME_MIN = env_float("MAX_RUNTIME_MIN", 0)

# Ручная разметка того, что нельзя надёжно отличить от акций по параметрам рынка.
# Можно дополнять через .env (через запятую).
ETF_SYMBOLS = {
    "SPY", "QQQ", "SOXL", "USO", "SLV", "SGOV", "IWM", "DIA", "TQQQ", "SQQQ", "GLD", "EWY", "EWJ", "EWZ", "EWT",
    "FXI", "KWEB", "URA", "KORU", "ARKK", "XLE", "XLF", "XLK", "GDX", "GDXJ", "TLT", "HYG", "SMH", "SOXX", "VTI",
    "VOO", "IBIT", "ETHA", "BITO", "UVXY", "SOXS", "TNA", "YINN", "NVDL", "TSLL", "MSTU", "CONL", "UNG", "COPX",
} | env_set("EXTRA_ETF")
INDEX_SYMBOLS = {
    "US500", "US100", "US30", "US2000", "SPX", "NDX", "DJI", "VIX", "JP225", "DE40", "UK100", "HK50", "KR200",
    "KOSPI", "NIKKEI", "STONK", "DXY", "US10Y", "US02Y", "US2Y", "US30Y",
} | env_set("EXTRA_INDEX")
PREIPO_SYMBOLS = {"OPENAI", "ANTHROPIC", "SHEIN", "SPACEX", "XAI", "STRIPE", "DATABRICKS", "ANDURIL",
                  "PERPLEXITY", "KRAKEN"} | env_set("EXTRA_PREIPO")
COMMODITY_SYMBOLS = {"XAU", "XAG", "XPT", "XPD", "WTI", "BRENT", "NATGAS", "NG", "COPPER", "XCU",
                     "CORN", "WHEAT", "SOY"} | env_set("EXTRA_COMMODITIES")
# одна и та же акция может называться на биржах по-разному — для сравнения RH ↔ Core
ALIASES = {"SKHYNIX": "SKHY", "SPACEX": "SPCX"}
EXTRA_STOCKS = env_set("EXTRA_STOCKS")       # принудительно считать акцией
EXTRA_CRYPTO = env_set("EXTRA_CRYPTO")       # принудительно считать криптой
FIAT = {"USD", "EUR", "JPY", "GBP", "CHF", "AUD", "CAD", "KRW", "CNH", "CNY", "MXN", "BRL", "INR", "TRY", "NZD",
        "HKD", "SGD", "ZAR", "SEK", "NOK", "PLN", "TWD", "IDR", "PHP", "THB", "ILS", "HUF", "CZK", "DKK"}


@dataclass
class Settings:
    """Фильтры одного пользователя. Значения по умолчанию — из .env / Variables, дальше каждый меняет через /set."""
    spread: float = env_float("MAX_SPREAD_PCT", 0.003)      # макс. спред, % от mid
    size: float = env_float("ORDER_SIZE_USD", 5000)         # размер заявки (с плечом), $
    impact: float = env_float("MAX_IMPACT_PCT", 0.005)      # макс. проскальзывание от лучшей цены на size, %
    band: float = env_float("DEPTH_BAND_PCT", 0.02)         # полоса вокруг mid для подсчёта глубины, %
    depth: float = env_float("MIN_DEPTH_USD", 5000)         # мин. глубина в полосе на КАЖДОЙ стороне, $
    hold: float = env_float("HOLD_SECONDS", 10)             # сколько секунд условие должно держаться
    cooldown: float = env_float("COOLDOWN_MIN", 15)         # пауза между алертами по одной паре, мин
    grace: float = env_float("LOST_GRACE_SECONDS", 10)      # через сколько сек плохого стакана пара «ушла»
    notify_lost: bool = env_bool("NOTIFY_LOST", True)       # слать ли «пара ушла»
    venue: str = env_str("VENUE", "rh").lower()             # rh — только Robinhood, both — RH + Core (/core)

    def wants(self, m: "Market") -> bool:
        return self.venue in ("both", m.ex)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Settings":
        s = cls()
        for f in fields(cls):
            if f.name in data:
                try:
                    setattr(s, f.name, type(getattr(s, f.name))(data[f.name]))
                except (TypeError, ValueError):
                    pass
        return s


ALLOW_PUBLIC = env_bool("ALLOW_PUBLIC", True)   # false — бот отвечает только chat id из TG_CHAT_ID
MAX_USERS = int(env_float("MAX_USERS", 1000))

SETTING_HELP = {
    "spread": "макс. спред, %",
    "size": "размер заявки с плечом, $",
    "impact": "макс. проскальзывание на этот размер, %",
    "depth": "мин. глубина стакана на каждую сторону, $",
    "band": "полоса от цены для подсчёта глубины, ±%",
    "hold": "сколько секунд условия держатся до алерта",
    "cooldown": "пауза между алертами по одной паре, мин",
    "grace": "через сколько секунд слать «🔴 пара ушла»",
    "notify_lost": "слать «🔴 пара ушла» (on/off)",
}


# ───────────────────────────── рынки ─────────────────────────────

@dataclass
class Market:
    id: int
    symbol: str          # как на бирже: "NVDA" или "NVDA/USDG"
    base: str            # "NVDA" (для сопоставления между биржами)
    kind: str            # stock / etf / preipo / commodity / fx / index / crypto / other
    mtype: str           # perp / spot
    price_decimals: int
    last_price: float
    ex: str = "rh"       # rh / core

    @property
    def key(self) -> str:
        return f"{self.ex}:{self.id}"

    @property
    def ex_name(self) -> str:
        return EXCHANGES.get(self.ex, (self.ex,))[0]

    @property
    def tick(self) -> float:
        return 10 ** -self.price_decimals

    @property
    def label(self) -> str:
        spot = " (спот)" if self.mtype == "spot" else ""
        return f"{self.symbol}{spot} · {self.ex_name}"


def classify_perp(m: dict) -> str:
    sym = m["symbol"].upper()
    if sym in EXTRA_CRYPTO:
        return "crypto"
    if sym in EXTRA_STOCKS:
        return "stock"
    if sym in COMMODITY_SYMBOLS:
        return "commodity"
    if sym in ETF_SYMBOLS:
        return "etf"
    if sym in PREIPO_SYMBOLS:
        return "preipo"
    if sym in INDEX_SYMBOLS:
        return "index"
    if len(sym) == 6 and sym[:3] in FIAT and sym[3:] in FIAT:
        return "fx"
    # Отдельного поля «класс актива» в API нет. Признак по параметрам рынка:
    # базовая ставка 0.0100 — крипта, 0.0032 — RWA (акции/ETF/сырьё/FX), остальное — прочее.
    try:
        rate = float(m.get("base_interest_rate", ""))
        mult = float(m.get("funding_premium_multiplier", 50))
    except (TypeError, ValueError):
        return "other"
    if rate >= 0.0075:
        return "crypto"
    if abs(rate - 0.0032) > 1e-6:
        return "other"
    if mult <= 1:          # у pre-IPO на Lighter множитель премии фандинга 1
        return "preipo"
    if mult >= 100:        # нетипичный RWA (например, корзины) — не трогаем
        return "other"
    return "stock"


def parse_markets(data: dict, ex: str = "rh") -> dict[str, Market]:
    perps: dict[str, Market] = {}
    kind_by_base: dict[str, str] = {}
    for m in data.get("order_book_details") or []:
        kind = classify_perp(m)
        sym = m["symbol"].upper()
        kind_by_base[sym] = kind
        if m.get("status") != "active" or m.get("is_frozen"):
            continue
        mk = Market(int(m["market_id"]), sym, ALIASES.get(sym, sym), kind, "perp",
                    int(m.get("price_decimals", 2)), float(m.get("last_trade_price") or 0), ex)
        perps[mk.key] = mk
    spots: dict[str, Market] = {}
    for m in data.get("spot_order_book_details") or []:
        sym = m["symbol"].upper()
        base = sym.split("/")[0]
        kind = "stock" if base in EXTRA_STOCKS else kind_by_base.get(base, "crypto")
        if m.get("status") != "active" or m.get("is_frozen"):
            continue
        mk = Market(int(m["market_id"]), sym, ALIASES.get(base, base), kind, "spot",
                    int(m.get("price_decimals", 2)), float(m.get("last_trade_price") or 0), ex)
        spots[mk.key] = mk
    return {**perps, **spots}


def select_markets(all_markets: dict[str, Market]) -> dict[str, Market]:
    kinds = {"stock"}
    if INCLUDE_ETF:
        kinds.add("etf")
    if INCLUDE_PREIPO:
        kinds.add("preipo")
    if INCLUDE_COMMODITIES:
        kinds.add("commodity")
    out = {}
    for mid, m in all_markets.items():
        if m.mtype == "spot" and not INCLUDE_SPOT:
            continue
        if ONLY_SYMBOLS:
            if m.base not in ONLY_SYMBOLS:
                continue
        elif m.kind not in kinds:
            continue
        if m.base in EXCLUDE_SYMBOLS or m.symbol in EXCLUDE_SYMBOLS or m.key.upper() in EXCLUDE_SYMBOLS:
            continue
        out[mid] = m
    return out


# ───────────────────────────── стакан ─────────────────────────────

class Book:
    """Локальная копия стакана: снапшот при подписке + дельты (size — новый итог уровня, 0 — удалить)."""
    __slots__ = ("asks", "bids", "nonce", "updated")

    def __init__(self) -> None:
        self.asks: dict[float, float] = {}
        self.bids: dict[float, float] = {}
        self.nonce = -1
        self.updated = 0.0

    @property
    def ready(self) -> bool:
        return self.nonce >= 0

    def reset(self) -> None:
        self.asks.clear()
        self.bids.clear()
        self.nonce = -1

    def apply(self, ob: dict, snapshot: bool) -> bool:
        """Возвращает False, если пропущено обновление и нужна пересинхронизация."""
        if snapshot:
            self.asks.clear()
            self.bids.clear()
        else:
            if self.nonce < 0:
                return True  # ждём снапшот
            begin = ob.get("begin_nonce")
            if begin is not None and begin != self.nonce:
                self.reset()
                return False
        for side, levels in ((self.asks, ob.get("asks") or ()), (self.bids, ob.get("bids") or ())):
            for lv in levels:
                p, s = float(lv["price"]), float(lv["size"])
                if s <= 0:
                    side.pop(p, None)
                else:
                    side[p] = s
        if ob.get("nonce") is not None:
            self.nonce = int(ob["nonce"])
        elif snapshot:
            self.nonce = 0
        self.updated = time.time()
        return True


@dataclass
class Metrics:
    bid: float
    ask: float
    mid: float
    spread_pct: float
    spread_ticks: float
    buy_impact: float     # % от лучшего ask, на который уедет средняя цена покупки на size
    sell_impact: float    # то же для продажи от лучшего bid
    depth_bid: float      # $ в полосе ниже mid
    depth_ask: float      # $ в полосе выше mid

    @property
    def impact(self) -> float:
        return max(self.buy_impact, self.sell_impact)

    @property
    def depth(self) -> float:
        return min(self.depth_bid, self.depth_ask)


def _impact(levels: list[tuple[float, float]], best: float, size_usd: float) -> float:
    remaining, cost, qty = size_usd, 0.0, 0.0
    for p, s in levels:
        take = min(p * s, remaining)
        cost += take
        qty += take / p
        remaining -= take
        if remaining <= 1e-9:
            break
    if remaining > 1e-6 or qty == 0:
        return math.inf  # в стакане не хватает ликвидности на твой размер
    return abs(cost / qty - best) / best * 100


def compute_metrics(book: Book, tick: float, size_usd: float, band_pct: float) -> Metrics | None:
    if not book.asks or not book.bids:
        return None
    asks = sorted(book.asks.items())
    bids = sorted(book.bids.items(), reverse=True)
    ask, bid = asks[0][0], bids[0][0]
    if ask <= bid:
        return None  # перекрёстный стакан — мусор между обновлениями
    mid = (ask + bid) / 2
    lo, hi = mid * (1 - band_pct / 100), mid * (1 + band_pct / 100)
    return Metrics(
        bid=bid, ask=ask, mid=mid,
        spread_pct=(ask - bid) / mid * 100,
        spread_ticks=round((ask - bid) / tick, 2),
        buy_impact=_impact(asks, ask, size_usd),
        sell_impact=_impact(bids, bid, size_usd),
        depth_bid=sum(p * s for p, s in bids if p >= lo),
        depth_ask=sum(p * s for p, s in asks if p <= hi),
    )


def passes(m: Metrics | None, s: Settings) -> bool:
    return (m is not None and m.spread_pct <= s.spread + 1e-12
            and m.impact <= s.impact + 1e-12 and m.depth >= s.depth)


# ───────────────────────────── форматирование ─────────────────────────────

def fmt_usd(x: float) -> str:
    if math.isinf(x):
        return "∞"
    if x >= 1e6:
        return f"${x / 1e6:.2f}M"
    if x >= 1e3:
        return f"${x / 1e3:.0f}k"
    return f"${x:.0f}"


def fmt_pct(x: float, nd: int = 4) -> str:
    return "нет ликв." if math.isinf(x) else f"{x:.{nd}f}%"


def fmt_price(x: float, m: Market) -> str:
    return f"{x:.{m.price_decimals}f}"


def min_spread_pct(m: Market, price: float) -> float:
    return m.tick / price * 100 if price > 0 else math.inf


# ───────────────────────────── приложение ─────────────────────────────

@dataclass
class PairState:
    ok_since: float | None = None
    bad_since: float | None = None
    alerted: bool = False
    last_alert: float = 0.0


@dataclass
class User:
    chat_id: str
    s: Settings = field(default_factory=Settings)
    paused: bool = False
    joined: float = field(default_factory=time.time)
    pairs: dict[str, PairState] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"v": 2, "s": self.s.to_dict(), "paused": self.paused, "joined": self.joined,
                "pairs": {key: {"alerted": st.alerted, "last_alert": st.last_alert}
                          for key, st in self.pairs.items() if st.alerted or st.last_alert}}

    @classmethod
    def from_dict(cls, chat_id: str, d: dict) -> "User":
        u = cls(chat_id, Settings.from_dict(d.get("s") or {}), bool(d.get("paused")),
                float(d.get("joined") or time.time()))
        if int(d.get("v", 1)) < 2:      # до версии 2 Core был включён у всех по умолчанию
            u.s.venue = "rh"
        for key, st in (d.get("pairs") or {}).items():
            key = f"rh:{key}" if str(key).isdigit() else str(key)   # старый формат — только RH
            u.pairs[key] = PairState(alerted=bool(st.get("alerted")),
                                     last_alert=float(st.get("last_alert", 0)))
        return u


def is_owner(chat_id: str) -> bool:
    return str(chat_id) in TG_CHAT_IDS


class App:
    def __init__(self) -> None:
        self.defaults = Settings()
        self.markets: dict[str, Market] = {}     # ключ "rh:15" / "core:115"
        self.books: dict[str, Book] = {}
        self.users: dict[str, User] = {}
        self.feeds: list[asyncio.Task] = []
        self.http: aiohttp.ClientSession | None = None
        self.started = time.time()
        self.tg_offset = 0
        self.known_ids: set[str] = set()
        self.restored = self.load_state()
        for cid in TG_CHAT_IDS:  # владелец подписан всегда
            self.users.setdefault(cid, User(cid))
        self._migrate_legacy_settings()

    # ---------- состояние между перезапусками ----------
    def load_state(self) -> bool:
        if not STATE_FILE.exists():
            return False
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            for cid, ud in (data.get("users") or {}).items():
                self.users[cid] = User.from_dict(cid, ud)
            if "users" not in data and TG_CHAT_IDS:  # формат однопользовательской версии
                owner = User(TG_CHAT_IDS[0], paused=bool(data.get("paused")))
                for mid, st in (data.get("pairs") or {}).items():
                    owner.pairs[f"rh:{mid}"] = PairState(alerted=bool(st.get("alerted")),
                                                      last_alert=float(st.get("last_alert", 0)))
                self.users[owner.chat_id] = owner
            self.tg_offset = int(data.get("tg_offset", 0))
            self.known_ids = {f"rh:{i}" if str(i).isdigit() else str(i) for i in data.get("known_ids", [])}
            log.info("Состояние восстановлено: пользователей %d", len(self.users))
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("state.json не прочитан: %s", e)
            return False

    def _migrate_legacy_settings(self) -> None:
        """Настройки из однопользовательской версии (settings.json) переносим владельцу."""
        if not SETTINGS_FILE.exists() or not TG_CHAT_IDS:
            return
        try:
            data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
            owner = self.users[TG_CHAT_IDS[0]]
            owner.s = Settings.from_dict({**owner.s.to_dict(), **data})
            SETTINGS_FILE.rename(SETTINGS_FILE.with_suffix(".migrated.json"))
            log.info("Старые настройки перенесены владельцу")
        except Exception as e:  # noqa: BLE001
            log.warning("Миграция settings.json не удалась: %s", e)

    def save_state(self) -> None:
        data = {
            "users": {cid: u.to_dict() for cid, u in self.users.items()},
            "tg_offset": self.tg_offset,
            "known_ids": sorted(self.markets) or sorted(self.known_ids),
        }
        try:
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(data), encoding="utf-8")
            tmp.replace(STATE_FILE)
        except Exception as e:  # noqa: BLE001
            log.warning("state.json не сохранён: %s", e)

    # ---------- Telegram ----------
    async def tg(self, method: str, **params):
        if not TG_TOKEN:
            return None
        url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
        timeout = aiohttp.ClientTimeout(total=params.get("timeout", 0) + 15)
        async with self.http.post(url, json=params, timeout=timeout) as r:
            data = await r.json(content_type=None)
            if not data.get("ok"):
                log.warning("Telegram %s: %s", method, data)
            return data

    @staticmethod
    def _chunks(text: str) -> list[str]:
        chunks, cur = [], ""
        for block in text.split("\n\n"):
            if len(cur) + len(block) + 2 > 3900:
                chunks.append(cur)
                cur = ""
            cur = f"{cur}\n\n{block}" if cur else block
        if cur:
            chunks.append(cur)
        return chunks

    async def send(self, text: str, chat_ids: list[str] | None = None) -> None:
        """Отправка. Без chat_ids — владельцу (TG_CHAT_ID)."""
        targets = chat_ids if chat_ids is not None else TG_CHAT_IDS
        if not targets:
            log.info("[нет получателя] %s", re.sub(r"<[^>]+>", "", text))
            return
        for cid in targets:
            for ch in self._chunks(text):
                try:
                    data = await self.tg("sendMessage", chat_id=cid, text=ch, parse_mode="HTML",
                                         disable_web_page_preview=True)
                except Exception as e:  # noqa: BLE001
                    log.warning("Не удалось отправить в %s: %s", cid, e)
                    break
                if data and not data.get("ok"):
                    if data.get("error_code") == 403 and not is_owner(cid):
                        # пользователь заблокировал бота — отписываем
                        self.users.pop(str(cid), None)
                        log.info("Пользователь %s заблокировал бота — удалён", cid)
                        break
                    retry = (data.get("parameters") or {}).get("retry_after")
                    if retry:
                        await asyncio.sleep(float(retry))
            if len(targets) > 1:
                await asyncio.sleep(0.05)  # лимит Telegram ~30 сообщений/с

    async def send_photo(self, chat_id: str, png: bytes, caption: str) -> bool:
        if not TG_TOKEN:
            return False
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        form.add_field("caption", caption)
        form.add_field("parse_mode", "HTML")
        form.add_field("photo", png, filename="status.png", content_type="image/png")
        async with self.http.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto", data=form,
                                  timeout=aiohttp.ClientTimeout(total=60)) as r:
            data = await r.json(content_type=None)
        if not data.get("ok"):
            log.warning("Telegram sendPhoto: %s", data)
        return bool(data.get("ok"))

    async def broadcast(self, text: str) -> None:
        await self.send(text, [cid for cid, u in self.users.items() if not u.paused])

    # ---------- рынки ----------
    async def fetch_markets(self) -> dict[str, Market]:
        """Рынки со всех бирж. Если одна биржа не ответила — оставляем её прежний список."""
        out: dict[str, Market] = {}
        errors = 0
        for ex, (_, api, _) in EXCHANGES.items():
            try:
                async with self.http.get(f"{api}/api/v1/orderBookDetails",
                                         timeout=aiohttp.ClientTimeout(total=30)) as r:
                    r.raise_for_status()
                    out.update(select_markets(parse_markets(await r.json(content_type=None), ex)))
            except Exception as e:  # noqa: BLE001
                errors += 1
                log.warning("Не удалось получить рынки %s: %s", ex, e)
                out.update({k: m for k, m in self.markets.items() if m.ex == ex})
        if errors == len(EXCHANGES):
            raise RuntimeError("ни одна биржа не ответила")
        # на Core оставляем только те акции, что есть на RH — Core нужен лишь для сравнения
        rh = {(m.base, m.mtype) for m in out.values() if m.ex == "rh"}
        if rh:
            out = {k: m for k, m in out.items() if m.ex == "rh" or (m.base, m.mtype) in rh}
        return out

    async def markets_loop(self) -> None:
        first = True
        while True:
            try:
                selected = await self.fetch_markets()
                old_ids, new_ids = set(self.markets), set(selected)
                added = [selected[i] for i in new_ids - old_ids]
                self.markets = selected
                if new_ids != old_ids or not self.feeds:
                    self.restart_feeds()
                if first:
                    first = False
                    if not self.restored:
                        await self.send(self.startup_text())
                    else:  # тихий перезапуск: сообщаем только о листингах, появившихся пока бот не работал
                        fresh = [m for i, m in selected.items() if self.known_ids and i not in self.known_ids]
                        if fresh:
                            await self.broadcast("🆕 Новые пары в мониторинге: " +
                                                 ", ".join(html.escape(m.label) for m in fresh))
                elif added:
                    await self.broadcast("🆕 Новые пары в мониторинге: " +
                                         ", ".join(html.escape(m.label) for m in added))
                log.info("Рынков в мониторинге: %d, пользователей: %d", len(self.markets), len(self.users))
            except Exception as e:  # noqa: BLE001
                log.warning("Не удалось обновить список рынков: %s", e)
                if first:
                    await asyncio.sleep(15)
                    continue
            await asyncio.sleep(MARKETS_REFRESH_SEC)

    def unreachable_note(self, s: Settings) -> str:
        bad = [m for m in self.markets.values() if s.wants(m) and min_spread_pct(m, m.last_price) > s.spread]
        if not bad:
            return ""
        return (f"⚠️ У {len(bad)} пар даже спред в 1 тик больше {s.spread:g}% (шаг цены $0.01 при низкой цене), "
                "с текущим порогом они не пройдут никогда: " +
                ", ".join(f"{html.escape(m.base)}·{m.ex_name} ({min_spread_pct(m, m.last_price):.4f}%)"
                          for m in sorted(bad, key=lambda m: m.base)) +
                "\nЧтобы ловить и их: <code>/set spread 0.005</code>")

    def startup_text(self) -> str:
        ms = sorted(self.markets.values(), key=lambda m: m.label)
        return f"🚀 <b>Мониторинг запущен</b>\n\n" + self.pairs_text()

    def pairs_text(self) -> str:
        parts = []
        for ex, (name, _, _) in EXCHANGES.items():
            ms = sorted((m for m in self.markets.values() if m.ex == ex), key=lambda m: m.symbol)
            parts.append(f"<b>{name}</b> — {len(ms)} пар:\n" +
                         ", ".join(html.escape(m.symbol + (" (спот)" if m.mtype == "spot" else "")) for m in ms))
        return "\n\n".join(parts)

    # ---------- WebSocket ----------
    def restart_feeds(self) -> None:
        for t in self.feeds:
            t.cancel()
        self.feeds = []
        for ex in EXCHANGES:
            ids = sorted(m.id for m in self.markets.values() if m.ex == ex)
            for i in range(0, len(ids), MARKETS_PER_WS):
                chunk = ids[i:i + MARKETS_PER_WS]
                self.feeds.append(asyncio.create_task(self.feed(ex, chunk), name=f"feed-{ex}-{i}"))

    async def feed(self, ex: str, ids: list[int]) -> None:
        ws_url = EXCHANGES[ex][2]
        backoff = 1
        while True:
            try:
                async with self.http.ws_connect(ws_url, heartbeat=None, compress=15,
                                                max_msg_size=0, autoping=True) as ws:
                    for mid in ids:
                        self.books[f"{ex}:{mid}"] = Book()
                        await ws.send_json({"type": "subscribe", "channel": f"order_book/{mid}"})
                    log.info("WS %s подключён, подписка на %d рынков", ex, len(ids))
                    backoff = 1
                    pinger = asyncio.create_task(self._ping(ws))
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self.on_ws(ws, ex, json.loads(msg.data))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    finally:
                        pinger.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("WS %s ошибка: %s", ex, e)
            for mid in ids:
                if f"{ex}:{mid}" in self.books:
                    self.books[f"{ex}:{mid}"].reset()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    @staticmethod
    async def _ping(ws) -> None:
        while True:
            await asyncio.sleep(50)  # сервер рвёт соединение после 2 минут тишины
            await ws.send_json({"type": "ping"})

    async def on_ws(self, ws, ex: str, msg: dict) -> None:
        t = msg.get("type", "")
        if t in ("subscribed/order_book", "update/order_book"):
            try:
                mid = int(str(msg.get("channel", "")).split(":")[1])
            except (IndexError, ValueError):
                return
            book = self.books.setdefault(f"{ex}:{mid}", Book())
            if not book.apply(msg.get("order_book") or {}, snapshot=t.startswith("subscribed")):
                log.info("Пропуск обновлений по рынку %s:%s — пересинхронизация", ex, mid)
                await ws.send_json({"type": "unsubscribe", "channel": f"order_book/{mid}"})
                await ws.send_json({"type": "subscribe", "channel": f"order_book/{mid}"})
        elif t == "error" or "error" in msg:
            log.warning("WS сообщение об ошибке: %s", msg)

    # ---------- оценка и алерты ----------
    def metrics_for(self, m: Market, s: Settings, cache: dict | None = None) -> Metrics | None:
        key = (m.key, s.size, s.band)
        if cache is not None and key in cache:
            return cache[key]
        book = self.books.get(m.key)
        met = compute_metrics(book, m.tick, s.size, s.band) if book and book.ready else None
        if cache is not None:
            cache[key] = met
        return met

    def evaluate(self, now: float) -> dict[str, tuple[list, list]]:
        """Возвращает {chat_id: (новые хорошие пары, ушедшие пары)}."""
        cache: dict = {}
        out: dict[str, tuple[list, list]] = {}
        for cid, u in list(self.users.items()):
            if u.paused:
                continue
            s = u.s
            good_new, lost = [], []
            for m in self.markets.values():
                if m.ex != "rh":        # алерты только по RH; Core показывается строкой сравнения
                    continue
                met = self.metrics_for(m, s, cache)
                st = u.pairs.setdefault(m.key, PairState())
                if passes(met, s):
                    st.bad_since = None
                    if st.ok_since is None:
                        st.ok_since = now
                    if (not st.alerted and now - st.ok_since >= s.hold
                            and now - st.last_alert >= s.cooldown * 60):
                        st.alerted, st.last_alert = True, now
                        good_new.append((m, met, now - st.ok_since, self.twin_line(m, s, cache)))
                else:
                    st.ok_since = None
                    if st.alerted:
                        st.bad_since = st.bad_since or now
                        if now - st.bad_since >= s.grace:
                            st.alerted, st.bad_since = False, None
                            lost.append((m, met))
            if good_new or lost:
                out[cid] = (good_new, lost)
        return out

    def twin_line(self, m: Market, s: Settings, cache: dict | None = None) -> str:
        """Та же акция на другой бирже — для сравнения в алерте (если у пользователя включён Core)."""
        if s.venue != "both":
            return ""
        for o in self.markets.values():
            if o.ex != m.ex and o.base == m.base and o.mtype == m.mtype:
                om = self.metrics_for(o, s, cache)
                if om is None:
                    return f"На {o.ex_name}: стакан недоступен"
                ok = " ✅" if passes(om, s) else ""
                diff = (om.mid / self.metrics_for(m, s, cache).mid - 1) * 100 if self.metrics_for(m, s, cache) else 0
                where = "выше" if diff > 0 else "ниже"
                price = f"цена там на {abs(diff):.3f}% {where}" if abs(diff) >= 0.0005 else "цена та же"
                return (f"На {o.ex_name}: спред {fmt_pct(om.spread_pct)} ({om.spread_ticks:g} тик.), "
                        f"impact {fmt_pct(om.impact)}, {price}{ok}")
        return ""

    @staticmethod
    def good_text(m: Market, met: Metrics, held: float, s: Settings, twin: str = "") -> str:
        both = twin.endswith("✅")
        head = (f"🟢🟢 <b>{html.escape(m.symbol)} · RH + CORE</b> — узкий спред на обеих биржах"
                if both else f"🟢 <b>{html.escape(m.label)}</b> — узкий спред, плотный стакан")
        return (f"{head}\n"
                f"Спред: <b>{fmt_pct(met.spread_pct)}</b> ({met.spread_ticks:g} тик.) · "
                f"bid {fmt_price(met.bid, m)} / ask {fmt_price(met.ask, m)}\n"
                f"Проскальзывание на {fmt_usd(s.size)}: buy {fmt_pct(met.buy_impact)} / "
                f"sell {fmt_pct(met.sell_impact)}\n"
                f"Глубина ±{s.band:g}%: bid {fmt_usd(met.depth_bid)} / ask {fmt_usd(met.depth_ask)}\n"
                f"Держится {held:.0f} с" + (f"\n{twin}" if twin else ""))

    @staticmethod
    def lost_text(m: Market, met: Metrics | None, s: Settings) -> str:
        if met is None:
            return f"🔴 <b>{html.escape(m.label)}</b> — стакан недоступен"
        why = []
        if met.spread_pct > s.spread:
            why.append(f"спред {fmt_pct(met.spread_pct)}")
        if met.impact > s.impact:
            why.append(f"проскальзывание {fmt_pct(met.impact)}")
        if met.depth < s.depth:
            why.append(f"глубина {fmt_usd(met.depth)}")
        return f"🔴 <b>{html.escape(m.label)}</b> — условия ушли: " + ", ".join(why)

    async def eval_loop(self) -> None:
        await asyncio.sleep(5)  # дать стаканам прийти
        while True:
            try:
                for cid, (good_new, lost) in self.evaluate(time.time()).items():
                    u = self.users.get(cid)
                    if not u:
                        continue
                    parts = [self.good_text(m, met, held, u.s, twin) for m, met, held, twin in good_new]
                    if u.s.notify_lost:
                        parts += [self.lost_text(m, met, u.s) for m, met in lost]
                    if parts:
                        await self.send("\n\n".join(parts), [cid])
            except Exception:  # noqa: BLE001
                log.exception("Ошибка в цикле оценки")
            await asyncio.sleep(EVAL_INTERVAL_SEC)

    # ---------- команды ----------
    @staticmethod
    def settings_text(u: User) -> str:
        s = u.s
        return ("⚙️ <b>Твои фильтры</b>\n"
                f"spread ≤ {s.spread:g}%\n"
                f"impact ≤ {s.impact:g}% на size {fmt_usd(s.size)}\n"
                f"depth ≥ {fmt_usd(s.depth)} на сторону в band ±{s.band:g}%\n"
                f"hold {s.hold:g} с · cooldown {s.cooldown:g} мин\n"
                f"grace {s.grace:g} с · notify_lost {'on' if s.notify_lost else 'off'}\n"
                f"биржи: {'RH + CORE' if s.venue == 'both' else 'только RH'} (/core — переключить)"
                + ("\n\n⏸ <b>Алерты на паузе</b> — /resume" if u.paused else ""))

    def status_text(self, u: User, top: int = 15, only: str | None = None) -> str:
        """only=None: при включённом Core — сводная таблица RH|CORE в одну строку на акцию;
        only='rh'/'core' — подробная таблица по одной бирже."""
        s = u.s
        if only is None and s.venue == "both" and "core" in EXCHANGES:
            return self.status_merged(u, top)
        ex = only or "rh"
        rows = [(m, met) for m in self.markets.values() if m.ex == ex and (met := self.metrics_for(m, s))]
        if not rows:
            return "Стаканы ещё загружаются, попробуй через несколько секунд."
        rows.sort(key=lambda r: (r[1].spread_pct, r[1].impact))
        ok = sum(1 for _, met in rows if passes(met, s))
        name = EXCHANGES[ex][0]
        out = [f"📊 <b>{name}: топ-{min(top, len(rows))} по спреду</b> (проходят фильтры: {ok}/{len(rows)})",
               "<code>" + html.escape(f"{'Спред%':>8}{'Тик':>4}{'Impact':>7}{'Глуб.':>6}") + "</code>"]
        for m, met in rows[:top]:
            out.append(f"<b>{html.escape(m.symbol)}</b>" + (" ✅" if passes(met, s) else ""))
            out.append("<code>" + html.escape(self._row("", met, 0)) + "</code>")
        out.append(f"\nImpact — проскальзывание на {fmt_usd(s.size)}, %, "
                   f"глубина — меньшая из сторон в ±{s.band:g}%")
        return "\n".join(out)

    def status_image(self, u: User, top: int, only: str | None) -> tuple[bytes, str] | None:
        """PNG-таблица /status и подпись к ней; None — если рисовать нечего или нет Pillow."""
        if status_image is None:
            return None
        s = u.s
        top = min(top, 25)
        merged = only is None and s.venue == "both" and "core" in EXCHANGES
        groups = []
        if merged:
            by_base: dict[str, dict[str, Metrics]] = {}
            for m in self.markets.values():
                if m.mtype == "perp" and (met := self.metrics_for(m, s)):
                    by_base.setdefault(m.base, {})[m.ex] = met
            rows = sorted(by_base.items(), key=lambda kv: min(x.spread_pct for x in kv[1].values()))[:top]
            for base, mets in rows:
                cells = []
                for ex, tag in (("rh", "RH"), ("core", "CORE")):
                    met = mets.get(ex)
                    cells.append(status_image.Cell(tag, None, None, None, None) if met is None else
                                 status_image.Cell(tag, met.spread_pct, met.spread_ticks, met.impact,
                                                   met.depth, passes(met, s)))
                groups.append(status_image.Group(base, cells))
            title = f"Топ-{len(groups)} акций по спреду · RH и CORE"
            caption = "📊 Подробно по одной бирже: /status_rh · /status_core · текстом: /status_text"
        else:
            ex = only or "rh"
            rows = sorted(((m, met) for m in self.markets.values()
                           if m.ex == ex and (met := self.metrics_for(m, s))),
                          key=lambda r: (r[1].spread_pct, r[1].impact))[:top]
            tag = EXCHANGES[ex][0]
            for m, met in rows:
                groups.append(status_image.Group(m.symbol, [status_image.Cell(
                    tag, met.spread_pct, met.spread_ticks, met.impact, met.depth, passes(met, s))]))
            title = f"Топ-{len(groups)} по спреду · {tag}"
            caption = "📊 Текстом: /status_text"
        if not groups:
            return None
        png = status_image.render_status(title, groups, spread=s.spread, impact=s.impact, depth=s.depth,
                                         size=s.size, band=s.band)
        return png, caption

    def status_merged(self, u: User, top: int) -> str:
        """Core включён: по каждой акции блок из строк RH и CORE друг под другом."""
        s = u.s
        by_base: dict[str, dict[str, Metrics]] = {}
        for m in self.markets.values():
            if m.mtype != "perp":
                continue
            met = self.metrics_for(m, s)
            if met:
                by_base.setdefault(m.base, {})[m.ex] = met
        if not by_base:
            return "Стаканы ещё загружаются, попробуй через несколько секунд."
        rows = sorted(by_base.items(), key=lambda kv: min(x.spread_pct for x in kv[1].values()))
        out = [f"📊 <b>Топ-{min(top, len(rows))} акций по спреду, RH и CORE</b>",
               "<code>" + html.escape(f"{'':5}{'Спред%':>8}{'Тик':>4}{'Impact':>7}{'Глуб.':>6}") + "</code>"]
        ok = 0
        for base, mets in rows[:top]:
            out.append(f"\n<b>{html.escape(base)}</b>")
            for ex, tag in (("rh", "RH"), ("core", "CORE")):
                met = mets.get(ex)
                if met is None:
                    out.append("<code>" + html.escape(f"{tag:<5}{'—':>8}") + "</code>")
                    continue
                good = passes(met, s)
                ok += good
                out.append("<code>" + html.escape(self._row(tag, met, 5)) + "</code>" + (" ✅" if good else ""))
        out.append(f"\n✅ проходят фильтры: {ok}. Impact — проскальзывание на {fmt_usd(s.size)}, %, "
                   f"глубина — меньшая из сторон в ±{s.band:g}%, — — пары нет на CORE.\n"
                   "Только одна биржа: /status_rh · /status_core")
        return "\n".join(out)

    @staticmethod
    def _row(tag: str, met: Metrics, w: int) -> str:
        imp = "∞" if math.isinf(met.impact) else f"{met.impact:.3f}"
        return f"{tag:<{w}}{met.spread_pct:>8.4f}{met.spread_ticks:>4g}{imp:>7}{fmt_usd(met.depth):>6}"

    def welcome_text(self, u: User) -> str:
        s = u.s
        return ("👋 <b>Привет!</b> Я слежу за стаканами всех акций и pre-IPO на Lighter Robinhood "
                "и пишу, когда по паре можно зайти почти без потерь на спреде.\n\n"
                "Каждую секунду проверяю у каждой пары:\n"
                f"▫️ спред ≤ {s.spread:g}%\n"
                f"▫️ рыночная заявка на {fmt_usd(s.size)} сдвинет цену не больше чем на {s.impact:g}%\n"
                f"▫️ в ±{s.band:g}% от цены стоит минимум {fmt_usd(s.depth)} на покупку и на продажу\n\n"
                f"Если всё держится {s.hold:g} с — пришлю 🟢 с цифрами, когда уйдёт — 🔴.\n\n"
                "Командой /core можно добавить сравнение с основным Lighter: в алертах и /status будет видно, "
                "что с той же акцией там.\n\n"
                "Настрой под себя: /help\n\n@ogcrypter")

    def help_text(self, chat_id) -> str:
        sets = "\n".join(f"<code>{k}</code> — {v}" for k, v in SETTING_HELP.items())
        return ("<b>Настройки</b> — командой <code>/set</code>, например:\n"
                "<code>/set spread 0.005</code>\n<code>/set size 10000</code>\n\n"
                f"{sets}\n\n"
                "Можно несколько команд одним сообщением, каждую с новой строки.\n\n"
                "<b>Команды</b>\n"
                "/status — топ пар по спреду прямо сейчас\n"
                "/settings — твои текущие фильтры\n"
                "/pairs — пары в мониторинге\n"
                "/pause · /resume — выключить и включить алерты\n"
                "/core — добавить или убрать сравнение с основным Lighter (Core)\n"
                "/reset — вернуть настройки по умолчанию\n"
                "/help — эта справка")

    def stats_text(self) -> str:
        active = sum(1 for u in self.users.values() if not u.paused)
        day = sum(1 for u in self.users.values() if time.time() - u.joined < 86400)
        return (f"👥 Пользователей: {len(self.users)} (активных {active}, новых за сутки {day})\n"
                f"Пар в мониторинге: {len(self.markets)}\n"
                f"Аптайм этого запуска: {(time.time() - self.started) / 3600:.1f} ч")

    async def handle_command(self, chat_id: str, text: str) -> None:
        chat_id = str(chat_id)
        cmd, *args = text.strip().split()
        cmd = cmd.split("@")[0].lower()
        u = self.users.get(chat_id)
        if u is None:
            if not (ALLOW_PUBLIC or is_owner(chat_id)):
                await self.send("Это приватный бот.", [chat_id])
                return
            if len(self.users) >= MAX_USERS:
                await self.send("Бот сейчас переполнен, попробуй позже.", [chat_id])
                return
            u = self.users[chat_id] = User(chat_id)
            log.info("Новый пользователь %s (всего %d)", chat_id, len(self.users))
            await self.send(self.welcome_text(u), [chat_id])
            if cmd == "/start":
                return
        if cmd == "/start":
            reply = self.welcome_text(u)
        elif cmd == "/help":
            reply = self.help_text(chat_id)
        elif cmd in ("/status", "/status_rh", "/status_core", "/status_text"):
            only = {"/status_rh": "rh", "/status_core": "core"}.get(cmd)
            as_text = cmd == "/status_text"
            for a in args:
                if a.lower() in ("rh", "core"):
                    only = a.lower()
                if a.lower() in ("text", "текст"):
                    as_text = True
            if only == "core" and "core" not in EXCHANGES:
                only = "rh"
            num = next((int(a) for a in args if a.isdigit()), 15)
            if not as_text:
                try:
                    pic = self.status_image(u, num, only)
                    if pic and await self.send_photo(chat_id, pic[0], pic[1]):
                        return
                except Exception:  # noqa: BLE001
                    log.exception("Не удалось нарисовать /status — шлю текстом")
            reply = self.status_text(u, min(num, 50), only)
        elif cmd == "/settings":
            reply = self.settings_text(u)
            if note := self.unreachable_note(u.s):
                reply += "\n\n" + note
        elif cmd == "/pairs":
            reply = self.pairs_text()
        elif cmd == "/pause":
            u.paused = True
            reply = "⏸ Алерты на паузе. /resume — включить."
        elif cmd == "/resume":
            u.paused = False
            reply = "▶️ Алерты включены."
        elif cmd == "/core":
            if "core" not in EXCHANGES:
                reply = "Мониторинг Core сейчас выключен на уровне бота."
            else:
                u.s.venue = "rh" if u.s.venue == "both" else "both"
                for st in u.pairs.values():
                    st.ok_since = None
                reply = ("➕ Core добавлен. Алерты по-прежнему по Lighter RH, но в каждом видна та же акция "
                         "на основном Lighter, а если пара хороша на обеих биржах — придёт 🟢🟢. "
                         "В /status — обе биржи. /core — убрать."
                         if u.s.venue == "both" else
                         "➖ Core убран: снова только Lighter RH. /core — вернуть.")
        elif cmd == "/reset":
            u.s = Settings()
            u.pairs.clear()
            reply = "♻️ Настройки сброшены.\n\n" + self.settings_text(u)
        elif cmd == "/set":
            reply = self.apply_set(u, args)
        elif cmd == "/stats" and is_owner(chat_id):
            reply = self.stats_text()
        else:
            reply = "Не знаю такую команду. /help"
        await self.send(reply, [chat_id])

    def apply_set(self, u: User, args: list[str]) -> str:
        if len(args) != 2 or args[0].lower() not in SETTING_HELP:
            return "Формат: <code>/set spread 0.005</code>\n\n" + "\n".join(
                f"<code>{k}</code> — {v}" for k, v in SETTING_HELP.items())
        key, raw = args[0].lower(), args[1].replace(",", ".").replace("%", "").replace("$", "")
        try:
            if key == "notify_lost":
                val = raw.lower() in ("on", "1", "true", "yes", "да", "вкл")
            else:
                val = float(raw)
                if val < 0 or math.isinf(val) or math.isnan(val):
                    raise ValueError
                if key == "size" and not 10 <= val <= 10_000_000:
                    return "size — от $10 до $10M."
                if key == "band" and val > 5:
                    return "band — не больше 5%."
        except ValueError:
            return "Не понял значение."
        setattr(u.s, key, val)
        for st in u.pairs.values():  # переоценить с нуля по новым порогам
            st.ok_since = None
        return f"✅ {key} = {args[1]}\n\n" + self.settings_text(u)

    async def tg_loop(self) -> None:
        if not TG_TOKEN:
            log.warning("TG_TOKEN не задан — Telegram отключён, алерты пишутся только в лог")
            return
        await self.tg("setMyCommands", commands=[
            {"command": "status", "description": "Топ пар по спреду сейчас"},
            {"command": "settings", "description": "Мои фильтры"},
            {"command": "help", "description": "Как настроить (/set)"},
            {"command": "pairs", "description": "Пары в мониторинге"},
            {"command": "pause", "description": "Выключить алерты"},
            {"command": "resume", "description": "Включить алерты"},
            {"command": "core", "description": "Сравнение с основным Lighter вкл/выкл"},
            {"command": "reset", "description": "Сбросить настройки"},
        ])
        while True:
            try:
                data = await self.tg("getUpdates", offset=self.tg_offset, timeout=30, allowed_updates=["message"])
                for upd in (data or {}).get("result", []):
                    self.tg_offset = upd["update_id"] + 1
                    msg = upd.get("message") or {}
                    text = msg.get("text") or ""
                    if "chat" not in msg:
                        continue
                    # можно несколько команд в одном сообщении — каждая с новой строки
                    for line in text.splitlines()[:15]:
                        if line.strip().startswith("/"):
                            await self.handle_command(str(msg["chat"]["id"]), line.strip())
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("Telegram polling: %s", e)
                await asyncio.sleep(5)

    async def autosave_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            self.save_state()

    async def run(self) -> None:
        async with aiohttp.ClientSession() as self.http:
            loops = asyncio.gather(self.markets_loop(), self.eval_loop(), self.tg_loop(), self.autosave_loop())
            try:
                if MAX_RUNTIME_MIN > 0:
                    try:
                        await asyncio.wait_for(loops, timeout=MAX_RUNTIME_MIN * 60)
                    except asyncio.TimeoutError:
                        log.info("Время работы (%g мин) истекло — плановое завершение", MAX_RUNTIME_MIN)
                else:
                    await loops
            finally:
                for t in self.feeds:
                    t.cancel()
                self.save_state()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    if not TG_CHAT_IDS:
        log.warning("TG_CHAT_ID (владелец) не задан — служебные сообщения будут только в логе")
    try:
        asyncio.run(App().run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
