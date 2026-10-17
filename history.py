"""История спреда: бот сам записывает спред каждой пары (Lighter не хранит историю стакана).

Каждые несколько секунд берётся снимок спреда, снимки складываются в 5-минутные корзины:
[начало, число снимков, мин %, сумма %, макс %, сумма mid, шаг цены, [число снимков со спредом 1, 2 … 10, >10 тиков]].
По тикам потом точно считается, сколько времени спред был не шире порога пользователя.
Хранится KEEP_DAYS дней, файл сохраняется между перезапусками вместе с остальным состоянием.
"""
from __future__ import annotations

import json
import math
import time
from pathlib import Path

BUCKET = 300
COARSE = 1800          # старше суток — корзины по 30 минут, чтобы файл не разрастался
KEEP_DAYS = 7
MAX_TICKS = 10
MSK = 3 * 3600


class History:
    def __init__(self, path: Path):
        self.path = path
        self.data: dict[str, list[list]] = {}     # "rh:NVDA" → корзины
        self.since: float = time.time()           # с какого момента копим
        self.load()

    # ---------- запись ----------
    def add(self, key: str, t: float, spread_pct: float, ticks: float, mid: float, tick: float) -> None:
        start = int(t // BUCKET * BUCKET)
        lst = self.data.setdefault(key, [])
        if not lst or lst[-1][0] != start:
            lst.append([start, 0, spread_pct, 0.0, spread_pct, 0.0, tick, [0] * (MAX_TICKS + 1)])
        b = lst[-1]
        b[1] += 1
        b[2] = min(b[2], spread_pct)
        b[3] += spread_pct
        b[4] = max(b[4], spread_pct)
        b[5] += mid
        b[6] = tick
        i = min(max(int(round(ticks)), 1), MAX_TICKS + 1) - 1
        b[7][i] += 1

    def prune(self, now: float | None = None) -> None:
        now = now or time.time()
        cut, fine = now - KEEP_DAYS * 86400, now - 86400
        for k in list(self.data):
            out: list[list] = []
            for b in self.data[k]:
                if b[0] < cut:
                    continue
                if b[0] < fine:                              # старое — сливаем в 30-минутные корзины
                    start = int(b[0] // COARSE * COARSE)
                    if out and out[-1][0] == start and width(out[-1]) == COARSE:
                        _merge(out[-1], b)
                        continue
                    if width(b) != COARSE:
                        b = [start, b[1], b[2], b[3], b[4], b[5], b[6], list(b[7]), COARSE]
                out.append(b)
            if out:
                self.data[k] = out
            else:
                del self.data[k]

    # ---------- файл ----------
    def load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.data = raw.get("data") or {}
            self.since = float(raw.get("since") or self.since)
        except FileNotFoundError:
            pass
        except Exception:  # noqa: BLE001 — испорченный файл: начинаем заново
            self.data = {}
        self.prune()

    def save(self) -> None:
        self.prune()
        out = {k: [[b[0], b[1], round(b[2], 6), round(b[3], 5), round(b[4], 6), round(b[5], 4), b[6], b[7]]
                   + ([COARSE] if width(b) == COARSE else []) for b in v] for k, v in self.data.items()}
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"since": self.since, "data": out}, separators=(",", ":")), encoding="utf-8")
        tmp.replace(self.path)

    # ---------- чтение ----------
    def buckets(self, key: str, seconds: float, now: float | None = None) -> list[list]:
        cut = (now or time.time()) - seconds
        return [b for b in self.data.get(key, []) if b[0] + width(b) > cut and b[1] > 0]


def width(b: list) -> int:
    return b[8] if len(b) > 8 else BUCKET


def _merge(a: list, b: list) -> None:
    a[1] += b[1]
    a[2] = min(a[2], b[2])
    a[3] += b[3]
    a[4] = max(a[4], b[4])
    a[5] += b[5]
    a[6] = b[6]
    a[7] = [x + y for x, y in zip(a[7], b[7])]


def points(buckets: list[list]) -> list[tuple[float, float, float, float]]:
    """(время середины корзины, средний, мин, макс спред %)"""
    return [(b[0] + width(b) / 2, b[3] / b[1], b[2], b[4]) for b in buckets]


def stats(buckets: list[list], threshold: float) -> dict | None:
    n = sum(b[1] for b in buckets)
    if not n:
        return None
    below = 0.0
    for b in buckets:
        mid = b[5] / b[1]
        if b[6] <= 0 or mid <= 0:
            continue
        thr_ticks = threshold / 100 * mid / b[6] + 1e-9
        below += sum(c for i, c in enumerate(b[7][:MAX_TICKS]) if i + 1 <= thr_ticks)
        if thr_ticks > MAX_TICKS and b[4] <= threshold + 1e-12:
            below += b[7][MAX_TICKS]
    # средний спред по часам МСК → самые узкие часы
    by_hour: dict[int, list[float]] = {}
    for b in buckets:
        h = int((b[0] + MSK) // 3600 % 24)
        acc = by_hour.setdefault(h, [0.0, 0])
        acc[0] += b[3]
        acc[1] += b[1]
    hours = sorted(((acc[0] / acc[1], h) for h, acc in by_hour.items() if acc[1]), key=lambda x: x[0])
    return {
        "avg": sum(b[3] for b in buckets) / n,
        "min": min(b[2] for b in buckets),
        "max": max(b[4] for b in buckets),
        "below": below / n * 100,
        "hours": hours,                                       # [(средний спред, час МСК)] от узкого к широкому
        "span": buckets[-1][0] + width(buckets[-1]) - buckets[0][0],      # сколько секунд покрывают данные
        "median_ticks": _median_ticks(buckets),
    }


def _median_ticks(buckets: list[list]) -> float:
    cnt = [0] * (MAX_TICKS + 1)
    for b in buckets:
        for i, c in enumerate(b[7]):
            cnt[i] += c
    total, acc = sum(cnt), 0
    for i, c in enumerate(cnt):
        acc += c
        if acc * 2 >= total:
            return i + 1 if i < MAX_TICKS else math.inf
    return math.inf
