"""Рисует таблицу /status картинкой (PNG). Используется ботом; без Pillow бот шлёт текст."""
from __future__ import annotations

import io
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

FONT_DIR = Path(__file__).resolve().parent / "fonts"

BG = (11, 16, 32)
CARD = (17, 24, 44)
STRIPE = (21, 29, 53)
LINE = (38, 48, 78)
TEXT = (232, 236, 246)
MUTED = (134, 145, 175)
GREEN = (34, 197, 94)
GREEN_BG = (20, 58, 44)
RED = (248, 113, 113)
TAG_RH = (56, 189, 248)
TAG_CORE = (167, 139, 250)
TAG_ENTROPY = (251, 146, 60)

W = 940
PAD = 28
ROW_H = 38
SCALE = 2          # рисуем в 2x для чёткости, Telegram сам ужмёт до 1280 по большей стороне


def _font(name: str, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(str(FONT_DIR / name), size * SCALE)
    except OSError:
        return ImageFont.load_default(size * SCALE)


@dataclass
class Cell:
    tag: str                 # "RH" / "CORE" / "ENTROPY"
    spread: float | None     # %
    ticks: float | None
    impact: float | None     # %
    depth: float | None      # $
    ok: bool = False


@dataclass
class Group:
    ticker: str
    cells: list[Cell]


def _usd(x: float) -> str:
    if x is None:
        return "—"
    if math.isinf(x):
        return "∞"
    if x >= 1e6:
        return f"${x / 1e6:.1f}M"
    if x >= 1e3:
        return f"${x / 1e3:.0f}k"
    return f"${x:.0f}"


def _u(x: float, bps: bool, nd: int) -> str:
    """Спред/проскальзывание: в процентах или в базисных пунктах (1 bps = 0.01%)."""
    return f"{x * 100:.{max(nd - 2, 1)}f}" if bps else f"{x:.{nd}f}"


def _thr(x: float, bps: bool) -> str:
    return f"{x * 100:g} bps" if bps else f"{x:g}%"


def render_status(title: str, groups: list[Group], *, spread: float, impact: float, depth: float,
                  size: float, band: float, bps: bool = False) -> bytes:
    s = SCALE
    f_title = _font("Inter-Bold.otf", 26)
    f_sub = _font("Inter-Regular.otf", 15)
    f_head = _font("Inter-SemiBold.otf", 14)
    f_tick = _font("Inter-Bold.otf", 19)
    f_num = _font("Inter-Regular.otf", 17)
    f_tag = _font("Inter-Bold.otf", 12)

    rows = sum(len(g.cells) for g in groups)
    head_h = 112
    table_top = head_h + 44
    foot_h = 70
    H = table_top + rows * ROW_H + (len(groups) - 1) * 6 + foot_h

    img = Image.new("RGB", (W * s, H * s), BG)
    d = ImageDraw.Draw(img)

    # заголовок
    msk = datetime.now(timezone.utc) + timedelta(hours=3)
    d.text((PAD * s, 26 * s), title, font=f_title, fill=TEXT)
    d.text((PAD * s, 66 * s), f"{'Lighter · Entropy' if any(c.tag == 'ENTROPY' for g in groups for c in g.cells) else 'Lighter'} · обновлено {msk:%H:%M:%S} МСК", font=f_sub, fill=MUTED)

    # колонки: (заголовок, правая граница x, выравнивание)
    cols = {
        "ticker": (PAD + 8, "l"),
        "tag": (232, "l"),
        "spread": (430, "r"),
        "ticks": (500, "r"),
        "impact": (680, "r"),
        "depth": (790, "r"),
        "ok": (W - PAD - 40, "c"),
    }
    unit = "bps" if bps else "%"
    heads = {"ticker": "Тикер", "tag": "Биржа", "spread": f"Спред, {unit}", "ticks": "Тики",
             "impact": f"Проскальзывание, {unit}", "depth": "Глубина", "ok": "Фильтр"}

    def put(col: str, y: int, txt: str, font, fill):
        x, al = cols[col]
        w = d.textlength(txt, font=font)
        if al == "r":
            xx = x * s - w
        elif al == "c":
            xx = x * s - w / 2
        else:
            xx = x * s
        d.text((xx, y), txt, font=font, fill=fill)

    # карточка таблицы
    d.rounded_rectangle([PAD * s, (head_h) * s, (W - PAD) * s, (H - foot_h + 8) * s], radius=16 * s, fill=CARD)
    for col, txt in heads.items():
        put(col, (head_h + 14) * s, txt, f_head, MUTED)
    d.line([(PAD * s, (table_top - 2) * s), ((W - PAD) * s, (table_top - 2) * s)], fill=LINE, width=s)

    y = table_top
    for gi, g in enumerate(groups):
        gh = len(g.cells) * ROW_H
        if gi % 2 == 1:
            d.rectangle([(PAD + 1) * s, y * s, (W - PAD - 1) * s, (y + gh) * s], fill=STRIPE)
        for ci, c in enumerate(g.cells):
            ry = y + ci * ROW_H
            if c.ok:
                d.rectangle([(PAD + 1) * s, ry * s, (W - PAD - 1) * s, (ry + ROW_H) * s], fill=GREEN_BG)
            ty = (ry + 9) * s
            if ci == 0:
                put("ticker", (ry + 8) * s, g.ticker, f_tick, TEXT)
            # бейдж биржи
            tc = {"RH": TAG_RH, "CORE": TAG_CORE, "ENTROPY": TAG_ENTROPY}.get(c.tag, TAG_CORE)
            tw = d.textlength(c.tag, font=f_tag)
            bx, by = cols["tag"][0] * s, (ry + 10) * s
            d.rounded_rectangle([bx, by, bx + tw + 16 * s, by + 19 * s], radius=9 * s, outline=tc, width=s * 1)
            d.text((bx + 8 * s, by + 3 * s), c.tag, font=f_tag, fill=tc)
            if c.spread is None:
                put("spread", ty, "—", f_num, MUTED)
                continue
            put("spread", ty, _u(c.spread, bps, 4), f_num, GREEN if c.spread <= spread + 1e-12 else RED)
            put("ticks", ty, f"{c.ticks:g}", f_num, MUTED)
            imp_ok = c.impact is not None and c.impact <= impact + 1e-12
            put("impact", ty, "∞" if c.impact is None or math.isinf(c.impact) else _u(c.impact, bps, 3),
                f_num, GREEN if imp_ok else RED)
            put("depth", ty, _usd(c.depth), f_num, GREEN if (c.depth or 0) >= depth else RED)
            if c.ok:  # галочка
                cx, cy = cols["ok"][0] * s, (ry + ROW_H / 2) * s
                r = 10 * s
                d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=GREEN)
                d.line([(cx - 5 * s, cy), (cx - 1 * s, cy + 4 * s), (cx + 6 * s, cy - 4 * s)],
                       fill=BG, width=3 * s, joint="curve")
        y += gh + 6
        if gi < len(groups) - 1:
            d.line([((PAD + 12) * s, (y - 3) * s), ((W - PAD - 12) * s, (y - 3) * s)], fill=LINE, width=s)

    # подвал с фильтрами
    foot = (f"Фильтры: спред ≤ {_thr(spread, bps)}  ·  проскальзывание ≤ {_thr(impact, bps)} на {_usd(size)}  ·  "
            f"глубина ≥ {_usd(depth)} в ±{band:g}%")
    d.text((PAD * s, (H - foot_h + 26) * s), foot, font=f_sub, fill=MUTED)
    d.text((PAD * s, (H - foot_h + 48) * s), "зелёный — проходит фильтр, красный — нет", font=f_sub, fill=MUTED)

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


# ───────────────────────────── /history: график спреда ─────────────────────────────

@dataclass
class Series:
    tag: str                                        # "RH" / "CORE" / "ENTROPY"
    points: list[tuple[float, float, float, float]]  # (unix-время, средний, мин, макс спред %)
    lines: list[str]                                # строки статистики под графиком


MSK = 3 * 3600
TAG_COLORS = {"RH": TAG_RH, "CORE": TAG_CORE, "ENTROPY": TAG_ENTROPY}


def _blend(c, bg, a):
    return tuple(int(bg[i] + (c[i] - bg[i]) * a) for i in range(3))


def render_history(title: str, subtitle: str, series: list[Series], *, threshold: float,
                   t0: float, t1: float, bps: bool = False) -> bytes:
    if bps:                                   # всё в базисных пунктах: значения × 100
        series = [Series(x.tag, [(t, a * 100, b * 100, c * 100) for t, a, b, c in x.points], x.lines) for x in series]
        threshold *= 100
    s = SCALE
    f_title = _font("Inter-Bold.otf", 26)
    f_sub = _font("Inter-Regular.otf", 15)
    f_axis = _font("Inter-Regular.otf", 13)
    f_tag = _font("Inter-Bold.otf", 12)
    f_line = _font("Inter-Regular.otf", 15)

    head_h, chart_h = 112, 330
    text_h = sum(30 + 24 * len(x.lines) for x in series) + 16
    H = head_h + chart_h + 60 + text_h + 40
    img = Image.new("RGB", (W * s, H * s), BG)
    d = ImageDraw.Draw(img)
    d.text((PAD * s, 26 * s), title, font=f_title, fill=TEXT)
    d.text((PAD * s, 66 * s), subtitle, font=f_sub, fill=MUTED)

    # область графика
    left, right = PAD + 64, W - PAD - 16
    top, bottom = head_h + 16, head_h + chart_h
    d.rounded_rectangle([PAD * s, head_h * s, (W - PAD) * s, (bottom + 46) * s], radius=16 * s, fill=CARD)

    avgs = sorted(p[1] for x in series for p in x.points) or [threshold]
    p98 = avgs[min(len(avgs) - 1, int(len(avgs) * 0.98))]
    ymax = max(threshold * 1.6, p98 * 1.25, 1e-6)

    def X(t):
        return (left + (t - t0) / max(t1 - t0, 1) * (right - left)) * s

    def Y(v):
        return (bottom - min(v, ymax) / ymax * (bottom - top)) * s

    # сетка и ось Y
    for i in range(5):
        v = ymax * i / 4
        y = Y(v)
        d.line([(left * s, y), (right * s, y)], fill=LINE, width=s)
        lab = (f"{v:.2f}" if ymax < 10 else f"{v:.1f}") if bps else (f"{v:.3f}" if ymax < 0.1 else f"{v:.2f}")
        d.text(((left - 8) * s - d.textlength(lab, font=f_axis), y - 9 * s), lab, font=f_axis, fill=MUTED)
    # ось X: подписи по МСК
    span = t1 - t0
    step = 3600 * (3 if span <= 26 * 3600 else 24 if span <= 8 * 86400 else 48)
    if span <= 7 * 3600:
        step = 3600
    if span <= 2.5 * 3600:
        step = 900
    t = (int(t0 + MSK) // step + 1) * step - MSK
    while t < t1:
        x = X(t)
        d.line([(x, top * s), (x, bottom * s)], fill=_blend(LINE, CARD, 0.6), width=s)
        msk = datetime.fromtimestamp(t, timezone.utc) + timedelta(hours=3)
        lab = f"{msk:%H:%M}" if step < 86400 else f"{msk:%d.%m}"
        d.text((x - d.textlength(lab, font=f_axis) / 2, (bottom + 10) * s), lab, font=f_axis, fill=MUTED)
        t += step
    d.text(((right) * s - d.textlength("МСК", font=f_axis), (bottom + 26) * s), "МСК", font=f_axis, fill=MUTED)

    # линии: коридор мин–макс и средний спред
    for x_ in reversed(series):          # RH рисуем последним — поверх
        col = TAG_COLORS.get(x_.tag, TAG_CORE)
        pts = sorted(x_.points)
        diffs = sorted(b[0] - a[0] for a, b in zip(pts, pts[1:])) or [300]
        gap = max(181, 2.5 * diffs[len(diffs) // 2])
        seg: list[tuple] = []
        segments = []
        for p in pts:
            if seg and p[0] - seg[-1][0] > gap:
                segments.append(seg)
                seg = []
            seg.append(p)
        if seg:
            segments.append(seg)
        for sg in segments:
            if len(sg) >= 2:
                d.line([(X(p[0]), Y(p[1])) for p in sg], fill=col, width=s if len(pts) > 500 else s * 2,
                       joint="curve")
            else:
                p = sg[0]
                d.ellipse([X(p[0]) - 3 * s, Y(p[1]) - 3 * s, X(p[0]) + 3 * s, Y(p[1]) + 3 * s], fill=col)

    # порог пользователя — пунктир
    yt = Y(threshold)
    x = left * s
    while x < right * s:
        d.line([(x, yt), (min(x + 8 * s, right * s), yt)], fill=GREEN, width=s * 2)
        x += 14 * s
    lab = f"порог {threshold:g} bps" if bps else f"порог {threshold:g}%"
    d.text(((left + 6) * s, yt - 20 * s), lab, font=f_axis, fill=GREEN)

    # статистика под графиком
    y = bottom + 46 + 22
    for x_ in series:
        col = TAG_COLORS.get(x_.tag, TAG_CORE)
        tw = d.textlength(x_.tag, font=f_tag)
        d.rounded_rectangle([PAD * s, y * s, PAD * s + tw + 16 * s, (y + 19) * s], radius=9 * s, outline=col, width=s)
        d.text((PAD * s + 8 * s, (y + 3) * s), x_.tag, font=f_tag, fill=col)
        y += 30
        for ln in x_.lines:
            d.text((PAD * s, y * s), ln, font=f_line, fill=TEXT)
            y += 24
        y += 8
    d.text((PAD * s, (H - 34) * s), "Линия — средний спред " +
           ("за каждую минуту" if span <= 6 * 3600 * 1.05 else "за 5 минут" if span <= 86400 * 1.05
            else "за 15 минут" if span <= 2 * 86400 * 1.05 else "за 30 минут"), font=f_axis, fill=MUTED)

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()
