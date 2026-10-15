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
    tag: str                 # "RH" / "CORE"
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


def render_status(title: str, groups: list[Group], *, spread: float, impact: float, depth: float,
                  size: float, band: float) -> bytes:
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
    d.text((PAD * s, 66 * s), f"Lighter · обновлено {msk:%H:%M:%S} МСК", font=f_sub, fill=MUTED)

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
    heads = {"ticker": "Тикер", "tag": "Биржа", "spread": "Спред, %", "ticks": "Тики",
             "impact": "Проскальзывание, %", "depth": "Глубина", "ok": "Фильтр"}

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
            tc = TAG_RH if c.tag == "RH" else TAG_CORE
            tw = d.textlength(c.tag, font=f_tag)
            bx, by = cols["tag"][0] * s, (ry + 10) * s
            d.rounded_rectangle([bx, by, bx + tw + 16 * s, by + 19 * s], radius=9 * s, outline=tc, width=s * 1)
            d.text((bx + 8 * s, by + 3 * s), c.tag, font=f_tag, fill=tc)
            if c.spread is None:
                put("spread", ty, "—", f_num, MUTED)
                continue
            put("spread", ty, f"{c.spread:.4f}", f_num, GREEN if c.spread <= spread + 1e-12 else RED)
            put("ticks", ty, f"{c.ticks:g}", f_num, MUTED)
            imp_ok = c.impact is not None and c.impact <= impact + 1e-12
            put("impact", ty, "∞" if c.impact is None or math.isinf(c.impact) else f"{c.impact:.3f}",
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
    foot = (f"Фильтры: спред ≤ {spread:g}%  ·  проскальзывание ≤ {impact:g}% на {_usd(size)}  ·  "
            f"глубина ≥ {_usd(depth)} в ±{band:g}%")
    d.text((PAD * s, (H - foot_h + 26) * s), foot, font=f_sub, fill=MUTED)
    d.text((PAD * s, (H - foot_h + 48) * s), "зелёный — проходит фильтр, красный — нет", font=f_sub, fill=MUTED)

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()
