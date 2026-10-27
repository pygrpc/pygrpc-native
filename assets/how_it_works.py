"""Draws the README's animation, how-it-works-{light,dark}.svg.

Both sides serve the same four clients, each sending its next call as soon as
the last one is answered, as benchmarks/echo.py does. The packet paths loop
seamlessly and the counters keep rising, so the animation has no visible
restart. The timings are illustrative, not measured.

    python assets/how_it_works.py
"""

import os
from typing import Any

W, H = 1200, 600
EASE = "cubic-bezier(.45,0,.2,1)"
SANS = "-apple-system,BlinkMacSystemFont,'Segoe UI','Helvetica Neue',Arial,sans-serif"
MONO = "ui-monospace,SFMono-Regular,'SF Mono',Menlo,Consolas,monospace"

THEMES = {
    "light": dict(
        text="#1f2328", muted="#656d76", faint="#8c959f",
        card="#ffffff", card2="#f6f8fa", border="#d8dee4", shadow="#1f2328",
        shadow_op=0.08,
        py="#3d6fc8", py_fill="#eef3fc", py_fill2="#e3ebfa", py_border="#b9cbee",
        rs="#e8742a", rs_fill="#fff4ec", rs_fill2="#fde9db", rs_border="#f5c6a5",
        core="#656d76", core_fill="#f6f8fa", core_fill2="#eff2f5", core_border="#d0d7de",
        packet="#3d6fc8", done="#1a7f37", lane="#f2c4a3", lane_on="#e8742a",
        chip="#ffffff",
    ),
    "dark": dict(
        text="#e6edf3", muted="#9198a1", faint="#6e7681",
        card="#161b22", card2="#13181f", border="#30363d", shadow="#000000",
        shadow_op=0.5,
        py="#79a2f0", py_fill="#15213a", py_fill2="#192a4a", py_border="#2c4778",
        rs="#f58a3c", rs_fill="#2a1b10", rs_fill2="#352113", rs_border="#6b3a1c",
        core="#9198a1", core_fill="#1c2128", core_fill2="#1a1f26", core_border="#3d444d",
        packet="#8fb2f5", done="#3fb950", lane="#5a321a", lane_on="#f58a3c",
        chip="#0d1117",
    ),
}  # fmt: skip

CLIENTS = 4
SERVICE = 1.6  # Python time per call on the left
LEFT_CYCLE = CLIENTS * SERVICE
RIGHT_CYCLE = 3.2  # one round trip on the right, so twice the calls
RIGHT_PHASE = 2.0  # start the right side mid-flight, as the left is

LEFT_X, RIGHT_X, PANEL_W = 20, 610, 570
SOURCE_Y = 128
DONE_Y = 548
DONE_X = {LEFT_X: LEFT_X + PANEL_W / 2, RIGHT_X: RIGHT_X + PANEL_W / 2}
RIGHT_LANES = [RIGHT_X + 120 + 52 * k for k in range(4)]
DIGITS = 5
DIGIT_W = 16.5

Theme = dict[str, Any]


def pct(t: float, period: float) -> float:
    return round(100 * t / period, 4)


class Track:
    """A packet's loop of (t, x, y, opacity, color), ending where it started."""

    def __init__(self, period: float) -> None:
        self.period = period
        self.points: list[tuple[float, float, float, float, str]] = []

    def add(self, t: float, x: float, y: float, color: str, opacity: float = 1) -> None:
        self.points.append((t, x, y, opacity, color))

    def hold(self, t: float) -> None:
        self.points.append((t,) + self.points[-1][1:])

    def css(self, name: str, delay: float) -> str:
        rows, last = [], -1.0
        for t, x, y, o, col in self.points + [(self.period,) + self.points[0][1:]]:
            p = max(pct(t, self.period), round(last + 0.01, 4))
            last = p
            rows.append(
                f"{p}%{{transform:translate({x}px,{y}px);opacity:{o};fill:{col};"
                f"animation-timing-function:{EASE}}}"
            )
        return (
            f"@keyframes {name}{{{''.join(rows)}}}"
            f".{name}{{animation:{name} {self.period}s {-delay}s infinite}}"
        )


def left_tracks(c: Theme) -> list[tuple[Track, float]]:
    """One Python thread takes completion queue events one at a time."""
    lane_x = LEFT_X + 150
    core_y, queue_y, frame_y, handler_y = 190, 240, 337, 449
    blue, green = c["packet"], c["done"]

    def queue_x(k: int) -> float:
        return lane_x + 30 + 24 * k

    out = []
    for j in range(CLIENTS):
        # Time 0 of the cycle is when this client's call reaches Python.
        tr = Track(LEFT_CYCLE)
        tr.add(0, queue_x(0), queue_y, blue)
        tr.add(0.3, lane_x, frame_y, blue)
        tr.hold(0.8)
        tr.add(1.05, lane_x, handler_y, blue)
        tr.hold(1.45)
        tr.add(1.85, DONE_X[LEFT_X], DONE_Y, green)
        tr.hold(2.0)
        tr.add(2.3, DONE_X[LEFT_X], DONE_Y + 28, green, 0)
        tr.add(2.32, LEFT_X + PANEL_W / 2, SOURCE_Y, blue, 0)
        tr.add(2.4, LEFT_X + PANEL_W / 2, SOURCE_Y, blue)
        tr.add(2.6, lane_x + 150 + 26 * j, core_y, blue)
        tr.hold(2.75)
        tr.add(3.0, queue_x(2), queue_y, blue)  # two calls are still ahead
        tr.hold(2 * SERVICE)
        tr.add(2 * SERVICE + 0.3, queue_x(1), queue_y, blue)
        tr.hold(3 * SERVICE)
        tr.add(3 * SERVICE + 0.3, queue_x(0), queue_y, blue)
        # Client j's call reaches Python at j * SERVICE.
        out.append((tr, (CLIENTS - j) % CLIENTS * SERVICE))
    return out


def right_tracks(c: Theme) -> list[tuple[Track, float]]:
    """tokio handles the calls side by side; Python gets them as one batch."""
    lane_top, lane_bottom, stage_y, handler_y = 236, 318, 360, 452
    source_x = RIGHT_X + PANEL_W / 2
    blue, green = c["packet"], c["done"]
    batch = 1.55
    out = []
    for k in range(CLIENTS):
        tr = Track(RIGHT_CYCLE)
        a = 0.09 * k
        tr.add(0, source_x, SOURCE_Y, blue, 0)
        tr.add(a, source_x, SOURCE_Y, blue, 0)
        tr.add(a + 0.08, source_x, SOURCE_Y, blue)
        tr.add(a + 0.45, RIGHT_LANES[k], lane_top, blue)
        tr.add(a + 1.0, RIGHT_LANES[k], lane_bottom, blue)
        stage_x = RIGHT_X + 161 + 26 * k
        tr.add(a + 1.25, stage_x, stage_y, blue)
        tr.hold(batch)
        tr.add(batch + 0.35, stage_x, handler_y, blue)
        tr.hold(batch + 0.55 + 0.12 * k)
        tr.add(batch + 0.9 + 0.12 * k, DONE_X[RIGHT_X], DONE_Y, green)
        tr.hold(batch + 0.97 + 0.12 * k)
        tr.add(batch + 1.25 + 0.12 * k, DONE_X[RIGHT_X], DONE_Y + 28, green, 0)
        out.append((tr, RIGHT_PHASE))
    return out


def done_times(
    tracks: list[tuple[Track, float]], green: str
) -> tuple[float, list[float]]:
    """When in each cycle the packets turn green, counted from page load."""
    period = tracks[0][0].period
    out = []
    for tr, delay in tracks:
        t = next(p[0] for p in tr.points if p[4] == green)
        out.append((t - delay) % period)
    return period, sorted(out)


def counter(
    prefix: str, unit: float, offsets: list[float], x: float, y: float, c: Theme
) -> tuple[list[str], list[str]]:
    """An odometer: digit k cycles every 10^(k+1) completions, so the count
    keeps rising for hours instead of resetting."""
    per = len(offsets)
    css, parts = [], []

    def time_of(n: int) -> float:  # time of the n-th completion, n >= 1
        u, i = divmod(n - 1, per)
        return u * unit + offsets[i]

    for k in range(DIGITS):
        step = 10**k
        units = 1
        while (per * units) % (10 * step):
            units += 1
        period = units * unit
        changes = [
            (time_of(n), (n // step) % 10) for n in range(step, per * units + 1, step)
        ]
        group = []
        for v in range(10):
            on = [0.0] if v == 0 else []
            off: list[float] = []
            for t, d in changes:
                if d == v:
                    on.append(t)
                elif len(off) < len(on):
                    off.append(t)
            if len(off) < len(on):
                off.append(period)  # back to 0 at the end of the period
            rows = [f"0%{{opacity:{1 if v == 0 else 0}}}"]
            for a, b in zip(on, off):
                if a > 0:
                    rows.append(f"{pct(a, period)}%{{opacity:0}}")
                    rows.append(f"{pct(a + 0.06, period)}%{{opacity:1}}")
                if b < period:
                    rows.append(f"{pct(b, period)}%{{opacity:1}}")
                    rows.append(f"{pct(b + 0.06, period)}%{{opacity:0}}")
            rows.append(f"100%{{opacity:{1 if v == 0 else 0}}}")
            name = f"{prefix}d{k}v{v}"
            css.append(
                f"@keyframes {name}{{{''.join(rows)}}}"
                f".{name}{{animation:{name} {period}s linear infinite}}"
            )
            group.append(
                f'<text class="{name}" x="{x - k * DIGIT_W}" y="{y}" font-family="{SANS}" '
                f'font-size="26" font-weight="700" fill="{c["text"]}" text-anchor="end" '
                f'style="font-variant-numeric:tabular-nums">{v}</text>'
            )
        if k == 0:
            parts.append("".join(group))
        else:
            # Leading digits appear once the count reaches them.
            css.append(
                f".{prefix}r{k}{{opacity:0;"
                f"animation:reveal .15s linear {time_of(step)}s forwards}}"
            )
            parts.append(f'<g class="{prefix}r{k}">{"".join(group)}</g>')
    return css, parts


def lane_css(c: Theme) -> list[str]:
    css = []
    for k in range(4):
        on, off = 0.09 * k + 0.4, 0.09 * k + 1.05
        rows = (
            f"0%{{stroke:{c['lane']}}}{pct(on - 0.05, RIGHT_CYCLE)}%{{stroke:{c['lane']}}}"
            f"{pct(on, RIGHT_CYCLE)}%{{stroke:{c['lane_on']}}}"
            f"{pct(off, RIGHT_CYCLE)}%{{stroke:{c['lane_on']}}}"
            f"{pct(off + 0.3, RIGHT_CYCLE)}%{{stroke:{c['lane']}}}100%{{stroke:{c['lane']}}}"
        )
        css.append(
            f"@keyframes lane{k}{{{rows}}}"
            f".lane{k}{{animation:lane{k} {RIGHT_CYCLE}s {-RIGHT_PHASE}s linear infinite}}"
        )
    return css


def lock(x: float, y: float, color: str, unlocked: bool) -> str:
    shackle = (
        f"M{x + 3} {y + 7}V{y + 3.5}a3 3 0 0 1 6 0"
        if unlocked
        else f"M{x + 3} {y + 7}V{y + 4.5}a3 3 0 0 1 6 0V{y + 7}"
    )
    return (
        f'<path d="{shackle}" fill="none" stroke="{color}" stroke-width="1.6" '
        f'stroke-linecap="round"/>'
        f'<rect x="{x + 1}" y="{y + 7}" width="10" height="8" rx="2" fill="{color}"/>'
    )


def svg(theme: str) -> str:
    c = THEMES[theme]
    out: list[str] = []

    def text(
        x: float,
        y: float,
        s: str,
        size: float = 13,
        weight: int = 400,
        color: str = "muted",
        anchor: str = "start",
        family: str = SANS,
    ) -> None:
        out.append(
            f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
            f'font-weight="{weight}" fill="{c[color]}" text-anchor="{anchor}">{s}</text>'
        )

    def badge(x: float, y: float, label: str, color: str, unlocked: bool) -> None:
        out.append(
            f'<rect x="{x}" y="{y}" width="{22 + 7.2 * len(label)}" height="22" rx="11" '
            f'fill="{c["chip"]}" stroke="{color}" stroke-opacity="0.55"/>'
        )
        out.append(lock(x + 8, y + 3, color, unlocked))
        text(x + 25, y + 15.5, label, 11.5, 600, "text")

    def container(
        x: float, y: float, w: float, h: float, kind: str, title: str, gil: bool
    ) -> None:
        out.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" '
            f'fill="url(#{kind}g)" stroke="{c[kind + "_border"]}"/>'
        )
        out.append(f'<circle cx="{x + 20}" cy="{y + 23}" r="4" fill="{c[kind]}"/>')
        text(x + 32, y + 28, title, 14.5, 650, "text")
        label = "holds the GIL" if gil else "no GIL"
        badge(x + w - 18 - (22 + 7.2 * len(label)), y + 11, label, c[kind], not gil)

    def stage(x: float, y: float, w: float, h: float, label: str, sub: str) -> None:
        out.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{c["card"]}" '
            f'fill-opacity="0.65" stroke="{c["border"]}" stroke-opacity="0.8"/>'
        )
        text(x + 62, y + h / 2 - 2, label, 13, 600, "text")
        text(x + 62, y + h / 2 + 15, sub, 12, 400, "muted")

    def gradient(name: str, top: str, bottom: str) -> str:
        return (
            f'<linearGradient id="{name}" x1="0" y1="0" x2="0" y2="1">'
            f'<stop offset="0" stop-color="{c[top]}"/>'
            f'<stop offset="1" stop-color="{c[bottom]}"/></linearGradient>'
        )

    out.append(
        "<defs>"
        + gradient("card", "card", "card2")
        + gradient("pyg", "py_fill", "py_fill2")
        + gradient("coreg", "core_fill", "core_fill2")
        + gradient("rsg", "rs_fill", "rs_fill2")
        + '<filter id="shadow" x="-10%" y="-10%" width="120%" height="130%">'
        f'<feDropShadow dx="0" dy="6" stdDeviation="10" flood-color="{c["shadow"]}" '
        f'flood-opacity="{c["shadow_op"]}"/></filter>'
        '<filter id="glow" x="-100%" y="-100%" width="300%" height="300%">'
        '<feGaussianBlur stdDeviation="3" result="b"/>'
        '<feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>'
        "</filter></defs>"
    )

    for px, title, tag, tag_color in (
        (LEFT_X, "grpc.server()", "grpcio", "py"),
        (RIGHT_X, "pygrpc_native.server()", "pygrpc-native", "rs"),
    ):
        out.append(
            f'<rect x="{px}" y="16" width="{PANEL_W}" height="{H - 32}" rx="20" '
            f'fill="url(#card)" stroke="{c["border"]}" filter="url(#shadow)"/>'
        )
        text(px + 28, 58, title, 19, 650, "text", family=MONO)
        tw = 18 + 7.3 * len(tag)
        out.append(
            f'<rect x="{px + PANEL_W - 28 - tw}" y="40" width="{tw}" height="24" rx="12" '
            f'fill="{c[tag_color]}" fill-opacity="0.12" stroke="{c[tag_color]}" '
            f'stroke-opacity="0.45"/>'
        )
        text(px + PANEL_W - 28 - tw / 2, 56.5, tag, 12, 600, tag_color, "middle")
        out.append(
            f'<line x1="{px + 28}" y1="82" x2="{px + PANEL_W - 28}" y2="82" '
            f'stroke="{c["border"]}"/>'
        )
        cx = px + PANEL_W / 2
        out.append(
            f'<rect x="{cx - 72}" y="{SOURCE_Y - 18}" width="144" height="36" rx="18" '
            f'fill="{c["chip"]}" stroke="{c["border"]}"/>'
        )
        text(cx, SOURCE_Y + 5, "4 clients", 12.5, 600, "muted", "middle")
        out.append(
            f'<line x1="{px + 28}" y1="{DONE_Y - 34}" x2="{px + PANEL_W - 28}" '
            f'y2="{DONE_Y - 34}" stroke="{c["border"]}" stroke-dasharray="3 5"/>'
        )
        text(px + 28, DONE_Y + 5, "calls answered", 12.5, 600, "muted")

    # grpcio: C-core does HTTP/2, then Python handles each event in turn.
    container(LEFT_X + 40, 166, PANEL_W - 80, 48, "core", "C-core, HTTP/2", False)
    container(LEFT_X + 40, 268, PANEL_W - 80, 226, "py", "Python", True)
    stage(
        LEFT_X + 108, 304, PANEL_W - 216, 66,
        "per-call gRPC work", "one completion queue event at a time",
    )  # fmt: skip
    stage(
        LEFT_X + 108, 416, PANEL_W - 216, 66, "your handler", "waits for the same GIL"
    )
    out.append(
        f'<path d="M{LEFT_X + 150} 370 V416" stroke="{c["py"]}" stroke-opacity="0.35" '
        f'stroke-width="2" stroke-dasharray="2 5" stroke-linecap="round"/>'
    )
    text(LEFT_X + 60, 244, "completion queue", 11.5, 500, "faint")

    # pygrpc-native: tokio does HTTP/2 and the per-call work, Python the handlers.
    container(RIGHT_X + 40, 172, PANEL_W - 80, 166, "rs", "Rust, tokio runtime", False)
    for k, lx in enumerate(RIGHT_LANES):
        out.append(
            f'<line class="lane{k}" x1="{lx}" y1="232" x2="{lx}" y2="322" '
            f'stroke="{c["lane"]}" stroke-width="3" stroke-linecap="round"/>'
        )
    text(RIGHT_LANES[-1] + 40, 270, "HTTP/2 and", 13, 600, "text")
    text(RIGHT_LANES[-1] + 40, 288, "per-call gRPC work", 13, 600, "text")
    text(RIGHT_LANES[-1] + 40, 306, "many calls at once", 12, 400, "muted")
    text(RIGHT_X + 330, 365, "one batch per trip", 11.5, 500, "faint")
    container(RIGHT_X + 40, 386, PANEL_W - 80, 108, "py", "Python", True)
    text(RIGHT_X + 330, 458, "your handler", 13, 600, "text")
    text(RIGHT_X + 330, 476, "called a batch at a time", 12, 400, "muted")

    styles = ["@keyframes reveal{from{opacity:0}to{opacity:1}}"]
    packets, counters = [], []
    for side, px, tracks in (
        ("l", LEFT_X, left_tracks(c)),
        ("r", RIGHT_X, right_tracks(c)),
    ):
        for i, (tr, delay) in enumerate(tracks):
            styles.append(tr.css(f"{side}{i}", delay))
            packets.append(
                f'<circle class="{side}{i}" r="8" opacity="0" filter="url(#glow)"/>'
            )
        unit, offsets = done_times(tracks, c["done"])
        css, parts = counter(
            f"{side}c", unit, offsets, px + PANEL_W - 28, DONE_Y + 9, c
        )
        styles += css
        counters += parts
    styles += lane_css(c)
    label = (
        "grpc.server() does HTTP/2 in C-core, then handles each call's events "
        "and runs handlers in Python, one event at a time under the GIL. "
        "pygrpc_native.server() does HTTP/2 and the per-call work on Rust threads "
        "outside the GIL and hands calls to Python in batches."
    )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" '
        f'height="{H}" role="img" aria-label="{label}"><title>{label}</title>'
        f"<style>{''.join(styles)}</style>"
        + "".join(out)
        + "".join(counters)
        + "".join(packets)
        + "</svg>\n"
    )


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    for theme in THEMES:
        with open(os.path.join(here, f"how-it-works-{theme}.svg"), "w") as f:
            f.write(svg(theme))
