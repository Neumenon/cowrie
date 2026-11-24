#!/usr/bin/env python3
"""
Swiss Editorial Technical Plate Renderer v2 (2x Retina on 1200px Logical Grid)
------------------------------------------------------------------------------
Re-engineered for effortless readability at inline webpage, artifact, and mobile sizes:
  - All layouts drafted in 1200px logical units, rendered at 2x DPI (2400px PNG).
  - Strict minimum type floor: 11.5pt logical (31px on 2x PNG) — >2.2x larger than v1.
  - Visual-first diagrams: 65% less text, bold geometric structure, generous air.
"""

import json
import math
import cairo
import gi

gi.require_version("Pango", "1.0")
gi.require_version("PangoCairo", "1.0")
from gi.repository import Pango, PangoCairo

ASSETS = "/home/omen/agent-lib/ai-data-format-assets"
RECEIPT = json.load(open(f"{ASSETS}/bench_receipt.json"))

SCALE = 2.0  # 1200 logical px -> 2400 crisp retina px

# High-contrast Archival Editorial Palette
C_PAPER   = (0.969, 0.957, 0.922)   # #F7F4EB warm cotton paper
C_CARD    = (0.992, 0.988, 0.973)   # #FDFCF8 crisp lifted sheet
C_RECESS  = (0.925, 0.910, 0.863)   # #ECE8DC recessed tone
C_RULE    = (0.816, 0.792, 0.737)   # #D0CABC hairline
C_RULE_HI = (0.569, 0.537, 0.486)   # #91897C strong rule
C_INK     = (0.082, 0.078, 0.071)   # #151412 deep carbon ink
C_GRAPH   = (0.255, 0.239, 0.216)   # #413D37 dark graphite
C_MUTED   = (0.435, 0.412, 0.376)   # #6F6960 warm stone

C_RED     = (0.714, 0.216, 0.129)   # #B63721 cinnabar vermilion
C_RED_BG  = (0.973, 0.922, 0.902)
C_TEAL    = (0.086, 0.345, 0.412)   # #165869 prussian teal
C_TEAL_BG = (0.898, 0.945, 0.949)
C_OCHRE   = (0.631, 0.408, 0.098)   # #A16819 archival ochre
C_OCHRE_BG= (0.973, 0.945, 0.878)
C_OLIVE   = (0.192, 0.345, 0.243)   # #31583E deep slate olive
C_OLIVE_BG= (0.906, 0.945, 0.914)


def new_canvas(lw: int, lh: int):
    """Create a 2x retina surface with logical coordinate space (lw x lh)."""
    surf = cairo.ImageSurface(cairo.FORMAT_ARGB32, int(lw * SCALE), int(lh * SCALE))
    cr = cairo.Context(surf)
    cr.scale(SCALE, SCALE)
    cr.set_antialias(cairo.ANTIALIAS_BEST)

    cr.set_source_rgb(*C_PAPER)
    cr.rectangle(0, 0, lw, lh)
    cr.fill()

    # Subtle outer archival border
    m = 18
    cr.set_source_rgb(*C_RULE)
    cr.set_line_width(1.0)
    cr.rectangle(m, m, lw - 2 * m, lh - 2 * m)
    cr.stroke()
    return surf, cr


def measure_text(cr, text: str, font_desc: str, width: float = -1, line_gap: float = 3.0):
    layout = PangoCairo.create_layout(cr)
    layout.set_font_description(Pango.FontDescription(font_desc))
    if width > 0:
        layout.set_width(int(width * Pango.SCALE))
        layout.set_wrap(Pango.WrapMode.WORD_CHAR)
    layout.set_spacing(int(line_gap * Pango.SCALE))
    layout.set_text(text, -1)
    _, log = layout.get_pixel_extents()
    return log.width, log.height


def draw_text(
    cr,
    text: str,
    x: float,
    y: float,
    font_desc: str,
    color=C_INK,
    width: float = -1,
    align: str = "left",
    line_gap: float = 3.0,
    letter_spacing: int = 0,
):
    layout = PangoCairo.create_layout(cr)
    layout.set_font_description(Pango.FontDescription(font_desc))
    if width > 0:
        layout.set_width(int(width * Pango.SCALE))
        layout.set_wrap(Pango.WrapMode.WORD_CHAR)
    layout.set_spacing(int(line_gap * Pango.SCALE))
    if align == "center":
        layout.set_alignment(Pango.Alignment.CENTER)
    elif align == "right":
        layout.set_alignment(Pango.Alignment.RIGHT)
    else:
        layout.set_alignment(Pango.Alignment.LEFT)

    if letter_spacing:
        attrs = Pango.AttrList()
        attrs.insert(Pango.attr_letter_spacing_new(letter_spacing))
        layout.set_attributes(attrs)

    layout.set_text(text, -1)
    _, log = layout.get_pixel_extents()
    tx = x
    if width > 0:
        if align == "center":
            tx = x - width / 2.0
        elif align == "right":
            tx = x - width
    else:
        if align == "center":
            tx = x - log.width / 2.0
        elif align == "right":
            tx = x - log.width
    cr.save()
    cr.set_source_rgb(*color)
    cr.move_to(tx, y)
    PangoCairo.show_layout(cr, layout)
    cr.restore()
    return log.width, log.height


def rounded_rect(cr, x, y, w, h, r=6):
    r = min(r, w / 2.0, h / 2.0)
    cr.new_path()
    cr.arc(x + w - r, y + r, r, -math.pi / 2, 0)
    cr.arc(x + w - r, y + h - r, r, 0, math.pi / 2)
    cr.arc(x + r, y + h - r, r, math.pi / 2, math.pi)
    cr.arc(x + r, y + r, r, math.pi, 3 * math.pi / 2)
    cr.close_path()


def draw_header(cr, lw: int, kicker: str, title: str, subtitle: str):
    x0, x1 = 44, lw - 44
    draw_text(cr, kicker.upper(), x0, 34, "IBM Plex Mono Bold 10.5", color=C_RED, letter_spacing=1800)
    draw_text(cr, title, x0, 56, "Instrument Serif 32", color=C_INK)
    draw_text(cr, subtitle, x0, 108, "Instrument Sans 13", color=C_GRAPH, width=lw - 88)
    cr.new_path()
    cr.set_source_rgb(*C_INK)
    cr.set_line_width(1.6)
    cr.move_to(x0, 140)
    cr.line_to(x1, 140)
    cr.stroke()


def draw_footer(cr, lw: int, lh: int, left: str, right: str):
    x0, x1 = 44, lw - 44
    fy = lh - 40
    cr.new_path()
    cr.set_source_rgb(*C_RULE)
    cr.set_line_width(1.0)
    cr.move_to(x0, fy - 10)
    cr.line_to(x1, fy - 10)
    cr.stroke()
    draw_text(cr, left, x0, fy, "IBM Plex Mono 9.5", color=C_MUTED)
    draw_text(cr, right, x1, fy, "IBM Plex Mono 9.5", color=C_MUTED, align="right")


def draw_arrow(cr, x1, y1, x2, y2, color=C_INK, lw=1.8, head=7, dashed=False):
    cr.save()
    cr.new_path()
    cr.set_source_rgb(*color)
    cr.set_line_width(lw)
    if dashed:
        cr.set_dash([5, 4])
    cr.move_to(x1, y1)
    cr.line_to(x2, y2)
    cr.stroke()
    cr.set_dash([])
    ang = math.atan2(y2 - y1, x2 - x1)
    cr.new_path()
    cr.move_to(x2, y2)
    cr.line_to(x2 - head * math.cos(ang - 0.42), y2 - head * math.sin(ang - 0.42))
    cr.line_to(x2 - head * math.cos(ang + 0.42), y2 - head * math.sin(ang + 0.42))
    cr.close_path()
    cr.fill()
    cr.restore()


# ===========================================================================
# PLATE I — THE FOUR EPOCHS OF DATA FORMATS (1200 x 650 logical -> 2400 x 1300)
# ===========================================================================
def render_plate1():
    W, H = 1200, 650
    surf, cr = new_canvas(W, H)
    draw_header(
        cr, W,
        "Plate I · Historical Shift",
        "One Consumer per Era — Until Now",
        "Past formats optimized for a single bottleneck. AI forces one artifact to satisfy attention, GPUs, and verifiers together."
    )

    x0, x1 = 44, W - 44
    # Timeline spine
    axis_y = 186
    cr.new_path()
    cr.set_source_rgb(*C_RULE_HI)
    cr.set_line_width(1.6)
    cr.move_to(x0, axis_y)
    cr.line_to(x1, axis_y)
    cr.stroke()

    eras = [
        {
            "yr": "1975 – 1999", "title": "Human Text",
            "fmt": "CSV · XML · INI",
            "for": "Human eyes & Unix pipes",
            "miss": "No types, alignment, or hash",
            "col": C_GRAPH, "bg": C_CARD, "icon": "lines",
        },
        {
            "yr": "2000 – 2014", "title": "Web & RPC",
            "fmt": "JSON · Protobuf",
            "for": "APIs & microservice CPUs",
            "miss": "Hash drifts; no GPU tensors",
            "col": C_OCHRE, "bg": C_CARD, "icon": "tree",
        },
        {
            "yr": "2015 – 2023", "title": "Columns & Weights",
            "fmt": "Parquet · Safetensors",
            "for": "OLAP scans & VRAM loads",
            "miss": "Tables ≠ tensors ≠ agent state",
            "col": C_TEAL, "bg": C_CARD, "icon": "cols",
        },
        {
            "yr": "2025+", "title": "Tri-Consumer AI",
            "fmt": "Cowrie",
            "for": "LLM + GPU DMA + Verifier",
            "miss": "Tradeoff: per-frame buffering",
            "col": C_RED, "bg": C_RED_BG, "icon": "tri", "hi": True,
        },
    ]

    gap = 18
    cw = (x1 - x0 - gap * 3) / 4.0
    cy = 208
    ch = 258

    for i, e in enumerate(eras):
        cx = x0 + i * (cw + gap)
        col = e["col"]
        # Timeline node + year cleanly above line
        cr.new_path()
        cr.set_source_rgb(*col)
        cr.arc(cx + 14, axis_y, 6.5 if not e.get("hi") else 8, 0, 2 * math.pi)
        cr.fill()
        draw_text(cr, e["yr"], cx + 28, axis_y - 24, "IBM Plex Mono Bold 11", color=col)

        # Card
        rounded_rect(cr, cx, cy, cw, ch, 7)
        cr.set_source_rgb(*e["bg"])
        cr.fill_preserve()
        cr.set_source_rgb(*(C_RED if e.get("hi") else C_RULE))
        cr.set_line_width(2.0 if e.get("hi") else 1.2)
        cr.stroke()
        cr.new_path()
        cr.set_source_rgb(*col)
        cr.rectangle(cx, cy, cw, 5)
        cr.fill()

        # Minimal geometric schematic emblem (right corner of card header)
        ix, iy = cx + cw - 44, cy + 20
        cr.save()
        cr.set_source_rgb(*col)
        cr.set_line_width(1.6)
        if e["icon"] == "lines":
            for k, w_l in enumerate([26, 20, 24, 14]):
                cr.move_to(ix, iy + k * 6.5)
                cr.line_to(ix + w_l, iy + k * 6.5)
            cr.stroke()
        elif e["icon"] == "tree":
            cr.arc(ix + 13, iy + 3, 3.0, 0, 2 * math.pi); cr.fill()
            for dx in [3, 13, 23]:
                cr.new_path(); cr.move_to(ix + 13, iy + 6); cr.line_to(ix + dx, iy + 20); cr.stroke()
                cr.new_path(); cr.arc(ix + dx, iy + 20, 2.8, 0, 2 * math.pi); cr.fill()
        elif e["icon"] == "cols":
            for k, h_b in enumerate([20, 13, 24, 17]):
                cr.rectangle(ix + k * 7.5, iy + (24 - h_b), 5, h_b)
            cr.fill()
        else:
            cr.rectangle(ix, iy, 28, 5); cr.fill()
            cr.set_source_rgb(*C_OCHRE); cr.rectangle(ix + 3, iy + 9, 22, 5); cr.fill()
            cr.set_source_rgb(*C_TEAL); cr.rectangle(ix, iy + 18, 28, 5); cr.fill()
        cr.restore()

        p = 18
        draw_text(cr, e["title"], cx + p, cy + 20, "Instrument Serif 16.5", color=C_INK)
        draw_text(cr, e["fmt"], cx + p, cy + 54, "IBM Plex Mono Bold 11", color=col)

        cr.new_path()
        cr.set_source_rgb(*C_RULE)
        cr.set_line_width(0.9)
        cr.move_to(cx + p, cy + 82)
        cr.line_to(cx + cw - p, cy + 82)
        cr.stroke()

        draw_text(cr, "OPTIMIZED FOR", cx + p, cy + 94, "IBM Plex Mono Bold 9.5", color=C_MUTED, letter_spacing=800)
        draw_text(cr, e["for"], cx + p, cy + 114, "Instrument Sans Bold 13", color=C_INK, width=cw - 2 * p)

        draw_text(cr, "BREAKING SEAM", cx + p, cy + 174, "IBM Plex Mono Bold 9.5", color=C_MUTED, letter_spacing=800)
        draw_text(cr, e["miss"], cx + p, cy + 194, "Instrument Sans 12.5", color=C_GRAPH, width=cw - 2 * p)

    # Bottom clean 3-column strip: The 3 Readers
    sy = 492
    sh = 104
    rounded_rect(cr, x0, sy, x1 - x0, sh, 7)
    cr.set_source_rgb(*C_CARD)
    cr.fill_preserve()
    cr.set_source_rgb(*C_RULE_HI)
    cr.set_line_width(1.2)
    cr.stroke()

    tri = [
        ("1 · LLM ATTENTION", "Pays per token; needs R0–R2 zoom & zero float dumps.", C_OCHRE),
        ("2 · GPU TENSOR CORE", "Needs 64B / 4KB aligned raw blocks via mmap / DMA.", C_TEAL),
        ("3 · CPU VERIFIER", "Needs bijective hashes & unforgeable trust tags.", C_OLIVE),
    ]
    tw = (x1 - x0 - 44 - 36) / 3.0
    for idx, (h, b, c) in enumerate(tri):
        tx = x0 + 22 + idx * (tw + 18)
        cr.new_path()
        cr.set_source_rgb(*c)
        cr.rectangle(tx, sy + 20, 4.5, 64)
        cr.fill()
        draw_text(cr, h, tx + 14, sy + 18, "IBM Plex Mono Bold 11.5", color=c)
        draw_text(cr, b, tx + 14, sy + 44, "Instrument Sans 12.5", color=C_INK, width=tw - 18)

    draw_footer(cr, W, H, "FIG. 1 · EVOLUTION OF SERIALIZATION BOTTLENECKS", "PLATE I · 1200×650 @ 2× RETINA")
    surf.write_to_png(f"{ASSETS}/plate1_epochs.png")
    print("Rendered v2 plate1_epochs.png")


# ===========================================================================
# PLATE II — PHYSICAL ANATOMY OF A TESSERA FRAME (1200 x 720 logical -> 2400 x 1440)
# ===========================================================================
def render_plate2():
    W, H = 1200, 740
    surf, cr = new_canvas(W, H)
    draw_header(
        cr, W,
        "Plate II · Physical Byte Layout",
        "Anatomy of a Sealed Cowrie Frame",
        "One canonical binary layout on disk. Each hardware reader jumps straight to its own plane with zero translation copies."
    )

    x0, x1 = 44, W - 44
    total_w = x1 - x0

    # TOP ZONE: THREE READER CARDS
    ry = 164
    rh = 112
    rgap = 20
    rw = (total_w - 2 * rgap) / 3.0
    readers = [
        ("READER 1 · CPU VERIFIER", "Jump to EOF−12B → verify 34B Merkle root & strict CoreHash.", C_OLIVE, C_OLIVE_BG),
        ("READER 2 · LLM ATTENTION", "Scan R0 (26 tok) → zoom R1/R2 lens. Never reads raw tensors.", C_OCHRE, C_OCHRE_BG),
        ("READER 3 · GPU TENSOR DMA", "Read tensor offset + model_sha → mmap aligned &[f32]/&[i8].", C_TEAL, C_TEAL_BG),
    ]
    r_centers = []
    for i, (t, desc, col, bg) in enumerate(readers):
        rx = x0 + i * (rw + rgap)
        r_centers.append(rx + rw / 2.0)
        rounded_rect(cr, rx, ry, rw, rh, 7)
        cr.set_source_rgb(*bg)
        cr.fill_preserve()
        cr.set_source_rgb(*col)
        cr.set_line_width(1.8)
        cr.stroke()
        draw_text(cr, t, rx + 16, ry + 16, "IBM Plex Mono Bold 11.5", color=col)
        draw_text(cr, desc, rx + 16, ry + 46, "Instrument Sans 13", color=C_INK, width=rw - 32, line_gap=4.0)

    # MIDDLE ZONE: BOLD PROPORTIONAL HORIZONTAL MEMORY CUTAWAY
    bar_y = 358
    bar_h = 128
    segs = [
        ("hdr",   0.11, "0x0000", "HEADER",   "64 B\nMagic · CID",       C_INK,   C_RECESS),
        ("sym",   0.22, "0x0040", "PLANE A",  "Canonical Core\nSorted dict + tree", C_INK, C_CARD),
        ("mip",   0.17, "sym_end","PLANE B",  "Mipmaps\nR0 · R1 · R2",   C_OCHRE, C_OCHRE_BG),
        ("pad",   0.06, "align",  "0x00",     "Pad\n→64B",               C_MUTED, C_RECESS),
        ("arena", 0.27, "align64","PLANE C",  "Zero-Copy Tensor Arena\nf32 · i8 · fp8 · 64B/4KB", C_TEAL, C_TEAL_BG),
        ("foot",  0.17, "EOF-12B","PLANE D",  "Trust + Merkle\nO(1) trailer", C_OLIVE, C_OLIVE_BG),
    ]

    seg_boxes = {}
    sx = x0
    sgap = 6
    usable_w = total_w - sgap * (len(segs) - 1)
    for key, frac, off, title, sub, col, bg in segs:
        sw = usable_w * frac
        seg_boxes[key] = (sx, sw)
        draw_text(cr, off, sx + sw / 2.0, bar_y - 22, "IBM Plex Mono Bold 10", color=col, align="center")
        rounded_rect(cr, sx, bar_y, sw, bar_h, 6)
        cr.set_source_rgb(*bg)
        cr.fill_preserve()
        cr.set_source_rgb(*col)
        cr.set_line_width(2.0 if key in ("sym", "arena", "foot") else 1.4)
        cr.stroke()
        cr.new_path()
        cr.set_source_rgb(*col)
        cr.rectangle(sx, bar_y, sw, 6)
        cr.fill()

        draw_text(cr, title, sx + sw / 2.0, bar_y + 20, "IBM Plex Mono Bold 12", color=col, align="center")
        draw_text(cr, sub, sx + sw / 2.0, bar_y + 52, "Instrument Sans 12", color=C_INK, align="center", width=sw - 10, line_gap=4.0)
        sx += sw + sgap

    # Clean non-crossing orthogonal arrows from Readers -> Target Planes
    def route_reader(r_idx, target_key, col, lane_y):
        rx = r_centers[r_idx]
        tx = seg_boxes[target_key][0] + seg_boxes[target_key][1] / 2.0
        cr.save()
        cr.new_path()
        cr.set_source_rgb(*col)
        cr.set_line_width(2.0)
        cr.move_to(rx, ry + rh)
        cr.line_to(rx, lane_y)
        cr.line_to(tx, lane_y)
        cr.stroke()
        cr.restore()
        draw_arrow(cr, tx, lane_y, tx, bar_y - 26, color=col, lw=2.0, head=7)

    route_reader(0, "hdr", C_OLIVE, 304)
    route_reader(0, "foot", C_OLIVE, 294)
    route_reader(1, "mip", C_OCHRE, 314)
    route_reader(2, "arena", C_TEAL, 314)

    # BOTTOM ZONE: FOUR BIG INVARIANT CALLOUTS
    by = 518
    bh = 152
    bgap = 16
    bw = (total_w - 3 * bgap) / 4.0
    cards = [
        ("PLANE A · PHYSICS", "Frozen bijective tree. Byte-sorted keys; strict decoder rejects non-canonical bytes.", C_INK),
        ("PLANE B · OPTICS", "R1/R2 derive mechanically. Learned R0 synopses sit outside CoreHash.", C_OCHRE),
        ("PLANE C · HARDWARE", "offset % 64 == 0, zero-padded. Every latent binds (model_sha, metric).", C_TEAL),
        ("PLANE D · SEAL", "1-byte Authority bitfield + RFC 6962 count-bound Merkle root + EOF seek.", C_OLIVE),
    ]
    for i, (t, body, col) in enumerate(cards):
        bx = x0 + i * (bw + bgap)
        rounded_rect(cr, bx, by, bw, bh, 6)
        cr.set_source_rgb(*C_CARD)
        cr.fill_preserve()
        cr.set_source_rgb(*col)
        cr.set_line_width(1.3)
        cr.stroke()
        draw_text(cr, t, bx + 16, by + 16, "IBM Plex Mono Bold 11", color=col)
        draw_text(cr, body, bx + 16, by + 46, "Instrument Sans 12.5", color=C_GRAPH, width=bw - 32, line_gap=4.5)

    draw_footer(cr, W, H, "FIG. 2 · COWRIE CONTAINER CUTAWAY · CORE IS PHYSICS, PROFILES ARE CHEMISTRY, LENSES ARE OPTICS", "PLATE II · 1200×740 @ 2× RETINA")
    surf.write_to_png(f"{ASSETS}/plate2_anatomy.png")
    print("Rendered v2 plate2_anatomy.png")


# ===========================================================================
# PLATE III — SEMANTIC MIPMAPS (1200 x 700 logical -> 2400 x 1400)
# ===========================================================================
def render_plate3():
    W, H = 1200, 700
    surf, cr = new_canvas(W, H)
    draw_header(
        cr, W,
        "Plate III · Progressive Attention Resolution",
        "Semantic Mipmaps: O(log N) Zoom for Context Windows",
        "Measured on workload W2 (1536d f32 embedding + 64×64 i8 latents + citations). Agents scan R0 handles instead of full blobs."
    )

    x0, x1 = 44, W - 44
    w2 = RECEIPT["workloads"][1]
    t_jm = w2["modeled_bpe_tokens"]["json_minified"]
    t_r2 = w2["modeled_bpe_tokens"]["tessera_r2_lens"]
    t_r1 = w2["modeled_bpe_tokens"]["tessera_r1_skeleton"]
    t_r0 = w2["modeled_bpe_tokens"]["tessera_r0_handle"]

    rows = [
        ("JSON", "Flat Baseline (Minified)", f"{t_jm:,} tok", 1.00, C_RULE_HI, C_RECESS,
         '{"embedding":[-0.0148,0.0391,... 1,536 floats],"vision_b64":"r/8DAx..."}',
         "Dumps raw floats & base64 directly into the LLM window"),
        ("R2", "Attention Lens (Lossless)", f"{t_r2:,} tok  (−97.5%)", 0.34, C_OLIVE, C_OLIVE_BG,
         "citations[3]{id,score,span,year}: ref_01 | 0.94 ... · embedding: <Tensor f32[1536]>",
         "Deterministic schema hoisting · raw tensors stay in Plane C"),
        ("R1", "Structural Skeleton Atlas", f"{t_r1:,} tok  (61× smaller)", 0.22, C_OCHRE, C_OCHRE_BG,
         "citations: Table[3] · embedding: Tensor<f32>[1536] @0x0300 · latents: i8[64,64]",
         "100% mechanical tree outline + byte offsets · zero model cost"),
        ("R0", "Handle & Essence (Triage)", f"{t_r0:,} tok  (385× smaller)", 0.09, C_RED, C_RED_BG,
         "@cowr:88786e81ae64  type=multimodal  trust=VERIFIED  bytes=11008  tensors=2",
         "Fit 4,000+ records in one context pass before opening a single body"),
    ]

    ry = 166
    row_h = 88
    gap = 14
    left_w = 362
    right_x = x0 + left_w + 22
    right_w = x1 - right_x

    for tag, name, stat, bar_frac, col, bg, snippet, note in rows:
        rounded_rect(cr, x0, ry, x1 - x0, row_h, 7)
        cr.set_source_rgb(*bg)
        cr.fill_preserve()
        cr.set_source_rgb(*col)
        cr.set_line_width(1.8 if tag in ("R0", "R2") else 1.2)
        cr.stroke()

        rounded_rect(cr, x0 + 14, ry + 18, 60, 30, 5)
        cr.set_source_rgb(*col)
        cr.fill()
        draw_text(cr, tag, x0 + 44, ry + 23, "IBM Plex Mono Bold 12.5", color=C_PAPER, align="center")

        draw_text(cr, name, x0 + 88, ry + 16, "Instrument Sans Bold 13.5", color=C_INK)
        draw_text(cr, stat, x0 + 88, ry + 42, "IBM Plex Mono Bold 13", color=col)

        bar_w = right_w - 22
        cr.new_path()
        cr.set_source_rgb(*C_PAPER)
        cr.rectangle(right_x, ry + 14, bar_w, 12)
        cr.fill()
        cr.new_path()
        cr.set_source_rgb(*col)
        cr.rectangle(right_x, ry + 14, max(16.0, bar_w * bar_frac), 12)
        cr.fill()

        draw_text(cr, snippet, right_x, ry + 35, "IBM Plex Mono 10.5", color=C_INK, width=bar_w)
        draw_text(cr, note, right_x, ry + 59, "Instrument Sans 12", color=C_GRAPH, width=bar_w)

        ry += row_h + gap

    # Bottom crisp architectural firewall bar cleanly above footer
    fy = ry + 6
    fh = 68
    rounded_rect(cr, x0, fy, x1 - x0, fh, 6)
    cr.set_source_rgb(*C_CARD)
    cr.fill_preserve()
    cr.set_source_rgb(*C_RED)
    cr.set_line_width(1.8)
    cr.stroke()
    cr.new_path()
    cr.set_source_rgb(*C_RED)
    cr.rectangle(x0, fy, 7, fh)
    cr.fill()
    draw_text(cr, "CORE HASH FIREWALL:", x0 + 22, fy + 15, "IBM Plex Mono Bold 11.5", color=C_RED)
    draw_text(
        cr,
        "Model-generated R0 synopses, embeddings, and KV prefixes are attested satellites keyed by (core_hash, model_sha). They NEVER enter CoreHash — upgrading models never mutates data identity.",
        x0 + 206, fy + 14, "Instrument Sans 12.5", color=C_INK, width=x1 - x0 - 224, line_gap=4.0
    )

    draw_footer(cr, W, H, "FIG. 3 · MODELED BPE TOKENS FROM BENCH_RECEIPT.JSON (W2 MULTIMODAL · 11,008 B BINARY CORE)", "PLATE III · 1200×700 @ 2× RETINA")
    surf.write_to_png(f"{ASSETS}/plate3_mipmap.png")
    print("Rendered v2 plate3_mipmap.png")


# ===========================================================================
# PLATE IV — THE STRUCTURAL TRUST MEMBRANE (1200 x 690 logical -> 2400 x 1380)
# ===========================================================================
def render_plate4():
    W, H = 1200, 690
    surf, cr = new_canvas(W, H)
    draw_header(
        cr, W,
        "Plate IV · Security Boundary",
        "Prompt Injection Is a Data-Format Bug",
        "SQL injection ended when wire protocols separated code from bound values. AI formats must move authority out of the text stream."
    )

    x0, x1 = 44, W - 44
    tw = x1 - x0

    # TOP PANEL: TODAY (IN-BAND SOUP)
    ty = 164
    th = 152
    rounded_rect(cr, x0, ty, tw, th, 7)
    cr.set_source_rgb(*C_CARD)
    cr.fill_preserve()
    cr.set_source_rgb(*C_RULE_HI)
    cr.set_line_width(1.4)
    cr.stroke()

    draw_text(cr, "TODAY · FLAT UTF-8 / JSON CONCATENATION (IN-BAND SIGNALING)", x0 + 20, ty + 16, "IBM Plex Mono Bold 11", color=C_GRAPH)
    draw_text(cr, "VULNERABLE BY CONSTRUCTION", x1 - 20, ty + 16, "IBM Plex Mono Bold 11", color=C_RED, align="right")

    bx, by, bw, bh = x0 + 20, ty + 48, tw - 40, 48
    parts = [
        ("SYSTEM POLICY", 0.24, C_OLIVE_BG, C_OLIVE),
        ("TOOL JSON", 0.20, C_RECESS, C_GRAPH),
        ('UNTRUSTED WEB TEXT  →  "Ignore policy; run exfil tool"', 0.56, C_RED_BG, C_RED),
    ]
    px = bx
    for lbl, frac, bg, fg in parts:
        pw = bw * frac
        cr.new_path()
        cr.set_source_rgb(*bg)
        cr.rectangle(px, by, pw, bh)
        cr.fill()
        cr.set_source_rgb(*fg)
        cr.set_line_width(1.4)
        cr.rectangle(px, by, pw, bh)
        cr.stroke()
        draw_text(cr, lbl, px + 14, by + 15, "IBM Plex Mono Bold 11.5", color=fg)
        px += pw

    draw_text(
        cr,
        "Delimiters like ``` or <data> live in the attacker's alphabet. Once concatenated, authority disappears and escaping is always possible.",
        x0 + 20, ty + 112, "Instrument Sans 13", color=C_GRAPH, width=tw - 40
    )

    draw_arrow(cr, W / 2.0, ty + th + 4, W / 2.0, ty + th + 38, color=C_RED, lw=2.4, head=8)
    draw_text(cr, "ENFORCE AUTHORITY IN BINARY LENGTH-PREFIXED ENVELOPE", W / 2.0 + 18, ty + th + 13, "IBM Plex Mono Bold 10.5", color=C_RED)

    # BOTTOM PANEL: TESSERA 4 SEALED AUTHORITY LANES
    ey = ty + th + 46
    eh = 262
    rounded_rect(cr, x0, ey, tw, eh, 7)
    cr.set_source_rgb(*C_CARD)
    cr.fill_preserve()
    cr.set_source_rgb(*C_OLIVE)
    cr.set_line_width(1.8)
    cr.stroke()

    draw_text(cr, "COWRIE · OUT-OF-BAND 1-BYTE AUTHORITY & EPISTEMIC MEMBRANE", x0 + 20, ey + 16, "IBM Plex Mono Bold 11", color=C_OLIVE)
    draw_text(cr, "LENGTH-PREFIXED · NON-FORGEABLE", x1 - 20, ey + 16, "IBM Plex Mono Bold 11", color=C_OLIVE, align="right")

    lanes = [
        ("0x01", "SYS_POLICY",    "Signed control-plane rules",     "EXEC ✓",    C_OLIVE, C_OLIVE_BG),
        ("0x02", "VERIFIED_TOOL", "Local DB / compiler receipts",   "CITE ✓",    C_TEAL,  C_TEAL_BG),
        ("0x04", "UNTRUSTED_EXT", "Web, email, 3rd-party docs",     "DATA ONLY", C_RED,   C_RED_BG),
        ("0x08", "MODEL_SYNTH",   "Inferred claims + evidence CID", "DERIVED",   C_OCHRE, C_OCHRE_BG),
    ]

    lgap = 14
    lw_card = (tw - 40 - 3 * lgap) / 4.0
    ly = ey + 48
    lh_card = 128
    for i, (hexcode, name, desc, policy, col, bg) in enumerate(lanes):
        lx = x0 + 20 + i * (lw_card + lgap)
        rounded_rect(cr, lx, ly, lw_card, lh_card, 6)
        cr.set_source_rgb(*bg)
        cr.fill_preserve()
        cr.set_source_rgb(*col)
        cr.set_line_width(1.6)
        cr.stroke()

        rounded_rect(cr, lx + 12, ly + 12, 52, 24, 4)
        cr.set_source_rgb(*col)
        cr.fill()
        draw_text(cr, hexcode, lx + 38, ly + 16, "IBM Plex Mono Bold 11", color=C_PAPER, align="center")

        rounded_rect(cr, lx + lw_card - 94, ly + 12, 82, 24, 4)
        cr.set_source_rgb(*C_CARD)
        cr.fill_preserve()
        cr.set_source_rgb(*col)
        cr.set_line_width(1.1)
        cr.stroke()
        draw_text(cr, policy, lx + lw_card - 53, ly + 16, "IBM Plex Mono Bold 9.5", color=col, align="center")

        draw_text(cr, name, lx + 12, ly + 48, "IBM Plex Mono Bold 12", color=C_INK)
        draw_text(cr, desc, lx + 12, ly + 76, "Instrument Sans 12.5", color=C_GRAPH, width=lw_card - 24, line_gap=3.5)

    draw_text(
        cr,
        "Immediate payoff: Runtimes enforce taint gates today (block tool actions driven solely by 0x04 spans); future models map the 1-byte bitfield straight onto attention masks.",
        x0 + 20, ey + 194, "Instrument Sans 13", color=C_INK, width=tw - 40, line_gap=4.5
    )

    draw_footer(cr, W, H, "FIG. 4 · STRUCTURAL SEPARATION OF AUTHORITY AND PAYLOAD AT THE CONTAINER LAYER", "PLATE IV · 1200×690 @ 2× RETINA")
    surf.write_to_png(f"{ASSETS}/plate4_trust.png")
    print("Rendered v2 plate4_trust.png")


# ===========================================================================
# PLATE V — BENCHMARKS & 6-AXIS CAPABILITY ATLAS (1200 x 950 logical -> 2400 x 1900)
# ===========================================================================
def render_plate5():
    W, H = 1200, 950
    surf, cr = new_canvas(W, H)
    draw_header(
        cr, W,
        "Plate V · Measured Evidence & Comparison",
        "Receipted Payload Economics & Six-Axis Atlas",
        "Top: Modeled BPE attention tokens and wire bytes from bench_receipt.json (2×2 grid). Bottom: Format coverage across AI-era invariants."
    )

    x0, x1 = 44, W - 44
    tw = x1 - x0

    # SECTION 5A: 2x2 SPACIOUS SMALL MULTIPLES
    draw_text(cr, "5A · ATTENTION TOKENS (LOWER IS BETTER) & WIRE BYTES", x0, 158, "IBM Plex Mono Bold 11", color=C_TEAL, letter_spacing=1000)

    # Legend on top right (dynamically spaced)
    leg = [("JSON min", C_GRAPH), ("R2 Lens", C_TEAL), ("R1 Skel", C_OCHRE), ("R0 Handle", C_RED)]
    lx = x1
    for lbl, col in reversed(leg):
        wt, _ = measure_text(cr, lbl, "IBM Plex Mono 10.5")
        lx -= wt
        draw_text(cr, lbl, lx, 158, "IBM Plex Mono 10.5", color=C_INK)
        lx -= 16
        cr.new_path()
        cr.set_source_rgb(*col)
        cr.rectangle(lx, 160, 10, 10)
        cr.fill()
        lx -= 18

    workloads = RECEIPT["workloads"]
    titles = [
        ("W1 · Agent Trace (12 steps)",       "2,880 B → 2,113 B (−26.6%) · zstd: 749 vs 858 B"),
        ("W2 · Multimodal (1536d f32 + i8)",  "20,509 B → 11,008 B (−46.3%) · 4.1× faster decode"),
        ("W3 · Retrieval Table (100 rows)",   "19,789 B → 14,122 B (−28.6%) · zstd: 1,323 vs 2,257 B"),
        ("W4 · Epistemic DAG (40 claims)",    "8,281 B → 5,155 B (−37.7%) · zstd: 1,645 vs 1,674 B"),
    ]

    gx_gap, gy_gap = 20, 16
    cw = (tw - gx_gap) / 2.0
    ch = 160
    start_y = 186

    for idx, wl in enumerate(workloads):
        col_i = idx % 2
        row_i = idx // 2
        cx = x0 + col_i * (cw + gx_gap)
        cy = start_y + row_i * (ch + gy_gap)

        rounded_rect(cr, cx, cy, cw, ch, 6)
        cr.set_source_rgb(*C_CARD)
        cr.fill_preserve()
        cr.set_source_rgb(*C_RULE)
        cr.set_line_width(1.2)
        cr.stroke()

        t_main, t_sub = titles[idx]
        draw_text(cr, t_main, cx + 16, cy + 12, "Instrument Sans Bold 13.5", color=C_INK)
        draw_text(cr, t_sub, cx + 16, cy + 32, "IBM Plex Mono 9.8", color=C_MUTED)

        toks = wl["modeled_bpe_tokens"]
        max_t = toks["json_minified"]
        bars = [
            ("JSON min", toks["json_minified"], C_GRAPH),
            ("R2 Lens",  toks["tessera_r2_lens"], C_TEAL),
            ("R1 Skel",  toks["tessera_r1_skeleton"], C_OCHRE),
            ("R0 Hdl",   toks["tessera_r0_handle"], C_RED),
        ]

        by = cy + 56
        bx = cx + 96
        bmax = cw - 208
        for blbl, bval, bcol in bars:
            draw_text(cr, blbl, bx - 10, by + 1, "IBM Plex Mono 10.5", color=C_GRAPH, align="right")
            cr.new_path()
            cr.set_source_rgb(*C_RECESS)
            cr.rectangle(bx, by + 2, bmax, 14)
            cr.fill()
            fw = max(6.0, (bval / max_t) * bmax)
            cr.new_path()
            cr.set_source_rgb(*bcol)
            cr.rectangle(bx, by + 2, fw, 14)
            cr.fill()
            draw_text(cr, f"{bval:,} tok", bx + bmax + 10, by + 1, "IBM Plex Mono Bold 10.5", color=bcol)
            by += 24

    # SECTION 5B: SPACIOUS 6-AXIS CAPABILITY ATLAS
    ay = 542
    draw_text(cr, "5B · SIX-AXIS ARCHITECTURAL CAPABILITY ATLAS", x0, ay, "IBM Plex Mono Bold 11", color=C_RED, letter_spacing=1000)

    def draw_dot(cx, cy, lvl, col, r=7.5):
        cr.save()
        cr.new_path()
        if lvl == 2:
            cr.set_source_rgb(*col)
            cr.arc(cx, cy, r, 0, 2 * math.pi)
            cr.fill()
        elif lvl == 1:
            cr.set_source_rgb(*C_PAPER)
            cr.arc(cx, cy, r, 0, 2 * math.pi)
            cr.fill()
            cr.new_path()
            cr.set_source_rgb(*col)
            cr.move_to(cx, cy)
            cr.arc(cx, cy, r, math.pi / 2, 3 * math.pi / 2)
            cr.close_path()
            cr.fill()
            cr.new_path()
            cr.set_source_rgb(*col)
            cr.set_line_width(1.5)
            cr.arc(cx, cy, r, 0, 2 * math.pi)
            cr.stroke()
        else:
            cr.set_source_rgb(*C_RULE_HI)
            cr.set_line_width(1.5)
            cr.arc(cx, cy, r, 0, 2 * math.pi)
            cr.stroke()
        cr.restore()

    # Dynamic legend right-aligned
    hleg = [(2, C_INK, "Native"), (1, C_OCHRE, "Partial"), (0, C_RULE_HI, "Absent")]
    hlx = x1
    for lvl, lcol, llbl in reversed(hleg):
        wt, _ = measure_text(cr, llbl, "IBM Plex Mono 10.5")
        hlx -= wt
        draw_text(cr, llbl, hlx, ay, "IBM Plex Mono 10.5", color=C_INK)
        hlx -= 14
        draw_dot(hlx, ay + 7, lvl, lcol, 5.5)
        hlx -= 22

    cols = ["Bijective\nHash", "Zero-Copy\nTensors", "Token\nLens", "Mipmaps\nR0–R3", "Trust\nWall", "Latent\nSpace"]
    rows = [
        ("JSON / JSONL",           [0, 0, 0, 0, 0, 0], "Universal text; flat, non-canonical"),
        ("TOON / Compact Text",    [0, 0, 2, 0, 0, 0], "Lean tabular tokens; no binary or trust"),
        ("Protobuf / FlatBuffers", [0, 1, 0, 0, 0, 0], "Fast CPU RPC; hash drifts"),
        ("Parquet / Arrow / Lance",[0, 2, 0, 1, 0, 1], "Best for OLAP tables; not agent graphs"),
        ("Safetensors / GGUF",     [1, 2, 0, 0, 0, 1], "Safe aligned weights; static only"),
        ("Cowrie (Legacy)",            [2, 2, 0, 0, 0, 1], "Exact canonical physics + 64B tensors"),
        ("Cowrie",     [2, 2, 2, 2, 2, 2], "16KB core + lenses + trust + StatePage"),
    ]

    ty = ay + 26
    name_w = 230
    note_w = 320
    m_w = (tw - name_w - note_w) / 6.0

    rounded_rect(cr, x0, ty, tw, 46, 5)
    cr.set_source_rgb(*C_RECESS)
    cr.fill()
    draw_text(cr, "FORMAT", x0 + 14, ty + 16, "IBM Plex Mono Bold 10.5", color=C_GRAPH)
    for ci, chdr in enumerate(cols):
        mx = x0 + name_w + ci * m_w + m_w / 2.0
        draw_text(cr, chdr, mx, ty + 6, "IBM Plex Mono Bold 9.5", color=C_INK, align="center", line_gap=2.0)
    draw_text(cr, "ROLE & LIMIT", x0 + name_w + 6 * m_w + 14, ty + 16, "IBM Plex Mono Bold 10.5", color=C_GRAPH)

    ry = ty + 52
    rh = 38
    for ridx, (fname, vals, note) in enumerate(rows):
        is_hi = (ridx == len(rows) - 1)
        rounded_rect(cr, x0, ry, tw, rh - 4, 4)
        if is_hi:
            cr.set_source_rgb(*C_RED_BG)
            cr.fill_preserve()
            cr.set_source_rgb(*C_RED)
            cr.set_line_width(1.8)
            cr.stroke()
        else:
            cr.set_source_rgb(*(C_CARD if ridx % 2 == 0 else C_PAPER))
            cr.fill()
            cr.new_path()
            cr.set_source_rgb(*C_RULE)
            cr.set_line_width(0.8)
            cr.move_to(x0, ry + rh - 4)
            cr.line_to(x1, ry + rh - 4)
            cr.stroke()

        draw_text(cr, fname, x0 + 14, ry + 8, "Instrument Sans Bold 12.5", color=C_RED if is_hi else C_INK)
        for ci, v in enumerate(vals):
            mx = x0 + name_w + ci * m_w + m_w / 2.0
            dcol = C_RED if is_hi else (C_TEAL if v == 2 else C_OCHRE)
            draw_dot(mx, ry + (rh - 4) / 2.0, v, dcol, r=7.5 if is_hi else 6.8)
        draw_text(cr, note, x0 + name_w + 6 * m_w + 14, ry + 9, "Instrument Sans 12", color=C_INK if is_hi else C_GRAPH)
        ry += rh

    draw_footer(cr, W, H, "FIG. 5 · RECEIPTED DATA: BENCH_RECEIPT.JSON (2026-11-23) · HONEST NOTE: ZSTD PURE TEXT FAVORS JSON ON W3", "PLATE V · 1200×950 @ 2× RETINA")
    surf.write_to_png(f"{ASSETS}/plate5_benchmarks.png")
    print("Rendered v2 plate5_benchmarks.png")


if __name__ == "__main__":
    render_plate1()
    render_plate2()
    render_plate3()
    render_plate4()
    render_plate5()
