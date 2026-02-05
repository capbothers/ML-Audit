"""
Brand Report PDF Generator

Generates a professional A4 PDF report for a single brand,
matching the data shown in the Brand Report tab of the Pricing Intel dashboard.
Uses reportlab Platypus for layout and matplotlib for trend charts.
"""
import io
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    Image, PageBreak, HRFlowable,
)

# ── Cass Brothers palette ──────────────────────────
CASS_BLACK = colors.HexColor("#1b1b1b")
CASS_CREAM = colors.HexColor("#f7f3ed")
CASS_GOLD  = colors.HexColor("#c49a4a")
CASS_TEAL  = colors.HexColor("#1f6f6b")
CASS_MOSS  = colors.HexColor("#2f3d33")
WHITE      = colors.white

PAGE_W, PAGE_H = A4  # 595 x 842 pts


def _styles():
    """Build custom paragraph styles."""
    ss = getSampleStyleSheet()
    ss.add(ParagraphStyle(
        "CoverTitle", parent=ss["Title"],
        fontName="Helvetica-Bold", fontSize=22, leading=28,
        textColor=CASS_BLACK, alignment=TA_LEFT, spaceAfter=6,
    ))
    ss.add(ParagraphStyle(
        "CoverSub", parent=ss["Normal"],
        fontName="Helvetica", fontSize=11, leading=14,
        textColor=colors.HexColor("#666666"), alignment=TA_LEFT,
        spaceAfter=20,
    ))
    ss.add(ParagraphStyle(
        "SectionHead", parent=ss["Heading2"],
        fontName="Helvetica-Bold", fontSize=13, leading=16,
        textColor=CASS_MOSS, spaceAfter=6, spaceBefore=14,
    ))
    ss.add(ParagraphStyle(
        "TableCell", parent=ss["Normal"],
        fontName="Helvetica", fontSize=7.5, leading=10,
        textColor=CASS_BLACK,
    ))
    ss.add(ParagraphStyle(
        "TableCellRight", parent=ss["Normal"],
        fontName="Helvetica", fontSize=7.5, leading=10,
        textColor=CASS_BLACK, alignment=TA_RIGHT,
    ))
    ss.add(ParagraphStyle(
        "TableHeader", parent=ss["Normal"],
        fontName="Helvetica-Bold", fontSize=7.5, leading=10,
        textColor=WHITE,
    ))
    ss.add(ParagraphStyle(
        "Footer", parent=ss["Normal"],
        fontName="Helvetica-Oblique", fontSize=7, leading=9,
        textColor=colors.HexColor("#999999"), alignment=TA_CENTER,
    ))
    ss.add(ParagraphStyle(
        "KpiValue", parent=ss["Normal"],
        fontName="Helvetica-Bold", fontSize=18, leading=22,
        textColor=CASS_TEAL, alignment=TA_CENTER,
    ))
    ss.add(ParagraphStyle(
        "KpiLabel", parent=ss["Normal"],
        fontName="Helvetica", fontSize=8, leading=10,
        textColor=colors.HexColor("#666666"), alignment=TA_CENTER,
    ))
    return ss


def _fmt(val, prefix="", suffix="", decimals=1):
    """Format a number for display."""
    if val is None:
        return "—"
    if isinstance(val, bool):
        return "Yes" if val else "No"
    if isinstance(val, float):
        return f"{prefix}{val:,.{decimals}f}{suffix}"
    return f"{prefix}{val:,}{suffix}"


def _make_table(headers, rows, col_widths=None, left_align_cols=None):
    """Build a reportlab Table with Cass Brothers styling.

    Args:
        left_align_cols: optional set/list of column indices that should be
            left-aligned (in addition to column 0 which is always left).
    """
    data = [headers] + rows
    t = Table(data, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        # Header row
        ("BACKGROUND", (0, 0), (-1, 0), CASS_MOSS),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 7.5),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("TOPPADDING", (0, 0), (-1, 0), 6),
        # Body
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 7.5),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
        ("TOPPADDING", (0, 1), (-1, -1), 4),
        # Cell padding
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        # Grid
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, CASS_GOLD),
        ("LINEBELOW", (0, 1), (-1, -2), 0.3, colors.HexColor("#e0ddd7")),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, CASS_MOSS),
        # Alignment: right-align numeric columns (all except first)
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]
    # Extra left-aligned columns (for text like Title, Competitor, etc.)
    if left_align_cols:
        for col_idx in left_align_cols:
            style_cmds.append(("ALIGN", (col_idx, 0), (col_idx, -1), "LEFT"))
    # Alternating row colors
    for i in range(1, len(data)):
        if i % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, i), (-1, i), CASS_CREAM))
    t.setStyle(TableStyle(style_cmds))
    return t


def _build_trend_chart(monthly_trends: list) -> io.BytesIO:
    """Render monthly trends as a matplotlib chart, return PNG bytes."""
    if not monthly_trends:
        return None

    months = [m["month_name"] for m in monthly_trends]
    discounts = [m.get("avg_discount_pct", 0) for m in monthly_trends]
    below = [m.get("skus_below_floor", 0) for m in monthly_trends]

    fig, ax1 = plt.subplots(figsize=(7, 2.8))
    fig.patch.set_facecolor("#ffffff")

    # Discount line
    color1 = "#1f6f6b"
    ax1.set_ylabel("Avg Discount %", color=color1, fontsize=8)
    line1 = ax1.plot(months, discounts, color=color1, marker="o", markersize=4,
                     linewidth=2, label="Avg Discount %")
    ax1.tick_params(axis="y", labelcolor=color1, labelsize=7)
    ax1.tick_params(axis="x", labelsize=7, rotation=30)
    ax1.set_ylim(bottom=0)

    # Below floor bars
    ax2 = ax1.twinx()
    color2 = "#c49a4a"
    ax2.set_ylabel("SKUs Below Floor", color=color2, fontsize=8)
    bars = ax2.bar(months, below, alpha=0.35, color=color2, width=0.5, label="Below Floor")
    ax2.tick_params(axis="y", labelcolor=color2, labelsize=7)
    ax2.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax2.set_ylim(bottom=0)

    # Legend
    lines = line1 + [bars]
    labels = ["Avg Discount %", "SKUs Below Floor"]
    ax1.legend(lines, labels, loc="upper left", fontsize=7, framealpha=0.8)

    ax1.grid(axis="y", alpha=0.2)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf


def _header_footer(canvas, doc, brand: str):
    """Draw header bar and footer on every page."""
    canvas.saveState()
    # Header bar
    canvas.setFillColor(CASS_BLACK)
    canvas.rect(0, PAGE_H - 28, PAGE_W, 28, fill=1, stroke=0)
    # Gold accent line
    canvas.setStrokeColor(CASS_GOLD)
    canvas.setLineWidth(1.5)
    canvas.line(0, PAGE_H - 28, PAGE_W, PAGE_H - 28)
    # Header text
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica-Bold", 9)
    canvas.drawString(20, PAGE_H - 19, "CASS BROTHERS")
    canvas.setFont("Helvetica", 8)
    canvas.drawRightString(PAGE_W - 20, PAGE_H - 19, f"Pricing Intelligence — {brand}")
    # Footer
    canvas.setFillColor(colors.HexColor("#999999"))
    canvas.setFont("Helvetica-Oblique", 7)
    canvas.drawCentredString(
        PAGE_W / 2, 18,
        f"Confidential — Prepared by Cass Brothers · Page {doc.page}"
    )
    canvas.restoreState()


def generate_brand_report_pdf(data: dict) -> io.BytesIO:
    """
    Generate a complete brand report PDF.

    Args:
        data: The 'data' dict from the /pricing/brand-report endpoint.

    Returns:
        BytesIO buffer containing the PDF.
    """
    brand = data.get("brand", "Unknown")
    snapshot_date = data.get("snapshot_date", "")
    total_skus = data.get("total_skus", 0)
    kpis = data.get("kpis", {})

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        topMargin=38,  # below header bar
        bottomMargin=32,
        leftMargin=20,
        rightMargin=20,
    )

    ss = _styles()
    story = []

    # ── PAGE 1: Cover + KPIs ──────────────────────
    story.append(Spacer(1, 12))
    story.append(Paragraph(
        f"Pricing Intelligence Report",
        ss["CoverTitle"],
    ))
    story.append(Paragraph(
        f"{brand}",
        ParagraphStyle(
            "BrandName", parent=ss["CoverTitle"],
            fontSize=28, leading=34, textColor=CASS_TEAL,
        ),
    ))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f"Generated {datetime.now().strftime('%d %B %Y')} · "
        f"Snapshot date: {snapshot_date} · {total_skus:,} SKUs analysed",
        ss["CoverSub"],
    ))

    # Gold divider
    story.append(HRFlowable(
        width="100%", thickness=1.5, color=CASS_GOLD,
        spaceAfter=16, spaceBefore=8,
    ))

    # KPI cards as a table
    kpi_data = [
        [
            Paragraph("Avg Market Discount", ss["KpiLabel"]),
            Paragraph("SKUs Below Floor", ss["KpiLabel"]),
            Paragraph("Total SKUs Tracked", ss["KpiLabel"]),
            Paragraph("Total Gap Below Floor", ss["KpiLabel"]),
        ],
        [
            Paragraph(_fmt(kpis.get("avg_market_discount_pct"), suffix="%"), ss["KpiValue"]),
            Paragraph(_fmt(kpis.get("skus_below_floor"), decimals=0), ss["KpiValue"]),
            Paragraph(_fmt(total_skus, decimals=0), ss["KpiValue"]),
            Paragraph(_fmt(kpis.get("total_gap_below_floor"), prefix="$"), ss["KpiValue"]),
        ],
    ]
    kpi_table = Table(kpi_data, colWidths=[(PAGE_W - 40) / 4] * 4)
    kpi_table.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, 0), 12),
        ("BOTTOMPADDING", (0, 1), (-1, 1), 12),
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#fafaf7")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#e0ddd7")),
        ("LINEAFTER", (0, 0), (-2, -1), 0.3, colors.HexColor("#e0ddd7")),
    ]))
    story.append(kpi_table)
    story.append(Spacer(1, 10))

    # ── Category Breakdown ─────────────────────────
    cats = data.get("category_breakdown", [])
    if cats:
        story.append(Paragraph("Category Breakdown", ss["SectionHead"]))
        headers = ["Category", "SKUs", "Avg Disc %", "Max Disc %", "Below Floor", "Avg Gap $", "Most Aggressive"]
        rows = []
        for c in cats:
            rows.append([
                c.get("category", "—"),
                _fmt(c.get("sku_count"), decimals=0),
                _fmt(c.get("avg_discount_pct"), suffix="%"),
                _fmt(c.get("max_discount_pct"), suffix="%"),
                _fmt(c.get("skus_below_floor"), decimals=0),
                _fmt(c.get("avg_gap_below"), prefix="$"),
                c.get("most_aggressive") or "—",
            ])
        col_w = [85, 42, 58, 58, 55, 58, PAGE_W - 40 - 85 - 42 - 58 - 58 - 55 - 58]
        story.append(_make_table(headers, rows, col_w))
        story.append(Spacer(1, 8))

    # ── Collection Breakdown ───────────────────────
    cols = data.get("collection_breakdown", [])
    if cols:
        story.append(Paragraph("Collection Breakdown", ss["SectionHead"]))
        headers = ["Collection", "SKUs", "Avg Disc %", "Below Floor", "Avg Gap $", "Most Aggressive"]
        rows = []
        # Show top 25 collections to keep it manageable
        for c in cols[:25]:
            rows.append([
                c.get("collection", "—"),
                _fmt(c.get("sku_count"), decimals=0),
                _fmt(c.get("avg_discount_pct"), suffix="%"),
                _fmt(c.get("skus_below_floor"), decimals=0),
                _fmt(c.get("avg_gap_below"), prefix="$"),
                c.get("most_aggressive") or "—",
            ])
        col_w = [100, 42, 65, 60, 65, PAGE_W - 40 - 100 - 42 - 65 - 60 - 65]
        story.append(_make_table(headers, rows, col_w))
        if len(cols) > 25:
            story.append(Paragraph(
                f"<i>Showing top 25 of {len(cols)} collections (sorted by avg discount %)</i>",
                ParagraphStyle("Note", parent=ss["TableCell"], fontSize=7, textColor=colors.HexColor("#999")),
            ))
        story.append(Spacer(1, 8))

    # ── PAGE BREAK ────────────────────────────────
    story.append(PageBreak())

    # ── Competitor Activity ────────────────────────
    comps = data.get("competitor_activity", [])
    if comps:
        story.append(Paragraph("Competitor Activity", ss["SectionHead"]))
        headers = ["Competitor", "Times Below Floor", "Avg Gap $", "Max Gap $", "Top Category", "Top Collection"]
        rows = []
        for c in comps:
            rows.append([
                c.get("competitor", "—"),
                _fmt(c.get("times_below_floor"), decimals=0),
                _fmt(c.get("avg_gap_when_below"), prefix="$"),
                _fmt(c.get("max_gap"), prefix="$"),
                c.get("top_category") or "—",
                c.get("top_collection") or "—",
            ])
        col_w = [105, 70, 65, 65, 75, PAGE_W - 40 - 105 - 70 - 65 - 65 - 75]
        story.append(_make_table(headers, rows, col_w))
        story.append(Spacer(1, 12))

    # ── Monthly Trends Chart ──────────────────────
    trends = data.get("monthly_trends", [])
    if trends:
        story.append(Paragraph("Monthly Trends", ss["SectionHead"]))
        chart_buf = _build_trend_chart(trends)
        if chart_buf:
            img = Image(chart_buf, width=480, height=192)
            story.append(img)
            story.append(Spacer(1, 8))

        # Trends table
        headers = ["Month", "Avg Discount %", "Below Floor", "Avg Gap $", "Total SKUs", "Snapshots"]
        rows = []
        for t in trends:
            rows.append([
                t.get("month_name", "—"),
                _fmt(t.get("avg_discount_pct"), suffix="%"),
                _fmt(t.get("skus_below_floor"), decimals=0),
                _fmt(t.get("avg_gap_below"), prefix="$"),
                _fmt(t.get("total_skus"), decimals=0),
                _fmt(t.get("snapshot_count"), decimals=0),
            ])
        col_w = [80, 70, 65, 65, 65, PAGE_W - 40 - 80 - 70 - 65 - 65 - 65]
        story.append(_make_table(headers, rows, col_w))
        story.append(Spacer(1, 8))

    # ── PAGE BREAK ────────────────────────────────
    story.append(PageBreak())

    # ── Heavily Discounted SKUs ────────────────────
    skus = data.get("heavily_discounted_skus", [])
    if skus:
        story.append(Paragraph("Heavily Discounted SKUs", ss["SectionHead"]))
        story.append(Paragraph(
            f"<i>Top {len(skus)} SKUs by market discount %. "
            f"'Below Floor' indicates the market price is below our minimum price.</i>",
            ParagraphStyle("Note", parent=ss["TableCell"], fontSize=7,
                           textColor=colors.HexColor("#666"), spaceAfter=6),
        ))
        headers = ["SKU", "Title", "Cat.", "RRP", "Our Min", "Market Low", "Disc %", "Floor?", "Gap $", "Cheapest"]
        cell_style = ss["TableCell"]
        rows = []
        for s in skus:
            title = s.get("title", "")
            rows.append([
                Paragraph(s.get("sku", "—"), cell_style),
                Paragraph(title, cell_style),
                s.get("category", "—"),
                _fmt(s.get("rrp"), prefix="$"),
                _fmt(s.get("our_min"), prefix="$"),
                _fmt(s.get("market_lowest"), prefix="$"),
                _fmt(s.get("market_discount_pct"), suffix="%"),
                "Yes" if s.get("below_floor") else "—",
                _fmt(s.get("gap"), prefix="$") if s.get("below_floor") else "—",
                s.get("cheapest_competitor") or "—",
            ])
        col_w = [62, 150, 42, 46, 46, 46, 36, 32, 36, PAGE_W - 40 - 62 - 150 - 42 - 46 - 46 - 46 - 36 - 32 - 36]
        # Left-align text columns: Title (1), Cat (2), Cheapest (9)
        story.append(_make_table(headers, rows, col_w, left_align_cols={1, 2, 9}))

    # ── BUILD PDF ─────────────────────────────────
    def on_page(canvas, doc):
        _header_footer(canvas, doc, brand)

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    buf.seek(0)
    return buf
