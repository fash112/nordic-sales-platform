"""
Generate synthetic sales data for four Nordic countries.

The point of this generator is the DIVERGENCE. Each country emits data the way a
real ERP export from that market plausibly would -- different column names, date
formats, decimal separators, encodings, and file formats. Reconciling that mess
is the actual engineering problem the platform solves, so the generator has to
create the mess faithfully rather than emitting four clean identical files.

Divergence matrix
-----------------
                 SE              NO              DK              FI
format           CSV ;           CSV ,           JSON lines      CSV \t
encoding         utf-8           latin-1         utf-8           utf-8-sig
date             YYYY-MM-DD      DD.MM.YYYY      DD/MM/YYYY       YYYYMMDD
decimal          1234,56         1234.56         1234,56          1234,56
thousands        space           none            .                none
currency         SEK             NOK             DKK              EUR
null marker      ""              NULL            null             -
customer id      cust_id         KundeNr         customer_no      asiakas_id
quantity         antal           antall          antal            maara
negative qty     returns         returns         returns          returns

Run:  python src/generate/generate_sources.py --rows 5000 --seed 42
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import date, timedelta
from pathlib import Path

RAW = Path(__file__).resolve().parents[2] / "data" / "raw"

PRODUCTS = [
    ("SKU-1001", "Ergonomic Desk Chair", "Furniture", 2400.00),
    ("SKU-1002", "Standing Desk 140cm", "Furniture", 5200.00),
    ("SKU-1003", "Monitor Arm Dual", "Accessories", 890.00),
    ("SKU-1004", "Mechanical Keyboard", "Peripherals", 1150.00),
    ("SKU-1005", "27in 4K Monitor", "Displays", 4300.00),
    ("SKU-1006", "USB-C Dock", "Accessories", 1750.00),
    ("SKU-1007", "Noise Cancelling Headset", "Peripherals", 2100.00),
    ("SKU-1008", "Laptop Stand Aluminium", "Accessories", 640.00),
    ("SKU-1009", "Webcam 1080p", "Peripherals", 780.00),
    ("SKU-1010", "Desk Lamp LED", "Furniture", 520.00),
]

CHANNELS = ["retail", "online", "partner", "direct"]

# Deliberately includes non-ASCII names. Latin-1 encoding on the Norwegian file
# means these round-trip badly if the reader assumes UTF-8 -- which is the point.
CUSTOMER_NAMES = [
    "Lindqvist AB", "Nordström Handel", "Bergström & Söner", "Ålesund Kontor",
    "Møller Innredning", "Kjærstad Utstyr", "Rødvig Møbler", "Søndergaard A/S",
    "Kärkkäinen Oy", "Mäkinen Toimisto", "Väisänen Kaluste", "Hämäläinen Oy",
    "Dansk Kontormiljø", "Jyllands Inventar", "Uppsala Interiör", "Trondheim Utstyr",
]


def _rows(n: int, rng: random.Random, start: date, days: int):
    """Shared record generation before per-country formatting is applied."""
    out = []
    for i in range(n):
        sku, name, category, base_price = rng.choice(PRODUCTS)
        qty = rng.randint(1, 12)

        # ~3% returns, expressed as negative quantity. Silver must preserve
        # these rather than filtering them, or revenue overstates.
        if rng.random() < 0.03:
            qty = -rng.randint(1, 3)

        # Price drifts +/-15% around base to simulate discounting.
        unit_price = round(base_price * rng.uniform(0.85, 1.15), 2)

        out.append(
            {
                "order_id": f"{i + 1:08d}",
                "order_date": start + timedelta(days=rng.randint(0, days)),
                "customer_no": f"C{rng.randint(1000, 1400)}",
                "customer_name": rng.choice(CUSTOMER_NAMES),
                "sku": sku,
                "product_name": name,
                "category": category,
                "quantity": qty,
                "unit_price": unit_price,
                "channel": rng.choice(CHANNELS),
            }
        )
    return out


def _sv_decimal(v: float) -> str:
    """Swedish: comma decimal, space thousands separator. '12 345,67'"""
    whole, frac = f"{v:.2f}".split(".")
    grouped = ""
    for idx, ch in enumerate(reversed(whole)):
        if idx and idx % 3 == 0:
            grouped = " " + grouped
        grouped = ch + grouped
    return f"{grouped},{frac}"


def _dk_decimal(v: float) -> str:
    """Danish: comma decimal, dot thousands. '12.345,67'"""
    whole, frac = f"{v:.2f}".split(".")
    grouped = ""
    for idx, ch in enumerate(reversed(whole)):
        if idx and idx % 3 == 0:
            grouped = "." + grouped
        grouped = ch + grouped
    return f"{grouped},{frac}"


def write_sweden(records, rng):
    """CSV, semicolon delimited, UTF-8, ISO dates, comma decimals, empty nulls."""
    path = RAW / "sweden" / "sales_se.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "order_id;order_date;cust_id;kundnamn;artikelnr;produkt;kategori;antal;pris;kanal"
    lines = [header]
    for r in records:
        # ~2% missing channel, emitted as an empty field.
        channel = "" if rng.random() < 0.02 else r["channel"]
        lines.append(
            ";".join(
                [
                    r["order_id"],
                    r["order_date"].strftime("%Y-%m-%d"),
                    r["customer_no"],
                    r["customer_name"],
                    r["sku"],
                    r["product_name"],
                    r["category"],
                    str(r["quantity"]),
                    _sv_decimal(r["unit_price"]),
                    channel,
                ]
            )
        )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_norway(records, rng):
    """CSV, comma delimited, LATIN-1, DD.MM.YYYY dates, dot decimals, 'NULL' marker."""
    path = RAW / "norway" / "sales_no.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "OrdreNr,Dato,KundeNr,KundeNavn,VareNr,Produkt,Kategori,Antall,Pris,Kanal"
    lines = [header]
    for r in records:
        channel = "NULL" if rng.random() < 0.02 else r["channel"]
        lines.append(
            ",".join(
                [
                    r["order_id"],
                    r["order_date"].strftime("%d.%m.%Y"),
                    r["customer_no"],
                    r["customer_name"].replace(",", ""),  # naive: strips commas
                    r["sku"],
                    r["product_name"],
                    r["category"],
                    str(r["quantity"]),
                    f'{r["unit_price"]:.2f}',
                    channel,
                ]
            )
        )
    # latin-1 cannot represent every character; replacement is deliberate so the
    # pipeline has real mojibake to detect rather than a theoretical risk.
    path.write_bytes("\n".join(lines).encode("latin-1", errors="replace"))
    return path


def write_denmark(records, rng):
    """JSON lines, UTF-8, DD/MM/YYYY, comma decimals with dot thousands, null."""
    path = RAW / "denmark" / "sales_dk.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            channel = None if rng.random() < 0.02 else r["channel"]
            fh.write(
                json.dumps(
                    {
                        "ordre_id": r["order_id"],
                        "dato": r["order_date"].strftime("%d/%m/%Y"),
                        "kunde_nr": r["customer_no"],
                        "kunde_navn": r["customer_name"],
                        "vare_nr": r["sku"],
                        "produkt": r["product_name"],
                        "kategori": r["category"],
                        "antal": r["quantity"],
                        "pris": _dk_decimal(r["unit_price"]),
                        "kanal": channel,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    return path


def write_finland(records, rng):
    """TSV, UTF-8 WITH BOM, YYYYMMDD dates, comma decimals, '-' null marker."""
    path = RAW / "finland" / "sales_fi.tsv"
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "tilaus_id\tpaivamaara\tasiakas_id\tasiakas_nimi\ttuote_koodi\ttuote\tluokka\tmaara\thinta\tkanava"
    lines = [header]
    for r in records:
        channel = "-" if rng.random() < 0.02 else r["channel"]
        lines.append(
            "\t".join(
                [
                    r["order_id"],
                    r["order_date"].strftime("%Y%m%d"),
                    r["customer_no"],
                    r["customer_name"],
                    r["sku"],
                    r["product_name"],
                    r["category"],
                    str(r["quantity"]),
                    f'{r["unit_price"]:.2f}'.replace(".", ","),
                    channel,
                ]
            )
        )
    # utf-8-sig writes a BOM. Readers that ignore it get a corrupted first header.
    path.write_text("\n".join(lines), encoding="utf-8-sig")
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5000, help="rows per country")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--days", type=int, default=730, help="date range span")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    start = date(2024, 1, 1)

    written = []
    for writer in (write_sweden, write_norway, write_denmark, write_finland):
        records = _rows(args.rows, rng, start, args.days)

        # ~1% exact duplicate orders per country. Silver must dedupe on the
        # natural key, and the dbt tests must prove it did.
        dupes = rng.sample(records, max(1, len(records) // 100))
        records.extend(dupes)

        written.append(writer(records, rng))

    for p in written:
        print(f"wrote {p.relative_to(RAW.parents[1])}  ({p.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
