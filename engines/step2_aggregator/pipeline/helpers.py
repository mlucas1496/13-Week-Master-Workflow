"""
Shared helper functions: date/amount parsing, string cleaning, FX lookup.
"""
import re
from datetime import datetime, timedelta

from config import STABLECOINS

# Excel epoch: 1899-12-30
_EPOCH = datetime(1899, 12, 30)


def date_to_serial(d):
    """Convert a datetime to Excel serial date number."""
    if d is None:
        return None
    delta = d - _EPOCH
    return delta.days


def serial_to_date(serial):
    """Convert an Excel serial date number to datetime."""
    if serial is None:
        return None
    return _EPOCH + timedelta(days=int(serial))


def serial_to_ymd(serial):
    """Convert an Excel serial date number to 'YYYY-MM-DD' string."""
    if serial is None:
        return ""
    d = serial_to_date(serial)
    return d.strftime("%Y-%m-%d")


def fmt_date(serial):
    """Convert Excel serial to 'M/D/YYYY' format."""
    if serial is None:
        return ""
    d = serial_to_date(serial)
    return f"{d.month}/{d.day}/{d.year}"


def parse_date_str(s):
    """Parse 'M/D/YYYY' or 'YYYY-MM-DD' string to Excel serial number."""
    if not s:
        return None
    s = str(s).strip()
    # Try M/D/YYYY
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', s)
    if m:
        dt = datetime(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        return date_to_serial(dt)
    # Try YYYY-MM-DD (possibly with time)
    m2 = re.match(r'^(\d{4})-(\d{2})-(\d{2})', s)
    if m2:
        dt = datetime(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
        return date_to_serial(dt)
    return None


def parse_iso_date(s):
    """Extract YYYY-MM-DD from an ISO datetime string."""
    if not s:
        return None
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', str(s))
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def parse_amount(v):
    """Parse a currency string like '(1,234.56)' to float. Returns 0 on failure."""
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    s = s.replace("$", "").replace(",", "")
    try:
        n = float(s)
    except ValueError:
        return 0.0
    return -n if neg else n


def clean_notes(s):
    """Remove all non-alphanumeric characters and uppercase."""
    if not s:
        return ""
    return re.sub(r'[^A-Za-z0-9]', '', str(s)).upper()


def clean_notes_underscore(s):
    """Replace non-alphanumeric runs with underscores and uppercase."""
    if not s:
        return ""
    return re.sub(r'[^a-zA-Z0-9]+', '_', str(s)).upper()


def is_stablecoin(code):
    """Check if an asset code is a stablecoin."""
    return str(code or "").upper().strip() in STABLECOINS


def fx_lookup(fx_map, date_ymd, ccy):
    """
    Look up FX rate for date|CCY, with +/-7 day fallback.
    Returns rate (float) or None.
    """
    if not ccy or ccy == "USD":
        return 1.0
    key = f"{date_ymd}|{ccy}"
    if key in fx_map:
        return fx_map[key]
    # Try +/-1..7 days
    try:
        base = datetime.strptime(date_ymd, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None
    for offset in range(1, 8):
        for direction in (-1, 1):
            nd = base + timedelta(days=direction * offset)
            nk = f"{nd.strftime('%Y-%m-%d')}|{ccy}"
            if nk in fx_map:
                return fx_map[nk]
    return None
