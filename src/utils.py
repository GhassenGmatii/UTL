import json
import re
from datetime import datetime
from dateutil import parser

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def safe_strip(x):
    if x is None:
        return None
    return str(x).strip()

def normalize_text(x):
    x = safe_strip(x)
    return x.lower() if x else None

def normalize_phone(x):
    x = safe_strip(x)
    if not x:
        return None
    digits = re.sub(r"\D", "", x)
    return digits if len(digits) >= 8 else None

def is_valid_email(x):
    if not x:
        return False
    return bool(EMAIL_REGEX.match(x.strip().lower()))

def parse_date_safe(x):
    if not x:
        return None
    try:
        dt = parser.parse(str(x), dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def save_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

def now_iso():
    return datetime.utcnow().isoformat() + "Z"
