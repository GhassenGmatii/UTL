"""
utils.py — Fonctions utilitaires partagées
===========================================
Normalisation de texte, téléphone, email, dates et
helpers JSON / horodatage.
"""

import json
import re
from datetime import datetime
from dateutil import parser

# Regex de validation d'email (format simple : xxx@xxx.xxx)
EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def safe_strip(x):
    """Convertit en chaîne et supprime les espaces, retourne None si vide."""
    if x is None:
        return None
    return str(x).strip()


def normalize_text(x):
    """Normalise un texte : strip + minuscules. Retourne None si vide."""
    x = safe_strip(x)
    return x.lower() if x else None


def normalize_phone(x):
    """
    Normalise un numéro de téléphone en gardant uniquement les chiffres.
    Retourne None si le résultat a moins de 8 chiffres.
    """
    x = safe_strip(x)
    if not x:
        return None
    digits = re.sub(r"\D", "", x)          # Supprime tout ce qui n'est pas un chiffre
    return digits if len(digits) >= 8 else None


def is_valid_email(x):
    """
    Vérifie qu'une adresse email respecte le format standard.
    Retourne False si x est None ou vide.
    """
    if not x:
        return False
    return bool(EMAIL_REGEX.match(str(x).strip().lower()))


def parse_date_safe(x):
    """
    Tente de parser une date dans n'importe quel format courant.
    Retourne la date au format ISO 'YYYY-MM-DD' ou None si échec.
    """
    if not x:
        return None
    try:
        dt = parser.parse(str(x), dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def save_json(path, payload):
    """Sérialise `payload` en JSON indenté et l'écrit dans `path`."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def now_iso():
    """Retourne l'horodatage UTC actuel au format ISO 8601."""
    return datetime.utcnow().isoformat() + "Z"
