"""
clean_rules.py — Nettoyage générique des données CSV
=====================================================
Applique des règles de nettoyage adaptées aux colonnes
réellement présentes dans le fichier (emails, téléphones,
dates, montants numériques, textes). Fonctionne avec
n'importe quel format CSV (clients, pharmacie, etc.).
"""

import pandas as pd
from .utils import normalize_text, normalize_phone, is_valid_email, parse_date_safe

# ── Dictionnaires d'aliases ────────────────────────────────────────────────────
# Chaque set regroupe les noms de colonnes possibles pour un même type de donnée.

_EMAIL_ALIASES  = {"email", "e_mail", "mail", "courriel"}

_PHONE_ALIASES  = {"phone", "telephone", "tel", "mobile", "gsm"}

_TEXT_ALIASES   = {
    "name", "nom", "prenom", "city", "ville", "country", "pays",
    "medecin", "specialite", "mutuelle", "medicament", "renouvellement"
}

_DATE_ALIASES   = {
    "signup_date", "date", "date_ordonnance", "date_dispensation",
    "created_at", "updated_at"
}

_AMOUNT_ALIASES = {
    "amount", "prix", "prix_dt", "price", "total", "montant",
    "age", "poids_kg", "dose_par_jour", "duree_traitement"
}

# Colonnes identifiant — exclues du test « ligne entièrement vide »
_ID_COLS = {"customer_id", "patient_id", "id"}


def apply_cleaning_rules(df: pd.DataFrame, detected_cols=None) -> tuple:
    """
    Nettoie un DataFrame en appliquant des règles sur les colonnes détectées.

    Règles appliquées selon le type de colonne :
    - Texte  → normalise (minuscules, strip)
    - Téléphone → chiffres uniquement, min. 8 chiffres
    - Email  → validation regex, invalides remplacés par None
    - Date   → parse multi-format vers 'YYYY-MM-DD'
    - Numérique → conversion pd.to_numeric, décompte négatifs

    Ensuite :
    - Supprime les lignes entièrement vides (hors colonnes ID)
    - Supprime les doublons exacts

    Retourne
    --------
    (df_cleaned, report) :
        df_cleaned : pd.DataFrame nettoyé
        report     : dict avec les métriques de nettoyage
    """
    report = {
        "rows_before":             int(len(df)),
        "exact_duplicates_removed": 0,
        "invalid_emails":          0,
        "invalid_dates":           0,
        "negative_amount_count":   0,
        "rows_after":              0,
    }

    # Normalisation des noms de colonnes
    df.columns = [c.strip().lower() for c in df.columns]
    actual_cols = set(df.columns)

    # ── Normalisation texte ────────────────────────────────────────────────────
    for col in actual_cols & _TEXT_ALIASES:
        df[col] = df[col].apply(normalize_text)

    # ── Normalisation téléphone ────────────────────────────────────────────────
    for col in actual_cols & _PHONE_ALIASES:
        df[col] = df[col].apply(normalize_phone)

    # ── Validation emails ──────────────────────────────────────────────────────
    for col in actual_cols & _EMAIL_ALIASES:
        invalid_mask = ~df[col].apply(is_valid_email)
        report["invalid_emails"] += int(invalid_mask.sum())
        df.loc[invalid_mask, col] = None          # Remplace les emails invalides

    # ── Parsing des dates ──────────────────────────────────────────────────────
    for col in actual_cols & _DATE_ALIASES:
        parsed = df[col].apply(parse_date_safe)
        report["invalid_dates"] += int(parsed.isna().sum())
        df[col] = parsed

    # ── Conversion numérique ───────────────────────────────────────────────────
    for col in actual_cols & _AMOUNT_ALIASES:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        report["negative_amount_count"] += int((df[col] < 0).sum())

    # ── Suppression des lignes entièrement vides ───────────────────────────────
    content_cols = list(actual_cols - _ID_COLS)
    if content_cols:
        df = df.dropna(subset=content_cols, how="all")

    # ── Suppression des doublons exacts ────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates()
    report["exact_duplicates_removed"] = int(before - len(df))

    report["rows_after"] = int(len(df))
    return df.reset_index(drop=True), report
