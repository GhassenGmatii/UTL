"""
features.py — Extraction de features pour la déduplication ML
==============================================================
Compare les paires de lignes (nom, email, téléphone) via
RapidFuzz pour produire un score de similarité utilisé par
le modèle de déduplication.

Les colonnes sont détectées automatiquement par aliases,
donc le module fonctionne avec n'importe quel format CSV.
"""

import pandas as pd
from rapidfuzz import fuzz

# ── Aliases de colonnes ────────────────────────────────────────────────────────
_NAME_ALIASES  = ["name", "nom", "prenom", "full_name", "patient_name", "client"]
_EMAIL_ALIASES = ["email", "mail", "e_mail", "courriel"]
_PHONE_ALIASES = ["phone", "telephone", "tel", "mobile", "gsm"]


def _find_col(df_cols: list, aliases: list):
    """
    Cherche la première colonne du DataFrame qui correspond à un alias connu.

    Paramètres
    ----------
    df_cols : list  — Colonnes du DataFrame (en minuscules)
    aliases : list  — Noms de colonnes acceptés

    Retourne
    --------
    Nom réel de la colonne, ou None si aucun alias ne correspond.
    """
    cols_lower = {c.lower(): c for c in df_cols}
    for alias in aliases:
        if alias in cols_lower:
            return cols_lower[alias]
    return None


# ── Fonctions de similarité ────────────────────────────────────────────────────

def compute_name_similarity(name1, name2) -> float:
    """
    Score de similarité entre deux noms (0.0 → 1.0) via token_set_ratio.
    Retourne 0.0 si l'une des valeurs est NaN/None.
    """
    if pd.isna(name1) or pd.isna(name2):
        return 0.0
    return fuzz.token_set_ratio(str(name1), str(name2)) / 100.0


def compute_email_similarity(email1, email2) -> float:
    """
    Score de similarité entre deux emails (0.0 → 1.0) via ratio exact.
    Retourne 0.0 si l'une des valeurs est NaN/None.
    """
    if pd.isna(email1) or pd.isna(email2):
        return 0.0
    return fuzz.ratio(str(email1), str(email2)) / 100.0


def compute_phone_similarity(phone1, phone2) -> float:
    """
    Score de similarité entre deux numéros de téléphone (0.0 → 1.0).
    Retourne 0.0 si l'une des valeurs est NaN/None.
    """
    if pd.isna(phone1) or pd.isna(phone2):
        return 0.0
    return fuzz.ratio(str(phone1), str(phone2)) / 100.0


def create_pair_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Génère un DataFrame de features pour toutes les paires (i, j) avec i < j.

    Pour chaque paire, calcule :
    - name_sim  : similarité de nom
    - email_sim : similarité d'email
    - phone_sim : similarité de téléphone
    - avg_sim   : moyenne des trois scores

    Retourne un DataFrame vide si aucune colonne pertinente n'est trouvée.
    """
    cols = df.columns.tolist()

    # Détection automatique des colonnes utilisables
    name_col  = _find_col(cols, _NAME_ALIASES)
    email_col = _find_col(cols, _EMAIL_ALIASES)
    phone_col = _find_col(cols, _PHONE_ALIASES)

    # Aucune colonne utile → on ne peut pas créer de features
    if not any([name_col, email_col, phone_col]):
        return pd.DataFrame()

    pairs = []
    n = len(df)

    for i in range(n):
        for j in range(i + 1, n):
            row_i = df.iloc[i]
            row_j = df.iloc[j]

            name_sim  = compute_name_similarity(
                row_i[name_col]  if name_col  else None,
                row_j[name_col]  if name_col  else None,
            )
            email_sim = compute_email_similarity(
                row_i[email_col] if email_col else None,
                row_j[email_col] if email_col else None,
            )
            phone_sim = compute_phone_similarity(
                row_i[phone_col] if phone_col else None,
                row_j[phone_col] if phone_col else None,
            )

            pairs.append({
                "idx_i":     i,
                "idx_j":     j,
                "name_sim":  name_sim,
                "email_sim": email_sim,
                "phone_sim": phone_sim,
                "avg_sim":   (name_sim + email_sim + phone_sim) / 3.0,
            })

    return pd.DataFrame(pairs) if pairs else pd.DataFrame()


def extract_features(df: pd.DataFrame) -> pd.DataFrame:
    """Point d'entrée principal — délègue à create_pair_features."""
    return create_pair_features(df)
