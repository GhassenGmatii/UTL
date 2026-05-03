"""
ingest.py — Chargement des données brutes
==========================================
Lit un fichier CSV et retourne le DataFrame ainsi que
la liste normalisée des noms de colonnes.
"""

import pandas as pd


def load_dataset(path: str):
    """
    Charge un fichier CSV depuis le chemin donné.

    Paramètres
    ----------
    path : str
        Chemin vers le fichier CSV à charger.

    Retourne
    --------
    df : pd.DataFrame
        Données brutes.
    detected_cols : list[str]
        Noms de colonnes normalisés (minuscules, sans espaces).
    """
    df = pd.read_csv(path)
    detected_cols = [col.strip().lower() for col in df.columns]
    return df, detected_cols
