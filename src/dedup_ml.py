"""
dedup_ml.py — Déduplication par Machine Learning
=================================================
Entraîne un RandomForestClassifier pour détecter les paires
de lignes dupliquées en se basant sur les scores de similarité
produits par features.py.
"""

import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from .features import extract_features


def train_dedup_model(df_clean: pd.DataFrame):
    """
    Entraîne un modèle de déduplication sur les paires de lignes.

    Stratégie d'étiquetage automatique :
    - avg_sim > 0.7  →  label = 1 (doublon probable)
    - avg_sim ≤ 0.7  →  label = 0 (lignes distinctes)

    Retourne None si le DataFrame n'a pas assez de données
    pour générer des paires de features.
    """
    features_df = extract_features(df_clean)

    # Pas assez de paires pour entraîner
    if features_df is None or len(features_df) == 0:
        return None

    X = features_df[["name_sim", "email_sim", "phone_sim", "avg_sim"]].values
    y = (features_df["avg_sim"] > 0.7).astype(int).values

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)
    return model


def load_dedup_model(path):
    """
    Charge un modèle de déduplication depuis un fichier .joblib.
    Retourne None si le fichier n'existe pas ou est corrompu.
    """
    try:
        return joblib.load(path)
    except Exception:
        return None


def predict_duplicate_pairs(df: pd.DataFrame, model, threshold: float = 0.75) -> list:
    """
    Prédit les paires de lignes dupliquées.

    Paramètres
    ----------
    df        : DataFrame à analyser
    model     : modèle RandomForest entraîné
    threshold : seuil de probabilité pour qualifier un doublon (défaut 0.75)

    Retourne
    --------
    Liste de paires [idx_i, idx_j] dont la probabilité de doublon ≥ threshold.
    """
    features_df = extract_features(df)

    # Aucune paire générée (CSV trop petit ou colonnes introuvables)
    if features_df is None or len(features_df) == 0:
        return []

    X = features_df[["name_sim", "email_sim", "phone_sim", "avg_sim"]].values

    try:
        proba = model.predict_proba(X)[:, 1]       # Probabilité de classe "doublon"
        dup_pairs = (
            features_df[proba >= threshold][["idx_i", "idx_j"]]
            .values.tolist()
        )
        return dup_pairs
    except Exception as e:
        print(f"[dedup] Erreur predict_proba : {e}")
        return []


def drop_predicted_duplicates(df: pd.DataFrame, dup_pairs: list) -> tuple:
    """
    Supprime les doublons détectés du DataFrame.

    Pour chaque paire (i, j), on conserve i et supprime j.

    Retourne
    --------
    (df_deduped, dropped_indices) :
        df_deduped       : DataFrame sans les doublons
        dropped_indices  : liste des indices supprimés
    """
    indices_to_drop = {j for _, j in dup_pairs}     # On garde toujours le premier (i)
    dropped = list(indices_to_drop)
    df_deduped = df.drop(index=dropped).reset_index(drop=True)
    return df_deduped, dropped


def save_dedup_model(model, path):
    """Sauvegarde le modèle de déduplication dans un fichier .joblib."""
    joblib.dump(model, path)
