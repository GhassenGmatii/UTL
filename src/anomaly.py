"""
anomaly.py — Détection d'anomalies par Isolation Forest
========================================================
Entraîne et applique un modèle IsolationForest sur les
colonnes numériques d'un DataFrame.

Gestion robuste des cas limites :
- DataFrame vide (0 lignes)
- Aucune colonne numérique
- Incompatibilité de features entre le modèle sauvegardé
  et les données courantes (ré-entraînement automatique)
"""

import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import IsolationForest


def train_anomaly_model(df_clean: pd.DataFrame):
    """
    Entraîne un IsolationForest sur les colonnes numériques du DataFrame.

    contamination=0.1 signifie qu'on suppose 10% d'anomalies dans les données.

    Retourne None si :
    - Il n'y a pas de colonnes numériques
    - Le DataFrame est vide après sélection des numériques
    """
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns.tolist()

    if not numeric_cols:
        print("[anomaly] Aucune colonne numérique — modèle non entraîné.")
        return None

    # Remplace les NaN par la moyenne de chaque colonne
    X = df_clean[numeric_cols].fillna(df_clean[numeric_cols].mean())

    if len(X) == 0:
        print("[anomaly] DataFrame vide — modèle non entraîné.")
        return None

    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(X)
    print(f"[anomaly] Modèle entraîné sur {len(X)} lignes, {len(numeric_cols)} features.")
    return model


def load_anomaly_model(path):
    """
    Charge un modèle IsolationForest depuis un fichier .joblib.
    Retourne None si le fichier est absent ou corrompu.
    """
    try:
        return joblib.load(path)
    except Exception:
        return None


def _is_compatible(model, X: pd.DataFrame) -> bool:
    """
    Vérifie que le modèle a été entraîné avec le même nombre de features
    que le DataFrame X courant.

    Retourne True si compatible (ou si l'attribut n'existe pas sur l'ancien modèle).
    """
    try:
        return model.n_features_in_ == X.shape[1]
    except AttributeError:
        return True   # Ancien modèle sans n_features_in_ → on tente


def predict_anomalies(df: pd.DataFrame, model) -> pd.DataFrame:
    """
    Applique le modèle IsolationForest pour annoter chaque ligne.

    Colonnes ajoutées au DataFrame :
    - is_anomaly    : 1 si anomalie détectée, 0 sinon
    - anomaly_score : score d'anomalie (plus élevé = plus suspect)

    Si le modèle est incompatible avec les données actuelles,
    il est ré-entraîné à la volée sur les données courantes.

    En cas d'erreur inattendue, retourne des colonnes à 0 (fail-safe).
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    # ── Cas 1 : DataFrame vide ou sans colonnes numériques ───────────────────
    if not numeric_cols or df.empty or len(df) == 0:
        df = df.copy()
        df["is_anomaly"]    = 0
        df["anomaly_score"] = 0.0
        return df

    # Remplace les NaN par la moyenne de chaque colonne numérique
    X = df[numeric_cols].fillna(df[numeric_cols].mean())

    # ── Cas 2 : X vide après fillna ──────────────────────────────────────────
    if len(X) == 0:
        df = df.copy()
        df["is_anomaly"]    = 0
        df["anomaly_score"] = 0.0
        return df

    # ── Cas 3 : Incompatibilité de features → ré-entraînement ────────────────
    if not _is_compatible(model, X):
        print(
            f"[anomaly] Incompatibilité détectée "
            f"(modèle={model.n_features_in_} features, données={X.shape[1]} features). "
            f"Ré-entraînement automatique..."
        )
        model = train_anomaly_model(df)
        if model is None:
            df = df.copy()
            df["is_anomaly"]    = 0
            df["anomaly_score"] = 0.0
            return df
        # Recalcule X avec le nouveau modèle (mêmes colonnes, déjà filtré)
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        X = df[numeric_cols].fillna(df[numeric_cols].mean())

    # ── Prédiction ────────────────────────────────────────────────────────────
    try:
        predictions = model.predict(X)          # +1 = normal, -1 = anomalie
        scores      = model.score_samples(X)    # Score brut (plus bas = plus anormal)

        df = df.copy()
        df["is_anomaly"]    = (predictions == -1).astype(int)
        df["anomaly_score"] = -scores           # On inverse pour que haut = suspect
    except Exception as e:
        print(f"[anomaly] Erreur inattendue lors de predict : {e} → scores mis à 0")
        df = df.copy()
        df["is_anomaly"]    = 0
        df["anomaly_score"] = 0.0

    return df


def save_anomaly_model(model, path):
    """Sauvegarde le modèle IsolationForest dans un fichier .joblib."""
    joblib.dump(model, path)
