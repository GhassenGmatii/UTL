"""
train.py — Entraînement des modèles ML
=======================================
Charge les données brutes, les nettoie, puis entraîne
et sauvegarde deux modèles :
  1. Modèle de déduplication (RandomForest)
  2. Modèle de détection d'anomalies (IsolationForest)
"""

import sys
import argparse
from .ingest      import load_dataset
from .clean_rules import apply_cleaning_rules
from .dedup_ml    import train_dedup_model, save_dedup_model
from .anomaly     import train_anomaly_model, save_anomaly_model
from .config      import DEDUP_MODEL_PATH, ANOMALY_MODEL_PATH

# Force l'encodage UTF-8 pour la console Windows
sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def main(input_path: str):
    """
    Entraîne les modèles ML et les sauvegarde sur disque.

    Paramètre
    ---------
    input_path : str
        Chemin vers le fichier CSV d'entraînement.
    """
    print(f"\n Entrainement des modèles — fichier : {input_path}\n")

    # ── Etape 1 : Chargement ──────────────────────────────────────────────────
    print("[1/3] Chargement des données...")
    raw, detected_cols = load_dataset(input_path)
    print(f"      {len(raw)} lignes chargées")

    # ── Etape 2 : Nettoyage ───────────────────────────────────────────────────
    print("[2/3] Nettoyage des données...")
    clean_df, _ = apply_cleaning_rules(raw, detected_cols)
    print(f"      {len(clean_df)} lignes après nettoyage")

    # ── Etape 3 : Entraînement des modèles ───────────────────────────────────
    print("[3/3] Entraînement des modèles...")

    # -- Modèle de déduplication --
    dedup_model = train_dedup_model(clean_df)
    if dedup_model:
        save_dedup_model(dedup_model, DEDUP_MODEL_PATH)
        print(f"      Modèle déduplication sauvegardé : {DEDUP_MODEL_PATH}")
    else:
        print("      Modèle déduplication non entraîné (données insuffisantes)")

    # -- Modèle de détection d'anomalies --
    anomaly_model = train_anomaly_model(clean_df)
    if anomaly_model:
        save_anomaly_model(anomaly_model, ANOMALY_MODEL_PATH)
        print(f"      Modèle anomalies sauvegardé    : {ANOMALY_MODEL_PATH}")
    else:
        print("      Modèle anomalies non entraîné (données insuffisantes)")

    print("\n Entraînement terminé !\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entraînement des modèles ML ETL")
    parser.add_argument("--input", required=True, help="Chemin du fichier CSV")
    args = parser.parse_args()
    main(args.input)
