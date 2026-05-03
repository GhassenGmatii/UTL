"""
pipeline.py — Pipeline ETL principal (inférence)
=================================================
Exécute les 5 étapes du pipeline ETL sur un fichier CSV :
  1. Chargement des données brutes
  2. Nettoyage (règles génériques)
  3. Déduplication ML (si modèle disponible)
  4. Détection d'anomalies (si modèle disponible)
  5. Sauvegarde des résultats et du rapport qualité
"""

import sys
import argparse
from .ingest      import load_dataset
from .clean_rules import apply_cleaning_rules
from .dedup_ml    import load_dedup_model, predict_duplicate_pairs, drop_predicted_duplicates
from .anomaly     import load_anomaly_model, predict_anomalies
from .utils       import save_json, now_iso
from .config import (
    CLEAN_OUTPUT,
    DEDUP_OUTPUT,
    QUALITY_REPORT_PATH,
    DEDUP_MODEL_PATH,
    ANOMALY_MODEL_PATH,
)

# Force l'encodage UTF-8 pour la console Windows
sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def main(input_path: str):
    """
    Lance le pipeline ETL complet sur le fichier CSV donné.

    Paramètre
    ---------
    input_path : str
        Chemin vers le fichier CSV brut à traiter.
    """
    print(f"\n Pipeline ETL — fichier : {input_path}\n")

    # ── Etape 1 : Chargement ──────────────────────────────────────────────────
    print("[1/5] Chargement des données...")
    raw, detected_cols = load_dataset(input_path)
    print(f"      {len(raw)} lignes chargées, {len(raw.columns)} colonnes")

    # ── Etape 2 : Nettoyage ───────────────────────────────────────────────────
    print("[2/5] Nettoyage des données...")
    clean_df, clean_report = apply_cleaning_rules(raw, detected_cols)
    clean_df.to_csv(CLEAN_OUTPUT, index=False)
    print(f"      {len(clean_df)} lignes conservées après nettoyage")

    # ── Etape 3 : Déduplication ML ────────────────────────────────────────────
    print("[3/5] Détection des doublons (ML)...")
    dedup_report = {"ml_duplicates_found": 0, "dropped_indices": []}
    dedup_df     = clean_df.copy()

    try:
        dedup_model = load_dedup_model(DEDUP_MODEL_PATH)
        if dedup_model:
            dup_pairs = predict_duplicate_pairs(clean_df, dedup_model, threshold=0.75)
            dedup_df, dropped = drop_predicted_duplicates(clean_df, dup_pairs)
            dedup_report["ml_duplicates_found"] = len(dup_pairs)
            dedup_report["dropped_indices"]     = dropped
            print(f"      {len(dup_pairs)} doublon(s) supprimé(s)")
        else:
            print("      Aucun modèle de déduplication — étape ignorée")
    except Exception as e:
        print(f"      Erreur déduplication (ignorée) : {e}")

    # ── Etape 4 : Détection d'anomalies ───────────────────────────────────────
    print("[4/5] Détection des anomalies (IsolationForest)...")
    anomaly_report = {"anomalies_found": 0}
    output_df      = dedup_df

    try:
        anomaly_model = load_anomaly_model(ANOMALY_MODEL_PATH)
        if anomaly_model:
            output_df = predict_anomalies(dedup_df, anomaly_model)
            anomaly_report["anomalies_found"] = int(output_df["is_anomaly"].sum())
            print(f"      {anomaly_report['anomalies_found']} anomalie(s) détectée(s)")
        else:
            print("      Aucun modèle d'anomalie — étape ignorée")
    except Exception as e:
        print(f"      Erreur anomalie (ignorée) : {e}")

    # ── Etape 5 : Sauvegarde ──────────────────────────────────────────────────
    print("[5/5] Sauvegarde des résultats...")
    output_df.to_csv(DEDUP_OUTPUT, index=False)

    final_report = {
        "generated_at": now_iso(),
        "input_path":   input_path,
        "outputs": {
            "clean_csv": str(CLEAN_OUTPUT),
            "dedup_csv": str(DEDUP_OUTPUT),
        },
        "cleaning":       clean_report,
        "deduplication":  dedup_report,
        "anomaly":        anomaly_report,
        "final_rows":     int(len(output_df)),
    }
    save_json(QUALITY_REPORT_PATH, final_report)

    # ── Résumé ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 50)
    print("Pipeline ETL terminé avec succès !")
    print("=" * 50)
    print(f"  Lignes originales   : {len(raw)}")
    print(f"  Après nettoyage     : {len(clean_df)}")
    print(f"  Après déduplication : {len(dedup_df)}")
    print(f"  Anomalies détectées : {anomaly_report['anomalies_found']}")
    print(f"  Résultat final      : {len(output_df)} lignes")
    print("=" * 50)
    print(f"  CSV nettoyé  : {CLEAN_OUTPUT}")
    print(f"  CSV final    : {DEDUP_OUTPUT}")
    print(f"  Rapport JSON : {QUALITY_REPORT_PATH}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline ETL — traitement d'un fichier CSV")
    parser.add_argument("--input", required=True, help="Chemin du fichier CSV brut")
    args = parser.parse_args()
    main(args.input)
