import argparse
from .ingest import load_dataset
from .clean_rules import apply_cleaning_rules
from .dedup_ml import train_dedup_model, save_dedup_model
from .anomaly import train_anomaly_model, save_anomaly_model
from .config import DEDUP_MODEL_PATH, ANOMALY_MODEL_PATH

def main(input_path: str):
    print("[1/3] Chargement des données...")
    raw = load_dataset(input_path)
    print(f"  -> {len(raw)} lignes chargées")

    print("[2/3] Nettoyage des données...")
    clean_df, clean_report = apply_cleaning_rules(raw)
    print(f"  -> {len(clean_df)} lignes après nettoyage")

    print("[3/3] Entraînement des modèles...")
    dedup_model = train_dedup_model(clean_df)
    if dedup_model:
        save_dedup_model(dedup_model, DEDUP_MODEL_PATH)
        print(f"  -> Modèle déduplication sauvegardé: {DEDUP_MODEL_PATH}")
    
    anomaly_model = train_anomaly_model(clean_df)
    if anomaly_model:
        save_anomaly_model(anomaly_model, ANOMALY_MODEL_PATH)
        print(f"  -> Modèle anomalies sauvegardé: {ANOMALY_MODEL_PATH}")
    
    print("\n✅ Entraînement terminé !")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path du CSV brut")
    args = parser.parse_args()
    main(args.input)
