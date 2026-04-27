import argparse

from .ingest import load_dataset
from .clean_rules import apply_cleaning_rules
from .dedup_ml import load_dedup_model, predict_duplicate_pairs, drop_predicted_duplicates
from .anomaly import load_anomaly_model, predict_anomalies
from .utils import save_json, now_iso
from .config import (
    CLEAN_OUTPUT,
    DEDUP_OUTPUT,
    QUALITY_REPORT_PATH,
    DEDUP_MODEL_PATH,
    ANOMALY_MODEL_PATH
)


def main(input_path: str):
    raw = load_dataset(input_path)

    clean_df, clean_report = apply_cleaning_rules(raw)
    clean_df.to_csv(CLEAN_OUTPUT, index=False)

    dedup_report = {"ml_duplicates_found": 0, "dropped_indices": []}
    try:
        dedup_model = load_dedup_model(DEDUP_MODEL_PATH)
        dup_pairs = predict_duplicate_pairs(clean_df, dedup_model, threshold=0.75)
        dedup_df, dropped = drop_predicted_duplicates(clean_df, dup_pairs)
        dedup_report["ml_duplicates_found"] = len(dup_pairs)
        dedup_report["dropped_indices"] = dropped
    except Exception:
        dedup_df = clean_df.copy()

    anomaly_report = {"anomalies_found": 0}
    try:
        anomaly_model = load_anomaly_model(ANOMALY_MODEL_PATH)
        scored_df = predict_anomalies(dedup_df, anomaly_model)
        anomaly_report["anomalies_found"] = int(scored_df["is_anomaly"].sum())
        output_df = scored_df
    except Exception:
        output_df = dedup_df

    output_df.to_csv(DEDUP_OUTPUT, index=False)

    final_report = {
        "generated_at": now_iso(),
        "input_path": input_path,
        "outputs": {
            "clean_csv": str(CLEAN_OUTPUT),
            "dedup_csv": str(DEDUP_OUTPUT),
        },
        "cleaning": clean_report,
        "deduplication": dedup_report,
        "anomaly": anomaly_report,
        "final_rows": int(len(output_df)),
    }

    save_json(QUALITY_REPORT_PATH, final_report)
    print("Pipeline terminé ✅")
    print(f"- Clean: {CLEAN_OUTPUT}")
    print(f"- Dedup/Scored: {DEDUP_OUTPUT}")
    print(f"- Report: {QUALITY_REPORT_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path du CSV brut")
    args = parser.parse_args()
    main(args.input)
