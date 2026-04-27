from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
DATA_MODELS = BASE_DIR / "data" / "models"
REPORTS_DIR = BASE_DIR / "reports" / "quality_reports"

CLEAN_OUTPUT = DATA_PROCESSED / "customers_clean.csv"
DEDUP_OUTPUT = DATA_PROCESSED / "customers_deduped.csv"

DEDUP_MODEL_PATH = DATA_MODELS / "dedup_model.joblib"
ANOMALY_MODEL_PATH = DATA_MODELS / "anomaly_model.joblib"

QUALITY_REPORT_PATH = REPORTS_DIR / "quality_report.json"

for p in [DATA_RAW, DATA_PROCESSED, DATA_MODELS, REPORTS_DIR]:
    p.mkdir(parents=True, exist_ok=True)
