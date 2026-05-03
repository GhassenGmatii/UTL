"""
config.py — Configuration centrale du projet ETL
=================================================
Ce fichier définit tous les chemins de fichiers et répertoires
utilisés par les différents modules du pipeline.

Il est importé par tous les autres modules pour garantir
la cohérence des chemins sans les dupliquer.

Structure des dossiers générés :
    ETL-project/
    ├── data/
    │   ├── raw/               ← Fichiers CSV bruts importés
    │   ├── processed/         ← CSV nettoyés et dédupliqués
    │   └── models/            ← Modèles ML (.joblib)
    └── reports/
        └── quality_reports/   ← Rapports JSON et HTML
"""

from pathlib import Path

# ── Répertoire racine du projet ───────────────────────────────────────────────
# __file__ = chemin de ce fichier (src/config.py)
# .parent  = dossier src/
# .parent  = dossier racine du projet
BASE_DIR = Path(__file__).resolve().parent.parent

# ── Répertoires de données ────────────────────────────────────────────────────
DATA_RAW       = BASE_DIR / "data" / "raw"         # Données brutes (CSV importés)
DATA_PROCESSED = BASE_DIR / "data" / "processed"   # Données nettoyées/traitées
DATA_MODELS    = BASE_DIR / "data" / "models"       # Modèles ML sauvegardés
REPORTS_DIR    = BASE_DIR / "reports" / "quality_reports"  # Rapports qualité

# ── Fichiers CSV de sortie ────────────────────────────────────────────────────
CLEAN_OUTPUT = DATA_PROCESSED / "customers_clean.csv"    # Après nettoyage (étape 2)
DEDUP_OUTPUT = DATA_PROCESSED / "customers_deduped.csv"  # Après déduplication (étape 3+4)

# ── Fichiers des modèles ML ───────────────────────────────────────────────────
DEDUP_MODEL_PATH   = DATA_MODELS / "dedup_model.joblib"    # RandomForestClassifier
ANOMALY_MODEL_PATH = DATA_MODELS / "anomaly_model.joblib"  # IsolationForest

# ── Fichier de rapport qualité ────────────────────────────────────────────────
QUALITY_REPORT_PATH = REPORTS_DIR / "quality_report.json"  # Rapport du pipeline

# ── Création automatique des répertoires ──────────────────────────────────────
# Si un répertoire n'existe pas encore, il est créé automatiquement
# (parents=True crée tous les répertoires parents manquants)
for _path in [DATA_RAW, DATA_PROCESSED, DATA_MODELS, REPORTS_DIR]:
    _path.mkdir(parents=True, exist_ok=True)
