# 🧹 UTL - Data Cleaning & ML Pipeline

**UTL** (Unified Treatment Layer) est un projet **ETL + Machine Learning** pour automatiser le nettoyage et la déduplication de datasets clients avec détection d'anomalies.

---

## 📋 Table des matières

- [Fonctionnalités](#fonctionnalités)
- [Architecture](#architecture)
- [Installation](#installation)
- [Utilisation](#utilisation)
- [Fichiers générés](#fichiers-générés)
- [Dépendances](#dépendances)

---

## ✨ Fonctionnalités

### 1. **Nettoyage des données** 🧼
- Normalisation des colonnes (lowercase)
- Normalisation du texte (trim, lowercase)
- Validation et normalisation des emails (regex)
- Normalisation des numéros de téléphone
- Parsing flexible des dates (2024-01-10, 10/01/2024, 2024/01/10)
- Suppression des doublons exacts
- Détection des valeurs négatives

### 2. **Déduplication avec ML** 🤖
- Fuzzy matching des paires (noms, emails, téléphones)
- **Modèle : Random Forest Classifier**
- Threshold configurable (défaut: 0.75)
- Identification automatique des clients en doublon

### 3. **Détection d'Anomalies** ⚠️
- **Modèle : Isolation Forest**
- Scoring des anomalies (0.0 à 1.0)
- Classification binaire (normal/anomalous)
- Identification des valeurs extrêmes

### 4. **Rapport de Qualité** 📊
- Statistiques complètes de nettoyage
- Métriques de déduplication
- Résumé des anomalies détectées
- Export JSON automatique

---

## 🏗️ Architecture

```
src/
├── config.py           # Configuration et chemins
├── utils.py            # Fonctions utilitaires
├── ingest.py           # Chargement des données
├── clean_rules.py      # Nettoyage et validation
├── features.py         # Extraction de features (fuzzy matching)
├── dedup_ml.py         # Déduplication avec Random Forest
├── anomaly.py          # Détection d'anomalies (Isolation Forest)
├── evaluate.py         # Évaluation des modèles
├── train.py            # Entraînement des modèles
└── pipeline.py         # Pipeline complet ETL + ML

data/
├── raw/
│   └── customers_raw.csv      # Données brutes
├── processed/
│   ├── customers_clean.csv    # Données nettoyées
│   └── customers_deduped.csv  # Données finales (sans doublons + anomalies)
└── models/
    ├── dedup_model.joblib     # Modèle Random Forest
    └── anomaly_model.joblib   # Modèle Isolation Forest

reports/
└── quality_reports/
    └── quality_report.json     # Rapport de qualité
```

---

## 📦 Installation

### Prérequis
- Python 3.8+
- pip

### Étapes

```bash
# 1. Clone le repository
git clone https://github.com/GhassenGmatii/UTL.git
cd UTL

# 2. Crée un environnement virtuel
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# 3. Installe les dépendances
pip install -r requirements.txt
```

---

## 🚀 Utilisation

### Étape 1 : Entraîner les modèles

```bash
python -m src.train --input data/raw/customers_raw.csv
```

**Génère :**
- `data/models/dedup_model.joblib`
- `data/models/anomaly_model.joblib`

### Étape 2 : Lancer le pipeline complet

```bash
python -m src.pipeline --input data/raw/customers_raw.csv
```

**Génère :**
- `data/processed/customers_clean.csv` ✅
- `data/processed/customers_deduped.csv` ✅
- `reports/quality_reports/quality_report.json` ✅

---

## 📊 Fichiers générés

### 1. `customers_clean.csv`
Données après nettoyage et suppression des doublons exacts.

```csv
customer_id,name,email,phone,city,country,signup_date,amount
1,alice,alice@example.com,33612345678,paris,fr,2024-01-10,120.5
```

### 2. `customers_deduped.csv`
Données finales sans doublons ML + scoring anomalies.

```csv
customer_id,name,email,phone,city,country,signup_date,amount,is_anomaly,anomaly_score
1,alice,alice@example.com,33612345678,paris,fr,2024-01-10,120.5,0,0.15
```

### 3. `quality_report.json`
Rapport complet avec statistiques.

```json
{
  "generated_at": "2026-04-27T20:45:36Z",
  "input_path": "data/raw/customers_raw.csv",
  "cleaning": {
    "rows_before": 7,
    "exact_duplicates_removed": 1,
    "invalid_emails": 1,
    "invalid_dates": 1,
    "negative_amount_count": 1,
    "rows_after": 5
  },
  "deduplication": {
    "ml_duplicates_found": 1,
    "dropped_indices": [2]
  },
  "anomaly": {
    "anomalies_found": 1
  },
  "final_rows": 4
}
```

---

## 🧪 Exemple complet

### Données brutes (input)

```csv
customer_id,name,email,phone,city,country,signup_date,amount
1,  Alice  ,Alice@example.com, +33 6 12 34 56 78 ,Paris,FR,2024-01-10,120.5
2,Bob,bob@example.com,+33-6-11-22-33-44,Lyon,FR,2024/02/03,99.9
3,ALICE,alice@example.com,0612345678,Paris,FR,10-01-2024,120.5
4,Charlie,charlie#example.com,not_a_phone,Marseille,FR,2024-13-01,5000
5,,david@example.com,+216 20 000 000,Tunis,TN,2023-11-22,80
6,Eve,eve@example.com,,Sfax,TN,2024-03-01,-30
7,Bob,bob@example.com,+33-6-11-22-33-44,Lyon,FR,2024/02/03,99.9
```

### Résultat final (output)

```csv
customer_id,name,email,phone,city,country,signup_date,amount,is_anomaly,anomaly_score
2,bob,bob@example.com,33611223344,lyon,fr,2024-02-03,99.9,0,0.12
```

**Changements :**
- ✅ Suppression des doublons exacts (lignes 7)
- ✅ Suppression des données invalides (lignes 4, 5, 6)
- ✅ Détection de doublon ML (Alice - lignes 1 et 3)
- ✅ Normalisation de tous les champs
- ✅ Scoring des anomalies

---

## 📊 Dépendances

```
pandas==2.2.3           # Manipulation de données
numpy==2.1.2            # Opérations numériques
scikit-learn==1.5.2     # Machine Learning (Random Forest, Isolation Forest)
joblib==1.4.2           # Sérialisation des modèles
rapidfuzz==3.10.0       # Fuzzy string matching
python-dateutil==2.9.0  # Parsing flexible de dates
```

---

## 🔧 Configuration

Modifier `src/config.py` pour personnaliser :

```python
# Chemins
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
DATA_MODELS = BASE_DIR / "data" / "models"

# Fichiers
CLEAN_OUTPUT = DATA_PROCESSED / "customers_clean.csv"
DEDUP_OUTPUT = DATA_PROCESSED / "customers_deduped.csv"
QUALITY_REPORT_PATH = REPORTS_DIR / "quality_report.json"
```

---

## 📈 Modèles ML

### Random Forest (Déduplication)
- **Entrée** : Features de similarité fuzzy (name, email, phone)
- **Sortie** : Probabilité de doublon
- **Threshold** : 0.75 (configurable)

### Isolation Forest (Anomalies)
- **Entrée** : Colonnes numériques
- **Sortie** : Anomaly score + classification
- **Contamination** : 0.1 (configurable)

---

## 📝 Licence

MIT License - Libre d'utilisation

---

## 👤 Auteur

**GhassenGmatii** - https://github.com/GhassenGmatii/UTL

---

## 📞 Support

Pour toute question ou bug, ouvrez une issue sur GitHub : https://github.com/GhassenGmatii/UTL/issues

---

**Dernière mise à jour** : 2026-04-27
