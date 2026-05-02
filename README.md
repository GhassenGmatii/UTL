# 🧹 UTL - Pipeline de Nettoyage & Machine Learning

**UTL** (Unified Treatment Layer) est un projet **ETL + Machine Learning** pour automatiser le nettoyage et la déduplication de datasets clients avec détection d'anomalies et sauvegarde en SQL Server.

---

## 📋 Table des matières

- [Fonctionnalités](#fonctionnalités)
- [Architecture](#architecture)
- [Installation](#installation)
- [Utilisation](#utilisation)
- [Fichiers générés](#fichiers-générés)
- [Intégration SQL Server](#intégration-sql-server)
- [Dépendances](#dépendances)
- [Configuration](#configuration)
- [Modèles ML](#modèles-ml)
- [Support](#support)

---

## ✨ Fonctionnalités

### 1. **Nettoyage des données** 🧼
- ✅ Normalisation des colonnes (minuscules)
- ✅ Normalisation du texte (trim, minuscules)
- ✅ Validation et normalisation des emails (regex)
- ✅ Normalisation des numéros de téléphone
- ✅ Parsing flexible des dates (2024-01-10, 10/01/2024, 2024/01/10)
- ✅ Suppression des doublons exacts
- ✅ Détection des valeurs négatives
- ✅ **Détection automatique des colonnes** (fonctionne avec n'importe quel CSV!)

### 2. **Déduplication avec Machine Learning** 🤖
- ✅ Fuzzy matching des paires (noms, emails, téléphones)
- ✅ **Modèle : Random Forest Classifier**
- ✅ Threshold configurable (défaut: 0.75)
- ✅ Identification automatique des clients en doublon
- ✅ Suppression intelligente des doublons

### 3. **Détection d'Anomalies** ⚠️
- ✅ **Modèle : Isolation Forest**
- ✅ Scoring des anomalies (0.0 à 1.0)
- ✅ Classification binaire (normal/anormal)
- ✅ Identification des valeurs extrêmes

### 4. **Rapport de Qualité** 📊
- ✅ Statistiques complètes de nettoyage
- ✅ Métriques de déduplication
- ✅ Résumé des anomalies détectées
- ✅ Export JSON automatique

### 5. **Intégration SQL Server** 🗄️ *(NOUVEAU)*
- ✅ Création automatique de la base de données `UTL_DB`
- ✅ Création automatique des tables
- ✅ Import des données nettoyées
- ✅ Import des données finales avec scores
- ✅ Vérification automatique des données

---

## 🏗️ Architecture

```
src/
├── config.py              # Configuration et chemins
├── utils.py               # Fonctions utilitaires
├── ingest.py              # Chargement des données (détection colonnes)
├── clean_rules.py         # Nettoyage et validation dynamique
├── features.py            # Extraction de features (fuzzy matching)
├── dedup_ml.py            # Déduplication avec Random Forest
├── anomaly.py             # Détection d'anomalies (Isolation Forest)
├── evaluate.py            # Évaluation des modèles
├── train.py               # Entraînement des modèles
├── pipeline.py            # Pipeline complet ETL + ML (avec messages FR)
└── load_to_db.py          # Chargement dans SQL Server (NOUVEAU)

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

├── .gitignore
├── README.md
└── requirements.txt
```

---

## 📦 Installation

### Prérequis
- **Python 3.8+**
- **pip**
- **SQL Server Express** (localhost\SQLEXPRESS) *(optionnel pour CSV seul)*

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

### Étape 1️⃣ : Entraîner les modèles

```bash
python -m src.train --input data/raw/customers_raw.csv
```

**Génère :**
- ✅ `data/models/dedup_model.joblib`
- ✅ `data/models/anomaly_model.joblib`

**Résultat :**
```
[1/3] Chargement des données...
  -> 7 lignes chargées
[2/3] Nettoyage des données...
  -> 7 lignes après nettoyage
[3/3] Entraînement des modèles...
  -> Modèle déduplication sauvegardé: data/models/dedup_model.joblib
  -> Modèle anomalies sauvegardé: data/models/anomaly_model.joblib

✅ Entraînement terminé !
```

### Étape 2️⃣ : Lancer le pipeline complet

```bash
python -m src.pipeline --input data/raw/customers_raw.csv
```

**Génère :**
- ✅ `data/processed/customers_clean.csv`
- ✅ `data/processed/customers_deduped.csv`
- ✅ `reports/quality_reports/quality_report.json`

**Résultat :**
```
==================================================
🚀 Traitement du fichier: customers_raw.csv
==================================================

[1/5] Chargement des données...
[2/5] Nettoyage des données...
[3/5] Détection des doublons...
[4/5] Détection des anomalies...
[5/5] Sauvegarde des résultats...

==================================================
✅ Traitement complété avec succès!
==================================================
📊 Fichier original: 7 lignes
🧹 Après nettoyage: 7 lignes
🔄 Après suppression doublons: 5 lignes
⚠️  Lignes anomalies: 0
📈 Résultat final: 5 lignes
==================================================
```

### Étape 3️⃣ : Charger dans SQL Server

```bash
python -m src.load_to_db
```

**Génère :**
- ✅ Base de données: `UTL_DB`
- ✅ Table: `customers_clean`
- ✅ Table: `customers_dedup`

**Résultat :**
```
==================================================
🗄️  Chargement des données dans SQL Server
==================================================

🔗 Serveur: localhost\SQLEXPRESS
📁 Base de données: UTL_DB

[1/5] Création de la base de données...
✅ Base de données créée: UTL_DB

[2/5] Connexion à SQL Server...
✅ Connecté à localhost\SQLEXPRESS\UTL_DB

[3/5] Création des tables...
✅ Table créée: customers_clean
✅ Table créée: customers_dedup

[4/5] Chargement des données nettoyées...
✅ 5 lignes chargées dans customers_clean

[5/5] Chargement des données finales...
✅ 5 lignes chargées dans customers_dedup

==================================================
📊 Vérification des données:
==================================================
📁 Table customers_clean: 5 lignes
📁 Table customers_dedup: 5 lignes
⚠️  Lignes anomalies: 0
==================================================

✅ Toutes les données ont été sauvegardées dans SQL Server avec succès!
```

---

## 📊 Fichiers générés

### 1️⃣ `customers_clean.csv`
Données après nettoyage et suppression des doublons exacts.

```csv
customer_id,name,email,phone,city,country,signup_date,amount
1,alice,alice@example.com,33612345678,paris,fr,2024-01-10,120.5
2,bob,bob@example.com,33611223344,lyon,fr,2024-02-03,99.9
```

### 2️⃣ `customers_deduped.csv`
Données finales sans doublons ML + scoring anomalies.

```csv
customer_id,name,email,phone,city,country,signup_date,amount,is_anomaly,anomaly_score
1,alice,alice@example.com,33612345678,paris,fr,2024-01-10,120.5,0,0.15
2,bob,bob@example.com,33611223344,lyon,fr,2024-02-03,99.9,0,0.12
```

### 3️⃣ `quality_report.json`
Rapport complet avec statistiques.

```json
{
  "generated_at": "2026-05-02T19:00:00Z",
  "input_path": "data/raw/customers_raw.csv",
  "outputs": {
    "clean_csv": "data/processed/customers_clean.csv",
    "dedup_csv": "data/processed/customers_deduped.csv"
  },
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
    "anomalies_found": 0
  },
  "final_rows": 5
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

**Changements appliqués :**
- ✅ Suppression des doublons exacts (lignes 7)
- ✅ Suppression des données invalides (lignes 4, 5, 6)
- ✅ Détection de doublon ML (Alice - lignes 1 et 3)
- ✅ Normalisation de tous les champs
- ✅ Scoring des anomalies

---

## 🗄️ Intégration SQL Server

### Vérifier les données dans SSMS

1. **Ouvrir SQL Server Management Studio**
2. **Serveur** : `localhost\SQLEXPRESS`
3. **Base de données** : `UTL_DB` (créée automatiquement)

### Requêtes utiles

```sql
-- Afficher tous les clients nettoyés
SELECT * FROM customers_clean;

-- Afficher tous les clients avec scores
SELECT * FROM customers_dedup;

-- Afficher les clients anormaux
SELECT * FROM customers_dedup WHERE is_anomaly = 1;

-- Statistiques
SELECT COUNT(*) as total FROM customers_dedup;
SELECT COUNT(*) as anomalies FROM customers_dedup WHERE is_anomaly = 1;

-- Clients par ville
SELECT city, COUNT(*) as count FROM customers_clean GROUP BY city;

-- Montants moyens par pays
SELECT country, AVG(amount) as avg_amount FROM customers_dedup GROUP BY country;
```

---

## 📦 Dépendances

```
pandas==2.2.3             # Manipulation de données
numpy==2.1.2              # Opérations numériques
scikit-learn==1.7.2       # ML (Random Forest, Isolation Forest)
joblib==1.4.2             # Sérialisation des modèles
rapidfuzz==3.10.0         # Fuzzy string matching
python-dateutil==2.9.0    # Parsing flexible de dates
pyodbc==5.1.0             # Connexion SQL Server
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

# SQL Server
SERVER = r'localhost\SQLEXPRESS'
DATABASE = 'UTL_DB'
```

---

## 📈 Modèles ML

### Random Forest (Déduplication)
- **Entrée** : Features de similarité fuzzy (name, email, phone)
- **Sortie** : Probabilité de doublon
- **Threshold** : 0.75 (configurable)
- **N estimateurs** : 100

### Isolation Forest (Anomalies)
- **Entrée** : Colonnes numériques
- **Sortie** : Anomaly score + classification
- **Contamination** : 0.1 (configurable)
- **Random state** : 42

---

## 💡 Cas d'utilisation

✅ **Nettoyage de bases de données clients**
✅ **Déduplication d'emails/contacts**
✅ **Détection de fraude potentielle**
✅ **Amélioration de la qualité des données**
✅ **Préparation des données pour BI/Analytics**
✅ **Pipeline de données automatisé**

---

## ⚡ Astuces

### Avec n'importe quel CSV
Le pipeline détecte automatiquement les colonnes. Fonctionne avec :
- `customer_id`, `user_id`, `id`
- `name`, `nom`, `full_name`
- `email`, `mail`, `email_address`
- `phone`, `telephone`, `mobile`
- `date`, `signup_date`, `created_date`
- `amount`, `montant`, `price`, `total`

### Accélérer le traitement
```bash
# Traiter plusieurs fichiers
python -m src.pipeline --input file1.csv
python -m src.pipeline --input file2.csv
python -m src.load_to_db
```

### Personnaliser les seuils
Modifier dans `src/dedup_ml.py` et `src/anomaly.py`

---

## 📝 Licence

**MIT License** - Libre d'utilisation

---

## 👤 Auteur

**GhassenGmatii** - https://github.com/GhassenGmatii/UTL

---

## 📞 Support

Pour toute question ou bug : https://github.com/GhassenGmatii/UTL/issues

---

## 🎯 Roadmap

- [ ] Interface Web (Flask/Streamlit)
- [ ] API REST
- [ ] Support PostgreSQL/MySQL
- [ ] Visualisations interactives
- [ ] Notifications par email
- [ ] Logs avancés

---

**Dernière mise à jour** : 2 Mai 2026
**Langage** : 🇫🇷 Français | 🇬🇧 English | 🇸🇦 العربية
