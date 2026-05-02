# ⚡ Quick Start Guide - UTL

## 🚀 Démarrage rapide en 5 minutes

### 1️⃣ Installation (2 min)

```bash
# Clone le projet
git clone https://github.com/GhassenGmatii/UTL.git
cd UTL

# Crée l'environnement virtuel
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# Installe les dépendances
pip install -r requirements.txt
```

### 2️⃣ Traite tes données (3 min)

```bash
# 1. Entraîne les modèles
python -m src.train --input data/raw/customers_raw.csv

# 2. Lance le pipeline
python -m src.pipeline --input data/raw/customers_raw.csv

# 3. Analyse les données
python -m src.profiling --input data/raw/customers_raw.csv

# 4. Compare les approches
python -m src.benchmark --input data/raw/customers_raw.csv

# 5. Exporte vers SQL Server (optionnel)
python -m src.load_to_db
```

### 3️⃣ Visualise avec le Dashboard

```bash
streamlit run dashboard.py
```

---

## 📂 Fichiers générés

```
data/
├── processed/
│   ├── customers_clean.csv      # Données nettoyées
│   └── customers_deduped.csv    # Résultats finaux
└── models/
    ├── dedup_model.joblib       # Modèle ML déduplication
    └── anomaly_model.joblib     # Modèle ML anomalies

reports/
└── quality_reports/
    ├── quality_report.json      # Rapport de qualité
    ├── profiling_report.json    # Data profiling
    ├── profiling_report.html    # Rapport HTML
    ├── benchmark_report.json    # Benchmark résultats
    └── benchmark_report.html    # Rapport HTML
```

---

## 🎯 Cas d'utilisation

### Nettoyer un dataset
```bash
python -m src.pipeline --input mon_dataset.csv
```

### Analyser la qualité des données
```bash
python -m src.profiling --input mon_dataset.csv
```

### Comparer ETL classique vs IA
```bash
python -m src.benchmark --input mon_dataset.csv
```

### Visualiser en dashboard
```bash
streamlit run dashboard.py
```

---

## 📊 Résultats attendus

**Avant traitement:**
```
100 lignes avec doublons, valeurs manquantes, anomalies
```

**Après traitement:**
```
95 lignes nettoyées sans doublons ML
 Score de qualité: 92/100 (Grade: A)
 3 anomalies détectées
```

---

## 💡 Tips

✅ Utilisez n'importe quel CSV (détection auto des colonnes)
✅ Les modèles s'entraînent automatiquement
✅ Les rapports sont générés en JSON + HTML
✅ Dashboard prêt pour la présentation

---

## ❓ Besoin d'aide?

Consultez:
- 📖 [README.md](README.md) - Documentation complète
- 🐛 [GitHub Issues](https://github.com/GhassenGmatii/UTL/issues)
- 💬 [Discussions](https://github.com/GhassenGmatii/UTL/discussions)

---

**Bon nettoyage de données!** 🧹✨
