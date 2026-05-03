"""
profiling.py — Analyse de qualité des données (Data Profiling)
==============================================================
Génère un rapport complet de qualité des données couvrant :

  1. Statistiques générales (dimensions, mémoire)
  2. Analyse colonne par colonne (type, nulls, uniques, stats)
  3. Valeurs manquantes
  4. Doublons exacts
  5. Valeurs aberrantes (méthode IQR)
  6. Corrélations entre colonnes numériques (r > 0.7)
  7. Test de normalité (Shapiro-Wilk)
  8. Score et grade de qualité global (0-100 / A-D)

Les résultats sont exportés en JSON et HTML interactif.
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import numpy as np
from scipy import stats

from src.config import REPORTS_DIR
from src.ingest import load_dataset

# Force l'encodage UTF-8 pour les emojis sous Windows
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Crée le dossier de rapports s'il n'existe pas
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# Fonctions d'analyse
# ══════════════════════════════════════════════════════════════════════════════

def detect_outliers_iqr(data: pd.Series) -> tuple:
    """
    Détecte les valeurs aberrantes par la méthode IQR (Interquartile Range).

    Une valeur est aberrante si elle dépasse :
      - Borne basse : Q1 - 1.5 × IQR
      - Borne haute : Q3 + 1.5 × IQR

    Retourne
    --------
    (outlier_count, lower_bound, upper_bound)
    """
    Q1  = data.quantile(0.25)
    Q3  = data.quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    outliers = (data < lower) | (data > upper)
    return int(outliers.sum()), lower, upper


def analyze_column(df: pd.DataFrame, col_name: str) -> dict:
    """
    Analyse complète d'une colonne du DataFrame.

    Pour les colonnes numériques : moyenne, médiane, écart-type, min/max,
    quartiles, nombre d'outliers, test de normalité Shapiro-Wilk.

    Pour les colonnes catégorielles : valeur la plus fréquente et son %.

    Retourne un dictionnaire de statistiques.
    """
    col = df[col_name]

    # Détermination du type de colonne
    col_type = "numeric" if col.dtype in [np.int64, np.float64, "int32", "float32"] else "categorical"

    base = {
        "name":             col_name,
        "type":             col_type,
        "non_null_count":   int(col.notna().sum()),
        "null_count":       int(col.isna().sum()),
        "null_percentage":  round(col.isna().mean() * 100, 2),
        "unique_count":     int(col.nunique()),
        "duplicate_count":  int(len(col) - col.nunique()),
    }

    if col_type == "numeric":
        # Statistiques descriptives
        base.update({
            "mean":   float(col.mean()),
            "median": float(col.median()),
            "std":    float(col.std()),
            "min":    float(col.min()),
            "max":    float(col.max()),
            "q25":    float(col.quantile(0.25)),
            "q75":    float(col.quantile(0.75)),
        })

        # Valeurs aberrantes (IQR)
        clean = col.dropna()
        outlier_count, _, _ = detect_outliers_iqr(clean)
        base["outliers_count"]      = outlier_count
        base["outliers_percentage"] = round(outlier_count / len(clean) * 100, 2) if len(clean) > 0 else 0.0

        # Test de normalité (Shapiro-Wilk) — nécessite au moins 3 valeurs
        if len(clean) > 3:
            try:
                _, p_value = stats.shapiro(clean)
                base["is_normal"] = bool(p_value > 0.05)   # H0 : distribution normale
                base["p_value"]   = float(p_value)
            except Exception:
                base["is_normal"] = None
                base["p_value"]   = None

    else:
        # Valeur la plus fréquente pour les colonnes catégorielles
        top = col.value_counts().head(1)
        if len(top) > 0:
            base["most_common"]            = str(top.index[0])
            base["most_common_count"]      = int(top.values[0])
            base["most_common_percentage"] = round(top.values[0] / len(df) * 100, 2)
        else:
            base["most_common"]            = None
            base["most_common_count"]      = 0
            base["most_common_percentage"] = 0.0

    return base


def detect_exact_duplicates(df: pd.DataFrame) -> dict:
    """
    Détecte les lignes en double exact dans le DataFrame.

    Retourne le nombre de doublons, leur pourcentage,
    et les 10 premiers indices concernés.
    """
    mask       = df.duplicated(keep=False)
    dup_count  = int(mask.sum())
    dup_pct    = round(dup_count / len(df) * 100, 2) if len(df) > 0 else 0.0
    dup_indices = [int(i) for i in df[mask].index.tolist()[:10]]   # Limité à 10

    return {
        "exact_duplicates":            dup_count,
        "exact_duplicates_percentage": dup_pct,
        "duplicate_indices":           dup_indices,
    }


def calculate_correlations(df: pd.DataFrame) -> dict:
    """
    Calcule les corrélations de Pearson entre toutes les paires de colonnes numériques.

    Seuil de corrélation forte : |r| > 0.7

    Retourne un dict {  "col1 <-> col2" : valeur  } pour les paires fortes uniquement.
    """
    numeric_df = df.select_dtypes(include=[np.number])

    # Besoin d'au moins 2 colonnes numériques
    if len(numeric_df.columns) < 2:
        return {}

    corr_matrix  = numeric_df.corr()
    correlations = {}

    for i in range(len(corr_matrix.columns)):
        for j in range(i + 1, len(corr_matrix.columns)):
            col1 = corr_matrix.columns[i]
            col2 = corr_matrix.columns[j]
            val  = float(corr_matrix.iloc[i, j])
            if abs(val) > 0.7:                        # Seulement les corrélations fortes
                correlations[f"{col1} <-> {col2}"] = val

    return correlations


def calculate_quality_score(report: dict) -> float:
    """
    Calcule un score de qualité global entre 0 et 100.

    Pénalités appliquées :
    - Valeurs manquantes  : jusqu'à -20 points
    - Doublons exacts     : jusqu'à -15 points
    - Valeurs aberrantes  : jusqu'à -15 points
    - Colonnes non-norm.  : jusqu'à -10 points (test Shapiro)
    """
    score = 100.0
    stats_vals = list(report["statistics"].values())

    # Pénalité valeurs manquantes (moyenne du % de nulls)
    avg_null_pct = sum(c["null_percentage"] for c in stats_vals) / len(stats_vals)
    score -= min(avg_null_pct * 0.5, 20)

    # Pénalité doublons
    dup_pct = report["duplicates"]["exact_duplicates_percentage"]
    score -= min(dup_pct * 2, 15)

    # Pénalité outliers (moyenne du % d'aberrantes sur colonnes numériques)
    avg_outlier_pct = sum(c.get("outliers_percentage", 0) for c in stats_vals) / len(stats_vals)
    score -= min(avg_outlier_pct, 15)

    # Pénalité colonnes dont la distribution n'est pas normale
    non_normal = sum(1 for c in stats_vals if c.get("is_normal") is False)
    score -= min(non_normal * 2, 10)

    return round(max(score, 0), 1)


def get_quality_grade(score: float) -> str:
    """
    Convertit un score numérique en grade lettre :
    A ≥ 90 | B ≥ 75 | C ≥ 60 | D < 60
    """
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 60:
        return "C"
    return "D"


# ══════════════════════════════════════════════════════════════════════════════
# Rapport HTML
# ══════════════════════════════════════════════════════════════════════════════

def generate_html_report(report: dict):
    """
    Génère un rapport HTML interactif et l'enregistre dans le dossier des rapports.
    """
    grade        = report["quality_grade"]
    grade_colors = {"A": "#4CAF50", "B": "#2196F3", "C": "#FFC107", "D": "#F44336"}
    grade_color  = grade_colors.get(grade, "#9E9E9E")

    rows_html = ""
    for col_name, s in report["statistics"].items():
        rows_html += f"""
            <tr>
                <td><strong>{col_name}</strong></td>
                <td>{s['type']}</td>
                <td>{s['non_null_count']}</td>
                <td>{s['unique_count']}</td>
                <td>{s['null_percentage']}%</td>
                <td>{s.get('outliers_count', '—')}</td>
            </tr>"""

    missing_html = "".join(
        f"<li><strong>{col}</strong> : {cnt} valeur(s) manquante(s)</li>"
        for col, cnt in report["missing_values"].items()
    ) or "<li>Aucune valeur manquante</li>"

    corr_html = "".join(
        f"<li>{pair} : <strong>{val:.3f}</strong></li>"
        for pair, val in report["correlations"].items()
    ) or "<li>Aucune corrélation forte (|r| > 0.7)</li>"

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Profiling Report — UTL</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: Arial, sans-serif; background: #f5f5f5; color: #333; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        header {{ background: linear-gradient(135deg, #667eea, #764ba2); color: white;
                  padding: 30px; border-radius: 8px; margin-bottom: 30px; }}
        h1 {{ font-size: 28px; margin-bottom: 6px; }}
        h2 {{ color: #667eea; margin: 30px 0 15px; border-bottom: 2px solid #667eea; padding-bottom: 8px; }}
        .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                    gap: 15px; margin: 15px 0; }}
        .card {{ background: white; padding: 20px; border-radius: 8px; text-align: center;
                 box-shadow: 0 2px 6px rgba(0,0,0,.1); }}
        .big {{ font-size: 32px; font-weight: bold; color: #667eea; }}
        .grade-badge {{ display: inline-block; padding: 8px 20px; border-radius: 6px;
                        font-size: 20px; font-weight: bold; color: white;
                        background: {grade_color}; margin-top: 8px; }}
        table {{ width: 100%; border-collapse: collapse; background: white;
                 border-radius: 8px; overflow: hidden; box-shadow: 0 2px 6px rgba(0,0,0,.1); }}
        th {{ background: #667eea; color: white; padding: 12px 14px; text-align: left; }}
        td {{ padding: 10px 14px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f9f9f9; }}
        ul {{ padding-left: 20px; line-height: 1.8; }}
        footer {{ text-align: center; color: #999; margin-top: 40px; padding-top: 20px;
                  border-top: 1px solid #ddd; }}
    </style>
</head>
<body>
<div class="container">
    <header>
        <h1>Rapport de Profiling des Données</h1>
        <p>Analyse qualité automatique — Pipeline ETL + IA</p>
        <p style="margin-top:8px;opacity:.8">Généré le {report['generated_at'][:19].replace('T',' ')}</p>
    </header>

    <section>
        <h2>Score de Qualité</h2>
        <div class="metrics">
            <div class="card">
                <div class="big">{report['quality_score']}/100</div>
                <div style="color:#666;margin:6px 0">Score global</div>
                <div class="grade-badge">Grade {grade}</div>
            </div>
            <div class="card"><div class="big">{report['general']['total_rows']}</div><div style="color:#666">Lignes</div></div>
            <div class="card"><div class="big">{report['general']['total_columns']}</div><div style="color:#666">Colonnes</div></div>
            <div class="card"><div class="big">{sum(report['missing_values'].values())}</div><div style="color:#666">Valeurs manquantes</div></div>
            <div class="card"><div class="big">{report['duplicates']['exact_duplicates']}</div><div style="color:#666">Doublons exacts</div></div>
        </div>
    </section>

    <section>
        <h2>Analyse par Colonne</h2>
        <table>
            <thead><tr>
                <th>Colonne</th><th>Type</th><th>Non-null</th>
                <th>Uniques</th><th>Manquantes %</th><th>Aberrantes</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
    </section>

    <section>
        <h2>Valeurs Manquantes</h2>
        <ul>{missing_html}</ul>
    </section>

    <section>
        <h2>Corrélations Fortes (|r| &gt; 0.7)</h2>
        <ul>{corr_html}</ul>
    </section>

    <footer>
        <p>Rapport généré par <strong>UTL — Data Profiling</strong></p>
        <p>Pipeline ETL + Machine Learning</p>
    </footer>
</div>
</body>
</html>"""

    path = REPORTS_DIR / "profiling_report.html"
    path.write_text(html, encoding="utf-8")
    print(f"Rapport HTML : {path}")


# ══════════════════════════════════════════════════════════════════════════════
# Point d'entrée principal
# ══════════════════════════════════════════════════════════════════════════════

def profiling(input_path: str):
    """
    Lance l'analyse complète de profiling sur le fichier CSV donné.

    Étapes :
    1. Chargement
    2. Statistiques générales
    3. Analyse par colonne
    4. Valeurs manquantes
    5. Doublons exacts
    6. Valeurs aberrantes (IQR)
    7. Corrélations (Pearson)
    8. Score et grade de qualité
    """
    print("\n" + "=" * 65)
    print("DATA PROFILING — Analyse complète de la qualité des données")
    print("=" * 65)

    # ── 1. Chargement ─────────────────────────────────────────────────────────
    print("\n[1/8] Chargement des données...")
    df, _ = load_dataset(input_path)
    print(f"      {len(df)} lignes, {len(df.columns)} colonnes")

    # ── 2. Statistiques générales ─────────────────────────────────────────────
    print("[2/8] Statistiques générales...")
    general = {
        "total_rows":      int(len(df)),
        "total_columns":   int(len(df.columns)),
        "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024 ** 2, 2),
        "column_names":    list(df.columns),
    }
    print(f"      Mémoire utilisée : {general['memory_usage_mb']} MB")

    # ── 3. Analyse par colonne ─────────────────────────────────────────────────
    print("[3/8] Analyse par colonne...")
    statistics = {col: analyze_column(df, col) for col in df.columns}
    print(f"      {len(statistics)} colonnes analysées")

    # ── 4. Valeurs manquantes ──────────────────────────────────────────────────
    print("[4/8] Valeurs manquantes...")
    missing_values = {col: int(df[col].isna().sum()) for col in df.columns if df[col].isna().sum() > 0}
    print(f"      {sum(missing_values.values())} valeurs manquantes au total")

    # ── 5. Doublons exacts ────────────────────────────────────────────────────
    print("[5/8] Doublons exacts...")
    duplicates = detect_exact_duplicates(df)
    print(f"      {duplicates['exact_duplicates']} doublon(s) trouvé(s)")

    # ── 6. Valeurs aberrantes ─────────────────────────────────────────────────
    print("[6/8] Valeurs aberrantes (IQR)...")
    total_outliers = sum(
        detect_outliers_iqr(df[col].dropna())[0]
        for col in df.select_dtypes(include=[np.number]).columns
    )
    print(f"      {total_outliers} valeur(s) aberrante(s) détectée(s)")

    # ── 7. Corrélations ───────────────────────────────────────────────────────
    print("[7/8] Corrélations fortes...")
    correlations = calculate_correlations(df)
    print(f"      {len(correlations)} paire(s) fortement corrélée(s)")

    # ── 8. Score de qualité ───────────────────────────────────────────────────
    print("[8/8] Calcul du score de qualité...")
    report = {
        "generated_at":  pd.Timestamp.now().isoformat(),
        "input_path":    str(input_path),
        "general":       general,
        "statistics":    statistics,
        "missing_values": missing_values,
        "duplicates":    duplicates,
        "correlations":  correlations,
    }
    report["quality_score"] = calculate_quality_score(report)
    report["quality_grade"] = get_quality_grade(report["quality_score"])
    print(f"      Score : {report['quality_score']}/100  —  Grade : {report['quality_grade']}")

    # ── Sauvegarde JSON ───────────────────────────────────────────────────────
    json_path = REPORTS_DIR / "profiling_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\nRapport JSON : {json_path}")

    # ── Rapport HTML ──────────────────────────────────────────────────────────
    generate_html_report(report)

    # ── Résumé console ────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("RESUME DU PROFILING")
    print("=" * 65)
    print(f"  Lignes                : {general['total_rows']}")
    print(f"  Colonnes              : {general['total_columns']}")
    print(f"  Valeurs manquantes    : {sum(missing_values.values())}")
    print(f"  Doublons exacts       : {duplicates['exact_duplicates']}")
    print(f"  Valeurs aberrantes    : {total_outliers}")
    print(f"  Correlations fortes   : {len(correlations)}")
    print(f"  Score de qualite      : {report['quality_score']}/100 (Grade {report['quality_grade']})")
    print("=" * 65 + "\n")


def main(input_path: str):
    """Point d'entrée — appelé depuis le dashboard ou en CLI."""
    profiling(input_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Profiling — Analyse complète")
    parser.add_argument("--input", required=True, help="Chemin du fichier CSV")
    args = parser.parse_args()
    main(args.input)
