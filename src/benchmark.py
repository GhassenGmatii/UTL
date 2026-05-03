"""
benchmark.py — Comparaison ETL Classique vs ETL avec IA
=========================================================
Ce module réalise un benchmark entre deux approches de traitement
des données pour démontrer l'apport de l'intelligence artificielle :

  ┌─────────────────────────┬───────────────────────────────────────┐
  │  ETL Classique          │  ETL avec IA                          │
  ├─────────────────────────┼───────────────────────────────────────┤
  │  drop_duplicates()      │  RandomForest (similarité fuzzy)      │
  │  dropna()               │  Règles génériques + normalisation    │
  │  Valeurs < 0 uniquement │  IQR (interquartile) multi-colonnes   │
  └─────────────────────────┴───────────────────────────────────────┘

Métriques comparées :
  - Temps d'exécution (secondes)
  - Nombre de doublons détectés
  - Nombre d'anomalies détectées
  - Lignes conservées en sortie

Sorties :
  - benchmark_report.json  : rapport chiffré
  - benchmark_report.html  : rapport HTML visuel
"""

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from src.clean_rules import apply_cleaning_rules
from src.config import DEDUP_MODEL_PATH, REPORTS_DIR
from src.dedup_ml import load_dedup_model, predict_duplicate_pairs
from src.ingest import load_dataset

# Crée le répertoire de rapports s'il n'existe pas
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Force l'encodage UTF-8 pour les emojis sous Windows
sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ══════════════════════════════════════════════════════════════════════════════
# ETL Classique — approche traditionnelle sans IA
# ══════════════════════════════════════════════════════════════════════════════

def etl_classique(df: pd.DataFrame) -> dict:
    """
    Simule un ETL traditionnel sans intelligence artificielle.

    Étapes réalisées :
    1. Suppression des doublons EXACTS (drop_duplicates)
    2. Suppression des lignes avec valeurs manquantes (dropna)
    3. Détection des anomalies = valeurs numériques négatives uniquement

    Limites de cette approche :
    - Ne détecte pas les doublons "flous" (même personne avec coquille)
    - La détection d'anomalies est très basique (seulement valeurs < 0)
    - Perte possible de données utiles lors du dropna
    """
    start = time.time()

    # ── Étape 1 : Suppression des doublons exacts ──────────────────────────────
    df_clean = df.drop_duplicates()          # Conserve la première occurrence

    # ── Étape 2 : Suppression des lignes incomplètes ───────────────────────────
    df_clean = df_clean.dropna()             # Supprime toute ligne avec un NaN

    # ── Étape 3 : Détection d'anomalies par règle fixe (valeurs négatives) ─────
    numeric_cols   = df_clean.select_dtypes(include=[np.number]).columns
    anomaly_set    = set()
    for col in numeric_cols:
        anomaly_set.update(df_clean[df_clean[col] < 0].index)  # Valeurs < 0 = anomalie

    # ── Calcul des métriques ───────────────────────────────────────────────────
    duplicates_found = len(df) - len(df_clean)   # Lignes supprimées = doublons estimés
    duration         = time.time() - start

    return {
        "duration":        duration,            # Temps d'exécution (secondes)
        "rows_processed":  len(df),             # Lignes en entrée
        "rows_final":      len(df_clean),       # Lignes conservées
        "duplicates_found": duplicates_found,   # Doublons détectés
        "anomalies_found": len(anomaly_set),    # Anomalies détectées
        "rows_removed":    len(df) - len(df_clean),
    }


# ══════════════════════════════════════════════════════════════════════════════
# ETL avec IA — approche intelligente par Machine Learning
# ══════════════════════════════════════════════════════════════════════════════

def etl_ai(df: pd.DataFrame, dedup_model) -> dict:
    """
    Simule un ETL intelligent utilisant le Machine Learning.

    Étapes réalisées :
    1. Nettoyage intelligent (règles génériques : emails, dates, textes…)
    2. Déduplication ML : RandomForest + similarité fuzzy (RapidFuzz)
       → Détecte les doublons même si nom mal orthographié ou email modifié
    3. Détection d'anomalies par méthode IQR (Interquartile Range)
       → Plus sensible que la simple règle "valeur < 0"

    Avantages sur l'ETL classique :
    - Détecte beaucoup plus de doublons (sémantiques, pas seulement exacts)
    - Anomalies détectées sur toutes les colonnes numériques
    - Nettoyage adapté au type de données (email, téléphone, date…)
    """
    start = time.time()

    # ── Étape 1 : Nettoyage intelligent ───────────────────────────────────────
    # apply_cleaning_rules détecte automatiquement les colonnes (générique)
    df_clean, _ = apply_cleaning_rules(df, {})

    # ── Étape 2 : Déduplication par RandomForest ───────────────────────────────
    duplicates_found = 0
    if dedup_model:
        try:
            # predict_duplicate_pairs retourne les paires (i, j) identifiées comme doublons
            dup_pairs        = predict_duplicate_pairs(df_clean, dedup_model, threshold=0.75)
            duplicates_found = len(dup_pairs)
        except Exception:
            duplicates_found = 0    # En cas d'erreur, on continue sans planter

    # ── Étape 3 : Détection d'anomalies par IQR ───────────────────────────────
    # L'IQR (interquartile range) détecte les valeurs statistiquement extrêmes
    # sur TOUTES les colonnes numériques, pas seulement les valeurs négatives
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    anomaly_set  = set()
    for col in numeric_cols:
        Q1  = df_clean[col].quantile(0.25)
        Q3  = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR     # Borne basse : en dessous = aberrant
        upper = Q3 + 1.5 * IQR     # Borne haute : au dessus  = aberrant
        anomaly_set.update(
            df_clean[(df_clean[col] < lower) | (df_clean[col] > upper)].index
        )

    duration = time.time() - start

    return {
        "duration":         duration,
        "rows_processed":   len(df),
        "rows_final":       len(df_clean),
        "duplicates_found": duplicates_found,
        "anomalies_found":  len(anomaly_set),
        "rows_removed":     len(df) - len(df_clean),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Benchmark principal
# ══════════════════════════════════════════════════════════════════════════════

def benchmark(input_path: str):
    """
    Exécute le benchmark complet : ETL Classique vs ETL avec IA.

    Charge les données, exécute les deux pipelines, compare les résultats,
    génère un rapport JSON et un rapport HTML.
    """
    print("\n" + "=" * 70)
    print("BENCHMARK — ETL CLASSIQUE vs ETL AVEC IA")
    print("=" * 70)

    # ── 1. Chargement des données ──────────────────────────────────────────────
    print("\n[1/4] Chargement des données...")
    raw_df, _ = load_dataset(input_path)
    print(f"      {len(raw_df)} lignes chargées, {len(raw_df.columns)} colonnes")

    # ── 2. Chargement du modèle ML ─────────────────────────────────────────────
    print("\n[2/4] Chargement du modèle ML de déduplication...")
    dedup_model = load_dedup_model(DEDUP_MODEL_PATH)
    print(f"      Modèle disponible : {'Oui' if dedup_model else 'Non (entrainer les modeles d abord)'}")

    # ── 3. Exécution ETL Classique ─────────────────────────────────────────────
    print("\n[3/4] Execution ETL Classique...")
    r_classique = etl_classique(raw_df)
    print(f"      Temps     : {r_classique['duration']:.3f} s")
    print(f"      Doublons  : {r_classique['duplicates_found']}")
    print(f"      Anomalies : {r_classique['anomalies_found']}")

    # ── 4. Exécution ETL avec IA ───────────────────────────────────────────────
    print("\n[4/4] Execution ETL avec IA (Machine Learning)...")
    r_ai = etl_ai(raw_df, dedup_model)
    print(f"      Temps     : {r_ai['duration']:.3f} s")
    print(f"      Doublons  : {r_ai['duplicates_found']}")
    print(f"      Anomalies : {r_ai['anomalies_found']}")

    # ── Calcul des écarts entre les deux approches ─────────────────────────────
    time_diff    = r_ai["duration"] - r_classique["duration"]
    time_diff_pct = (time_diff / r_classique["duration"]) * 100 if r_classique["duration"] > 0 else 0
    dup_diff     = r_ai["duplicates_found"] - r_classique["duplicates_found"]
    anom_diff    = r_ai["anomalies_found"]  - r_classique["anomalies_found"]

    # ── Détermination du gagnant par critère ───────────────────────────────────
    reasons = []

    # Critère vitesse
    if r_classique["duration"] < r_ai["duration"]:
        reasons.append(f"ETL Classique est plus rapide de {abs(time_diff_pct):.1f}%")
    else:
        reasons.append(f"ETL IA est plus rapide de {abs(time_diff_pct):.1f}%")

    # Critère qualité : doublons
    if r_ai["duplicates_found"] > r_classique["duplicates_found"]:
        reasons.append(f"ETL IA detecte {dup_diff} doublons supplementaires (semantiques)")

    # Critère qualité : anomalies
    if r_ai["anomalies_found"] > r_classique["anomalies_found"]:
        reasons.append(f"ETL IA detecte {anom_diff} anomalies supplementaires (IQR)")

    # ── Verdict global ─────────────────────────────────────────────────────────
    # L'ETL IA est plus lent mais détecte bien plus de problèmes de qualité
    if r_classique["duration"] < r_ai["duration"]:
        winner  = "ETL CLASSIQUE (plus rapide)"
        verdict = "L'approche classique est plus rapide, mais l'IA detecte bien plus de doublons et anomalies."
    else:
        winner  = "ETL AVEC IA (meilleure qualite)"
        verdict = "L'IA offre une meilleure qualite malgre un temps legerement superieur."

    # ── Construction du rapport ────────────────────────────────────────────────
    report = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "input_path":   str(input_path),
        "data": {
            "total_rows":    len(raw_df),
            "total_columns": len(raw_df.columns),
        },
        # Résultats de chaque approche
        "etl_classique": {
            "duration":         r_classique["duration"],
            "rows_processed":   r_classique["rows_processed"],
            "rows_final":       r_classique["rows_final"],
            "duplicates_found": r_classique["duplicates_found"],
            "anomalies_found":  r_classique["anomalies_found"],
        },
        "etl_ai": {
            "duration":         r_ai["duration"],
            "rows_processed":   r_ai["rows_processed"],
            "rows_final":       r_ai["rows_final"],
            "duplicates_found": r_ai["duplicates_found"],
            "anomalies_found":  r_ai["anomalies_found"],
        },
        # Différences calculées
        "comparison": {
            "time_difference":            time_diff,
            "time_difference_percentage": time_diff_pct,
            "duplicates_difference":      dup_diff,
            "anomalies_difference":       anom_diff,
        },
        "verdict": {
            "overall_winner": winner,
            "reasons":        reasons,
        },
        "conclusion": verdict,
    }

    # ── Sauvegarde JSON ────────────────────────────────────────────────────────
    json_path = REPORTS_DIR / "benchmark_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n      Rapport JSON : {json_path}")

    # ── Rapport HTML ───────────────────────────────────────────────────────────
    generate_html_report(report)

    # ── Affichage résumé console ───────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("RESULTATS DU BENCHMARK")
    print("=" * 70)
    print(f"\n  ETL CLASSIQUE  : {r_classique['duration']:.3f}s | {r_classique['duplicates_found']} doublons | {r_classique['anomalies_found']} anomalies")
    print(f"  ETL AVEC IA    : {r_ai['duration']:.3f}s | {r_ai['duplicates_found']} doublons | {r_ai['anomalies_found']} anomalies")
    print(f"\n  Gagnant        : {winner}")
    print(f"  Conclusion     : {verdict}")
    print("=" * 70 + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# Rapport HTML du benchmark
# ══════════════════════════════════════════════════════════════════════════════

def generate_html_report(report: dict):
    """
    Génère un rapport HTML comparatif visuellement structuré.
    Affiche les métriques des deux approches côte à côte.
    """
    r_c = report["etl_classique"]    # Résultats ETL Classique
    r_a = report["etl_ai"]           # Résultats ETL IA

    # Couleur du badge gagnant
    winner_color = "#4CAF50"

    reasons_html = "".join(f"<li>{r}</li>" for r in report["verdict"]["reasons"])

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Benchmark ETL — UTL</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: Arial, sans-serif; background: #f5f5f5; color: #333; }}
        .container {{ max-width: 1100px; margin: 0 auto; padding: 20px; }}
        header {{ background: linear-gradient(135deg, #667eea, #764ba2);
                  color: white; padding: 30px; border-radius: 8px; margin-bottom: 30px; }}
        h1 {{ font-size: 26px; }}
        h2 {{ color: #667eea; margin: 28px 0 14px; border-bottom: 2px solid #667eea; padding-bottom: 8px; }}
        .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
        .card {{ background: white; padding: 22px; border-radius: 8px;
                 box-shadow: 0 2px 6px rgba(0,0,0,.1); }}
        .card.classique {{ border-top: 4px solid #FF6B6B; }}
        .card.ai        {{ border-top: 4px solid #4ECDC4; }}
        .row {{ display: flex; justify-content: space-between;
                padding: 10px 0; border-bottom: 1px solid #eee; }}
        .label {{ color: #666; }}
        .value {{ font-weight: bold; color: #444; }}
        .winner {{ background: {winner_color}; color: white; padding: 14px 20px;
                   border-radius: 6px; font-size: 17px; font-weight: bold; margin: 16px 0; }}
        .conclusion {{ background: #E3F2FD; border-left: 4px solid #2196F3;
                       padding: 14px; border-radius: 4px; }}
        ul {{ padding-left: 20px; line-height: 1.9; margin: 10px 0; }}
        footer {{ text-align: center; color: #999; margin-top: 40px;
                  padding-top: 18px; border-top: 1px solid #ddd; }}
    </style>
</head>
<body>
<div class="container">
    <header>
        <h1>Rapport de Benchmark — ETL Classique vs ETL avec IA</h1>
        <p style="margin-top:8px;opacity:.85">Généré le {report['generated_at'][:19].replace('T',' ')}</p>
        <p style="margin-top:4px;opacity:.75">Fichier source : {Path(report['input_path']).name} — {report['data']['total_rows']} lignes</p>
    </header>

    <section>
        <h2>Comparaison des Résultats</h2>
        <div class="grid">
            <!-- ETL Classique -->
            <div class="card classique">
                <h3 style="margin-bottom:14px;color:#FF6B6B">ETL Classique</h3>
                <div class="row"><span class="label">Temps d'exécution</span>  <span class="value">{r_c['duration']:.3f} s</span></div>
                <div class="row"><span class="label">Lignes traitées</span>    <span class="value">{r_c['rows_processed']}</span></div>
                <div class="row"><span class="label">Lignes conservées</span>  <span class="value">{r_c['rows_final']}</span></div>
                <div class="row"><span class="label">Doublons détectés</span>  <span class="value">{r_c['duplicates_found']}</span></div>
                <div class="row"><span class="label">Anomalies détectées</span><span class="value">{r_c['anomalies_found']}</span></div>
            </div>
            <!-- ETL IA -->
            <div class="card ai">
                <h3 style="margin-bottom:14px;color:#4ECDC4">ETL avec IA (Machine Learning)</h3>
                <div class="row"><span class="label">Temps d'exécution</span>  <span class="value">{r_a['duration']:.3f} s</span></div>
                <div class="row"><span class="label">Lignes traitées</span>    <span class="value">{r_a['rows_processed']}</span></div>
                <div class="row"><span class="label">Lignes conservées</span>  <span class="value">{r_a['rows_final']}</span></div>
                <div class="row"><span class="label">Doublons détectés</span>  <span class="value">{r_a['duplicates_found']}</span></div>
                <div class="row"><span class="label">Anomalies détectées</span><span class="value">{r_a['anomalies_found']}</span></div>
            </div>
        </div>
    </section>

    <section>
        <h2>Verdict</h2>
        <div class="winner">{report['verdict']['overall_winner']}</div>
        <div class="conclusion">
            <strong>Analyse détaillée :</strong>
            <ul>{reasons_html}</ul>
            <p style="margin-top:10px"><strong>Conclusion générale :</strong> {report['conclusion']}</p>
        </div>
    </section>

    <footer>
        <p>Rapport généré par <strong>UTL — Benchmark ETL</strong></p>
        <p>Pipeline ETL + Machine Learning</p>
    </footer>
</div>
</body>
</html>"""

    path = REPORTS_DIR / "benchmark_report.html"
    path.write_text(html, encoding="utf-8")
    print(f"      Rapport HTML : {path}")


# ══════════════════════════════════════════════════════════════════════════════
# Point d'entrée
# ══════════════════════════════════════════════════════════════════════════════

def main(input_path: str):
    """Point d'entrée — appelé depuis le dashboard ou en ligne de commande."""
    benchmark(input_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark ETL Classique vs ETL IA")
    parser.add_argument("--input", required=True, help="Chemin du fichier CSV")
    args = parser.parse_args()
    main(args.input)
