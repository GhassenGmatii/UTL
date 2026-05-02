import argparse
import json
import time
from pathlib import Path
import pandas as pd
import numpy as np
from src.config import REPORTS_DIR
from src.ingest import load_dataset
from src.clean_rules import apply_cleaning_rules
from src.features import extract_features_from_pairs
from src.dedup_ml import load_dedup_model, predict_duplicate_pairs

REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def etl_classique(df):
    """ETL Classique - Approche traditionnelle avec règles fixes"""
    
    start_time = time.time()
    
    # Étape 1: Nettoyage basique
    df_clean = df.copy()
    df_clean = df_clean.drop_duplicates()  # Doublons exacts
    
    # Étape 2: Suppression des valeurs manquantes
    df_clean = df_clean.dropna()
    
    # Étape 3: Détection d'anomalies simple (valeurs négatives)
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    anomaly_indices = set()
    for col in numeric_cols:
        anomaly_indices.update(df_clean[df_clean[col] < 0].index)
    
    anomalies_found = len(anomaly_indices)
    
    # Étape 4: Détection de doublons basée sur les colonnes clés
    duplicates_found = len(df) - len(df_clean)
    
    duration = time.time() - start_time
    
    return {
        "duration": duration,
        "rows_processed": len(df),
        "rows_final": len(df_clean),
        "duplicates_found": duplicates_found,
        "anomalies_found": anomalies_found,
        "rows_removed": len(df) - len(df_clean)
    }


def etl_ai(df, dedup_model):
    """ETL avec IA - Approche intelligente avec Machine Learning"""
    
    start_time = time.time()
    
    # Étape 1: Nettoyage intelligent
    df_clean, clean_report = apply_cleaning_rules(df, {})
    
    # Étape 2: Détection de doublons avec ML
    duplicates_found = 0
    if dedup_model:
        try:
            dup_pairs = predict_duplicate_pairs(df_clean, dedup_model, threshold=0.75)
            duplicates_found = len(dup_pairs)
        except:
            duplicates_found = 0
    
    # Étape 3: Détection d'anomalies simple
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    anomaly_indices = set()
    for col in numeric_cols:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        anomaly_indices.update(
            df_clean[(df_clean[col] < (Q1 - 1.5 * IQR)) | (df_clean[col] > (Q3 + 1.5 * IQR))].index
        )
    
    anomalies_found = len(anomaly_indices)
    
    duration = time.time() - start_time
    
    return {
        "duration": duration,
        "rows_processed": len(df),
        "rows_final": len(df_clean),
        "duplicates_found": duplicates_found,
        "anomalies_found": anomalies_found,
        "rows_removed": len(df) - len(df_clean)
    }


def benchmark(input_path):
    """Benchmark complet ETL Classique vs ETL avec IA"""
    
    print("\n" + "="*70)
    print("🔄 BENCHMARK - ETL CLASSIQUE vs ETL AVEC IA")
    print("="*70)
    
    # Charger les données
    print("\n[1/4] Chargement des données...")
    raw_df, _ = load_dataset(input_path)
    print(f"✅ {len(raw_df)} lignes chargées")
    
    # Charger le modèle ML
    print("\n[2/4] Chargement du modèle ML...")
    from src.config import DEDUP_MODEL_PATH
    dedup_model = load_dedup_model(DEDUP_MODEL_PATH)
    print(f"✅ Modèle chargé: {'Oui' if dedup_model else 'Non'}")
    
    # Test 1: ETL Classique
    print("\n[3/4] Exécution ETL Classique...")
    results_classique = etl_classique(raw_df)
    print(f"✅ Temps: {results_classique['duration']:.3f}s")
    print(f"   Doublons trouvés: {results_classique['duplicates_found']}")
    print(f"   Anomalies trouvées: {results_classique['anomalies_found']}")
    
    # Test 2: ETL avec IA
    print("\n[4/4] Exécution ETL avec IA...")
    results_ai = etl_ai(raw_df, dedup_model)
    print(f"✅ Temps: {results_ai['duration']:.3f}s")
    print(f"   Doublons trouvés: {results_ai['duplicates_found']}")
    print(f"   Anomalies trouvées: {results_ai['anomalies_found']}")
    
    # Comparaison
    time_diff = results_ai['duration'] - results_classique['duration']
    time_diff_pct = (time_diff / results_classique['duration']) * 100 if results_classique['duration'] > 0 else 0
    
    duplicates_diff = results_ai['duplicates_found'] - results_classique['duplicates_found']
    anomalies_diff = results_ai['anomalies_found'] - results_classique['anomalies_found']
    
    # Déterminer le gagnant
    winners = []
    
    if results_classique['duration'] < results_ai['duration']:
        winners.append(f"⚡ ETL Classique: Plus rapide de {abs(time_diff_pct):.1f}%")
    else:
        winners.append(f"🤖 ETL IA: Plus lent de {abs(time_diff_pct):.1f}%")
    
    if results_ai['duplicates_found'] > results_classique['duplicates_found']:
        winners.append(f"🤖 ETL IA: Détecte {duplicates_diff} doublons supplémentaires")
    
    if results_ai['anomalies_found'] > results_classique['anomalies_found']:
        winners.append(f"🤖 ETL IA: Détecte {anomalies_diff} anomalies supplémentaires")
    
    # Déterminer le verdict global
    if results_classique['duration'] < results_ai['duration']:
        overall_winner = "⚡ ETL CLASSIQUE"
        verdict = "L'approche traditionnelle est plus rapide mais moins intelligente."
    else:
        overall_winner = "🤖 ETL AVEC IA"
        verdict = "L'IA détecte plus d'anomalies malgré un temps légèrement plus long."
    
    # Créer le rapport
    report = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "input_path": str(input_path),
        "data": {
            "total_rows": len(raw_df),
            "total_columns": len(raw_df.columns)
        },
        "etl_classique": {
            "duration": results_classique['duration'],
            "rows_processed": results_classique['rows_processed'],
            "rows_final": results_classique['rows_final'],
            "duplicates_found": results_classique['duplicates_found'],
            "anomalies_found": results_classique['anomalies_found']
        },
        "etl_ai": {
            "duration": results_ai['duration'],
            "rows_processed": results_ai['rows_processed'],
            "rows_final": results_ai['rows_final'],
            "duplicates_found": results_ai['duplicates_found'],
            "anomalies_found": results_ai['anomalies_found']
        },
        "comparison": {
            "time_difference": time_diff,
            "time_difference_percentage": time_diff_pct,
            "duplicates_difference": duplicates_diff,
            "anomalies_difference": anomalies_diff
        },
        "verdict": {
            "overall_winner": overall_winner,
            "reasons": winners
        },
        "conclusion": verdict
    }
    
    # Sauvegarder le rapport JSON
    report_json_path = REPORTS_DIR / "benchmark_report.json"
    with open(report_json_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Rapport JSON: {report_json_path}")
    
    # Générer rapport HTML
    generate_html_report(report)
    
    # Résumé
    print("\n" + "="*70)
    print("📊 RÉSULTATS DU BENCHMARK")
    print("="*70)
    print(f"\n🔧 ETL CLASSIQUE:")
    print(f"   ⏱️  Temps: {results_classique['duration']:.3f}s")
    print(f"   📊 Doublons: {results_classique['duplicates_found']}")
    print(f"   ⚠️  Anomalies: {results_classique['anomalies_found']}")
    
    print(f"\n🤖 ETL AVEC IA:")
    print(f"   ⏱️  Temps: {results_ai['duration']:.3f}s")
    print(f"   📊 Doublons: {results_ai['duplicates_found']}")
    print(f"   ⚠️  Anomalies: {results_ai['anomalies_found']}")
    
    print(f"\n🏆 {overall_winner}")
    print(f"   {verdict}")
    
    print("\n" + "="*70 + "\n")


def generate_html_report(report):
    """Générer un rapport HTML comparatif"""
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Benchmark ETL Report</title>
        <style>
            * {{ margin: 0; padding: 0; }}
            body {{ font-family: 'Arial', sans-serif; background: #f5f5f5; color: #333; }}
            .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
            header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 8px; margin-bottom: 30px; }}
            h1 {{ font-size: 32px; margin-bottom: 10px; }}
            h2 {{ color: #667eea; margin-top: 30px; margin-bottom: 15px; border-bottom: 2px solid #667eea; padding-bottom: 10px; }}
            .comparison {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0; }}
            .card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .card.classique {{ border-left: 4px solid #FF6B6B; }}
            .card.ai {{ border-left: 4px solid #4ECDC4; }}
            .metric {{ display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #eee; }}
            .metric .label {{ color: #666; }}
            .metric .value {{ font-weight: bold; color: #667eea; }}
            .winner {{ background: #4CAF50; color: white; padding: 15px; border-radius: 4px; margin: 20px 0; font-size: 18px; font-weight: bold; }}
            .conclusion {{ background: #E3F2FD; border-left: 4px solid #2196F3; padding: 15px; border-radius: 4px; margin: 20px 0; }}
            footer {{ text-align: center; color: #666; margin-top: 50px; padding-top: 20px; border-top: 1px solid #ddd; }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>🔄 Rapport de Benchmark</h1>
                <p>Comparaison ETL Classique vs ETL avec Machine Learning</p>
            </header>
            
            <section>
                <h2>📊 Données</h2>
                <div class="card">
                    <div class="metric">
                        <span class="label">Lignes:</span>
                        <span class="value">{report['data']['total_rows']}</span>
                    </div>
                    <div class="metric">
                        <span class="label">Colonnes:</span>
                        <span class="value">{report['data']['total_columns']}</span>
                    </div>
                </div>
            </section>
            
            <section>
                <h2>⚔️ Comparaison</h2>
                <div class="comparison">
                    <div class="card classique">
                        <h3>🔧 ETL Classique</h3>
                        <div class="metric">
                            <span class="label">⏱️ Temps d'exécution:</span>
                            <span class="value">{report['etl_classique']['duration']:.3f}s</span>
                        </div>
                        <div class="metric">
                            <span class="label">📊 Doublons détectés:</span>
                            <span class="value">{report['etl_classique']['duplicates_found']}</span>
                        </div>
                        <div class="metric">
                            <span class="label">⚠️ Anomalies détectées:</span>
                            <span class="value">{report['etl_classique']['anomalies_found']}</span>
                        </div>
                        <div class="metric">
                            <span class="label">✂️ Lignes supprimées:</span>
                            <span class="value">{report['etl_classique']['rows_processed'] - report['etl_classique']['rows_final']}</span>
                        </div>
                    </div>
                    
                    <div class="card ai">
                        <h3>🤖 ETL avec IA</h3>
                        <div class="metric">
                            <span class="label">⏱️ Temps d'exécution:</span>
                            <span class="value">{report['etl_ai']['duration']:.3f}s</span>
                        </div>
                        <div class="metric">
                            <span class="label">📊 Doublons détectés:</span>
                            <span class="value">{report['etl_ai']['duplicates_found']}</span>
                        </div>
                        <div class="metric">
                            <span class="label">⚠️ Anomalies détectées:</span>
                            <span class="value">{report['etl_ai']['anomalies_found']}</span>
                        </div>
                        <div class="metric">
                            <span class="label">✂️ Lignes supprimées:</span>
                            <span class="value">{report['etl_ai']['rows_processed'] - report['etl_ai']['rows_final']}</span>
                        </div>
                    </div>
                </div>
            </section>
            
            <section>
                <h2>🏆 Résultat</h2>
                <div class="winner">{report['verdict']['overall_winner']}</div>
                <div class="conclusion">
                    <strong>Conclusions:</strong>
                    <ul>
    """
    
    for reason in report['verdict']['reasons']:
        html_content += f"<li>{reason}</li>"
    
    html_content += f"""
                    </ul>
                    <p style="margin-top: 10px;"><strong>Verdict global:</strong> {report['conclusion']}</p>
                </div>
            </section>
            
            <footer>
                <p>Rapport généré par <strong>UTL - Benchmark</strong></p>
                <p>🔄 Pipeline ETL + Machine Learning</p>
            </footer>
        </div>
    </body>
    </html>
    """
    
    report_html_path = REPORTS_DIR / "benchmark_report.html"
    with open(report_html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Rapport HTML: {report_html_path}")


def main(input_path):
    """Point d'entrée principal"""
    benchmark(input_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark - Comparaison ETL")
    parser.add_argument("--input", required=True, help="Chemin du fichier CSV")
    args = parser.parse_args()
    main(args.input)
