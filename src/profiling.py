import argparse
import json
from pathlib import Path
import pandas as pd
import numpy as np
from scipy import stats
from src.config import REPORTS_DIR
from src.ingest import load_dataset

REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def detect_outliers_iqr(data):
    """Détecter les outliers avec la méthode IQR"""
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = (data < lower_bound) | (data > upper_bound)
    return outliers.sum(), lower_bound, upper_bound


def analyze_column(df, col_name):
    """Analyser une colonne en détail"""
    col_data = df[col_name]
    
    analysis = {
        "name": col_name,
        "type": "numeric" if col_data.dtype in [np.int64, np.float64] else "categorical",
        "non_null_count": col_data.notna().sum(),
        "null_count": col_data.isna().sum(),
        "null_percentage": round((col_data.isna().sum() / len(df)) * 100, 2),
        "unique_count": col_data.nunique(),
        "duplicate_count": len(col_data) - col_data.nunique()
    }
    
    if analysis["type"] == "numeric":
        analysis.update({
            "mean": float(col_data.mean()),
            "median": float(col_data.median()),
            "std": float(col_data.std()),
            "min": float(col_data.min()),
            "max": float(col_data.max()),
            "q25": float(col_data.quantile(0.25)),
            "q75": float(col_data.quantile(0.75)),
        })
        
        # Outliers
        outlier_count, lower_bound, upper_bound = detect_outliers_iqr(col_data.dropna())
        analysis["outliers_count"] = int(outlier_count)
        analysis["outliers_percentage"] = round((outlier_count / col_data.notna().sum()) * 100, 2)
        
        # Test de normalité (Shapiro-Wilk)
        if len(col_data.dropna()) > 3:
            try:
                stat, p_value = stats.shapiro(col_data.dropna())
                analysis["is_normal"] = p_value > 0.05
                analysis["p_value"] = float(p_value)
            except:
                analysis["is_normal"] = None
                analysis["p_value"] = None
    else:
        most_common = col_data.value_counts().head(1)
        analysis.update({
            "most_common": str(most_common.index[0]) if len(most_common) > 0 else None,
            "most_common_count": int(most_common.values[0]) if len(most_common) > 0 else 0,
            "most_common_percentage": round((most_common.values[0] / len(df)) * 100, 2) if len(most_common) > 0 else 0,
        })
    
    return analysis


def detect_exact_duplicates(df):
    """Détecter les doublons exacts"""
    duplicates = df.duplicated(keep=False).sum()
    duplicate_rows = df[df.duplicated(keep=False)].index.tolist()
    return {
        "exact_duplicates": int(duplicates),
        "exact_duplicates_percentage": round((duplicates / len(df)) * 100, 2),
        "duplicate_indices": duplicate_rows[:10]  # Top 10
    }


def calculate_correlations(df):
    """Calculer les corrélations entre colonnes numériques"""
    numeric_df = df.select_dtypes(include=[np.number])
    
    if len(numeric_df.columns) < 2:
        return {}
    
    correlations = {}
    corr_matrix = numeric_df.corr()
    
    for i in range(len(corr_matrix.columns)):
        for j in range(i + 1, len(corr_matrix.columns)):
            col1 = corr_matrix.columns[i]
            col2 = corr_matrix.columns[j]
            corr_value = float(corr_matrix.iloc[i, j])
            
            if abs(corr_value) > 0.7:  # Seuil de corrélation forte
                correlations[f"{col1} <-> {col2}"] = corr_value
    
    return correlations


def calculate_quality_score(report):
    """Calculer un score de qualité global (0-100)"""
    
    score = 100
    
    # Pénalité pour valeurs manquantes
    total_null_pct = sum(col['null_percentage'] for col in report['statistics'].values()) / len(report['statistics'])
    score -= min(total_null_pct * 0.5, 20)
    
    # Pénalité pour doublons
    duplicate_pct = report['duplicates']['exact_duplicates_percentage']
    score -= min(duplicate_pct * 2, 15)
    
    # Pénalité pour outliers
    total_outlier_pct = sum(
        col.get('outliers_percentage', 0) for col in report['statistics'].values()
    ) / len(report['statistics'])
    score -= min(total_outlier_pct, 15)
    
    # Pénalité pour colonnes anormales
    abnormal_cols = sum(1 for col in report['statistics'].values() 
                       if col.get('is_normal') == False)
    score -= min(abnormal_cols * 2, 10)
    
    score = max(score, 0)
    
    return round(score, 1)


def get_quality_grade(score):
    """Retourner le grade de qualité"""
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 60:
        return "C"
    else:
        return "D"


def profiling(input_path):
    """Analyser complètement les données"""
    
    print("\n" + "="*70)
    print("📊 DATA PROFILING - ANALYSE COMPLÈTE DES DONNÉES")
    print("="*70)
    
    # Étape 1: Charger les données
    print("\n[1/8] Chargement des données...")
    df, _ = load_dataset(input_path)
    print(f"✅ {len(df)} lignes, {len(df.columns)} colonnes")
    
    # Étape 2: Statistiques générales
    print("\n[2/8] Statistiques générales...")
    general_stats = {
        "total_rows": int(len(df)),
        "total_columns": int(len(df.columns)),
        "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
        "column_names": list(df.columns)
    }
    print(f"✅ Mémoire utilisée: {general_stats['memory_usage_mb']} MB")
    
    # Étape 3: Analyse par colonne
    print("\n[3/8] Analyse par colonne...")
    statistics = {}
    for col in df.columns:
        statistics[col] = analyze_column(df, col)
    print(f"✅ {len(statistics)} colonnes analysées")
    
    # Étape 4: Valeurs manquantes
    print("\n[4/8] Analyse des valeurs manquantes...")
    missing_values = {col: int(df[col].isna().sum()) for col in df.columns if df[col].isna().sum() > 0}
    total_missing = sum(missing_values.values())
    print(f"✅ {total_missing} valeurs manquantes détectées")
    
    # Étape 5: Doublons
    print("\n[5/8] Détection des doublons...")
    duplicates = detect_exact_duplicates(df)
    print(f"✅ {duplicates['exact_duplicates']} doublons exacts trouvés")
    
    # Étape 6: Valeurs aberrantes
    print("\n[6/8] Détection des valeurs aberrantes...")
    numeric_df = df.select_dtypes(include=[np.number])
    total_outliers = 0
    for col in numeric_df.columns:
        outlier_count, _, _ = detect_outliers_iqr(numeric_df[col].dropna())
        total_outliers += outlier_count
    print(f"✅ {total_outliers} valeurs aberrantes détectées")
    
    # Étape 7: Corrélations
    print("\n[7/8] Analyse des corrélations...")
    correlations = calculate_correlations(df)
    print(f"✅ {len(correlations)} corrélations fortes trouvées")
    
    # Étape 8: Score de qualité
    print("\n[8/8] Calcul du score de qualité...")
    
    report = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "input_path": str(input_path),
        "general": general_stats,
        "statistics": statistics,
        "missing_values": missing_values,
        "duplicates": duplicates,
        "correlations": correlations,
    }
    
    quality_score = calculate_quality_score(report)
    quality_grade = get_quality_grade(quality_score)
    
    report["quality_score"] = quality_score
    report["quality_grade"] = quality_grade
    
    print(f"✅ Score de qualité: {quality_score}/100 (Grade: {quality_grade})")
    
    # Sauvegarder le rapport JSON
    report_json_path = REPORTS_DIR / "profiling_report.json"
    with open(report_json_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Rapport JSON: {report_json_path}")
    
    # Générer rapport HTML
    generate_html_report(report)
    
    # Résumé
    print("\n" + "="*70)
    print("📊 RÉSUMÉ DU PROFILING")
    print("="*70)
    print(f"Lignes: {general_stats['total_rows']}")
    print(f"Colonnes: {general_stats['total_columns']}")
    print(f"Valeurs manquantes: {total_missing}")
    print(f"Doublons exacts: {duplicates['exact_duplicates']}")
    print(f"Valeurs aberrantes: {total_outliers}")
    print(f"Corrélations fortes: {len(correlations)}")
    print(f"Score de qualité: {quality_score}/100 ({quality_grade})")
    print("="*70 + "\n")


def generate_html_report(report):
    """Générer un rapport HTML interactif"""
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Data Profiling Report</title>
        <style>
            * {{ margin: 0; padding: 0; }}
            body {{ font-family: 'Arial', sans-serif; background: #f5f5f5; color: #333; }}
            .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
            header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 8px; margin-bottom: 30px; }}
            h1 {{ font-size: 32px; margin-bottom: 10px; }}
            h2 {{ color: #667eea; margin-top: 30px; margin-bottom: 15px; border-bottom: 2px solid #667eea; padding-bottom: 10px; }}
            .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }}
            .metric-card {{ background: white; padding: 20px; border-radius: 8px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .metric-value {{ font-size: 28px; font-weight: bold; color: #667eea; }}
            .metric-label {{ color: #666; margin-top: 5px; }}
            .quality-score {{ font-size: 48px; font-weight: bold; color: #4CAF50; }}
            .table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin: 20px 0; }}
            .table th {{ background: #667eea; color: white; padding: 15px; text-align: left; }}
            .table td {{ padding: 12px 15px; border-bottom: 1px solid #eee; }}
            .table tr:hover {{ background: #f9f9f9; }}
            footer {{ text-align: center; color: #666; margin-top: 50px; padding-top: 20px; border-top: 1px solid #ddd; }}
            .grade {{ padding: 10px 20px; border-radius: 4px; display: inline-block; font-weight: bold; color: white; }}
            .grade-a {{ background: #4CAF50; }}
            .grade-b {{ background: #2196F3; }}
            .grade-c {{ background: #FFC107; }}
            .grade-d {{ background: #F44336; }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>📊 Rapport de Profiling des Données</h1>
                <p>Analyse complète et détaillée des données</p>
            </header>
            
            <section>
                <h2>⭐ Score de Qualité</h2>
                <div class="metrics">
                    <div class="metric-card">
                        <div class="quality-score">{report['quality_score']}/100</div>
                        <div class="metric-label">Score de Qualité</div>
                        <div class="grade grade-{report['quality_grade'].lower()}">Grade: {report['quality_grade']}</div>
                    </div>
                </div>
            </section>
            
            <section>
                <h2>📈 Statistiques Générales</h2>
                <div class="metrics">
                    <div class="metric-card">
                        <div class="metric-value">{report['general']['total_rows']}</div>
                        <div class="metric-label">Lignes</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{report['general']['total_columns']}</div>
                        <div class="metric-label">Colonnes</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{report['general']['memory_usage_mb']}</div>
                        <div class="metric-label">Mémoire (MB)</div>
                    </div>
                </div>
            </section>
            
            <section>
                <h2>⚠️ Problèmes Détectés</h2>
                <div class="metrics">
                    <div class="metric-card">
                        <div class="metric-value">{sum(report['missing_values'].values())}</div>
                        <div class="metric-label">Valeurs Manquantes</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{report['duplicates']['exact_duplicates']}</div>
                        <div class="metric-label">Doublons Exacts</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{len(report['correlations'])}</div>
                        <div class="metric-label">Corrélations Fortes</div>
                    </div>
                </div>
            </section>
            
            <section>
                <h2>📋 Analyse par Colonne</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>Colonne</th>
                            <th>Type</th>
                            <th>Non-Null</th>
                            <th>Uniques</th>
                            <th>Manquantes %</th>
                        </tr>
                    </thead>
                    <tbody>
    """
    
    for col_name, stats in report['statistics'].items():
        html_content += f"""
                        <tr>
                            <td><strong>{col_name}</strong></td>
                            <td>{stats['type']}</td>
                            <td>{stats['non_null_count']}</td>
                            <td>{stats['unique_count']}</td>
                            <td>{stats['null_percentage']}%</td>
                        </tr>
        """
    
    html_content += """
                    </tbody>
                </table>
            </section>
            
            <footer>
                <p>Rapport généré par <strong>UTL - Data Profiling</strong></p>
                <p>🧹 Pipeline ETL + Machine Learning</p>
            </footer>
        </div>
    </body>
    </html>
    """
    
    report_html_path = REPORTS_DIR / "profiling_report.html"
    with open(report_html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Rapport HTML: {report_html_path}")


def main(input_path):
    """Point d'entrée principal"""
    profiling(input_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Profiling - Analyse complète")
    parser.add_argument("--input", required=True, help="Chemin du fichier CSV")
    args = parser.parse_args()
    main(args.input)
