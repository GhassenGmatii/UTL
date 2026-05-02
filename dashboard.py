import streamlit as st
import pandas as pd
import json
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from src.train import main as train_main
from src.pipeline import main as pipeline_main
from src.profiling import main as profiling_main
from src.benchmark import main as benchmark_main
from src.load_to_db import main as load_db_main

# Configuration Streamlit
st.set_page_config(
    page_title="UTL - ETL & AI Dashboard",
    page_icon="🧹",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Titre
st.markdown("# 🧹 UTL - Pipeline ETL + Machine Learning")
st.markdown("*Automatiser le nettoyage et la déduplication de données avec l'IA*")

# Sidebar
with st.sidebar:
    st.markdown("## ⚙️ Menu")
    page = st.radio(
        "Sélectionner une page:",
        ["📤 Traitement", "📊 Data Profiling", "🔄 Benchmark", "🗄️ SQL Server"]
    )

# PAGE 1: TRAITEMENT
if page == "📤 Traitement":
    st.markdown("## 📤 Traitement des données")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 1️⃣ Charger un fichier CSV")
        uploaded_file = st.file_uploader("Choisir un fichier CSV", type="csv")
        
        if uploaded_file:
            # Sauvegarder le fichier
            input_path = Path("data/raw") / uploaded_file.name
            input_path.parent.mkdir(parents=True, exist_ok=True)
            input_path.write_bytes(uploaded_file.getbuffer())
            
            st.success(f"✅ Fichier chargé: {uploaded_file.name}")
            
            # Afficher aperçu
            df = pd.read_csv(input_path)
            st.markdown("### 📋 Aperçu des données")
            st.dataframe(df.head(), use_container_width=True)
            
            st.markdown(f"**Dimensions:** {df.shape[0]} lignes × {df.shape[1]} colonnes")
    
    with col2:
        st.markdown("### 🎯 Actions")
        
        col_btn1, col_btn2 = st.columns(1)
        
        if st.button("🚀 Entraîner modèles", use_container_width=True):
            with st.spinner("⏳ Entraînement en cours..."):
                try:
                    train_main(str(input_path))
                    st.success("✅ Modèles entraînés!")
                except Exception as e:
                    st.error(f"❌ Erreur: {e}")
        
        if st.button("🔧 Lancer pipeline", use_container_width=True):
            with st.spinner("⏳ Traitement en cours..."):
                try:
                    pipeline_main(str(input_path))
                    st.success("✅ Pipeline terminé!")
                except Exception as e:
                    st.error(f"❌ Erreur: {e}")
    
    # Afficher les résultats
    results_path = Path("data/processed/customers_deduped.csv")
    if results_path.exists():
        st.markdown("### 📊 Résultats")
        
        df_results = pd.read_csv(results_path)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Lignes finales", len(df_results))
        
        with col2:
            anomalies = int(df_results["is_anomaly"].sum()) if "is_anomaly" in df_results.columns else 0
            st.metric("Anomalies", anomalies)
        
        with col3:
            score_moy = df_results["anomaly_score"].mean() if "anomaly_score" in df_results.columns else 0
            st.metric("Score moy anomalies", f"{score_moy:.2f}")
        
        with col4:
            st.metric("Colonnes", len(df_results.columns))
        
        st.dataframe(df_results, use_container_width=True)

# PAGE 2: DATA PROFILING
elif page == "📊 Data Profiling":
    st.markdown("## 📊 Analyse complète des données")
    
    input_file = st.selectbox(
        "Choisir un fichier CSV:",
        options=list(Path("data/raw").glob("*.csv")) if Path("data/raw").exists() else []
    )
    
    if st.button("🔍 Analyser les données", use_container_width=True):
        with st.spinner("⏳ Profiling en cours..."):
            try:
                profiling_main(str(input_file))
                st.success("✅ Profiling terminé!")
                
                # Charger et afficher le rapport
                report_path = Path("reports/profiling_report.json")
                if report_path.exists():
                    with open(report_path) as f:
                        report = json.load(f)
                    
                    st.markdown("### 📈 Résultats")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Lignes", report['general']['total_rows'])
                    with col2:
                        st.metric("Colonnes", report['general']['total_columns'])
                    with col3:
                        total_missing = sum(v['count'] for v in report['missing_values'].values())
                        st.metric("Manquantes", total_missing)
                    with col4:
                        st.metric("Score", f"{report['quality_score']:.0f}/100")
                    
                    # Grade
                    grade_color = "🟢" if report['quality_score'] >= 80 else "🟡" if report['quality_score'] >= 60 else "🔴"
                    st.markdown(f"### {grade_color} Grade: {report['quality_grade']}")
                    
                    # Statistiques détaillées
                    st.markdown("### 📊 Statistiques par colonne")
                    
                    stats_data = []
                    for col, stat in report['statistics'].items():
                        if stat['type'] == 'numeric':
                            stats_data.append({
                                'Colonne': col,
                                'Type': 'Numérique',
                                'Moyenne': f"{stat['mean']:.2f}",
                                'Min': f"{stat['min']:.2f}",
                                'Max': f"{stat['max']:.2f}"
                            })
                        else:
                            stats_data.append({
                                'Colonne': col,
                                'Type': 'Texte',
                                'Uniques': stat['unique_count'],
                                'Plus commun': stat['most_common']
                            })
                    
                    st.dataframe(pd.DataFrame(stats_data), use_container_width=True)
                    
                    # Rapport HTML
                    html_path = Path("reports/profiling_report.html")
                    if html_path.exists():
                        with open(html_path) as f:
                            html_content = f.read()
                        st.components.v1.html(html_content, height=600, scrolling=True)
                        
                        st.download_button(
                            label="⬇️ Télécharger rapport HTML",
                            data=html_content,
                            file_name="profiling_report.html",
                            mime="text/html"
                        )
            
            except Exception as e:
                st.error(f"❌ Erreur: {e}")

# PAGE 3: BENCHMARK
elif page == "🔄 Benchmark":
    st.markdown("## 🔄 Benchmark: ETL Classique vs ETL avec IA")
    
    input_file = st.selectbox(
        "Choisir un fichier CSV:",
        options=list(Path("data/raw").glob("*.csv")) if Path("data/raw").exists() else []
    )
    
    if st.button("⚡ Lancer le benchmark", use_container_width=True):
        with st.spinner("⏳ Benchmark en cours..."):
            try:
                benchmark_main(str(input_file))
                st.success("✅ Benchmark terminé!")
                
                # Charger le rapport
                report_path = Path("reports/benchmark_report.json")
                if report_path.exists():
                    with open(report_path) as f:
                        report = json.load(f)
                    
                    # Résultats
                    st.markdown("### 🎯 Comparaison")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("#### 🔧 ETL Classique")
                        st.metric("Temps", f"{report['etl_classique']['duration']:.3f}s")
                        st.metric("Doublons détectés", report['etl_classique']['duplicates_found'])
                        st.metric("Anomalies", report['etl_classique']['anomalies_found'])
                    
                    with col2:
                        st.markdown("#### 🤖 ETL avec IA")
                        st.metric("Temps", f"{report['etl_ai']['duration']:.3f}s")
                        st.metric("Doublons détectés", report['etl_ai']['duplicates_found'])
                        st.metric("Anomalies", report['etl_ai']['anomalies_found'])
                    
                    # Graphique comparatif
                    data = {
                        'Méthode': ['ETL Classique', 'ETL IA'],
                        'Temps (s)': [report['etl_classique']['duration'], report['etl_ai']['duration']],
                        'Doublons': [report['etl_classique']['duplicates_found'], report['etl_ai']['duplicates_found']],
                    }
                    df_comp = pd.DataFrame(data)
                    
                    fig = px.bar(df_comp, x='Méthode', y=['Temps (s)', 'Doublons'], 
                                barmode='group', title="Comparaison des performances")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Verdict
                    st.markdown(f"### 🏆 Verdict")
                    st.info(report['verdict']['overall_winner'])
                    st.write(report['conclusion'])
            
            except Exception as e:
                st.error(f"❌ Erreur: {e}")

# PAGE 4: SQL SERVER
elif page == "🗄️ SQL Server":
    st.markdown("## 🗄️ Export vers SQL Server")
    
    st.markdown("### Importer les données dans SQL Server")
    st.markdown("""
    **Configuration:**
    - Serveur: `localhost\\SQLEXPRESS`
    - Base de données: `UTL_DB` (créée automatiquement)
    - Tables:
      - `customers_clean` - Données nettoyées
      - `customers_dedup` - Données finales + anomalies
    """)
    
    if st.button("🚀 Charger les données dans SQL Server", use_container_width=True):
        with st.spinner("⏳ Chargement en cours..."):
            try:
                load_db_main()
                st.success("✅ Données chargées dans SQL Server!")
                
                st.markdown("### 📊 Requêtes SQL utiles")
                
                queries = {
                    "Voir tous les clients": "SELECT * FROM customers_clean;",
                    "Voir les anomalies": "SELECT * FROM customers_dedup WHERE is_anomaly = 1;",
                    "Total de lignes": "SELECT COUNT(*) as total FROM customers_dedup;",
                    "Clients par pays": "SELECT country, COUNT(*) as count FROM customers_clean GROUP BY country;",
                }
                
                for name, query in queries.items():
                    st.code(query, language="sql")
            
            except Exception as e:
                st.error(f"❌ Erreur: {e}")

# Footer
st.markdown("---")
st.markdown("🧹 **UTL** - Pipeline ETL + Machine Learning | Made with ❤️ by GhassenGmatii")
st.markdown("[GitHub](https://github.com/GhassenGmatii/UTL) | [Documentation](https://github.com/GhassenGmatii/UTL/blob/main/README.md)")
