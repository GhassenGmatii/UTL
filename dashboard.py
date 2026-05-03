"""
dashboard.py — Interface utilisateur Streamlit du pipeline ETL
==============================================================
Ce fichier constitue le point d'entrée de l'application web.

Pages disponibles :
  📤 Traitement   — Import CSV, entraînement IA, pipeline ETL
  📊 Profiling    — Analyse qualité des données (stats, outliers…)
  🔄 Benchmark    — Comparaison ETL Classique vs ETL IA
  🗄️ SQL Server   — Export des données vers la base UTL_DB

Technologie : Streamlit (framework Python pour interfaces data)
Lancement   : streamlit run dashboard.py
"""

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Configuration de la page Streamlit ────────────────────────────────────────
# Doit être le premier appel Streamlit dans le script
st.set_page_config(
    page_title="UTL - ETL & AI Dashboard",
    page_icon="🧹",
    layout="wide",                  # Utilise toute la largeur de l'écran
    initial_sidebar_state="expanded"
)

# ── Chemins des fichiers générés par le pipeline ───────────────────────────────
REPORTS_DIR    = Path("reports/quality_reports")
PROFILING_JSON = REPORTS_DIR / "profiling_report.json"   # Rapport profiling
PROFILING_HTML = REPORTS_DIR / "profiling_report.html"   # Rapport HTML profiling
BENCHMARK_JSON = REPORTS_DIR / "benchmark_report.json"   # Rapport benchmark
BENCHMARK_HTML = REPORTS_DIR / "benchmark_report.html"   # Rapport HTML benchmark
QUALITY_JSON   = REPORTS_DIR / "quality_report.json"     # Rapport pipeline ETL
CLEAN_CSV      = Path("data/processed/customers_clean.csv")   # Données après nettoyage
DEDUP_CSV      = Path("data/processed/customers_deduped.csv") # Données finales
RAW_DIR        = Path("data/raw")                             # Dossier des CSV bruts


# ── Chargement des modules ETL (mis en cache pour éviter les rechargements) ────
@st.cache_resource
def load_etl_modules():
    """
    Importe et met en cache les fonctions principales du pipeline ETL.
    Le décorateur @st.cache_resource garantit que l'import n'est fait
    qu'une seule fois, même si l'utilisateur interagit avec l'interface.

    Retourne
    --------
    (train_main, pipeline_main, profiling_main, benchmark_main)
    """
    from src.benchmark import main as benchmark_main
    from src.pipeline  import main as pipeline_main
    from src.profiling import main as profiling_main
    from src.train     import main as train_main
    return train_main, pipeline_main, profiling_main, benchmark_main


# Chargement au démarrage de l'application
train_main, pipeline_main, profiling_main, benchmark_main = load_etl_modules()

# ── Titre principal ────────────────────────────────────────────────────────────
st.markdown("# 🧹 UTL — Pipeline ETL + Machine Learning")
st.markdown("*Automatiser le nettoyage et la déduplication de données avec l'IA*")

# ── Barre latérale (navigation + statut des fichiers) ─────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Navigation")

    # Sélection de la page active
    page = st.radio(
        "Sélectionner une page:",
        ["📤 Traitement", "📊 Data Profiling", "🔄 Benchmark", "🗄️ SQL Server"]
    )

    st.markdown("---")
    st.markdown("**Fichiers générés**")

    # Indicateurs visuels de l'état des fichiers (✅ si présent, ❌ sinon)
    st.markdown(f"{'✅' if CLEAN_CSV.exists()      else '❌'} Données nettoyées")
    st.markdown(f"{'✅' if DEDUP_CSV.exists()      else '❌'} Données finales")
    st.markdown(f"{'✅' if PROFILING_JSON.exists() else '❌'} Rapport profiling")
    st.markdown(f"{'✅' if BENCHMARK_JSON.exists() else '❌'} Rapport benchmark")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — TRAITEMENT
# Permet d'importer un CSV, d'entraîner les modèles IA et de lancer le pipeline
# ══════════════════════════════════════════════════════════════════════════════
if page == "📤 Traitement":
    st.markdown("## 📤 Traitement des données")

    # Disposition en deux colonnes : upload à gauche, actions à droite
    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("### 1️⃣ Charger un fichier CSV")

        # Widget d'upload : accepte uniquement les fichiers .csv
        uploaded_file = st.file_uploader("Choisir un fichier CSV", type="csv")

        input_path = None

        if uploaded_file:
            # Sauvegarde du fichier uploadé dans data/raw/
            input_path = RAW_DIR / uploaded_file.name
            RAW_DIR.mkdir(parents=True, exist_ok=True)
            input_path.write_bytes(uploaded_file.getbuffer())
            st.success(f"✅ Fichier chargé : **{uploaded_file.name}**")

            # Aperçu des 10 premières lignes
            df = pd.read_csv(input_path)
            st.markdown("### 📋 Aperçu des données brutes")
            st.dataframe(df.head(10), use_container_width=True)
            st.markdown(f"**Dimensions :** {df.shape[0]} lignes × {df.shape[1]} colonnes")

        elif RAW_DIR.exists():
            # Si aucun upload, propose les fichiers déjà présents dans data/raw/
            existing = list(RAW_DIR.glob("*.csv"))
            if existing:
                selected = st.selectbox("…ou utiliser un fichier existant :", existing)
                if selected:
                    input_path = selected
                    df = pd.read_csv(input_path)
                    st.markdown("### 📋 Aperçu des données")
                    st.dataframe(df.head(10), use_container_width=True)
                    st.markdown(f"**Dimensions :** {df.shape[0]} lignes × {df.shape[1]} colonnes")

    with col2:
        st.markdown("### 🎯 Actions")

        if input_path is None:
            # Invite l'utilisateur à charger un fichier avant de pouvoir agir
            st.info("⬅️ Chargez un fichier CSV pour activer les actions.")
        else:
            # ── Bouton 1 : Entraîner les modèles IA ───────────────────────────
            # Lance train.py → entraîne RandomForest (dedup) + IsolationForest (anomalies)
            # et sauvegarde les modèles dans data/models/
            if st.button("🚀 1. Entraîner les modèles IA", use_container_width=True):
                with st.spinner("⏳ Entraînement en cours…"):
                    try:
                        train_main(str(input_path))
                        st.success("✅ Modèles entraînés et sauvegardés !")
                    except Exception as e:
                        st.error(f"❌ Erreur : {e}")

            st.markdown("---")

            # ── Bouton 2 : Lancer le pipeline ETL complet ──────────────────────
            # Lance pipeline.py → nettoyage + déduplication + anomalies + export CSV
            if st.button("🔧 2. Lancer le pipeline ETL", use_container_width=True):
                with st.spinner("⏳ Pipeline en cours…"):
                    try:
                        pipeline_main(str(input_path))
                        st.success("✅ Pipeline ETL terminé !")
                        st.rerun()   # Rafraîchit la page pour afficher les résultats
                    except Exception as e:
                        st.error(f"❌ Erreur : {e}")

    # ── Section résultats (visible seulement si le pipeline a déjà tourné) ─────
    if DEDUP_CSV.exists():
        st.markdown("---")
        st.markdown("### 📊 Résultats du pipeline")

        df_results = pd.read_csv(DEDUP_CSV)

        # Métriques clés en 4 colonnes
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("📄 Lignes finales",    len(df_results))
        m2.metric("🚨 Anomalies",
                  int(df_results["is_anomaly"].sum()) if "is_anomaly" in df_results.columns else "—")
        m3.metric("📐 Score anomalie moy",
                  f"{df_results['anomaly_score'].mean():.3f}" if "anomaly_score" in df_results.columns else "—")
        m4.metric("📊 Colonnes",          len(df_results.columns))

        # Rapport qualité (avant/après nettoyage)
        if QUALITY_JSON.exists():
            with open(QUALITY_JSON, encoding="utf-8") as f:
                qr = json.load(f)
            st.markdown("#### 🔎 Rapport qualité")
            c1, c2, c3 = st.columns(3)
            c1.metric("Lignes originales", qr.get("cleaning", {}).get("rows_before", "—"))
            c2.metric("Après nettoyage",   qr.get("cleaning", {}).get("rows_after",  "—"))
            c3.metric("Doublons ML",       qr.get("deduplication", {}).get("ml_duplicates_found", 0))

        # Tableau complet des données nettoyées
        st.markdown("#### 📋 Tableau des données nettoyées")
        st.dataframe(df_results, use_container_width=True)

        # Bouton de téléchargement du CSV nettoyé
        st.download_button(
            "⬇️ Télécharger les données nettoyées (CSV)",
            data=df_results.to_csv(index=False).encode("utf-8"),
            file_name="donnees_nettoyees.csv",
            mime="text/csv",
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — DATA PROFILING
# Analyse complète de la qualité des données : stats, outliers, doublons…
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊 Data Profiling":
    st.markdown("## 📊 Analyse complète des données (Profiling)")

    # Vérifie qu'il y a des fichiers CSV disponibles
    raw_files = list(RAW_DIR.glob("*.csv")) if RAW_DIR.exists() else []
    if not raw_files:
        st.warning("⚠️ Aucun fichier CSV dans `data/raw/`. Allez d'abord dans **📤 Traitement** pour uploader un fichier.")
    else:
        # Sélection du fichier à analyser
        input_file = st.selectbox("Choisir un fichier CSV à analyser :", raw_files)

        # Bouton de lancement du profiling
        if st.button("🔍 Lancer l'analyse Profiling", use_container_width=True):
            with st.spinner("⏳ Profiling en cours…"):
                try:
                    profiling_main(str(input_file))
                    st.success("✅ Profiling terminé !")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Erreur profiling : {e}")

        # ── Affichage du rapport si disponible ────────────────────────────────
        if PROFILING_JSON.exists():
            with open(PROFILING_JSON, encoding="utf-8") as f:
                report = json.load(f)

            st.markdown("---")
            st.markdown("### 📈 Résultats du Profiling")

            # Métriques principales
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("📄 Lignes",            report["general"]["total_rows"])
            m2.metric("📊 Colonnes",           report["general"]["total_columns"])

            # Calcul du total des valeurs manquantes (dict col → count)
            total_missing = sum(report["missing_values"].values()) if report["missing_values"] else 0
            m3.metric("❓ Valeurs manquantes", total_missing)
            m4.metric("⭐ Score qualité",      f"{report['quality_score']:.0f}/100")

            # Grade de qualité avec icône colorée
            grade = report["quality_grade"]
            grade_icon = {"A": "🟢", "B": "🔵", "C": "🟡", "D": "🔴"}.get(grade, "⚪")
            st.markdown(f"### {grade_icon} Grade de qualité : **{grade}**")

            # Informations sur les doublons
            dup = report["duplicates"]
            st.markdown(
                f"- **Doublons exacts :** {dup['exact_duplicates']} "
                f"({dup['exact_duplicates_percentage']}%)"
            )

            # Corrélations fortes (si présentes)
            if report.get("correlations"):
                st.markdown("#### 🔗 Corrélations fortes (> 0.7)")
                corr_data = [
                    {"Paire": k, "Corrélation": round(v, 3)}
                    for k, v in report["correlations"].items()
                ]
                st.dataframe(pd.DataFrame(corr_data), use_container_width=True)

            # Tableau des statistiques par colonne
            st.markdown("#### 📋 Statistiques par colonne")
            stats_rows = []
            for col_name, stat in report["statistics"].items():
                row = {
                    "Colonne":     col_name,
                    "Type":        stat["type"],
                    "Non-null":    stat["non_null_count"],
                    "Manquants %": f"{stat['null_percentage']}%",
                    "Uniques":     stat["unique_count"],
                }
                if stat["type"] == "numeric":
                    row.update({
                        "Moyenne": f"{stat.get('mean', 0):.2f}",
                        "Min":     f"{stat.get('min', 0):.2f}",
                        "Max":     f"{stat.get('max', 0):.2f}",
                        "Outliers": stat.get("outliers_count", 0),
                    })
                else:
                    row["Plus fréquent"] = stat.get("most_common", "—")
                stats_rows.append(row)

            st.dataframe(pd.DataFrame(stats_rows), use_container_width=True)

            # Graphique des valeurs manquantes (si présentes)
            if report["missing_values"]:
                fig_missing = px.bar(
                    x=list(report["missing_values"].keys()),
                    y=list(report["missing_values"].values()),
                    labels={"x": "Colonne", "y": "Valeurs manquantes"},
                    title="Valeurs manquantes par colonne",
                    color=list(report["missing_values"].values()),
                    color_continuous_scale="reds",
                )
                st.plotly_chart(fig_missing, use_container_width=True)

            # Rapport HTML complet intégré dans la page
            if PROFILING_HTML.exists():
                with open(PROFILING_HTML, encoding="utf-8") as f:
                    html_content = f.read()
                st.markdown("---")
                st.markdown("#### 📄 Rapport HTML interactif")
                st.components.v1.html(html_content, height=600, scrolling=True)
                st.download_button(
                    "⬇️ Télécharger le rapport HTML",
                    data=html_content,
                    file_name="profiling_report.html",
                    mime="text/html",
                )
        else:
            st.info("ℹ️ Cliquez sur **Lancer l'analyse Profiling** pour générer le rapport.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — BENCHMARK
# Compare l'ETL classique et l'ETL IA sur les mêmes données
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔄 Benchmark":
    st.markdown("## 🔄 Benchmark : ETL Classique vs ETL avec IA")

    raw_files = list(RAW_DIR.glob("*.csv")) if RAW_DIR.exists() else []
    if not raw_files:
        st.warning("⚠️ Aucun fichier CSV dans `data/raw/`.")
    else:
        input_file = st.selectbox("Choisir un fichier CSV :", raw_files)

        # Bouton de lancement du benchmark
        if st.button("⚡ Lancer le Benchmark", use_container_width=True):
            with st.spinner("⏳ Benchmark en cours…"):
                try:
                    benchmark_main(str(input_file))
                    st.success("✅ Benchmark terminé !")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Erreur benchmark : {e}")

        # ── Affichage des résultats si le rapport existe ──────────────────────
        if BENCHMARK_JSON.exists():
            with open(BENCHMARK_JSON, encoding="utf-8") as f:
                report = json.load(f)

            st.markdown("---")
            st.markdown("### 🎯 Comparaison des résultats")

            # Colonnes côte à côte : ETL Classique | ETL IA
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 🔧 ETL Classique")
                st.metric("⏱️ Temps",              f"{report['etl_classique']['duration']:.3f} s")
                st.metric("📋 Lignes traitées",    report["etl_classique"]["rows_processed"])
                st.metric("📋 Lignes finales",      report["etl_classique"]["rows_final"])
                st.metric("🔁 Doublons détectés",   report["etl_classique"]["duplicates_found"])
                st.metric("⚠️ Anomalies détectées", report["etl_classique"]["anomalies_found"])

            with col2:
                st.markdown("#### 🤖 ETL avec IA")
                st.metric("⏱️ Temps",              f"{report['etl_ai']['duration']:.3f} s")
                st.metric("📋 Lignes traitées",    report["etl_ai"]["rows_processed"])
                st.metric("📋 Lignes finales",      report["etl_ai"]["rows_final"])
                st.metric("🔁 Doublons détectés",   report["etl_ai"]["duplicates_found"])
                st.metric("⚠️ Anomalies détectées", report["etl_ai"]["anomalies_found"])

            # ── Graphiques comparatifs ─────────────────────────────────────────
            st.markdown("#### 📊 Visualisation comparative")

            # Graphique 1 : Temps d'exécution
            fig_time = px.bar(
                x=["ETL Classique", "ETL avec IA"],
                y=[report["etl_classique"]["duration"], report["etl_ai"]["duration"]],
                labels={"x": "Méthode", "y": "Temps (s)"},
                title="Comparaison des temps d'exécution",
                color=["ETL Classique", "ETL avec IA"],
                color_discrete_map={"ETL Classique": "#FF6B6B", "ETL avec IA": "#4ECDC4"},
            )
            st.plotly_chart(fig_time, use_container_width=True)

            # Graphique 2 : Doublons détectés
            fig_dup = px.bar(
                x=["ETL Classique", "ETL avec IA"],
                y=[report["etl_classique"]["duplicates_found"], report["etl_ai"]["duplicates_found"]],
                labels={"x": "Méthode", "y": "Doublons détectés"},
                title="Doublons détectés par méthode",
                color=["ETL Classique", "ETL avec IA"],
                color_discrete_map={"ETL Classique": "#FF6B6B", "ETL avec IA": "#4ECDC4"},
            )
            st.plotly_chart(fig_dup, use_container_width=True)

            # ── Verdict final ──────────────────────────────────────────────────
            st.markdown("---")
            st.markdown("### 🏆 Verdict")
            st.success(report["verdict"]["overall_winner"])
            for reason in report["verdict"].get("reasons", []):
                st.markdown(f"- {reason}")
            st.info(f"**Conclusion :** {report['conclusion']}")

            # Rapport HTML téléchargeable
            if BENCHMARK_HTML.exists():
                with open(BENCHMARK_HTML, encoding="utf-8") as f:
                    html_content = f.read()
                st.download_button(
                    "⬇️ Télécharger le rapport Benchmark HTML",
                    data=html_content,
                    file_name="benchmark_report.html",
                    mime="text/html",
                )
        else:
            st.info("ℹ️ Cliquez sur **Lancer le Benchmark** pour générer le rapport.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — SQL SERVER
# Export des données traitées vers la base de données UTL_DB
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🗄️ SQL Server":
    st.markdown("## 🗄️ Export vers SQL Server")

    # Instructions de configuration requise
    st.info("""
    **Configuration requise :**
    - SQL Server Express installé localement
    - Serveur : `localhost\\SQLEXPRESS`
    - Base de données : `UTL_DB` *(créée automatiquement)*
    """)

    # Vérification de la disponibilité des fichiers CSV sources
    col_status1, col_status2 = st.columns(2)
    col_status1.markdown(
        f"**Données nettoyées :** "
        f"{'✅ Disponibles' if CLEAN_CSV.exists() else '❌ Manquantes — lancez le pipeline d abord'}"
    )
    col_status2.markdown(
        f"**Données finales :** "
        f"{'✅ Disponibles' if DEDUP_CSV.exists() else '❌ Manquantes — lancez le pipeline d abord'}"
    )

    # Avertissement si les CSV ne sont pas encore générés
    if not CLEAN_CSV.exists() or not DEDUP_CSV.exists():
        st.warning("⚠️ Lancez d'abord le **Pipeline ETL** dans la page **📤 Traitement**.")
    else:
        # Bouton d'export vers SQL Server
        # Appelle load_to_db.main() qui :
        #   1. Crée la base UTL_DB (si elle n'existe pas)
        #   2. Crée les tables dynamiquement selon les colonnes des CSV
        #   3. Insère toutes les lignes
        if st.button("🚀 Charger les données dans SQL Server", use_container_width=True):
            with st.spinner("⏳ Chargement en cours…"):
                try:
                    from src.load_to_db import main as load_db_main
                    load_db_main()
                    st.success("✅ Données chargées dans SQL Server !")
                except Exception as e:
                    st.error(f"❌ Erreur : {e}")

        # ── Requêtes SQL utiles pour vérifier les données dans SSMS ───────────
        st.markdown("---")
        st.markdown("### 📋 Requêtes SQL utiles")
        queries = {
            "Voir tous les patients":   "SELECT * FROM patients_clean;",
            "Voir les anomalies":       "SELECT * FROM patients_dedup WHERE is_anomaly = 1;",
            "Total de lignes":          "SELECT COUNT(*) AS total FROM patients_dedup;",
            "Patients par ville":       "SELECT ville, COUNT(*) AS nb FROM patients_clean GROUP BY ville ORDER BY nb DESC;",
        }
        for label, query in queries.items():
            st.markdown(f"**{label}**")
            st.code(query, language="sql")


# ── Pied de page ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("🧹 **UTL** — Pipeline ETL + Machine Learning &nbsp;|&nbsp; Made with ❤️ by GhassenGmatii")
