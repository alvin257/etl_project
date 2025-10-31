import streamlit as st
from pathlib import Path
import json

# =======================
# PAGE CONFIGURATION
# =======================
st.set_page_config(
    page_title="ETL Dashboard",
    page_icon="⚙️",
    layout="wide",
)

# =======================
# HEADER
# =======================
st.title("⚙️ Pipeline ETL Dashboard")
st.markdown(
    """
    <div style='font-size:18px;'>
    Bienvenue dans votre interface **ETL pédagogique et interactive**.<br>
    Cette application illustre pas à pas le fonctionnement d’un pipeline ETL moderne :
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("---")

# =======================
# ÉTAPE 1 : SCHÉMA VISUEL ETL
# =======================

st.image("app\images\etl_schema.png", caption="Schéma du pipeline ETL", use_container_width=True)

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("📥 Extract")
    st.markdown("""
    - Lecture de données **massives**
    - Chargement parallèle via **Dask**
    - Uniformisation du schéma
    """)
with col2:
    st.subheader("🔄 Transform")
    st.markdown("""
    - Nettoyage, normalisation, enrichissement  
    - Calcul de **durées, distances, zones, horaires**
    - Profiling et validation des données
    """)
with col3:
    st.subheader("📦 Load")
    st.markdown("""
    - Sauvegarde au format **Parquet**
    - Création de **rapports et agrégats**
    - Génération du **manifest.json** pour le dashboard
    """)

st.markdown("---")

# =======================
# ÉTAPE 2 : EXPLICATION DU PROJET
# =======================
with st.expander("🧠 Comprendre ce projet (pour les curieux)", expanded=True):
    st.markdown("""
    Ce projet a pour objectif de **montrer comment un pipeline ETL peut être parallélisé et monitoré** grâce à :
    - **Dask** → pour paralléliser les traitements sur des fichiers volumineux
    - **YAML** → pour configurer le pipeline sans changer le code
    - **Streamlit** → pour visualiser et contrôler le processus en temps réel

    Le dataset de test utilisé est le jeu de données  **NYC Taxi (AWS Open Data)** :  
    chaque fichier représente un mois de trajets de taxis à New York 🗽
    """)

st.markdown("---")

# =======================
# ÉTAPE 3 : MANIFEST / RÉSULTATS
# =======================
st.subheader("📊 Résultats du pipeline (manifest.json)")

manifest_path = Path("outputs/manifest.json")

if not manifest_path.exists():
    st.warning("Aucun manifest trouvé. Lancez le pipeline depuis l’onglet **Run Pipeline** à gauche.")
    st.stop()

if manifest_path.stat().st_size == 0:
    st.error("Le fichier manifest.json est vide. Exécutez le pipeline pour le régénérer.")
    st.stop()

try:
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    st.success("Manifest chargé ✅")
    st.json(manifest)
except json.JSONDecodeError:
    st.error("Le manifest est corrompu. Relancez le pipeline.")
    st.stop()

# =======================
# FOOTER
# =======================
st.markdown("---")
st.caption("🧑‍💻 Projet Master MIASHS — Parallel Computing & ETL — Streamlit + Dask + Python")
