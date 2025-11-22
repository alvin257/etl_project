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
st.title("⚙️ ETL Pipeline Dashboard")
st.markdown(
    """
    <div style='font-size:18px;'>
    Bienvenue dans votre interface **ETL pédagogique et interactive** 🎓<br><br>
    Cette application illustre le fonctionnement complet d’un pipeline **ETL (Extract – Transform – Load)**, 
    depuis la collecte de données jusqu’à leur visualisation et analyse.
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown("---")

# =======================
# VISUEL DU PIPELINE
# =======================
st.subheader("🧩 Schéma général du pipeline ETL")

st.image(
    "app/images/etl_schema.png",
    caption="Représentation visuelle du processus ETL : Extract → Transform → Load",
    use_container_width=True
)

st.markdown("---")

# =======================
# DESCRIPTION DU PROJET
# =======================
st.subheader("🧠 À propos du projet")
st.markdown(
    """
    Ce projet a pour but de **montrer comment un pipeline ETL peut être parallélisé, 
    monitoré et visualisé** à l’aide d’outils modernes.

    ### 🔧 Technologies principales
    - **🐍 Python** – langage principal du projet  
    - **⚙️ Dask** – traitement parallèle et scalable des gros volumes  
    - **📄 YAML** – configuration du pipeline sans modification du code  
    - **💻 Streamlit** – interface de pilotage et de visualisation interactive  

    ### 📦 Données utilisées
    Le dataset de test est celui des **trajets de taxis de New York (NYC TLC – AWS Open Data)**.  
    Chaque fichier représente un mois complet de courses, utilisé ici pour illustrer un cas d’usage concret 
    de **nettoyage, transformation et analyse de données massives**.
    """
)


st.markdown("---")

# =======================
# MANIFEST (résultats s’ils existent déjà)
# =======================
st.subheader("📊 Résultats du pipeline (manifest.json)")

manifest_path = Path("outputs/manifest.json")

if manifest_path.exists() and manifest_path.stat().st_size > 0:
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        st.success("Manifest chargé ✅")
        st.json(manifest)
    except json.JSONDecodeError:
        st.error("Le fichier manifest.json semble corrompu. Relancez le pipeline pour le régénérer.")
else:

    st.info("Aucun manifest trouvé pour l’instant. Lancez le pipeline depuis la page **🚀 Lancer le pipeline** pour générer les résultats.")

# =======================
# FOOTER
# =======================
st.markdown("---")
st.caption("🧑‍💻 Projet Master MIASHS — Parallel Computing & ETL — Streamlit + Dask + Python")
