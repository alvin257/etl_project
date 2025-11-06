import streamlit as st
import pandas as pd
from pathlib import Path

# ======================
# TITRE + INTRO
# ======================
st.set_page_config(page_title="Aperçu des données", page_icon="👀", layout="wide")
st.title("👀 Aperçu des données extraites")

with st.sidebar:
    st.markdown("### 📊 Navigation")
    st.info("💡 Vous êtes dans la section *Preview* : aperçu des données extraites.")

with st.expander("🧠 À quoi sert cette étape ?"):
    st.markdown("""
    Cette page affiche un **aperçu des données extraites et transformées** par le pipeline ETL.
    
    - Vérifier que les colonnes sont bien lues 🧾  
    - Contrôler les types de données (dates, numériques, catégorielles)  
    - Identifier d’éventuelles anomalies avant l’analyse 📊  
    
    Le fichier affiché ici est **`outputs/sample_preview.parquet`**, généré automatiquement à la fin de la phase *Extract + Transform*.
    """)

st.divider()

# ======================
# LECTURE DU FICHIER PARQUET
# ======================
preview_path = Path("outputs/sample_preview.parquet")

if not preview_path.exists():
    st.error("❌ Aucun fichier `sample_preview.parquet` trouvé.")
    st.info("Lancez d'abord le pipeline depuis la page **Run Pipeline**.")
    st.stop()

try:
    df = pd.read_parquet(preview_path)
    st.success("✅ Données chargées avec succès.")
except Exception as e:
    st.error(f"Erreur lors du chargement du fichier : {e}")
    st.stop()

# ======================
# INFOS GÉNÉRALES
# ======================
st.markdown("### 📋 Informations générales")
col1, col2, col3 = st.columns(3)
col1.metric("Nombre de lignes", f"{len(df):,}")
col2.metric("Nombre de colonnes", df.shape[1])
col3.metric("Taille estimée", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} Mo")

# ======================
# STRUCTURE DU DATAFRAME
# ======================
with st.expander("🧩 Structure des colonnes", expanded=False):
    st.dataframe(
        pd.DataFrame({
            "Nom de colonne": df.columns,
            "Type": df.dtypes.astype(str).values
        }),
        use_container_width=True,
        hide_index=True
    )

# ======================
# VUE DES DONNÉES
# ======================
st.markdown("### 🧾 Aperçu des données")
st.dataframe(df.head(100), use_container_width=True)

st.caption("💡 Astuce : seul un échantillon des données est affiché pour éviter de surcharger la mémoire.")
