import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px

# ======================
# CONFIGURATION
# ======================
st.set_page_config(page_title="Agrégats et Visualisations", page_icon="📈", layout="wide")
st.title("📈 Visualisation des agrégats: BROUILLON")

with st.sidebar:
    st.markdown("### 📊 Navigation")
    st.info("💡 Vous êtes dans la section *Aggregates* : visualisations des données agrégées.")

with st.expander("🧠 À quoi sert cette étape ?"):
    st.markdown("""
    Cette page affiche des **visualisations interactives** construites à partir des agrégats produits par le pipeline ETL.
    
    Ces agrégats permettent :
    - de **résumer les données** (moyennes, totaux, volumes par heure ou zone),
    - de **détecter des tendances**,
    - et d’**interpréter visuellement** le résultat du traitement des données.
    
    Les fichiers affichés ici proviennent de `outputs/analytics/`.
    """)

st.divider()

# ======================
# LECTURE DES FICHIERS DISPONIBLES
# ======================
analytics_dir = Path("outputs/analytics")

if not analytics_dir.exists():
    st.error("❌ Aucun dossier `outputs/analytics` trouvé.")
    st.info("Lancez d'abord le pipeline depuis la page **Run Pipeline**.")
    st.stop()

parquet_files = list(analytics_dir.glob("*.parquet"))

if not parquet_files:
    st.warning("⚠️ Aucun fichier Parquet trouvé dans `outputs/analytics`.")
    st.stop()

file_names = [f.name for f in parquet_files]
selected_file = st.selectbox("📁 Sélectionnez un fichier d’agrégats à visualiser :", file_names)

# Lecture du fichier sélectionné
try:
    df = pd.read_parquet(analytics_dir / selected_file)
    st.success(f"✅ Fichier `{selected_file}` chargé avec succès.")
except Exception as e:
    st.error(f"Erreur lors du chargement du fichier : {e}")
    st.stop()

# ======================
# APERÇU RAPIDE
# ======================
with st.expander("🧾 Aperçu des données agrégées"):
    st.dataframe(df.head(20), use_container_width=True)

st.markdown("---")

# ======================
# VISUALISATION INTELLIGENTE
# ======================
st.subheader("📊 Visualisation des agrégats")

numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
non_numeric_cols = df.select_dtypes(exclude=["number"]).columns.tolist()

if len(numeric_cols) == 0:
    st.warning("⚠️ Aucun indicateur numérique à afficher.")
    st.stop()

x_axis = st.selectbox("🧭 Axe des X :", options=non_numeric_cols or numeric_cols)
y_axis = st.selectbox("📏 Axe des Y :", options=numeric_cols)
chart_type = st.radio("📊 Type de graphique :", ["Barres", "Lignes", "Camembert"], horizontal=True)

if chart_type == "Barres":
    fig = px.bar(df, x=x_axis, y=y_axis, color=x_axis, title=f"{y_axis} par {x_axis}")
elif chart_type == "Lignes":
    fig = px.line(df, x=x_axis, y=y_axis, markers=True, title=f"Évolution de {y_axis} par {x_axis}")
elif chart_type == "Camembert":
    fig = px.pie(df, names=x_axis, values=y_axis, title=f"Répartition de {y_axis} par {x_axis}")

st.plotly_chart(fig, use_container_width=True)

st.caption("💡 Vous pouvez interagir avec les graphiques : zoom, survol, téléchargement, etc.")
