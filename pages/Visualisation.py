import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.title("📊 Visualisation Automatique du Dataset")

# Vérifier si dataset final est dispo
if "results" not in st.session_state or st.session_state.results is None:
    st.warning("⚠️ Aucun résultat disponible.")
    st.stop()

output_path = st.session_state.results["statistics"]["output_path"]

# Charger dataset final
df = None
try:
    if output_path.endswith(".parquet"):
        df = pd.read_parquet(output_path)
    else:
        df = pd.read_csv(output_path)
except:
    st.error("Impossible de charger le dataset final.")
    st.stop()

st.success(f"Dataset chargé : {len(df):,} lignes")

# --- Téléchargement ---
st.download_button(
    "📥 Télécharger dataset final (CSV)",
    df.to_csv(index=False).encode(),
    file_name="pipeline_output.csv"
)

st.markdown("## 🎨 Visualisation")

# --- Choix Graphique ---
plot_type = st.selectbox("Type de graphique", ["Bar", "Scatter", "Line", "Histogram"])

num_cols = df.select_dtypes(include=["int", "float"]).columns.tolist()
cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
date_cols = df.select_dtypes(include=["datetime64"]).columns.tolist()

col_x = st.selectbox("Axe X", num_cols + cat_cols + date_cols)
col_y = st.selectbox("Axe Y", num_cols)

# --- Génération automatique ---
if plot_type == "Bar":
    fig = px.bar(df, x=col_x, y=col_y)

elif plot_type == "Scatter":
    fig = px.scatter(df, x=col_x, y=col_y, trendline="lowess")

elif plot_type == "Line":
    fig = px.line(df.sort_values(col_x), x=col_x, y=col_y)

elif plot_type == "Histogram":
    fig = px.histogram(df, x=col_x)

st.plotly_chart(fig, use_container_width=True)
