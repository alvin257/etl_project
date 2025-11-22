import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px
import json as json


st.set_page_config(page_title="Visualisation ETL", page_icon="📊", layout="wide")
st.title("📊 Visualisation des Résultats du Pipeline")

manifest_path = Path("outputs/manifest.json")

if not manifest_path.exists():
    st.error("⚠️ Aucun fichier `manifest.json` trouvé. Lancez d'abord le pipeline.")
    st.stop()

with open(manifest_path, "r", encoding="utf-8") as f:
    manifest = json.load(f)

datasets = manifest.get("datasets", {})
if not datasets:
    st.warning("Aucun dataset détecté dans le manifest.")
    st.stop()

# Sélecteur de dataset
dataset_choice = st.selectbox("📁 Choisir un dataset à visualiser :", list(datasets.keys()))

selected_data = datasets[dataset_choice]
if isinstance(selected_data, dict):
    sub_choice = st.selectbox("📄 Sélectionner un fichier :", list(selected_data.values()))
else:
    sub_choice = selected_data

st.info(f"Chargement de : `{sub_choice}`")

# Chargement automatique du fichier (Parquet/CSV)
data_path = Path(sub_choice)
if data_path.suffix == ".parquet":
    df = pd.read_parquet(data_path)
elif data_path.suffix == ".csv":
    df = pd.read_csv(data_path)
else:
    st.error("Format de fichier non supporté.")
    st.stop()

# --- Aperçu du dataset ---
st.subheader("👁️ Aperçu des données")
st.dataframe(df.head(), use_container_width=True)

# --- Sélection de colonnes pour la visualisation ---
st.subheader("📈 Création de graphiques dynamiques")

num_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

x_axis = st.selectbox("Axe X :", options=df.columns)
y_axis = st.selectbox("Axe Y :", options=num_cols)
color_col = st.selectbox("Couleur :", options=[None] + cat_cols)

# --- Choix du type de graphique ---
chart_type = st.radio("Type de graphique :", ["Barres", "Lignes", "Nuage de points", "Histogramme"])

if chart_type == "Barres":
    fig = px.bar(df, x=x_axis, y=y_axis, color=color_col)
elif chart_type == "Lignes":
    fig = px.line(df, x=x_axis, y=y_axis, color=color_col)
elif chart_type == "Nuage de points":
    fig = px.scatter(df, x=x_axis, y=y_axis, color=color_col)
else:
    fig = px.histogram(df, x=y_axis, color=color_col)

st.plotly_chart(fig, use_container_width=True)
