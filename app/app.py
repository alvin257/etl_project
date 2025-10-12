import streamlit as st
from pathlib import Path
import json

st.set_page_config(page_title="NYC Taxi ETL Dashboard", layout="wide")

st.title("🚖 NYC Taxi ETL Dashboard")
st.markdown("Ce tableau de bord vous permet d'explorer les sorties du pipeline ETL.")

# Lecture du manifest
manifest_path = Path("outputs/manifest.json")
if not manifest_path.exists():
    st.error("Manifest non trouvé. Lancez d'abord le pipeline.")
    st.stop()

with open(manifest_path) as f:
    manifest = json.load(f)

st.success("Manifest chargé")
st.json(manifest)
