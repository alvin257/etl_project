import streamlit as st
import json
import pandas as pd
import plotly.express as px
from pathlib import Path

# ======================
# CONFIGURATION
# ======================
st.set_page_config(page_title="Monitoring du pipeline", page_icon="📊", layout="wide")
st.title("📊 Monitoring et performances du pipeline ETL")

with st.sidebar:
    st.markdown("### 🧭 Navigation")
    st.info("💡 Vous êtes dans la section *Monitoring* : suivez les performances du pipeline ETL.")

with st.expander("🧠 À quoi sert cette étape ?"):
    st.markdown("""
    Cette page affiche les **métriques clés** du pipeline ETL, issues des fichiers de logs générés
    pendant l’exécution. Elle permet de :
    
    - suivre le **temps d’exécution** des différentes étapes,  
    - analyser la **répartition du travail** (nombre de partitions, fichiers, etc.),  
    - et détecter les éventuels **goulots d’étranglement** (bottlenecks) du pipeline.  
    """)

st.divider()

# ======================
# LECTURE DES FICHIERS DE MÉTRIQUES
# ======================
metrics_dir = Path("outputs/metrics")

extract_path = metrics_dir / "extract_metrics.json"
transform_path = metrics_dir / "transform_metrics.json"

if not metrics_dir.exists():
    st.error("❌ Aucun dossier `outputs/metrics` trouvé. Lancez d'abord le pipeline.")
    st.stop()

data = {}

for path in [extract_path, transform_path]:
    if path.exists() and path.stat().st_size > 0:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data[path.stem] = json.load(f)
        except json.JSONDecodeError:
            st.warning(f"⚠️ Fichier corrompu : {path.name}")
    else:
        st.warning(f"⚠️ Fichier manquant ou vide : {path.name}")

if not data:
    st.info("Aucune métrique disponible pour le moment.")
    st.stop()

# ======================
# VISUALISATION DES TEMPS D’EXÉCUTION
# ======================
st.subheader("⏱️ Temps d'exécution par étape")

timing_records = []
for step_name, metrics in data.items():
    timing = metrics.get("timing", {}) or metrics
    for k, v in timing.items():
        if isinstance(v, (int, float)):
            timing_records.append({"étape": step_name, "sous-étape": k, "durée (s)": v})

if timing_records:
    df_timing = pd.DataFrame(timing_records)
    fig = px.bar(
        df_timing,
        x="durée (s)",
        y="sous-étape",
        color="étape",
        orientation="h",
        title="Durée par étape du pipeline",
        text_auto=".2f",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Aucune donnée de durée trouvée dans les métriques.")

# ======================
# KPIs GLOBAUX
# ======================
st.subheader("📦 Indicateurs clés")

kpi_cols = st.columns(3)
extract = data.get("extract_metrics", {})
transform = data.get("transform_metrics", {})

files_count = extract.get("files_count", "—")
partitions = extract.get("partitions_count", "—")
rows_est = transform.get("rows_after_clean_est", "—")

kpi_cols[0].metric("📁 Fichiers traités", files_count)
kpi_cols[1].metric("🧩 Partitions Dask", partitions)
kpi_cols[2].metric("🧮 Lignes (approx.)", rows_est)

# ======================
# DÉTAILS DES MÉTRIQUES
# ======================
st.markdown("### 📋 Détails techniques")

for step, metrics in data.items():
    with st.expander(f"📄 {step.replace('_metrics', '').capitalize()}"):
        st.json(metrics)

st.caption("💡 Ces métriques sont extraites automatiquement des journaux du pipeline ETL (JSON).")
