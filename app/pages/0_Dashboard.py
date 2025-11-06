import streamlit as st
import pandas as pd
import json
from pathlib import Path
import plotly.express as px
import time

# ======================
# CONFIGURATION
# ======================
st.set_page_config(page_title="Résumé du pipeline", page_icon="🏁", layout="wide")
st.title("🏁 Tableau de bord global du pipeline ETL")

with st.sidebar:
    st.image("app/images/etl_schema.png", caption="Pipeline ETL", use_container_width=True)
    st.markdown("### 🧭 Navigation")
    st.page_link("app.py", label="🏠 Accueil")
    st.page_link("pages/0_Dashboard.py", label="🏁 Dashboard global")
    st.page_link("pages/run_pipeline.py", label="🚀 Lancer le pipeline")

with st.expander("🧠 À propos de cette page"):
    st.markdown("""
    Cette page offre une **vue d’ensemble** du pipeline ETL complet :
    - État des fichiers générés  
    - Résumé des métriques  
    - Temps d’exécution global  
    - Liens vers les différentes étapes de l’analyse  
    """)

st.divider()

# ======================
# RAFRAÎCHISSEMENT
# ======================
if st.button("🔄 Rafraîchir les données"):
    with st.spinner("Mise à jour des fichiers et des métriques..."):
        time.sleep(1)
    st.success("✅ Données rechargées avec succès.")

# ======================
# MANIFEST
# ======================
manifest_path = Path("outputs/manifest.json")
metrics_dir = Path("outputs/metrics")

if manifest_path.exists() and manifest_path.stat().st_size > 0:
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    st.success("📄 Manifest détecté")
    st.json(manifest)
else:
    st.warning("⚠️ Aucun manifest trouvé. Lancez le pipeline depuis **Run Pipeline**.")
    st.stop()

st.divider()

# ======================
# MÉTRIQUES RAPIDES
# ======================
st.subheader("📊 Indicateurs clés du pipeline")

extract_path = metrics_dir / "extract_metrics.json"
transform_path = metrics_dir / "transform_metrics.json"

kpi_cols = st.columns(3)

def load_json(path):
    if path.exists() and path.stat().st_size > 0:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

extract = load_json(extract_path)
transform = load_json(transform_path)

files_count = extract.get("files_count", "—")
partitions = extract.get("partitions_count", "—")
rows_est = transform.get("rows_after_clean_est", "—")

kpi_cols[0].metric("📁 Fichiers traités", files_count)
kpi_cols[1].metric("🧩 Partitions Dask", partitions)
kpi_cols[2].metric("🧮 Lignes (approx.)", rows_est)

st.divider()

# ======================
# DURÉES ET VISU
# ======================
st.subheader("⏱️ Temps d’exécution global")

timing_records = []
for path, name in [(extract_path, "Extract"), (transform_path, "Transform")]:
    data = load_json(path)
    timing = data.get("timing", {}) or data
    for k, v in timing.items():
        if isinstance(v, (int, float)):
            timing_records.append({"Étape": name, "Sous-étape": k, "Durée (s)": v})

if timing_records:
    df_timing = pd.DataFrame(timing_records)
    fig = px.bar(
        df_timing,
        x="Durée (s)",
        y="Sous-étape",
        color="Étape",
        orientation="h",
        title="Durée des sous-étapes ETL",
        text_auto=".2f"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("⚠️ Aucune donnée de durée disponible.")

st.divider()

# ======================
# LIENS RAPIDES
# ======================
st.subheader("🔗 Accès rapide aux sections")
col1, col2, col3, col4 = st.columns(4)
col1.page_link("pages/1_Preview.py", label="👀 Preview")
col2.page_link("pages/2_Describe.py", label="📊 Describe")
col3.page_link("pages/3_Aggregates.py", label="📈 Aggregates")
col4.page_link("pages/4_Metrics.py", label="📋 Metrics")

st.markdown("---")
st.caption("💡 Tableau de bord central pour superviser et comprendre le pipeline ETL.")
