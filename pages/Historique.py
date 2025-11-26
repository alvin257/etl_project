import streamlit as st
import json
from pathlib import Path

st.title("📜 Historique des Pipelines")

history = sorted(Path("results_cache").glob("results_*.json"))

if not history:
    st.info("Aucun pipeline encore exécuté.")
    st.stop()

for res_file in history:
    with res_file.open() as f:
        data = json.load(f)

    run_id = data["timestamp"]
    yaml_path = data["yaml_path"]

    with st.expander(f"📌 Pipeline {run_id}"):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.text("YAML config")
            st.download_button(
                "📥 Télécharger YAML",
                Path(yaml_path).read_text(),
                file_name=f"config_{run_id}.yaml"
            )

        with col2:
            st.text("Résultats")
            st.download_button(
                "📥 Télécharger résultats",
                json.dumps(data, indent=2),
                file_name=f"results_{run_id}.json"
            )

        with col3:
            if st.button(f"👁 Voir résultats", key=f"view_{run_id}"):
                st.session_state.results = data
                st.switch_page("streamlit_app.py")

        with col4:
            if st.button(f"▶️ Rejouer", key=f"rerun_{run_id}"):
                st.session_state.yaml_to_load = yaml_path
                st.switch_page("streamlit_app.py")
