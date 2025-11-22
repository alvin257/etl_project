import streamlit as st
from pathlib import Path
import time
import os
import glob

st.set_page_config(page_title="📜 Logs du pipeline", layout="wide")
st.title("📜 Logs du pipeline ETL")
st.markdown("Affichage des logs en direct et consultation des exécutions précédentes.")
st.markdown("---")

logs_dir = Path("outputs/logs")
logs_dir.mkdir(parents=True, exist_ok=True)

# Vérifier les fichiers de log existants
log_files = sorted(logs_dir.glob("pipeline_run_*.log"), reverse=True)

# Sélecteur pour les logs existants
selected_log = st.selectbox("🗂️ Choisissez un log à consulter :", [f.name for f in log_files])
selected_log_path = logs_dir / selected_log if selected_log else None

# Option de streaming en direct (uniquement si le fichier est encore ouvert)
live_mode = st.toggle("🔄 Affichage en direct (rafraîchit automatiquement)", value=False)

# Placeholder pour le contenu du log
log_display = st.empty()

# Offset de lecture (pour suivre en direct)
if "log_offset" not in st.session_state:
    st.session_state["log_offset"] = 0

# Lecture des logs
if selected_log_path and selected_log_path.exists():
    if live_mode:
        st.info("🟢 Mode live activé — les logs se mettront à jour automatiquement.")
        # Boucle de suivi live
        with open(selected_log_path, "r") as f:
            f.seek(st.session_state["log_offset"])
            lines = f.read()
            st.session_state["log_offset"] = f.tell()

        old_log = st.session_state.get("live_log_text", "")
        new_log = old_log + lines
        st.session_state["live_log_text"] = new_log

        log_display.text_area("📡 Logs en direct", new_log, height=400)
        time.sleep(1.0)
        st.experimental_rerun()

    else:
        with open(selected_log_path, "r", encoding="utf-8") as f:
            content = f.read()
        log_display.text_area("📄 Contenu du log", content, height=400)
        st.download_button("⬇️ Télécharger ce log", content, file_name=selected_log)
else:
    st.warning("Aucun fichier de log sélectionné ou disponible.")
