import streamlit as st
import subprocess
from pathlib import Path
import time
import sys

st.title("⚙️ Lancer le pipeline ETL")

# Vérifie si un ancien manifest existe
manifest_path = Path("outputs/manifest.json")

if st.button("🚀 Exécuter le pipeline complet"):
    st.toast("⚙️ Chargement des fichiers en cours...", icon="🕒")

    # Lance le script en subprocess (équivalent à python -m scripts.run_pipeline)
    with st.spinner("Pipeline en cours..."):
        start = time.time()
        try:
            result = subprocess.run(
                [sys.executable, "-m", "scripts.run_pipeline"],
                capture_output=True,
                text=True,
                check=True
            )
            duration = time.time() - start
            st.success(f"Pipeline terminé en {duration:.2f} secondes ✅")
            st.code(result.stdout[-3000:], language="bash")  # Affiche la fin des logs
        except subprocess.CalledProcessError as e:
            st.error("Erreur lors de l'exécution du pipeline ❌")
            st.code(e.stderr, language="bash")

    # Vérifie si le manifest a été bien généré
    if manifest_path.exists() and manifest_path.stat().st_size > 0:
        st.success("`manifest.json` généré avec succès.")
    else:
        st.warning("Le pipeline a terminé, mais aucun manifest n'a été généré.")
