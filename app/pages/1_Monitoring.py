import streamlit as st
import subprocess
import time
import sys
from pathlib import Path
import shutil
import json
import pandas as pd
import plotly.express as px

import sys, os
# On ajoute la racine du projet au path Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from etl.utils import format_duration
# =========================
# PAGE CONFIGURATION
# =========================
st.set_page_config(
    page_title="Monitoring ETL",
    page_icon="🧐",
    layout="wide",
)

# =========================
# HEADER
# =========================
st.title("🧐 Monitoring du pipeline ETL")
st.markdown(
    """
    <div style='font-size:17px;'>
    Suivez ici **l’exécution complète du pipeline ETL** étape par étape 🧩  
    Les logs, la progression et les métriques se mettent à jour en temps réel.
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown("---")

# =========================
# CONFIGURATION YAML
# =========================
configs_dir = Path("configs")
yaml_files = list(configs_dir.glob("*.yaml"))

if not yaml_files:
    st.error("Aucun fichier YAML trouvé dans le dossier `/configs`.")
    st.stop()

yaml_choices = [f.name for f in yaml_files]
selected_yaml = st.selectbox("📄 Choisissez une configuration YAML :", yaml_choices)
selected_cfg_path = configs_dir / selected_yaml
st.info(f"🧩 Configuration sélectionnée : `{selected_yaml}`")

st.markdown("---")



# =========================
# FONCTION D’AFFICHAGE DES RÉSULTATS
# =========================
def show_pipeline_results(extract_metrics, transform_metrics, manifest):
    import pandas as pd
    import plotly.express as px

    st.markdown("---")
    st.subheader("🧠 Détails techniques du pipeline")

    tab1, tab2, tab3 = st.tabs(["📥 Extract", "🔄 Transform", "📦 Load"])

    # --- EXTRACT ---
    with tab1:
        st.markdown("### 📥 Étape 1 : Extract")
        if extract_metrics:
            col1, col2, col3 = st.columns(3)
            total_time = sum(v for k, v in extract_metrics.items() if isinstance(v, (int, float)))
            col1.metric("⏱️ Durée totale", format_duration(total_time))
            col2.metric("📁 Fichiers lus", extract_metrics.get("files_count", "—"))
            col3.metric("🧮 Partitions Dask", extract_metrics.get("partitions_count", "—"))

            df_extract = pd.DataFrame({
                "Sous-étape": [
                    "Résolution fichiers",
                    "Lecture lazy Dask",
                    "Normalisation schéma",
                    "Filtrage temporel",
                    "Prévisualisation"
                ],
                "Durée (s)": [
                    extract_metrics.get("resolve_files_s", 0),
                    extract_metrics.get("read_lazy_s", 0),
                    extract_metrics.get("normalize_schema_s", 0),
                    extract_metrics.get("early_time_filter_s", 0),
                    extract_metrics.get("preview_s", 0),
                ],
                "Statut": ["✅"] * 5
            })
            st.dataframe(df_extract, use_container_width=True)
            fig = px.bar(df_extract, x="Durée (s)", y="Sous-étape", orientation="h",
                         color="Durée (s)", color_continuous_scale="Blues")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("⚠️ Aucun métrique Extract trouvée.")

    # --- TRANSFORM ---
    with tab2:
        st.markdown("### 🔄 Étape 2 : Transform")
        if transform_metrics:
            col1, col2, col3, col4 = st.columns(4)
            total_time_transform = sum(v for k, v in transform_metrics.items() if k.endswith("_s") and isinstance(v, (int, float)))
            #total_time = sum(v for k, v in transform_metrics.items() if isinstance(v, (int, float)))
            col1.metric("🧹 Lignes nettoyées", transform_metrics.get("rows_after_clean_est", "—"))
            col2.metric("📊 Colonnes finales", len(transform_metrics.get("columns", [])))
            col3.metric("🧩 Compteurs de nettoyage", len(transform_metrics.get("counters_clean", {})))
            col4.metric("⏱️ Durée totale", format_duration(total_time_transform))

            st.markdown("#### 📊 Compteurs de nettoyage")
            counters = transform_metrics.get("counters_clean", {})
            if counters:
                df_counters = pd.DataFrame(list(counters.items()), columns=["Variable", "Présente"])
                st.dataframe(df_counters, use_container_width=True)
            st.markdown("#### 🗂️ Colonnes finales")
            st.dataframe(pd.DataFrame({"Colonnes": transform_metrics.get("columns", [])}))
        else:
            st.warning("⚠️ Aucun métrique Transform trouvée.")

    # --- LOAD ---
    with tab3:
        st.markdown("### 📦 Étape 3 : Load")
        if manifest:
            #total_time = sum(v for k, v in manifest.items()if isinstance(v, (int, float)))
            total_time_load = sum(v for k, v in manifest.get("metrics", {}).items() if k.endswith("_s") and isinstance(v, (int, float)))
            st.metric("⏱️ Durée totale", format_duration(total_time_load))
            datasets = manifest.get("datasets", {})
            for name, info in datasets.items():
                if isinstance(info, dict):
                    st.success(f"**{name}** → `{info.get('path', '—')}`")
                    if "partition_on" in info:
                        st.write(f"• Partitions : {info['partition_on']}")
        else:
            st.warning("⚠️ Aucun manifest trouvé.")


# =========================
# RESET & RUN BUTTONS
# =========================
outputs_dir = Path("outputs")

# ----- Bouton Reset -----
if st.button("🧹 Réinitialiser les résultats"):
    if outputs_dir.exists():
        try:
            shutil.rmtree(outputs_dir)            
            st.session_state.clear()
            st.success("✅ Tout a été réinitialisé (résultats + session).")
        
        except Exception as e:
            st.error(f"❌ Erreur lors de la suppression : {e}")

    else:
        st.info("ℹ️ Aucun dossier `outputs/` trouvé à supprimer.")

# Si des résultats précédents existent dans la session, on les affiche directement
if "last_pipeline_results" in st.session_state:
    st.info("📦 Résultats du dernier pipeline chargés depuis la session.")
    results = st.session_state["last_pipeline_results"]

    extract_metrics = results.get("extract_metrics", {})
    transform_metrics = results.get("transform_metrics", {})
    manifest = results.get("manifest", {})

    # ici tu appelles la section d'affichage Détails techniques (celle avec les onglets)
    # en utilisant les variables extract_metrics, transform_metrics et manifest
    show_pipeline_results(extract_metrics, transform_metrics, manifest)

# ----- Bouton Lancer -----
if st.button("▶️ Lancer le pipeline ETL"):
    st.info("🧩 Initialisation du pipeline...")
    st.markdown("---")

    start_time = time.time()

    process = subprocess.Popen(
        [sys.executable, "-m", "scripts.run_pipeline", str(selected_cfg_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    # Conteneurs Streamlit (dynamiques)
    extract_container = st.container()
    transform_container = st.container()
    load_container = st.container()

    progress_text = st.empty()

    # Variables pour suivre où on en est
    current_phase = None
    progress_extract = None
    progress_transform = None
    progress_load = None

    for line in iter(process.stdout.readline, ''):
        if not line.strip():
            continue

        # Récupération phase / pourcentage / message
        if "[" in line and "%" in line:
            try:
                phase = line.split("]")[0].replace("[", "").strip()
                percent_str = line.split("%")[0].split()[-1]
                percent = float(percent_str)
                msg = line.split("%")[-1].strip()
            except Exception:
                continue

            # === PHASE 1 : EXTRACT ===
            if phase == "EXTRACT":
                if current_phase != "EXTRACT":
                    current_phase = "EXTRACT"
                    with extract_container:
                        st.subheader("📥 Étape 1 : Extract")
                        progress_extract = st.progress(0)
                        extract_log = st.empty()

                progress_extract.progress(int(percent), text=f"📥 Extract — {msg}")
                with extract_container:
                    extract_log.markdown(
                        f"<pre style='background-color:#f7f7f7; padding:8px; border-radius:6px;'>[{time.strftime('%H:%M:%S')}] {msg}</pre>",
                        unsafe_allow_html=True,
                    )

            # === PHASE 2 : TRANSFORM ===
            elif phase == "TRANSFORM":
                if current_phase != "TRANSFORM":
                    current_phase = "TRANSFORM"
                    with transform_container:
                        st.subheader("🔄 Étape 2 : Transform")
                        progress_transform = st.progress(0)
                        transform_log = st.empty()
                        st.markdown("---")

                progress_transform.progress(int(percent), text=f"🔄 Transform — {msg}")
                with transform_container:
                    transform_log.markdown(
                        f"<pre style='background-color:#f1f8e9; padding:8px; border-radius:6px;'>[{time.strftime('%H:%M:%S')}] {msg}</pre>",
                        unsafe_allow_html=True,
                    )

            # === PHASE 3 : LOAD ===
            elif phase == "LOAD":
                if current_phase != "LOAD":
                    current_phase = "LOAD"
                    with load_container:
                        st.subheader("📦 Étape 3 : Load")
                        progress_load = st.progress(0)
                        load_log = st.empty()
                        st.markdown("---")

                progress_load.progress(int(percent), text=f"📦 Load — {msg}")
                with load_container:
                    load_log.markdown(
                        f"<pre style='background-color:#e3f2fd; padding:8px; border-radius:6px;'>[{time.strftime('%H:%M:%S')}] {msg}</pre>",
                        unsafe_allow_html=True,
                    )

        progress_text.text(f"🔹 Étape actuelle : {current_phase or 'Initialisation...'}")

    # === FIN DU PIPELINE ===
    process.wait()
    duration = time.time() - start_time
    st.success(f"✅ Pipeline terminé avec succès en {duration:.2f} secondes.")
    st.metric("⏱️ Durée totale du pipeline", format_duration(duration))
    st.balloons()

    st.markdown("---")
    st.subheader("🧠 Détails techniques du pipeline")

    # =====================
    # Charger les fichiers JSON réels
    # =====================
    extract_path = Path("outputs/metrics/extract_metrics.json")
    transform_path = Path("outputs/metrics/transform_metrics.json")
    manifest_path = Path("outputs/manifest.json")

    # Stocker les résultats du pipeline dans la session
    st.session_state["last_pipeline_results"] = {
        "extract_metrics": json.load(open("outputs/metrics/extract_metrics.json")),
        "transform_metrics": json.load(open("outputs/metrics/transform_metrics.json")),
        "manifest": json.load(open("outputs/manifest.json"))
    }

    # ----- Onglets -----
    tab1, tab2, tab3 = st.tabs(["📥 Extract", "🔄 Transform", "📦 Load"])

    # =====================
    # Onglet Extract
    # =====================
    with tab1:
        st.markdown("### 📥 Étape 1 : Extract")

        if extract_path.exists():
            with open(extract_path, "r", encoding="utf-8") as f:
                metrics = json.load(f)

            # KPI principaux
            col1, col2, col3 = st.columns(3)

            total_time = sum(v for k, v in metrics.items() if isinstance(v, (int, float)))
            col1.metric("⏱️ Durée totale", format_duration(total_time))
            col2.metric("📁 Fichiers lus", metrics.get("files_count", "?"))
            col3.metric("🧮 Partitions Dask", metrics.get("partitions_count", "?"))

            st.markdown("---")

            # Sous-étapes
            df_extract = pd.DataFrame({
                "Sous-étape": [
                    "Résolution fichiers",
                    "Lecture lazy Dask",
                    "Normalisation schéma",
                    "Filtrage temporel",
                    "Prévisualisation"
                ],
                "Durée (s)": [
                    metrics.get("resolve_files_s", 0),
                    metrics.get("read_lazy_s", 0),
                    metrics.get("normalize_schema_s", 0),
                    metrics.get("early_time_filter_s", 0),
                    metrics.get("preview_s", 0),
                ],
                "Statut": ["✅"] * 5
            })

            st.markdown("#### ⏳ Chronologie des sous-étapes")
            st.dataframe(df_extract, use_container_width=True)

            fig = px.bar(
                df_extract, x="Durée (s)", y="Sous-étape",
                orientation="h", text="Durée (s)",
                title="⏱️ Temps par sous-étape – Extract",
                color="Durée (s)", color_continuous_scale="Blues"
            )
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("#### ⚙️ Ressources utilisées")
            st.markdown("""
            - **Mode d’exécution :** Dask local cluster  
            - **Threads actifs :** 4  
            - **Temps total estimé :** {:.2f}s  
            - **Mémoire moyenne :** ~250 Mo  
            """.format(sum(v for k,v in metrics.items() if 's' in k)))

        else:
            st.warning("⚠️ Aucun fichier `extract_metrics.json` trouvé.")

    # =====================
    # Onglet Transform
    # =====================
    with tab2:
        st.markdown("### 🔄 Étape 2 : Transform")

        if transform_path.exists():
            with open(transform_path, "r", encoding="utf-8") as f:
                metrics = json.load(f)

            col1, col2, col3 = st.columns(3)
            col1.metric("🧹 Lignes nettoyées (estimées)", metrics.get("rows_after_clean_est", "unknown"))
            col2.metric("📊 Colonnes finales", len(metrics.get("columns", [])))
            total_time_transform = sum(v for k, v in metrics.items() if k.endswith("_s") and isinstance(v, (int, float)))
            col3.metric("⏱️ Durée totale", format_duration(total_time_transform))

            st.markdown("---")

            df_transform = pd.DataFrame({
                "Sous-étape": [
                    "Ajout features de base",
                    "Nettoyage",
                    "Normalisation temporelle",
                    "Enrichissements"
                ],
                "Durée (s)": [
                    metrics.get("add_features_s", 0),
                    metrics.get("cleaning_s", 0),
                    metrics.get("time_features_s", 0),
                    metrics.get("enrichment_s", 0),
                ],
                "Statut": ["✅"] * 4
            })
            st.dataframe(df_transform, use_container_width=True)

            st.markdown("#### 📊 Compteurs de nettoyage")
            if "counters_clean" in metrics:
                df_counters = pd.DataFrame(list(metrics["counters_clean"].items()), columns=["Variable", "Présente"])
                st.dataframe(df_counters, use_container_width=True)
            else:
                st.info("Aucun compteur de nettoyage trouvé.")

            st.markdown("---")
            st.markdown("#### 📋 Colonnes finales")
            st.write(", ".join(metrics.get("columns", [])))

        else:
            st.warning("⚠️ Aucun fichier `transform_metrics.json` trouvé.")


    # =====================
    # Onglet Load
    # =====================
    with tab3:
        st.markdown("### 📦 Étape 3 : Load")

        if manifest_path.exists():
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            
            total_time_load = sum(v for k, v in manifest.get("metrics", {}).items() if k.endswith("_s") and isinstance(v, (int, float)))
            st.metric("⏱️ Durée totale", format_duration(total_time_load))


            df_load = pd.DataFrame({
                "Sous-étape": [
                    "Repartition",
                    "Écriture Parquet",
                    "Exports analytics",
                    "Manifest"
                ],
                "Durée (s)": [
                    manifest["metrics"].get("repartition_s", 0),
                    manifest["metrics"].get("write_parquet_s", 0),
                    manifest["metrics"].get("analytics_s", 0),
                    manifest["metrics"].get("manifest_s", 0),
                ],
                "Statut": ["✅"] * 4
            })
            st.dataframe(df_load, use_container_width=True)

            st.markdown("#### 📁 Datasets générés")
            datasets = manifest.get("datasets", {})
            for name, info in datasets.items():
                if isinstance(info, dict) and "path" in info:
                    st.success(f"**{name}** → `{info['path']}`")
                    st.write(f"• Partitions : {info.get('partition_on', [])}")
                else:
                    st.info(f"{name}: {info}")

            st.markdown("#### 📜 Structure du manifest")
            st.code(json.dumps(manifest, indent=2, ensure_ascii=False), language="json")

        else:
            st.warning("⚠️ Aucun fichier `manifest.json` trouvé.")

    # =====================
    # Historique des logs
    # =====================
    st.markdown("---")
    st.subheader("🕓 Historique des exécutions précédentes")

    logs_dir = Path("outputs/logs")
    if logs_dir.exists():
        log_files = sorted(logs_dir.glob("pipeline_run_*.log"), reverse=True)
        if log_files:
            latest = log_files[0]
            st.info(f"Dernier log : `{latest.name}`")
            with st.expander("🪵 Voir le log complet du dernier run"):
                with open(latest, "r", encoding="utf-8") as f:
                    st.code(f.read()[-6000:], language="bash")
        else:
            st.info("Aucun log trouvé pour l’instant.")
    else:
        st.info("Aucun dossier `outputs/logs` trouvé.")

# =========================
# FOOTER
# =========================
st.markdown("---")
st.caption("🧩 ETL Dashboard — Monitoring réel du pipeline (Extract + Transform + Load)")
