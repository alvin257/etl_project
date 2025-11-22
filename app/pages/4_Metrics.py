# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================
# ======================





import streamlit as st
import subprocess
import time
import sys
from pathlib import Path
import shutil

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
st.title("🧐 TIENT PAS COMPTE DE CETTE PAGE")
st.markdown(
    """
    <div style='font-size:17px;'>
    Suivez ici **l’exécution complète du pipeline ETL** étape par étape 🧩  
    Cette version inclut désormais les trois phases : **Extract → Transform → Load**.
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
# RESET & RUN BUTTONS
# =========================
outputs_dir = Path("outputs")

col1, col2 = st.columns([1, 1])

# ----- Bouton Reset -----

if st.button("🧹 Réinitialiser les résultats"):
    if outputs_dir.exists():
        try:
            shutil.rmtree(outputs_dir)
            st.success("✅ Tous les fichiers générés ont été supprimés avec succès.")
        except Exception as e:
            st.error(f"❌ Erreur lors de la suppression : {e}")
    else:
        st.info("ℹ️ Aucun dossier `outputs/` trouvé à supprimer.")

# ----- Bouton Lancer -----
if st.button("▶️ Lancer le pipeline ETL"):
    st.info("🧩 Initialisation du pipeline...")
    st.markdown("---")

    # =====================
    # ÉTAPE 1 : EXTRACT
    # =====================
    st.subheader("📥 Étape 1 : Extract")
    progress_extract = st.progress(0)
    extract_log = st.empty()
    extract_status = st.empty()

    for i in range(101):
        progress_extract.progress(i)
        if i < 20:
            msg = "Lecture des fichiers sources..."
        elif i < 60:
            msg = "Chargement parallèle via Dask..."
        elif i < 90:
            msg = "Normalisation du schéma..."
        else:
            msg = "Finalisation de l’étape Extract ✅"

        extract_status.markdown(f"**Progression : {i}%** — {msg}")
        extract_log.markdown(
            f"<pre style='background-color:#f7f7f7; padding:8px; border-radius:6px;'>[{time.strftime('%H:%M:%S')}] {msg}</pre>",
            unsafe_allow_html=True,
        )
        time.sleep(0.03)

    st.success("✅ Étape Extract terminée avec succès !")
    st.markdown("---")

    # Fait défiler la page vers le bas
    st.markdown(
        """
        <script>
        window.scrollTo(0, document.body.scrollHeight);
        </script>
        """,
        unsafe_allow_html=True,
    )

            # =====================
    # ÉTAPE 2 : TRANSFORM
    # =====================
    st.subheader("🔄 Étape 2 : Transform")
    progress_transform = st.progress(0)
    transform_log = st.empty()
    transform_status = st.empty()

    for i in range(101):
        progress_transform.progress(i)
        if i < 15:
            msg = "Nettoyage des données..."
        elif i < 35:
            msg = "Filtrage des valeurs aberrantes..."
        elif i < 55:
            msg = "Imputation des valeurs manquantes..."
        elif i < 75:
            msg = "Création des features temporelles..."
        elif i < 90:
            msg = "Jointure des données d’enrichissement..."
        else:
            msg = "Finalisation de l’étape Transform ✅"

        transform_status.markdown(f"**Progression : {i}%** — {msg}")
        transform_log.markdown(
            f"<pre style='background-color:#f1f8e9; padding:8px; border-radius:6px;'>[{time.strftime('%H:%M:%S')}] {msg}</pre>",
            unsafe_allow_html=True,
        )
        time.sleep(0.04)

    st.success("✅ Étape Transform terminée avec succès !")
    st.markdown("---")


    # Fait défiler la page vers le bas
    st.markdown(
        """
        <script>
        window.scrollTo(0, document.body.scrollHeight);
        </script>
        """,
        unsafe_allow_html=True,
    )

    # =====================
    # ÉTAPE 3 : LOAD
    # =====================
    st.subheader("📦 Étape 3 : Load")
    progress_load = st.progress(0)
    load_log = st.empty()
    load_status = st.empty()

    for i in range(101):
        progress_load.progress(i)
        if i < 25:
            msg = "Écriture des fichiers Parquet..."
        elif i < 50:
            msg = "Agrégation des données..."
        elif i < 75:
            msg = "Génération des métriques et du manifest..."
        elif i < 95:
            msg = "Nettoyage final et optimisation..."
        else:
            msg = "Finalisation de l’étape Load ✅"

        load_status.markdown(f"**Progression : {i}%** — {msg}")
        load_log.markdown(
            f"<pre style='background-color:#e3f2fd; padding:8px; border-radius:6px;'>[{time.strftime('%H:%M:%S')}] {msg}</pre>",
            unsafe_allow_html=True,
        )
        time.sleep(0.03)

    st.success("✅ Étape Load terminée avec succès !")

    # =====================
    # RÉSUMÉ FINAL
    # =====================
    st.markdown("---")
    st.subheader("🎉 Pipeline terminé avec succès !")
    st.balloons()

    total_duration = 12.7  # simulation
    st.metric("Durée totale", f"{total_duration:.1f} secondes")

    st.markdown(
        """
        **Récapitulatif :**
        - 📥 Extract : Lecture et normalisation des fichiers sources  
        - 🔄 Transform : Nettoyage, enrichissement, dérivation des features  
        - 📦 Load : Écriture, agrégation et génération du manifest  
        """
    )

    # Vérifie si le manifest existe
    manifest_path = outputs_dir / "manifest.json"
    if manifest_path.exists() and manifest_path.stat().st_size > 0:
        st.success("📄 Le fichier `manifest.json` a été généré avec succès.")
    else:
        st.warning("⚠️ Aucun manifest trouvé — relancez le pipeline si besoin.")

    # =====================
    # 🔍 DÉTAILS TECHNIQUES (avec onglets)
    # =====================
    st.markdown("---")
    st.subheader("🧠 Détails techniques du pipeline")

    tab1, tab2, tab3 = st.tabs(["📥 Extract", "🔄 Transform", "📦 Load"])

    # ----- Onglet Extract -----
    with tab1:
        st.markdown("### 📥 Étape 1 : Extract — Détails techniques")

        # KPI principaux
        col1, col2, col3 = st.columns(3)
        col1.metric("⏱️ Durée totale", "3.21 s")
        col2.metric("📁 Fichiers lus", "1")
        col3.metric("🧮 Partitions Dask", "4")

        st.markdown("---")

        # Sous-étapes + chronologie
        st.markdown("#### ⏳ Chronologie des sous-étapes")
        st.dataframe({
            "Sous-étape": [
                "Lecture des fichiers sources",
                "Chargement parallèle (Dask)",
                "Normalisation du schéma",
                "Écriture preview parquet"
            ],
            "Durée (s)": [0.45, 1.25, 0.98, 0.53],
            "Statut": ["✅", "✅", "✅", "✅"]
        })

        st.markdown("---")

        # Graphique temps par sous-étape (barres horizontales)
        import pandas as pd
        import plotly.express as px

        df_extract = pd.DataFrame({
            "Sous-étape": [
                "Lecture fichiers",
                "Chargement Dask",
                "Normalisation",
                "Écriture preview"
            ],
            "Durée (s)": [0.45, 1.25, 0.98, 0.53]
        })

        fig = px.bar(
            df_extract,
            x="Durée (s)",
            y="Sous-étape",
            orientation="h",
            text="Durée (s)",
            title="⏱️ Temps par sous-étape – Extract",
            color="Durée (s)",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Ressources parallélisation / CPU / partitions
        st.markdown("#### ⚙️ Ressources utilisées")
        st.markdown("""
        - **Mode d’exécution :** Dask local cluster  
        - **Threads actifs :** 4  
        - **Mémoire utilisée :** ~250 Mo  
        - **Temps d’I/O (lecture) :** 0.45s  
        - **Débit disque :** 120 MB/s  
        """)

        st.markdown("---")

        # Logs techniques
        with st.expander("🪵 Voir les logs de l’étape Extract"):
            st.code(
                """[16:23:12] Lecture des fichiers Parquet...
    [16:23:13] Dask a créé 4 partitions (8 threads)
    [16:23:14] Normalisation des colonnes : pickup_datetime, trip_distance, fare_amount...
    [16:23:15] Écriture du fichier sample_preview.parquet (28.7 MB)
    [16:23:15] Étape Extract terminée ✅""",
                language="bash"
            )

            # ----- Onglet Transform -----
    with tab2:
        st.markdown("### 🔄 Étape 2 : Transform — Détails techniques")

        # KPI principaux
        col1, col2, col3 = st.columns(3)
        col1.metric("⏱️ Durée totale", "4.87 s")
        col2.metric("🧹 Données nettoyées", "250 000 lignes")
        col3.metric("📊 Colonnes finales", "15")

        st.markdown("---")

        # Sous-étapes + chronologie
        st.markdown("#### ⏳ Chronologie des sous-étapes")
        st.dataframe({
            "Sous-étape": [
                "Nettoyage des valeurs nulles",
                "Filtrage des valeurs aberrantes",
                "Imputation légère",
                "Création des features temporelles",
                "Jointure avec zones taxi",
                "Ajout des indicateurs calendrier"
            ],
            "Durée (s)": [0.62, 1.10, 0.48, 1.05, 0.96, 0.66],
            "Statut": ["✅", "✅", "✅", "✅", "✅", "✅"]
        })

        st.markdown("---")

        # Graphique temps par sous-étape
        import pandas as pd
        import plotly.express as px

        df_transform = pd.DataFrame({
            "Sous-étape": [
                "Nettoyage valeurs nulles",
                "Filtrage aberrantes",
                "Imputation",
                "Features temporelles",
                "Jointure zones taxi",
                "Ajout calendrier"
            ],
            "Durée (s)": [0.62, 1.10, 0.48, 1.05, 0.96, 0.66]
        })

        fig2 = px.bar(
            df_transform,
            x="Durée (s)",
            y="Sous-étape",
            orientation="h",
            text="Durée (s)",
            title="⏱️ Temps par sous-étape – Transform",
            color="Durée (s)",
            color_continuous_scale="Greens"
        )
        st.plotly_chart(fig2, use_container_width=True)

        st.markdown("---")

        # Ressources parallélisation / CPU / mémoire
        st.markdown("#### ⚙️ Ressources utilisées")
        st.markdown("""
        - **Threads Dask actifs :** 4  
        - **Mémoire max utilisée :** ~410 Mo  
        - **Temps d’exécution total :** 4.87s  
        - **Fonctions appliquées en parallèle :** clean(), enrich(), feature_engineering()  
        - **Pourcentage de lignes conservées :** 96.4 %  
        """)

        st.markdown("---")

        # Logs techniques
        with st.expander("🪵 Voir les logs de l’étape Transform"):
            st.code(
                """[16:23:17] Démarrage de la phase Transform...
[16:23:18] Nettoyage des colonnes : trip_distance, fare_amount...
[16:23:19] Imputation des valeurs manquantes terminée
[16:23:20] Création des features temporelles (year, month, hour, day_bucket)
[16:23:21] Jointure avec le fichier taxi_zones.csv (LocationID)
[16:23:22] Étape Transform terminée ✅""",
                language="bash"
            )

    # ----- Onglet Load -----
    with tab3:
        st.markdown("### 📦 Étape 3 : Load — Détails techniques")

        # KPI principaux
        col1, col2, col3 = st.columns(3)
        col1.metric("⏱️ Durée totale", "3.08 s")
        col2.metric("📂 Fichiers écrits", "6")
        col3.metric("📁 Taille totale", "145 MB")

        st.markdown("---")

        # Sous-étapes + chronologie
        st.markdown("#### ⏳ Chronologie des sous-étapes")
        st.dataframe({
            "Sous-étape": [
                "Écriture des fichiers Parquet",
                "Création des agrégats (groupby)",
                "Calcul des métriques",
                "Génération du manifest.json"
            ],
            "Durée (s)": [0.88, 1.05, 0.74, 0.41],
            "Statut": ["✅", "✅", "✅", "✅"]
        })

        st.markdown("---")

        # Graphique temps par sous-étape
        df_load = pd.DataFrame({
            "Sous-étape": [
                "Écriture Parquet",
                "Agrégats groupby",
                "Calcul métriques",
                "Manifest.json"
            ],
            "Durée (s)": [0.88, 1.05, 0.74, 0.41]
        })

        fig3 = px.bar(
            df_load,
            x="Durée (s)",
            y="Sous-étape",
            orientation="h",
            text="Durée (s)",
            title="⏱️ Temps par sous-étape – Load",
            color="Durée (s)",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig3, use_container_width=True)

        st.markdown("---")

        # Ressources & sorties
        st.markdown("#### ⚙️ Résumé de sortie")
        st.markdown("""
        - **Mode d’écriture :** Parquet partitionné  
        - **Répertoire base :** `outputs/`  
        - **Fichiers générés :**
            - `clean/` (post-transform)
            - `features/` (enrichi)
            - `predictions/`
            - `analytics/` (agrégats)
            - `manifest.json`  
        - **Compression :** Snappy  
        """)

        st.markdown("---")

        # Logs techniques
        with st.expander("🪵 Voir les logs de l’étape Load"):
            st.code(
                """[16:23:23] Écriture des fichiers Parquet (6 fichiers)
[16:23:24] Calcul des agrégats par hour, borough, bucket
[16:23:25] Sauvegarde des métriques dans outputs/metrics/
[16:23:26] Manifest généré : outputs/manifest.json
[16:23:26] Étape Load terminée ✅""",
                language="bash"
            )

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
                    st.code(f.read()[-5000:], language="bash")
        else:
            st.info("Aucun log trouvé pour l’instant.")
    else:
        st.info("Aucun dossier `outputs/logs` trouvé.")

# =========================
# FOOTER
# =========================
st.markdown("---")
st.caption("🧩 ETL Dashboard — Monitoring complet du pipeline (Extract + Transform + Load)")
