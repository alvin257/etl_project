"""
Streamlit App - ETL Pipeline avec Configuration YAML
Interface simple avec éditeur YAML intégré
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import yaml
from pathlib import Path
import time
import re

from etl_pipeline_yaml import YAMLETLPipeline, load_yaml_config
from metrics_page import show_metrics_page

# Configuration de la page
st.set_page_config(
    page_title="ETL Pipeline Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# État
if 'results' not in st.session_state:
    st.session_state.results = None


def main():
    # En-tête
    st.title("⚡ ETL Pipeline Monitor")
    st.markdown("**Pipeline ETL Configurable via YAML**")
    
    # Sidebar
    with st.sidebar:
        st.header("📚 Ressources")
        
        if st.button("📖 Lire le Guide Complet", use_container_width=True):
            st.session_state.show_readme = True
        
        st.markdown("---")
        
        
    
    # Contenu principal
    if st.session_state.get('show_readme'):
        show_readme_view()
    elif st.session_state.results:
        show_results_view()
    else:
        show_editor_view()


def show_editor_view():
    """Vue avec éditeur YAML"""
    
    st.markdown("### 📝 Configuration du Pipeline")
    
    # Charger le template par défaut
    template_path = Path('TEMPLATE.yaml')
    
    if template_path.exists():
        with open(template_path, 'r') as f:
            default_yaml = f.read()
    else:
        default_yaml = """# Template de configuration YAML pour le pipeline ETL
source:
  type: csv
  path: "data/raw/ecommerce_data.csv"

transforms:
  clean_nulls:
    enabled: true
    columns: [id]

output:
  format: parquet
  path: "data/processed/result"

dask:
  workers: 2
  memory_per_worker: "1GB"
  threads_per_worker: 2
"""
    
    # Éditeur YAML
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.info("💡 **Modifiez la configuration ci-dessous selon vos besoins**")
    
    with col2:
        if st.button("🔄 Réinitialiser", help="Recharger le template par défaut"):
            st.rerun()
    
    yaml_config = st.text_area(
        "Configuration YAML",
        value=default_yaml,
        height=500,
        help="Activez/désactivez avec 'enabled', modifiez les valeurs, supprimez ce dont vous n'avez pas besoin",
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    # Boutons d'action
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        validate = st.button("✅ Valider YAML", use_container_width=True)
    
    with col2:
        launch = st.button("🚀 Lancer le Pipeline", type="primary", use_container_width=True)
    
    # Validation
    if validate or launch:
        config = validate_yaml(yaml_config)
        
        if config:
            st.success("✅ Configuration YAML valide !")
            
            # Afficher un aperçu
            with st.expander("📋 Aperçu du plan d'exécution"):
                show_pipeline_plan(config)
            
            # Lancer si demandé
            if launch:
                st.session_state.results = None
                with st.spinner("⚙️ Le pipeline tourne… Dask Dashboard disponible ci-dessous ⬇️"):
                    results = run_pipeline(config, yaml_config)
                    st.session_state.results = results

                st.rerun()


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    parts = []
    if days > 0:
        parts.append(f"{days}j")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def validate_yaml(yaml_text: str) -> dict:
    """Valide le YAML"""
    try:
        config = yaml.safe_load(yaml_text)
        
        # Vérifications de base
        if 'source' not in config:
            st.error("❌ Manque la section 'source'")
            return None
        
        if 'output' not in config:
            st.error("❌ Manque la section 'output'")
            return None
        
        # ---- Extraire correctement le chemin source ----
        source = config.get("source", {})
        input_path = source.get("path")

        if not input_path:
            st.error("❌ La source ne contient pas de champ 'path'")
            return None
        # Vérifier que le fichier source existe
        def is_http_url(path: str):
            return isinstance(path, str) and re.match(r"^https?://", path)

        # ---- Validation Source ----

        # CAS 1 : une liste de fichiers (OK)
        if isinstance(input_path, list):
            st.success(f"📁 {len(input_path)} fichiers détectés — OK !")
            return config
        
        if is_http_url(input_path):
            # URL valide → on skip le check local
            st.success("🌐 Source externe détectée (URL) — OK !")
        elif Path(input_path).exists():
            st.success("📁 Fichier local détecté — OK !")
        else:
            st.error(f"❌ Source introuvable : {input_path}")
            st.stop()


        return config
        
    except yaml.YAMLError as e:
        st.error(f"❌ YAML invalide : {e}")
        return None
    except Exception as e:
        st.error(f"❌ Erreur : {e}")
        return None


def show_pipeline_plan(config: dict):
    """Affiche un aperçu du plan d'exécution"""
    
    # Source
    st.markdown("**📁 Source**")
    input_path = config["source"]
    st.code(f"{input_path.get('type')}: {input_path.get('path')}")
    
    # Transformations actives
    st.markdown("**🔧 Transformations Activées**")
    
    transforms = config.get('transforms', {})
    active_count = sum(1 for t in transforms.values() if isinstance(t, dict) and t.get('enabled'))
    
    if active_count == 0:
        st.warning("Aucune transformation activée (conversion directe)")
    else:
        for i, (name, cfg) in enumerate(transforms.items(), 1):
            if isinstance(cfg, dict) and cfg.get('enabled'):
                st.markdown(f"{i}. **{name}**")
                
                if name == 'clean_nulls':
                    st.caption(f"   → Colonnes: {cfg.get('columns', [])}")
                
                elif name == 'calculate':
                    for col in cfg.get('new_columns', []):
                        st.caption(f"   → {col.get('name')} = {col.get('formula')}")
                
                elif name == 'filter':
                    for cond in cfg.get('conditions', []):
                        st.caption(f"   → {cond.get('column')} {cond.get('operator')} {cond.get('value')}")
                
                elif name == 'aggregate':
                    st.caption(f"   → Group by: {cfg.get('groupby', [])}")
                
                elif name == 'date_features':
                    st.caption(f"   → Extraire: {cfg.get('extract', [])}")
    
    # Sortie
    st.markdown("**💾 Sortie**")
    output = config.get('output', {})
    st.code(f"{output.get('format')}: {output.get('path')}")
    
    # Cluster
    st.markdown("**🖥️ Cluster Dask**")
    dask_cfg = config.get("dask")

    if not dask_cfg:
        st.info(
            "⚙️ Aucun paramètre Dask spécifié.\n"
            "➡️ Le cluster sera configuré automatiquement par Dask (workers, threads, mémoire)."
        )
    else:
        workers = dask_cfg.get("workers", "auto")
        mem = dask_cfg.get("memory_per_worker", "auto")
        threads = dask_cfg.get("threads", "auto")

        st.code(f"{workers} workers × {threads} threads × {mem} / worker")


def run_pipeline(config: dict, yaml_text: str):
    """Exécute le pipeline et affiche le dashboard immédiatement."""

    # Sauvegarde config
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    config_backup_path = Path(f'configs/config_{timestamp}.yaml')
    config_backup_path.parent.mkdir(exist_ok=True)
    with open(config_backup_path, 'w') as f:
        f.write(yaml_text)

    # 1️⃣ Créer pipeline AVANT le run
    pipeline = YAMLETLPipeline(config)

    # 2️⃣ Récupérer lien dashboard immédiatement
    dashboard = pipeline.client.dashboard_link

    # 3️⃣ Le mettre dans session_state pour Streamlit
    st.session_state["dashboard_link"] = dashboard

    # 4️⃣ L’afficher tout de suite (pendant que le cluster vit still alive 🔥)
    st.info(f"📊 **Dask Dashboard disponible dès maintenant**")
    st.link_button("🔗 Ouvrir Dashboard Dask", dashboard)

    # Petit délai pour que l’utilisateur clique
    time.sleep(2)

    # 5️⃣ Maintenant seulement → lancer le pipeline
    results = pipeline.run()

    # Ajout du run_id
    results["timestamp"] = timestamp
    results["yaml_path"] = str(config_backup_path)

    # --- Sauvegarde résultats ---
    import json
    results_path = Path(f"results_cache/results_{timestamp}.json")
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.write_text(json.dumps(results, indent=2))

    # 6️⃣ Ajouter config sauvegardée
    results['config_saved'] = str(config_backup_path)

    return results


def show_results_view():
    """Vue des résultats — version améliorée (identique à v2)"""

    results = st.session_state.results

    if results.get("status") == "FAILED":
        st.error("❌ Le pipeline a échoué")
        st.code(results.get("error", "Erreur inconnue"))
        return

    # Banner de succès
    st.success("✅ Pipeline Terminé avec Succès!")
    st.markdown("---")

    # ========= MÉTRIQUES PRINCIPALES =========
    stats = results.get("statistics", {})
    stage_times = results.get("stage_times", {})

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📊 Records Traités", f"{stats.get('total_records', 0):,}")
    col2.metric("⏱️ Temps Total", format_duration(results.get("total_time", 0)))
    col3.metric("⚡ Throughput", f"{stats.get('throughput', 0):,.0f} rec/s")
    col4.metric("💾 Taille Sortie", f"{stats.get('file_size_mb', 0):.2f} MB")

    st.markdown("---")

    # ===== TEMPS PAR ÉTAPE =====
    st.subheader("⏱️ Temps par étape")
    df_times = pd.DataFrame([
        {"Étape": "Extract", "Temps (s)": stage_times.get("extract", 0)},
        {"Étape": "Transform", "Temps (s)": stage_times.get("transform", 0)},
        {"Étape": "Load", "Temps (s)": stage_times.get("load", 0)}
    ])

    fig = px.bar(df_times, x="Étape", y="Temps (s)", color="Temps (s)",
                 color_continuous_scale=["#8095F2", "#353A55"], title="Durée de chaque étape")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ===== INFORMATIONS SORTIE =====
    st.subheader("📦 Informations sortie")
    st.text(f"Format : {stats.get('format')}")
    st.text(f"Partitions : {stats.get('partitions')}")
    st.text(f"Fichier : {stats.get('output_path')}")

    st.markdown("---")

    # ========= MÉTRIQUES DASK =========
    from metrics_page import show_metrics_page
    show_metrics_page(results)

    st.markdown("---")

    # ========= BOUTONS =========
    colA, colB, colC, colD = st.columns(4)

    with colA:
        if st.button("◀ Retour à l'éditeur", use_container_width=True):
            st.session_state.results = None
            st.rerun()




def show_readme_view():
    """Affiche le README"""

    if st.button("◀ Retour à l'éditeur"):
        st.session_state.show_readme = False
        st.rerun()
    readme_path = Path('docs/guide.md')
    
    if readme_path.exists():
        with open(readme_path, 'r') as f:
            readme_content = f.read()
        
        st.markdown(readme_content)
    else:
        st.warning("README.md non trouvé")
    


if __name__ == "__main__":
    main()