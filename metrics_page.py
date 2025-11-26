# metrics_page.py
import streamlit as st
import pandas as pd
import plotly.express as px

def show_metrics_page(results: dict):


    if results.get("status") == "FAILED":
        st.error("❌ Le pipeline a échoué")
        st.code(results.get("error"))
        return


    # ===== MÉTRIQUES DASK =====
    st.subheader("🖥️ Métriques du Cluster Dask")

    dask_m = results.get("dask_metrics", {})

    # ---- CAS : aucun worker (cluster fermé, pipeline fini, etc.) ----
    if not dask_m or dask_m.get("n_workers", 0) == 0:
        st.warning("⚠️ Aucun worker détecté (cluster fermé après exécution ou cluster non initialisé)")

        # Afficher les 3 métriques principales à 0
        col1, col2, col3 = st.columns(3)
        col1.metric("Workers", 0)
        col2.metric("Total cores", 0)
        col3.metric("Total RAM", "0 GB")

        st.info("ℹ️ Les workers Dask disparaissent **après la fermeture du cluster**, ce qui est normal.")
        return

    # ---- CAS : workers présents ----
    workers = dask_m.get("workers_details", [])

    # résumé global
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Workers", dask_m.get("n_workers", 0))
    col2.metric("Total cores", dask_m.get("total_cores", 0))
    col3.metric("Total RAM", f"{dask_m.get('total_memory_gb', 0):.1f} GB")

    if workers:
        avg_mem = sum(
            w["memory_used_gb"] / w["memory_limit_gb"] * 100 for w in workers
        ) / len(workers)
        col4.metric("Mémoire moyenne", f"{avg_mem:.1f}%")
    else:
        col4.metric("Mémoire moyenne", "0%")

    # ===== TABLEAU WORKERS =====
    with st.expander("📋 Détails des workers"):
        if not workers:
            st.info("Aucune donnée worker à afficher.")
        else:
            df_workers = pd.DataFrame(
                [{
                    "Worker_ID": w["id"][-8:],  # transforme en identifiant court
                    "CPU (%)": w["cpu_percent"],
                    "RAM utilisée (GB)": round(w["memory_used_gb"], 2),
                    "RAM limite (GB)": round(w["memory_limit_gb"], 2),
                    "RAM (%)": round((w["memory_used_gb"] / w["memory_limit_gb"]) * 100, 1),
                } for w in workers]
            )

            st.dataframe(df_workers, use_container_width=True)

            # Graphique RAM uniquement si workers non vides
            fig = px.bar(
                df_workers,
                x="Worker_ID",
                y="RAM (%)",
                title="Utilisation mémoire (%) par worker",
                color="RAM (%)",
                color_continuous_scale=["#f4beae", "#ee565b"]
            )
            st.plotly_chart(fig, use_container_width=True)
