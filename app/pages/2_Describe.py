import streamlit as st
import pandas as pd
from pathlib import Path

# ======================
# CONFIG & TITRE
# ======================
st.set_page_config(page_title="Statistiques descriptives", page_icon="📊", layout="wide")
st.title("📊 Statistiques descriptives")

with st.sidebar:
    st.markdown("### 📊 Navigation")
    st.info("💡 Vous êtes dans la section *Describe* : statistiques globales des données.")

with st.expander("🧠 À quoi sert cette étape ?"):
    st.markdown("""
    Cette page présente les **statistiques globales** des données transformées.
    
    - Vérifier la **cohérence** des valeurs (moyennes, minimums, maximums)  
    - Identifier des **valeurs aberrantes** potentielles  
    - Comprendre la **répartition** générale des données avant modélisation  
    
    Le fichier utilisé est `outputs/analytics/describe.parquet`, généré automatiquement à la fin de la phase *Load*.
    """)

st.divider()

# ======================
# LECTURE DU FICHIER DESCRIPTIF
# ======================
describe_path = Path("outputs/analytics/describe.parquet")

if not describe_path.exists():
    st.warning("⚠️ Aucun fichier `describe.parquet` trouvé.")
    st.info("Le pipeline n’a peut-être pas encore généré les statistiques.")
    st.stop()

try:
    df_desc = pd.read_parquet(describe_path)
    st.success("✅ Statistiques descriptives chargées avec succès.")
except Exception as e:
    st.error(f"Erreur lors du chargement du fichier : {e}")
    st.stop()

# ======================
# AFFICHAGE GLOBAL
# ======================
st.subheader("📈 Vue globale des statistiques")
st.dataframe(df_desc.style.highlight_max(axis=0), use_container_width=True)

# ======================
# KPIs RAPIDES
# ======================
st.markdown("### 📍 Indicateurs clés")
numeric_cols = df_desc.columns.tolist()

col1, col2, col3 = st.columns(3)
if len(numeric_cols) >= 3:
    col1.metric(f"Moyenne ({numeric_cols[0]})", f"{df_desc.loc['mean', numeric_cols[0]]:.2f}")
    col2.metric(f"Écart-type ({numeric_cols[1]})", f"{df_desc.loc['std', numeric_cols[1]]:.2f}")
    col3.metric(f"Max ({numeric_cols[2]})", f"{df_desc.loc['max', numeric_cols[2]]:.2f}")

# ======================
# VISUALISATION RAPIDE (OPTIONNEL)
# ======================
with st.expander("📉 Distribution d'une variable"):
    col_select = st.selectbox("Choisissez une colonne à visualiser :", options=numeric_cols)
    if col_select:
        st.bar_chart(df_desc[col_select])

st.caption("💡 Ces statistiques sont issues d’un échantillon représentatif des données finales.")
