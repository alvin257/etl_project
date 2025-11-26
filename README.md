etl_project

ETL Dask Pipeline + Front‑end (Streamlit)

✨ Description

Ce projet consiste à construire un pipeline ETL (Extract – Transform – Load) scalable avec Dask, et une interface Streamlit pour :

le contrôle de l'exécution,

le monitoring en temps réel,

la visualisation des résultats,

la gestion multi-configurations,

le streaming de fichiers logs en direct,

la relecture des exécutions précédentes,

et le support de données uploadées dynamiquement ou de fichiers en streaming.

L'objectif est de démontrer comment un ETL peut être :

parallélisé localement (grâce à Dask),

exécuté en arrière-plan, sans bloquer la navigation Streamlit,

contrôlé via une interface simple,

modulaire et reconfigurable via des fichiers YAML,

extensible pour le temps réel et les données utilisateurs.

📄 Fonctionnalités principales

🌐 Lecture de fichiers multiples (CSV, Parquet, etc.)

⚖️ Mapping schéma & nettoyage (valeurs aberrantes, NA, etc.)

⏰ Monitoring temps réel avec logs + barres de progression

📈 Agrégats, KPIs, analytics automatiques

🌟 Visualisation de chaque étape (Extract, Transform, Load)

♻️ Cache automatique pour rejouer ou inspecter un run précédent

🔜 Upload de fichiers utilisateurs avec config YAML personnalisée

🔊 Page logs en live (mode "streaming") avec historique

✅ Support multi-dataset via YAML (ex : nyc_taxi.yaml, tmdb.yaml)

Déliverables
ETL code + front-end, README, sample dataset and demo video.

# ⚡ ETL Pipeline Monitor - Guide Complet

Pipeline ETL distribué avec Dask et interface Streamlit configurable via YAML.

---

## 🎯 Vue d'Ensemble

Ce projet vous permet de :
- ✅ Traiter des **millions de lignes** en parallèle
- ✅ Configurer votre pipeline via **un simple fichier YAML**
- ✅ Visualiser l'exécution en **temps réel**
- ✅ Appliquer des transformations **sans coder**

---

## 📦 Installation Rapide

```bash
# 1. Cloner le projet
git clone <votre-repo>
cd etl-pipeline-dask

# 2. Installer les dépendances
pip install dask[complete] distributed streamlit pandas numpy pyyaml plotly

# 3. Générer les datasets de démo
python generate_datasets.py

# 4. Lancer l'application
streamlit run streamlit_app_yaml.py
```

---

## 🚀 Utilisation en 3 Étapes

### Étape 1 : Générer les Datasets

```bash
python generate_datasets.py
```

**Résultat :**
- `data/raw/ecommerce_1M.csv` (1 million de transactions)
- `data/raw/iot_2M.csv` (2 millions de mesures IoT)

---

### Étape 2 : Modifier le YAML

Ouvrez `TEMPLATE.yaml` ou utilisez l'éditeur dans l'interface Streamlit.

#### Exemple 1 : Pipeline Basique (Nettoyage + Calcul)

```yaml
source:
  type: csv
  path: "data/raw/ecommerce_1M.csv"

transforms:
  clean_nulls:
    enabled: true
    columns: [id, price, quantity]
  
  calculate:
    enabled: true
    new_columns:
      - name: total_amount
        formula: price * quantity

output:
  format: parquet
  path: "data/processed/result"

dask:
  workers: 4
  memory_per_worker: "1GB"
```

#### Exemple 2 : Pipeline Complet (Tout Activé)

```yaml
source:
  type: csv
  path: "data/raw/ecommerce_1M.csv"

transforms:
  clean_nulls:
    enabled: true
    columns: [id, date, price, quantity]
  
  calculate:
    enabled: true
    new_columns:
      - name: total_amount
        formula: price * quantity
      - name: price_per_unit
        formula: price / quantity
  
  filter:
    enabled: true
    conditions:
      - column: status
        operator: "=="
        value: completed
      - column: price
        operator: ">"
        value: 0
  
  aggregate:
    enabled: true
    groupby: [category]
    metrics:
      price: mean
      quantity: sum
      total_amount: sum
  
  date_features:
    enabled: true
    column: date
    extract:
      - year
      - month
      - day

output:
  format: parquet
  path: "data/processed/ecommerce_aggregated"

dask:
  workers: 8
  memory_per_worker: "2GB"
```

---

### Étape 3 : Lancer le Pipeline

#### Option A : Via l'Interface Streamlit

```bash
streamlit run streamlit_app_yaml.py
```

1. Modifiez le YAML dans l'éditeur
2. Cliquez sur **"🚀 Lancer le Pipeline"**
3. Consultez les résultats en temps réel

#### Option B : En Ligne de Commande

```bash
python etl_pipeline_yaml.py
```

---

## 📖 Documentation YAML

### 🔧 Section `source`

Définit la source de données.

```yaml
source:
  type: csv              # Options : csv | parquet
  path: "chemin/vers/fichier.csv"
```

---

### 🔧 Section `transforms`

#### 1. Nettoyage des Nulls

Supprime les lignes avec des valeurs manquantes.

```yaml
transforms:
  clean_nulls:
    enabled: true        # Activer/désactiver
    columns: [id, price] # Colonnes à vérifier
```

#### 2. Calculs

Crée de nouvelles colonnes avec des formules.

```yaml
transforms:
  calculate:
    enabled: true
    new_columns:
      - name: total           # Nom de la nouvelle colonne
        formula: price * quantity  # Formule (syntaxe pandas)
      - name: discount
        formula: price * 0.1
```

**Formules supportées :**
- Opérations : `+`, `-`, `*`, `/`, `**` (puissance)
- Comparaisons : `>`, `<`, `>=`, `<=`, `==`, `!=`
- Fonctions : `abs()`, `round()`, `sqrt()`, etc.

#### 3. Filtrage

Filtre les lignes selon des conditions.

```yaml
transforms:
  filter:
    enabled: true
    conditions:
      - column: status
        operator: "=="      # Options : == | != | > | < | >= | <=
        value: completed
      - column: price
        operator: ">"
        value: 100
```

#### 4. Agrégations

Groupe et agrège les données.

```yaml
transforms:
  aggregate:
    enabled: true
    groupby: [category, region]  # Colonnes de regroupement
    metrics:
      price: mean      # Options : sum | mean | count | min | max
      quantity: sum
      total: sum
```

#### 5. Features de Dates

Extrait des composantes de dates.

```yaml
transforms:
  date_features:
    enabled: true
    column: date             # Colonne contenant la date
    extract:
      - year                 # Extrait l'année
      - month                # Extrait le mois
      - day                  # Extrait le jour
      - day_of_week          # Extrait le jour de la semaine (0=lundi)
```

---

### 🔧 Section `output`

Définit le format de sortie.

```yaml
output:
  format: parquet          # Options : parquet | csv
  path: "data/processed/result"
```

**Formats :**
- `parquet` : Recommandé (plus rapide, plus compact)
- `csv` : Pour compatibilité avec d'autres outils

---

### 🔧 Section `dask`

Configure le cluster Dask.

```yaml
dask:
  workers: 4                   # Nombre de workers (1-8)
  memory_per_worker: "1GB"     # RAM par worker
  threads_per_worker: 2        # Threads by worker
```

**Recommandations :**
- **Petit dataset (< 1M lignes)** : 2-4 workers, 512MB-1GB
- **Dataset moyen (1-10M lignes)** : 4-8 workers, 1-2GB
- **Grand dataset (> 10M lignes)** : 8+ workers, 2-4GB

---

## 💡 Exemples de Use Cases

### Use Case 1 : E-commerce Analytics

**Objectif :** Calculer le revenu par catégorie

```yaml
source:
  type: csv
  path: "data/raw/ecommerce_1M.csv"

transforms:
  clean_nulls:
    enabled: true
    columns: [price, quantity]
  
  calculate:
    enabled: true
    new_columns:
      - name: revenue
        formula: price * quantity
  
  filter:
    enabled: true
    conditions:
      - column: status
        operator: "=="
        value: completed
  
  aggregate:
    enabled: true
    groupby: [category]
    metrics:
      revenue: sum
      quantity: sum

output:
  format: parquet
  path: "data/processed/revenue_by_category"
```

---

### Use Case 2 : IoT Monitoring

**Objectif :** Analyser les températures par capteur

```yaml
source:
  type: csv
  path: "data/raw/iot_2M.csv"

transforms:
  clean_nulls:
    enabled: true
    columns: [temperature, humidity]
  
  filter:
    enabled: true
    conditions:
      - column: status
        operator: "=="
        value: active
  
  aggregate:
    enabled: true
    groupby: [device_id, sensor_type]
    metrics:
      temperature: mean
      humidity: mean
      pressure: mean

output:
  format: parquet
  path: "data/processed/sensors_avg"
```

---

## 🔍 Dépannage

### Problème : "Fichier source introuvable"

**Solution :**
```bash
python generate_datasets.py
```

### Problème : "Out of Memory"

**Solution :** Réduisez le nombre de workers ou la mémoire par worker.

```yaml
dask:
  workers: 2
  memory_per_worker: "512MB"
```

### Problème : "YAML invalide"

**Solution :** Vérifiez l'indentation (utilisez des espaces, pas des tabulations).

---

## 📊 Performances

| Dataset | Lignes | Taille | Durée (4 workers) |
|---------|--------|--------|-------------------|
| E-commerce | 1M | ~70 MB | ~15-20s |
| IoT | 2M | ~140 MB | ~30-40s |
| Custom | 10M | ~700 MB | ~2-3 min |

*Tests sur : Intel i7, 16GB RAM*

---

## 🛠️ Architecture

```
┌─────────────────┐
│  Streamlit UI   │  ← Interface utilisateur
└────────┬────────┘
         │
         v
┌─────────────────┐
│  YAML Config    │  ← Configuration flexible
└────────┬────────┘
         │
         v
┌─────────────────┐
│  ETL Pipeline   │  ← Orchestration
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Dask Cluster   │  ← Traitement distribué
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Parquet/CSV    │  ← Résultats
└─────────────────┘
```

---

## 📁 Structure du Projet

```
etl-pipeline-dask/
├── README.md                    # Fichier README 
├── templates.yaml               # Template universel
├── etl_pipeline_yaml.py         # Pipeline ETL
├── streamlit_app.py             # Interface Streamlit
├── metrics_page.py              # Vue des métriques après le lancement du pipeline
├── docs/
    ├── guide.md                 # Documentation pour les fichiers de configurations du pipeline
└── data/
    ├── raw/                     # Données brutes
    │   ├── ecommerce_data.csv
    │   └── test_data.csv
    └── processed/               # Résultats
```

---

## 🎓 Pour Aller Plus Loin

### Ajouter Vos Propres Datasets

```python
# Dans generate_datasets.py
def generate_custom_dataset(n_rows: int):
    data = {
        'col1': [...],
        'col2': [...],
    }
    df = pd.DataFrame(data)
    df.to_csv('data/raw/custom.csv', index=False)
```

### Créer des Transformations Personnalisées

Modifiez `etl_pipeline_yaml.py` et ajoutez dans la classe `YAMLETLPipeline` :

```python
def _custom_transform(self, config: Dict):
    """Votre transformation personnalisée"""
    # Votre code ici
    pass
```

---

## 📞 Support

- **Documentation Dask :** https://docs.dask.org
- **Documentation Streamlit :** https://docs.streamlit.io
- **Issues :** Créez une issue sur GitHub

---

## 📝 Licence

MIT License - Libre d'utilisation et de modification.

---

## 🎉 Crédits

Développé avec ❤️ en utilisant :
- **Dask** - Traitement distribué
- **Streamlit** - Interface utilisateur
- **Pandas** - Manipulation de données
- **Plotly** - Visualisations

---

**Bon traitement de données ! 🚀**