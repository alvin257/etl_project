# ⚡ ETL Pipeline Monitor

Pipeline ETL distribué avec Dask et interface Streamlit configurable via YAML.

---

## 🎯 Vue d'Ensemble

Ce projet consiste à construire un pipeline ETL (Extract – Transform – Load) scalable avec Dask, et une interface Streamlit pour :

- ✅ le traitement des **millions de lignes** en parallèle: 

  - Distribution du travail sur plusieurs workers (processus)
  - Traitement de datasets plus gros que la RAM (out-of-core computing)
  - Contourne le GIL Python pour un vrai parallélisme multi-core
  - Scaling horizontal : ajoutez des workers selon vos besoins
  
- ✅ le contrôle de l'exécution
- ✅ le monitoring de l'exécution en **temps réel** à l'aide de Dask Dashboard,
- ✅ la configuration d'un pipeline suivant le schéma ETL via **un simple fichier YAML**
- ✅ l'application des transformations **sans coder**
- ✅ la visualisation des résultats
- ✅ la relecture des exécutions précédentes


L'objectif est de démontrer comment un ETL peut être :

- parallélisé localement (grâce à **Dask Distributed**),

- contrôlé via une interface simple,

- modulaire et reconfigurable via des fichiers YAML,

- et extensible pour le temps réel et les données utilisateurs.


---

## 📦 Prérequis

### Système

Python : 3.8 ou supérieur
RAM : 4 GB minimum (8 GB recommandé pour datasets > 1M lignes)
Espace disque : 500 MB minimum

### Systèmes d'exploitation supportés

✅ Linux
✅ macOS
✅ Windows 10/11



## 📦 Installation Rapide

```bash
# 1. Cloner le projet
git clone <votre-repo>
cd etl-pipeline-dask

# 2. Créer un environnement virtuel (Recommandé)
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# OU
.venv\Scripts\activate     # Windows

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Lancer l'application
streamlit run streamlit_app.py
```

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
│  Dask Cluster   │  ← Traitement distribué (Dask Distributed)
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
├── Slide_ETL.pdf                # Slides de la présentation
├── Demo.mp4                     # Vidéo de démonstration
├── docs/
    ├── guide.md                 # Documentation pour les fichiers de configurations du pipeline
└── data/
    ├── raw/                     # Données brutes
    │   ├── ecommerce_data.csv
    │   └── test_data.csv
    └── processed/               # Résultats
```

---


## 🚀 Utilisation en 3 Étapes

### Étape 1 : Modifier le YAML

Utilisez l'éditeur dans l'interface Streamlit.

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

### Étape 2 : Valider la Configuration (Optionnel)

- Cliquez sur "✅ Valider YAML"
- Vérifiez l'aperçu du plan d'exécution
- Assurez-vous que les chemins de fichiers sont corrects


### Étape 3 : Lancer le Pipeline

### Étape 4 : Consultez les résultats des steps de votre pipeline


---


## 📱 Pages de l'Application

### Page Principale (Éditeur)

#### Fonctionnalités :

Édition YAML avec validation en temps réel
Aperçu du plan d'exécution avant lancement
Lancement du pipeline
Lien vers le **Dask Dashboard** (pendant l'exécution)

#### Actions disponibles :

✅ Valider YAML : Vérifie la syntaxe et l'existence des fichiers
🚀 Lancer Pipeline : Démarre l'exécution
🔄 Réinitialiser : Recharge le template par défaut


##### Métriques Principales

📊 Records traités
⏱️ Temps total d'exécution
⚡ Throughput (records/sec)
💾 Taille du fichier de sortie


##### Temps par Étape

Graphique en barres (Extract, Transform, Load)
Permet d'identifier les goulots d'étranglement


##### Informations Sortie

Format (Parquet/CSV)
Nombre de partitions Dask
Chemin du fichier final


##### Métriques Dask

Nombre de workers
Utilisation CPU par worker
Utilisation RAM par worker
Graphique d'utilisation mémoire



##### Actions disponibles :

◀ Retour à l'éditeur : Modifier la config
📊 Voir Visualisations : Ouvre la page visualisation
📜 Voir Historique : Consulte les runs précédents


### 📜 Historique des Pipelines
- Consultation des runs précédents
- Téléchargement des configs YAML
- Rejeu de configurations
- Téléchargement des résultats JSON

### 📊 Visualisation Automatique
- Chargement automatique du dataset final
- Sélection du type de graphique :

  - Bar : Comparaison de catégories
  - Scatter : Relation entre 2 variables (avec trendline)
  - Line : Évolution temporelle
  - Histogram : Distribution d'une variable

- Choix des axes X/Y
- Téléchargement du dataset final (CSV)


💾 Historique et Cache
Chaque exécution du pipeline sauvegarde automatiquement :
📝 Configurations YAML

Emplacement : configs/config_YYYYMMDD_HHMMSS.yaml
Contenu : Configuration YAML exacte utilisée pour le run
Usage : Rejouer un pipeline avec la même configuration

Exemple :
```yaml
configs/
├── config_20241126_143025.yaml
├── config_20241126_151230.yaml
└── config_20241126_163045.yaml
```

📊 Résultats JSON

Emplacement : results_cache/results_YYYYMMDD_HHMMSS.json
Contenu :

Statistiques du pipeline (temps, records, throughput)
Métriques Dask (workers, RAM, CPU)
Chemin des fichiers de sortie
Timestamp d'exécution


Usage : Consultation des runs précédents dans l'historique

Exemple de structure JSON :

```json
{
  "status": "SUCCESS",
  "timestamp": "20241126_143025",
  "total_time": 18.5,
  "statistics": {
    "total_records": 1000000,
    "throughput": 54054.0,
    "file_size_mb": 68.5
  },
  "dask_metrics": {
    "n_workers": 4,
    "total_memory_gb": 4.0,
    "workers_details": [...]
  }
}
```

### 📊 Monitoring avec Dask Dashboard
#### Accès au Dashboard
Pendant l'exécution du pipeline, un lien vers le Dask Dashboard s'affiche automatiquement dans l'interface Streamlit.
URL par défaut : http://localhost:8787


#### Informations Disponibles
Le Dask Dashboard offre une vue en temps réel sur :

##### Status : Vue d'ensemble du cluster

Nombre de workers actifs
Tasks en cours d'exécution
État général du cluster


##### Workers : État de chaque worker

Utilisation CPU (%)
Utilisation RAM (GB)
Tasks assignées
Statut (actif/inactif)


##### Tasks : Progression des tâches

Tasks complétées
Tasks en cours
Tasks en attente
Durée par task


##### Graph : Visualisation du graphe de calcul

Dépendances entre tasks
Flux de données
Optimisations appliquées


##### Memory : Utilisation mémoire détaillée

RAM par worker
Pics de consommation
Distribution des données


##### Profile : Profiling des opérations

Temps par fonction
Bottlenecks identifiés


### ⚠️ Important
Le dashboard se ferme automatiquement à la fin du pipeline (comportement normal de Dask).
Cependant :

Les métriques importantes sont capturées avant la fermeture
Elles sont affichées dans la page Résultats
Elles sont sauvegardées dans le JSON (results_cache/)

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
