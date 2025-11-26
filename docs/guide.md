
---

### Étape 1 : Modifier le YAML

Ouvrez `TEMPLATE.yaml` ou utilisez l'éditeur dans l'interface Streamlit.

#### Exemple 1 : Pipeline Basique (Nettoyage + Calcul)

```yaml
source:
  type: csv
  path: "data/raw/ecommerce_1M.csv"             # Si fichier en local, toujours mettre dans le dossier data/raw/

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
  path: "data/processed/result"                 # Toujours placer dans data/processed

dask:
  workers: 2
  memory_per_worker: "1GB"
  threads_per_worker: 1
```

#### Exemple 2 : Pipeline Complet (Tout Activé)

```yaml
source:
  type: csv
  path: "data/raw/ecommerce_1M.csv"             # Si fichier en local, toujours mettre dans le dossier data/raw/

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
  path: "data/processed/ecommerce_aggregated"   # Toujours placer dans data/processed

dask:
  workers: 8
  memory_per_worker: "2GB"
  threads_per_worker: 2
```

---

### Étape 2 : Lancer le Pipeline


1. Valider le YAML pour voir un aperçu
2. Cliquez sur **"🚀 Lancer le Pipeline"**

---

## 📖 Documentation YAML

### 🔧 Section `source`

Définit la source de données.

```yaml
source:
  type: csv              # Options : csv | parquet
  path: "chemin/vers/fichier.csv"              # Si fichier en local, toujours placer dans data/raw
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
  path: "data/processed/result"                # Toujours placer dans data/processed 
```

**Formats :**
- `parquet`
- `csv`

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

**Bon traitement de données ! 🚀**