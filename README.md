# etl_project
ETL dask pipeline+ Front‑end (Streamlit)

Description

Ce projet consiste à construire un **pipeline ETL (Extract – Transform – Load)** scalable avec **Dask**,  
et une **interface Streamlit** pour le **contrôle**, le **monitoring** et la **visualisation** des résultats.

L'objectif est de démontrer comment un ETL peut être :
- parallélisé localement (grâce à Dask),
- contrôlé via une interface simple (Streamlit),
- et réutilisable pour différents jeux de données grâce à des fichiers de configuration.


Deliverables
ETL code + front-end, README, sample dataset and demo video.


etl_project/
    ├── app/                     # Interface Streamlit (contrôle, dashboard, visualisation)
    |
    │   ├── pages/               # (optionnel) pages Streamlit séparées : Extract, Transform, KPIs...
    │   └── __init__.py
    │
    ├── etl/                     # Le cœur du pipeline ETL
    │   ├── extract.py           # Lecture & uniformisation
    │   ├── transform.py         # Nettoyage, normalisation, enrichissement
    │   ├── predict.py           # Étape de prédiction simple
    │   ├── load.py              # Sauvegarde, manifest & agrégats
    │   └── utils.py             # Fonctions communes (timers, logs, profiling)
    │
    ├── configs/                 # Configurations dataset-agnostiques
    │   ├── nyc_taxi.yaml        # Première config (dataset principal)
    │   └── imdb.yaml            # (plus tard) Deuxième dataset pour la réutilisation
    │
    ├── outputs/                 # Résultats produits par le pipeline
    │   ├── clean/
    │   ├── features/
    │   ├── predictions/
    │   ├── analytics/
    │   ├── manifest.json
    │   └── metrics/
    │
    ├── data/                    # Données locales ou téléchargées (si besoin)
    │   └── sample/              # Petits jeux d’exemples (un mois NYC Taxi)
    │
    ├── requirements.txt         # Liste des dépendances Python (Dask, Streamlit, etc.)
    ├── README.md                # Documentation claire et pédagogique
    └── .gitignore (optionnel)