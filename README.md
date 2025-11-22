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

📚 Structure du projet

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



🔧 Modules clés

run_pipeline.py : orchestrateur principal

etl.extract.run_extract() : charge les fichiers sources (lazy)

etl.transform.run_transform() : nettoie, enrichit, dérive des features

etl.load.run_load() : écrit les outputs, calcule les stats

🔄 Exécution typique

Choix d’un fichier YAML dans l’interface

Lancement du pipeline (exécution asynchrone en arrière-plan)

Suivi temps réel via la page Monitoring (logs + progression)

Accès aux résultats techniques et KPIs dans les autres pages

🎥 Vidéo de démo

(à ajouter)

📊 Dépendances

Voir requirements.txt. Principaux packages :

Dask

Pandas

PyYAML

Streamlit

Plotly

requests (pour API TMDB)

📥 Déploiement

Compatible avec streamlit run app/ ou un déploiement via Streamlit Cloud.

🛠️ TODOs (idées futures)

Notebooks de tests unitaires sur les étapes ETL

Sauvegarde automatique dans un S3 bucket

Support WebSocket / Kafka pour un vrai mode streaming

Plus d’API supportées (IMDB, OpenWeather, etc.)