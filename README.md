# Big Data — Travaux Pratiques

**Réalisé par :** Khalid AIT M'HAMED

**Encadré par :** Pr. Abdelmajid BOUSSELHAM

**Université :** ENSET Mohammedia &nbsp;&nbsp;

**Filière :** MII-BDCC &nbsp;&nbsp;

**Année scolaire :** 2025–2026

---

## Présentation

Ce dépôt regroupe l'ensemble des travaux pratiques réalisés dans le cadre du module **Big Data** à l'ENSET Mohammedia. Chaque TP couvre une technologie clé de l'écosystème Big Data, de l'analyse SQL sur Spark jusqu'à l'orchestration de pipelines avec Apache Airflow.

---

## Structure du dépôt

```
BIG-DATA-BOUSELHAM-MII-BDCC/
├── TP 3 - SPARK SQL/
├── TP 4 - Analyse en temps quasi réel des mesures de capteurs avec Spark Structured Streaming et HDFS/
├── TP 5 - Traitement de flux avec Kafka Streams/
└── TP 6 - Atelier Big Data Apache Airflow pour l'orchestration des pipelines Big Data/
```

---

## TP 3 — Spark SQL : Analyse de données de location de vélos

**Technologies :** Apache Spark 4.1.1, Java 17, Maven, Docker

### Objectif

Analyser un dataset de **5 000 locations de vélos** en libre-service via des requêtes **Spark SQL**. Le projet couvre le chargement de données CSV, la création de vues temporaires SQL, et plusieurs catégories d'analyses.

### Analyses réalisées

| Analyse | Résultat |
|---|---|
| Nombre total de locations | 5 000 |
| Revenu total | 41 755.7 MAD |
| Station la plus active | Station Hôpital (447 locations) |
| Heure de pointe | 19h (507 locations) |
| Station la plus active le matin | Station Technopark (207 locations) |
| Âge moyen des utilisateurs | 41.5 ans |
| Tranche d'âge dominante | 51+ (1 567 locations) |

### Stack technique

| Composant | Version |
|---|---|
| Apache Spark | 4.1.1 |
| Java | 17 |
| Maven | 3.x |
| Docker | — |

---

## TP 4 — PySpark Structured Streaming : Analyse de capteurs sur HDFS

**Technologies :** PySpark, Hadoop HDFS 3.4.3, Apache Spark, Docker

### Objectif

Mettre en œuvre un pipeline de traitement de flux en **quasi temps réel** avec **PySpark Structured Streaming**, en lisant des fichiers CSV de capteurs déposés progressivement dans **HDFS**.

### Architecture

```
[Fichiers CSV locaux]
        |
        v
[docker cp → Namenode /tmp/]
        |
        v
[hdfs dfs -put → /streaming/capteurs/]
        |
        v
[PySpark Structured Streaming]
    |               |
    v               v
[Stats par     [Alertes si
 capteur]       valeur > 35]
```

### Fonctionnalités

- Lecture en continu avec `maxFilesPerTrigger=1`
- Statistiques par capteur : moyenne, min, max, nombre de mesures
- Détection d'anomalies : `valeur > 35.0`
- Checkpoints HDFS pour tolérance aux pannes (exactly-once)

### Infrastructure Docker

| Service | Image | Rôle |
|---|---|---|
| namenode | apache/hadoop:3.4.3 | NameNode HDFS |
| datanode1…5 | apache/hadoop:3.4.3 | 5 DataNodes |
| resourcemanager | apache/hadoop:3.4.3 | YARN ResourceManager |
| spark-master | spark:latest | Spark Master (port 8081) |
| spark-worker-1/2 | spark:latest | 2 Workers |

### Résultats

| Capteur | Moyenne | Min | Max | Mesures |
|---|---|---|---|---|
| CAPTEUR_TEMP_1 | 26.74 °C | 21.9 °C | 40.7 °C | 5 |
| CAPTEUR_TEMP_2 | 30.85 °C | 24.1 °C | 45.3 °C | 4 |
| CAPTEUR_HUM_1 | 65.33 % | 60.3 % | 70.6 % | 3 |

2 alertes déclenchées (`id=8` : 40.7 °C, `id=12` : 45.3 °C)

---

## TP 5 — Traitement de flux avec Kafka Streams

**Technologies :** Apache Kafka, Kafka Streams, Spring Boot, Java, Maven, Docker, Prometheus

### Objectif

Implémenter trois exercices de traitement de flux en temps réel avec **Apache Kafka Streams**.

### Exercices

#### Exercice 1 — Text Cleaner

Pipeline de nettoyage de texte :
- Trim + normalisation des espaces + mise en majuscules
- Filtrage des messages invalides (longueur > 100, mots interdits : `HACK`, `SPAM`, `XXX`)
- Routage vers `text-clean` (valides) ou `text-dead-letter` (rejetés)

#### Exercice 2 — Weather Analyzer

Analyse météorologique en temps réel :
- Filtrage des températures > 30 °C
- Conversion Celsius → Fahrenheit
- Agrégation des moyennes par station via `groupByKey().aggregate()`
- Exposition de métriques **Prometheus** sur `http://localhost:1234/metrics`

| Station | Moy. Temp | Moy. Humidité |
|---|---|---|
| station1 | 99.5 °F | 75.0 % |
| station2 | 93.2 °F | 62.5 % |

#### Exercice 3 — Click Stream Analytics

Architecture multi-applications Spring Boot :
- **Producer** : génère des événements de clics utilisateurs
- **Streams App** : compte les clics par utilisateur avec `count()`
- **Consumer** : affiche les compteurs en temps réel

### Infrastructure

| Composant | Image | Port |
|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 |
| Kafka Broker | apache/kafka:latest | 9092 |

---

## TP 6 — Apache Airflow : Orchestration de pipelines Big Data

**Technologies :** Apache Airflow, Python, Docker, Docker Compose

### Objectif

Orchestrer des pipelines de données avec **Apache Airflow** déployé via Docker Compose. Le TP couvre la définition de DAGs, la planification et la supervision de workflows Big Data.

### Fonctionnalités

- Déploiement d'Airflow via Docker Compose (webserver, scheduler, PostgreSQL)
- Création et déclenchement de DAGs avec `PythonOperator`
- Supervision des exécutions via l'interface web Airflow
- Organisation des pipelines en tâches chaînées

### Infrastructure

| Service | Rôle | Port |
|---|---|---|
| airflow-webserver | Interface web Airflow | 8080 |
| airflow-scheduler | Planificateur de DAGs | — |
| postgres | Base de métadonnées Airflow | 5432 |

---

## Technologies couvertes

| Technologie | TP | Usage |
|---|---|---|
| Apache Spark SQL | TP3 | Analyse batch sur CSV |
| PySpark Structured Streaming | TP4 | Traitement de flux quasi temps réel |
| Hadoop HDFS | TP4 | Stockage distribué des données |
| YARN | TP4 | Gestion des ressources cluster |
| Apache Kafka | TP5 | Broker de messages distribué |
| Kafka Streams | TP5 | Traitement de flux par topologie |
| Spring Boot | TP5 | Applications producer/consumer/streams |
| Prometheus | TP5 | Métriques temps réel |
| Apache Airflow | TP6 | Orchestration de pipelines |
| Docker / Docker Compose | TP3–6 | Déploiement de l'infrastructure |
