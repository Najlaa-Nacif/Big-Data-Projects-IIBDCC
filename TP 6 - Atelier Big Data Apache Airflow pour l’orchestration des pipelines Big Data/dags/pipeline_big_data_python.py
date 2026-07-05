import csv
import json
import os
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/ventes_raw.csv"
CLEAN_FILE = f"{DATA_DIR}/ventes_clean.csv"
RESULT_FILE = f"{DATA_DIR}/resultats_ventes.json"
REPORT_FILE = f"{DATA_DIR}/rapport_pipeline.txt"


def ingestion_donnees():
    os.makedirs(DATA_DIR, exist_ok=True)

    ventes = [
        ["id_vente", "ville", "produit", "prix", "quantite"],
        [1, "Casablanca", "PC", 8000, 2],
        [2, "Rabat", "Clavier", 300, 5],
        [3, "Marrakech", "Souris", 150, 10],
        [4, "Casablanca", "Ecran", 2500, 3],
        [5, "Tanger", "PC", 8500, 1],
        [6, "Rabat", "Ecran", 2300, 2],
    ]

    with open(RAW_FILE, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(ventes)

    print(f"Ingestion terminee. Fichier cree : {RAW_FILE}")


def stockage_zone_brute():
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier brut n’existe pas.")

    taille = os.path.getsize(RAW_FILE)

    print("Stockage zone brute termine.")
    print(f"Fichier brut : {RAW_FILE}")
    print(f"Taille : {taille} octets")


def validation_donnees():
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier de donnees est introuvable.")

    with open(RAW_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        header = next(reader)

    colonnes_attendues = ["id_vente", "ville", "produit", "prix", "quantite"]

    if header != colonnes_attendues:
        raise ValueError("Schema incorrect")

    print("Validation terminee avec succes.")
    print(f"Colonnes detectees : {header}")


def transformation_donnees():
    lignes = []

    with open(RAW_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            prix = float(row["prix"])
            quantite = int(row["quantite"])
            montant = prix * quantite

            lignes.append({
                "id_vente": row["id_vente"],
                "ville": row["ville"],
                "produit": row["produit"],
                "prix": prix,
                "quantite": quantite,
                "montant": montant
            })

    with open(CLEAN_FILE, mode="w", newline="", encoding="utf-8") as file:
        fieldnames = ["id_vente", "ville", "produit", "prix", "quantite", "montant"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(lignes)

    print("Transformation terminee.")


def traitement_analytique():
    ca = {}

    with open(CLEAN_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            ville = row["ville"]
            montant = float(row["montant"])
            ca[ville] = ca.get(ville, 0) + montant

    with open(RESULT_FILE, mode="w", encoding="utf-8") as file:
        json.dump(ca, file, indent=4, ensure_ascii=False)

    print("Traitement analytique termine.")
    print(ca)


def chargement_resultats():
    if not os.path.exists(RESULT_FILE):
        raise FileNotFoundError("Resultats introuvables")

    print("Chargement termine.")


def generation_rapport():
    with open(RESULT_FILE, mode="r", encoding="utf-8") as file:
        resultats = json.load(file)

    with open(REPORT_FILE, mode="w", encoding="utf-8") as file:
        file.write("Rapport Big Data\n")
        file.write("================\n\n")

        for ville, ca in resultats.items():
            file.write(f"{ville} : {ca} DH\n")

    print("Rapport genere.")


with DAG(
    dag_id="pipeline_big_data_python",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["big-data", "python-operator"],
) as dag:

    ingestion = PythonOperator(
        task_id="ingestion_donnees",
        python_callable=ingestion_donnees,
    )

    stockage = PythonOperator(
        task_id="stockage_zone_brute",
        python_callable=stockage_zone_brute,
    )

    validation = PythonOperator(
        task_id="validation_donnees",
        python_callable=validation_donnees,
    )

    transformation = PythonOperator(
        task_id="transformation_donnees",
        python_callable=transformation_donnees,
    )

    traitement = PythonOperator(
        task_id="traitement_analytique",
        python_callable=traitement_analytique,
    )

    chargement = PythonOperator(
        task_id="chargement_resultats",
        python_callable=chargement_resultats,
    )

    rapport = PythonOperator(
        task_id="generation_rapport",
        python_callable=generation_rapport,
    )

    ingestion >> stockage >> validation >> transformation >> traitement >> chargement >> rapport