import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def reception_fichier():
    print("Reception du fichier des etudiants")


def stockage_zone_brute():
    print("Stockage du fichier dans la zone brute")


def validation_fichier():
    print("Validation du fichier des etudiants")


def nettoyage_donnees():
    print("Nettoyage des donnees")


def affectation_groupes():
    print("Affectation des etudiants aux groupes")


def generation_statistiques():
    print("Generation des statistiques")


def rapport_final():
    print("Generation du rapport final")


with DAG(
    dag_id="pipeline_inscription_etudiants",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["mini-projet", "python-operator"],
) as dag:

    reception = PythonOperator(
        task_id="reception_fichier",
        python_callable=reception_fichier,
    )

    stockage = PythonOperator(
        task_id="stockage_zone_brute",
        python_callable=stockage_zone_brute,
    )

    validation = PythonOperator(
        task_id="validation_fichier",
        python_callable=validation_fichier,
    )

    nettoyage = PythonOperator(
        task_id="nettoyage_donnees",
        python_callable=nettoyage_donnees,
    )

    affectation = PythonOperator(
        task_id="affectation_groupes",
        python_callable=affectation_groupes,
    )

    statistiques = PythonOperator(
        task_id="generation_statistiques",
        python_callable=generation_statistiques,
    )

    rapport = PythonOperator(
        task_id="rapport_final",
        python_callable=rapport_final,
    )

    # ORDRE + PARALLELISME
    reception >> stockage >> validation >> nettoyage
    nettoyage >> [affectation, statistiques]
    [affectation, statistiques] >> rapport