from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from datetime import datetime
import os
import psycopg2
from psycopg2.extras import execute_values


# ---------------------------------------------------------
# 1. EXTRACT
# SQL Server (Ventes) -> Python
# ---------------------------------------------------------

def extract_sales(**context):

    hook = MsSqlHook(mssql_conn_id="mssql_ventes")

    sql = """
    SELECT
        c.CommandeId,
        c.ClientId,
        cl.NomSociete,
        cl.Ville,
        cl.Pays,
        c.DateCommande,
        lc.ProduitId,
        CAST(lc.PrixUnitaire AS FLOAT) AS PrixUnitaire,
        CAST(lc.Quantite AS INT) AS Quantite,
        CAST(lc.Remise AS FLOAT) AS Remise
    FROM dbo.Commande c
    INNER JOIN dbo.LigneCommandes lc
        ON c.CommandeId = lc.CommandeId
    INNER JOIN dbo.Client cl
        ON c.ClientId = cl.ClientId
    WHERE c.DateCommande IS NOT NULL
    """

    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(sql)

    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

    sales = [
        dict(zip(columns, row))
        for row in rows
    ]

    cursor.close()
    conn.close()

    print(f"{len(sales)} lignes extraites depuis SQL Server")

    context["ti"].xcom_push(
        key="raw_sales",
        value=sales
    )


# ---------------------------------------------------------
# 2. TRANSFORM
# Calcul du chiffre d'affaires
# ---------------------------------------------------------

def transform_sales(**context):

    sales = context["ti"].xcom_pull(
        task_ids="extract_sales",
        key="raw_sales"
    )

    transformed = []

    for row in sales:

        prix = float(row["PrixUnitaire"])
        quantite = int(row["Quantite"])
        remise = float(row["Remise"])

        montant_brut = prix * quantite
        montant_remise = montant_brut * remise
        chiffre_affaires = montant_brut - montant_remise

        transformed.append({
            "commande_id": row["CommandeId"],
            "client_id": row["ClientId"].strip(),
            "nom_societe": row["NomSociete"],
            "ville": row["Ville"],
            "pays": row["Pays"],
            "date_commande": row["DateCommande"],
            "produit_id": row["ProduitId"],
            "prix_unitaire": round(prix, 2),
            "quantite": quantite,
            "remise": remise,
            "montant_brut": round(montant_brut, 2),
            "montant_remise": round(montant_remise, 2),
            "chiffre_affaires": round(chiffre_affaires, 2)
        })

    print(f"{len(transformed)} lignes transformées")

    context["ti"].xcom_push(
        key="transformed_sales",
        value=transformed
    )


# ---------------------------------------------------------
# 3. LOAD
# Python -> PostgreSQL
# ---------------------------------------------------------

def load_postgres(**context):

    sales = context["ti"].xcom_pull(
        task_ids="transform_sales",
        key="transformed_sales"
    )

    conn = psycopg2.connect(
        host="postgres",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_fact (
            commande_id INTEGER,
            client_id VARCHAR(20),
            nom_societe VARCHAR(255),
            ville VARCHAR(100),
            pays VARCHAR(100),
            date_commande TIMESTAMP,
            produit_id INTEGER,
            prix_unitaire NUMERIC(12,2),
            quantite INTEGER,
            remise NUMERIC(8,4),
            montant_brut NUMERIC(14,2),
            montant_remise NUMERIC(14,2),
            chiffre_affaires NUMERIC(14,2),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Pour éviter les doublons à chaque exécution du DAG
    cursor.execute("TRUNCATE TABLE sales_fact;")

    values = [
        (
            row["commande_id"],
            row["client_id"],
            row["nom_societe"],
            row["ville"],
            row["pays"],
            row["date_commande"],
            row["produit_id"],
            row["prix_unitaire"],
            row["quantite"],
            row["remise"],
            row["montant_brut"],
            row["montant_remise"],
            row["chiffre_affaires"]
        )
        for row in sales
    ]

    insert_sql = """
        INSERT INTO sales_fact (
            commande_id,
            client_id,
            nom_societe,
            ville,
            pays,
            date_commande,
            produit_id,
            prix_unitaire,
            quantite,
            remise,
            montant_brut,
            montant_remise,
            chiffre_affaires
        )
        VALUES %s
    """

    execute_values(cursor, insert_sql, values)

    conn.commit()

    print(f"{len(values)} lignes chargées dans PostgreSQL")

    cursor.close()
    conn.close()


# ---------------------------------------------------------
# 4. DATA QUALITY CHECK
# ---------------------------------------------------------

def check_data():

    conn = psycopg2.connect(
        host="postgres",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*) AS nb_lignes,
            ROUND(SUM(chiffre_affaires), 2) AS chiffre_affaires_total
        FROM sales_fact;
    """)

    result = cursor.fetchone()

    print("===================================")
    print(f"Nombre de lignes : {result[0]}")
    print(f"Chiffre d'affaires total : {result[1]}")
    print("===================================")

    if result[0] == 0:
        raise ValueError("La table sales_fact est vide !")

    cursor.close()
    conn.close()


# ---------------------------------------------------------
# AIRFLOW DAG
# ---------------------------------------------------------

default_args = {
    "owner": "sazotech",
    "retries": 1,
}


with DAG(
    dag_id="sazotech_sales_pipeline",
    description="ETL SQL Server Ventes vers PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sazotech", "sales", "etl"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_sales",
        python_callable=extract_sales
    )

    transform_task = PythonOperator(
        task_id="transform_sales",
        python_callable=transform_sales
    )

    load_task = PythonOperator(
        task_id="load_postgres",
        python_callable=load_postgres
    )

    check_task = PythonOperator(
        task_id="check_data",
        python_callable=check_data
    )


    extract_task >> transform_task >> load_task >> check_task