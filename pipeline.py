# airflow/dags/pipeline.py
import json
import httpx

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

json_url = "https://raw.githubusercontent.com/LearnWebCode/json-example/refs/heads/master/pets-data.json"


def extract_data(url: str) -> str:
    with httpx.Client() as session:
        result = session.get(url=url)
        result.raise_for_status()
        return result.text


def transform_pets_json(ti):
    raw_json = ti.xcom_pull(task_ids="extract_json")

    raw_data = json.loads(raw_json).get("pets")
    pets, foods = [], []

    for entry in raw_data:
        name = entry.get("name")

        pets.append({
            "name": name,
            "species": entry.get("species"),
            "birth_year": entry.get("birthYear"),
            "photo": entry.get("photo"),
        })

        for food in entry.get("favFoods", []):
            foods.append({
                "name": name,
                "food": food
            })

    pets_insert = make_insert_sql("pets", pets)
    foods_insert = make_insert_sql("pet_favorite_foods", foods)

    return "; ".join([pets_insert, foods_insert])


def make_insert_sql(tablename: str, data: list[dict]):
    columns, rows = [], []
    for entry in data:
        for key in entry.keys():
            if key not in columns:
                columns.append(key)
            continue

    for i, entry in enumerate(data):
        row = []
        for column in columns:
            value = entry.get(column)
            if isinstance(value, (int, float)):
                row.append(str(value))
            elif isinstance(value, str):
                row.append(f"'{value}'")
            elif value is None:
                row.append("NULL")
            else:
                raise TypeError("Данный формат не поддерживается")

        rows.append(f"({', '.join(row)})")

    stmt = f"INSERT INTO {tablename} ({', '.join(columns).strip()}) VALUES {', '.join(rows)}"
    return stmt


with DAG(dag_id="etl_json_xml") as dag:
    create_tables = SQLExecuteQueryOperator(
        task_id="create_pet_table",
        conn_id="hse",
        sql="""
        drop table pets cascade;
        drop table pet_favorite_foods cascade;
        
        create table if not exists pets (
            name VARCHAR(30) PRIMARY KEY,
            species VARCHAR(40),
            birth_year INT,
            photo VARCHAR(255)
        );

        create table if not exists pet_favorite_foods (
            name VARCHAR(30) REFERENCES pets(name),
            food VARCHAR(40)
        );
        """
    )

    extract_json = PythonOperator(
        task_id="extract_json",
        python_callable=extract_data,
        op_kwargs={"url": json_url},
    )

    transform_json = PythonOperator(
        task_id="transform_json",
        python_callable=transform_pets_json
    )

    load_json = SQLExecuteQueryOperator(
        task_id="load_json",
        conn_id="hse",
        sql="{{ ti.xcom_pull(task_ids='transform_json') }}"
    )

    create_tables >> extract_json >> transform_json >> load_json
