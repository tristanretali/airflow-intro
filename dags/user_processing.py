from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_URL = "https://raw.githubusercontent.com/FatimaMoqran/airflow-datasets/refs/heads/main/fakeuser.json"


@dag(
    dag_id="user_processing",
    tags=["user"],
)
def user_processing():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql=""" 
        CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests

        response = requests.get(API_URL)
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    @task
    def extract_user(fake_user: dict) -> dict:
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    @task
    def process_user(user_info: dict):
        from datetime import datetime

        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("/tmp/user_info.csv", "a") as f:
            if f.tell() == 0:
                f.write("id,firstname,lastname,email,created_at\n")
            f.write(
                f"{user_info['id']},{user_info['firstname']},{user_info['lastname']},{user_info['email']},{user_info['created_at']}\n"
            )

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TEMP TABLE users_staging (LIKE users INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                with open("/tmp/user_info.csv") as f:
                    cur.copy_expert("COPY users_staging FROM STDIN WITH CSV HEADER", f)
                cur.execute("""
                    INSERT INTO users (id, firstname, lastname, email, created_at)
                    SELECT id, firstname, lastname, email, created_at FROM users_staging
                    ON CONFLICT (id) DO NOTHING
                    """)
            conn.commit()
        finally:
            conn.close()

    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    processed = process_user(user_info)
    create_table >> fake_user
    processed >> store_user()


user_processing()
