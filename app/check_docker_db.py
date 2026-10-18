# file purpose is just to check if data exist avter applying transformation or dbt

import psycopg
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import pandas as pd

current_folder = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_folder, "config", ".env.postgre")) 


db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

print("test", db_user, db_password, os.path.join(current_folder, "config", ".env.postgre"))
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="licenses_db",
    user="myuser",
    password="my_password"
)

sql_check = """select * from mart_license_metrics LIMIT 10;"""

with conn.cursor() as cursor:
    cursor.execute(sql_check)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

df = pd.DataFrame(rows, columns=columns)
print(df)

if len(df) < 9:
    raise ValueError("error while using dbt, no views for `mart_license_metrics` has been created")