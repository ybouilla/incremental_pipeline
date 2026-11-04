import psycopg
import os
from dotenv import load_dotenv

current_folder = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_folder, "config", ".env.postgre")) 


db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

print("test", db_user, db_password, os.path.join(current_folder, "config", ".env.postgre"))
conn = psycopg.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)


with conn.cursor() as cursor:
    with open(os.path.join(current_folder, "sql", "create.sql"), "r") as f:
        sql_script = f.read()

    cursor.execute(sql_script)

conn.commit()
conn.close()

print("Database initialized")