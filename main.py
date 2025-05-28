import oracledb
import os
from fastapi import FastAPI

app = FastAPI()

DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_SERVICE_NAME = os.environ["DB_SERVICE_NAME"]

@app.get("/transactions")
def get_transactions():
    dsn = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}"
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM credit_expenses")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return rows
