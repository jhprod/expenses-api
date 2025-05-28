from fastapi import FastAPI
import oracledb
import os

app = FastAPI()

# Read connection info from environment variables
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_DSN = os.environ["DB_DSN"]  # Should be like "yourdb_high.adb.oraclecloud.com"

@app.get("/transactions")
def get_transactions():
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM credit_expenses")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return rows
