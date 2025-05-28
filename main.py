import os
import base64
import requests
from fastapi import FastAPI, Request, HTTPException, Query

app = FastAPI()

API_KEY = os.environ["API_KEY"]
ORACLE_URL = os.environ["DB_URL"]
USERNAME = os.environ["DB_USER"]
PASSWORD = os.environ["DB_PASSWORD"]

@app.get("/transactions")
def get_transactions(request: Request, key: str = Query(None)):
    # Accept API key via header or ?key=... query param
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    sql_query = "SELECT * FROM credit_expenses"
    headers = {
        "Content-Type": "application/sql",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    response = requests.post(ORACLE_URL, headers=headers, data=sql_query)

    if not response.ok:
        return {"error": response.status_code, "message": response.text}

    data = response.json()
    rows = data["items"][0]["resultSet"]["items"]
    return rows

@app.get("/ping")
def ping():
    return {"message": "ping success"}

