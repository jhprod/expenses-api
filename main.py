import os
import base64
import requests
from fastapi import FastAPI, Request, HTTPException, Query

app = FastAPI()

# Secure values loaded from environment
API_KEY = os.environ["API_KEY"]
ORACLE_URL = os.environ["DB_URL"]
USERNAME = os.environ["DB_USER"]
PASSWORD = os.environ["DB_PASSWORD"]

@app.get("/transactions")
def get_transactions(request: Request, key: str = Query(None)):
    # Allow API key via header or query param
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    # SQL to run
    sql_query = "SELECT * FROM credit_expenses"

    # Build auth header for Oracle APEX REST SQL API
    headers = {
        "Content-Type": "application/sql",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    # Call Oracle REST SQL endpoint
    response = requests.post(ORACLE_URL, headers=headers, data=sql_query)

    if not response.ok:
        return {"error": response.status_code, "message": response.text}

    # Parse the response
    data = response.json()
    rows = data["items"][0]["resultSet"]["items"]

    return rows
