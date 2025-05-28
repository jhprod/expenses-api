import requests
import base64
import os
from fastapi import FastAPI

app = FastAPI()

# APEX REST SQL URL
ORACLE_URL = os.environ["DB_URL"]
USERNAME = os.environ["DB_USER"]
PASSWORD = os.environ["DB_PASSWORD"] # You can use os.environ if storing securely
API_KEY = os.environ["API_KEY"]

@app.get("/transactions")
def get_transactions(request: Request):
    client_key = request.headers.get("X-API-Key")
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
    result = []

    # Extract rows from resultSet
    items = data["items"][0]["resultSet"]["items"]
    for row in items:
        result.append(row)

    return result
