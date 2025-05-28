import requests
import base64
from fastapi import FastAPI

app = FastAPI()

# APEX REST SQL URL
ORACLE_URL = os.environ["DB_URL"]
USERNAME = os.environ["DB_USER"]
PASSWORD = os.environ["DB_PASSWORD"] # You can use os.environ if storing securely

@app.get("/transactions")
def get_transactions():
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
