import os
import base64
import requests
from fastapi import FastAPI, Request, HTTPException, Query

app = FastAPI()

API_KEY = os.environ["API_KEY"]
ORACLE_URL = os.environ["DB_URL"]
USERNAME = os.environ["DB_USER"]
PASSWORD = os.environ["DB_PASSWORD"]

def query_oracle(sql_query: str):
    headers = {
        "Content-Type": "application/sql",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    response = requests.post(ORACLE_URL, headers=headers, data=sql_query)

    if not response.ok:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    try:
        data = response.json()
        return data["items"][0]["resultSet"]["items"]
    except (KeyError, IndexError, TypeError):
        raise HTTPException(status_code=500, detail="Invalid response from Oracle DB")


@app.get("/expenses")
def get_expenses(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["transactions_query"]
    return query_oracle(SQL_QUERY)


# @app.get("/getExpenseID")
# def get_expensesId(request: Request, key: str = Query(None)):
#     client_key = request.headers.get("X-API-Key") or key
#     if client_key != API_KEY:
#         raise HTTPException(status_code=403, detail="Forbidden")
#     SQL_QUERY = os.environ["expenseid_query"]
#     return query_oracle(SQL_QUERY)

@app.get("/getExpenseID")
def get_expense_id():
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    payload = {
        "statementText": "BEGIN SELECT MY_EXPENSE_SEQ.NEXTVAL INTO :new_id FROM dual; END;",
        "binds": {
            "new_id": None
        }
    }

    response = requests.post(ORACLE_URL, headers=headers, json=payload)

    if not response.ok:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    try:
        result = response.json()
        return {
            "expense_id": result["items"][0]["binds"]["new_id"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Could not parse Oracle response")
@app.get("/ping")
def ping():
    return {"message": "ping success"}
