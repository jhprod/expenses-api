import os
import base64
import requests
from fastapi import FastAPI, Request, HTTPException, Query
from pydantic import BaseModel
import logging

# Add this line to define the logger
logger = logging.getLogger("uvicorn.error")

app = FastAPI()

API_KEY = os.environ["API_KEY"]
ORACLE_URL = os.environ["DB_URL"]
USERNAME = os.environ["DB_USER"]
PASSWORD = os.environ["DB_PASSWORD"]


class Expense(BaseModel):
    ID: int
    CARDID: int
    TRANSACTIONDATE: str  # 'YYYY-MM-DD'
    DESCRIPTION: str
    AMOUNT: float
    POSTSTATUS: str
    REWARDSVALUE: float
    VENUEFOUND: str
    BUDGETLABEL: int = None
    CARDCATEGORY: int = None
    TOSYNC: str    
    DELETEYN: str
    UPDATEDDT: str       

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
        raise HTTPException(status_code=500, detail=data)


@app.get("/expenses")
def get_expenses(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["transactions_query"]
    return query_oracle(SQL_QUERY)

@app.get("/get-expense-id")
def get_expense_id(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    ORDS_NEXT_ID_URL = os.environ["get_expense_url"]
    try:
        response = requests.get(ORDS_NEXT_ID_URL)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        data = response.json()

        # return just the ID field
        return {"expense_id": data.get("id")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cards")
def get_cards(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["cards_query"]
    return query_oracle(SQL_QUERY)

@app.get("/categories")
def get_categories(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["category_query"]
    return query_oracle(SQL_QUERY)

@app.get("/cardcategories")
def get_cardcategories(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["card_category_query"]
    return query_oracle(SQL_QUERY)


@app.post("/updateExpense")
def update_expense(
    request: Request,
    expense: Expense,
    key: str = Query(None)
):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    ORACLE_INSERT_EXPENSE_URL = os.environ["ORACLE_INSERT_EXPENSE_URL"]
    payload = {
        "p_id": expense.ID,
        "p_cardid": expense.CARDID,
        "p_trxdate": expense.TRANSACTIONDATE,
        "p_desc": expense.DESCRIPTION,
        "p_amount": expense.AMOUNT,
        "p_poststatus": expense.POSTSTATUS,
        "p_rewardsvalue": expense.REWARDSVALUE,
        "p_venuefound": expense.VENUEFOUND,
        "p_budgetlabel": expense.BUDGETLABEL,
        "p_cardcategory": expense.CARDCATEGORY,
        "p_tosync": expense.TOSYNC,
        "p_updateddt": expense.UPDATEDDT,
        "p_deleteyn": expense.DELETEYN
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    try:
        response = requests.post(f"{ORACLE_INSERT_EXPENSE_URL}", headers=headers, json=payload)
        if response.status_code == 200:
            return {"status": "success"}
        else:
            raise HTTPException(status_code=500, detail=response.text)
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail="ORDS communication failed")
        
@app.get("/ping")
def ping():
    return {"message": "ping success"}
