import os
import base64
import requests
from fastapi import FastAPI, Request, HTTPException, Query
from pydantic import BaseModel

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
    TOSYNC: str           # e.g. 'Y' or 'N'
    UPDATEDDT: str        # 'YYYY-MM-DD HH24:MI:SS' expected

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
def update_expense(expense: Expense):
    sql_query = f"""
    DECLARE
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count FROM credit_expenses WHERE ID = {expense.ID};
        
        IF v_count = 0 THEN
            INSERT INTO credit_expenses (
                ID, CARDID, TRANSACTIONDATE, DESCRIPTION, AMOUNT,
                POSTSTATUS, REWARDSVALUE, VENUEFOUND,
                BUDGETLABEL, CARDCATEGORY, TOSYNC, UPDATEDDT
            ) VALUES (
                {expense.ID}, {expense.CARDID}, TO_DATE('{expense.TRANSACTIONDATE}', 'YYYY-MM-DD'),
                '{expense.DESCRIPTION.replace("'", "''")}', {expense.AMOUNT}, '{expense.POSTSTATUS}',
                {expense.REWARDSVALUE}, '{expense.VENUEFOUND}',
                {expense.BUDGETLABEL if expense.BUDGETLABEL is not None else 'NULL'},
                {expense.CARDCATEGORY if expense.CARDCATEGORY is not None else 'NULL'},
                '{expense.TOSYNC}', TO_TIMESTAMP('{expense.UPDATEDDT}', 'YYYY-MM-DD HH24:MI:SS')
            );
        ELSE
            UPDATE credit_expenses SET
                CARDID = {expense.CARDID},
                TRANSACTIONDATE = TO_DATE('{expense.TRANSACTIONDATE}', 'YYYY-MM-DD'),
                DESCRIPTION = '{expense.DESCRIPTION.replace("'", "''")}',
                AMOUNT = {expense.AMOUNT},
                POSTSTATUS = '{expense.POSTSTATUS}',
                REWARDSVALUE = {expense.REWARDSVALUE},
                VENUEFOUND = '{expense.VENUEFOUND}',
                BUDGETLABEL = {expense.BUDGETLABEL if expense.BUDGETLABEL is not None else 'NULL'},
                CARDCATEGORY = {expense.CARDCATEGORY if expense.CARDCATEGORY is not None else 'NULL'},
                TOSYNC = '{expense.TOSYNC}',
                UPDATEDDT = TO_TIMESTAMP('{expense.UPDATEDDT}', 'YYYY-MM-DD HH24:MI:SS')
            WHERE ID = {expense.ID};
        END IF;
    END;
    """

    headers = {
        "Content-Type": "application/sql",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    response = requests.post(ORACLE_URL, headers=headers, data=sql_query)

    if response.status_code == 200:
        return {"status": "success"}
    else:
        raise HTTPException(status_code=500, detail=response.text)

@app.get("/ping")
def ping():
    return {"message": "ping success"}
