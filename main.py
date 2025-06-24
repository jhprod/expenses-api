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
    RECURYN: str

class RecurExpense(BaseModel):
    ID: int
    DESCRIPTION: str
    AMOUNT: float
    START_DATE: str  # 'YYYY-MM-DD'
    RECUR_DAY: int
    FREQUENCY: str
    LAST_GEN_DT: str # 'YYYY-MM-DD'
    CARD_ID: int = None
    CARD_CATEGORY_ID: int = None
    BUDGET_CATEGORY_ID: int = None
    TOSYNC: str    
    DELETEYN: str   
    EXPENSE_ID: int

class BudgetCategory(BaseModel):
    ID: int
    EXPENSECATEGORY: str
    TOSYNC: str    
    DELETEYN: str
    UPDATEDDT: str 

class CardRewardLimit(BaseModel):
    ID: int
    CARD_ID: int
    REWARDSLIMIT: int
    REWARDLIMITUSED: int
    BASEREWARDUNLIMITED: str
    TOSYNC: str 
    UPDATEDDT: str 
    DELETEYN: str
   

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

################# Get rows ##############################
@app.get("/expenses")
def get_expenses(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["transactions_query"]
    return query_oracle(SQL_QUERY)

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

@app.get("/recurExpenses")
def get_recurExpenses(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["recur_expense_query"]
    return query_oracle(SQL_QUERY)

@app.get("/rewardCategoryLimits")
def get_rewardCategoryLimits(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["reward_category_limit_query"]
    return query_oracle(SQL_QUERY)

@app.get("/cardCategoryLimits")
def get_cardCategoryLimit(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["card_category_limit_query"]
    return query_oracle(SQL_QUERY)

@app.get("/cardCycles")
def get_cardCycles(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["card_cycles_query"]
    return query_oracle(SQL_QUERY)

################# Get IDs ##############################

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

@app.get("/get-budget-id")
def get_budget_id(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    ORDS_NEXT_ID_URL = os.environ["get_budget_id_url"]
    try:
        response = requests.get(ORDS_NEXT_ID_URL)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        data = response.json()

        # return just the ID field
        return {"budget_id": data.get("id")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get-recur-expense-id")
def get_recur_expense_id(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    ORDS_NEXT_ID_URL = os.environ["get_recur_expense_id_url"]
    try:
        response = requests.get(ORDS_NEXT_ID_URL)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        data = response.json()

        # return just the ID field
        return {"recur_expense_id": data.get("id")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


################# CRUD ##############################

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
        "p_deleteyn": expense.DELETEYN,
        "p_recuryn": expense.RECURYN
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
            print(f"ORDS response error: {response.status_code} - {response.text}")
            raise HTTPException(status_code=500, detail=response.text)
    except requests.exceptions.RequestException as e:
        print(f"ORDS request failed: {e}")
        raise HTTPException(status_code=500, detail=f"ORDS communication failed: {e}")

@app.post("/updateBudgetCategory")
def update_budget_category(
    request: Request,
    category: BudgetCategory,
    key: str = Query(None)
):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    ORACLE_INSERT_BUDGET_URL = os.environ["ORACLE_INSERT_BUDGET_URL"]

    payload = {
        "p_id": category.ID,
        "p_expensecategory": category.EXPENSECATEGORY,
        "p_tosync": category.TOSYNC,
        "p_deleteyn": category.DELETEYN,
        "p_updateddt": category.UPDATEDDT
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    try:
        response = requests.post(ORACLE_INSERT_BUDGET_URL, headers=headers, json=payload)
        if response.status_code == 200:
            return {"status": "success"}
        else:
            logger.error(f"ORDS error: {response.status_code} - {response.text}")
            raise HTTPException(status_code=500, detail=response.text)
    except requests.exceptions.RequestException as e:
        logger.error(f"ORDS request failed: {e}")
        raise HTTPException(status_code=500, detail=f"ORDS communication failed: {e}")

@app.post("/updateRecurExpenses")
def update_recur_expense(
    request: Request,
    recurExpenses: RecurExpense,
    key: str = Query(None)
):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    ORACLE_INSERT_RECUR_EXPENSE_URL = os.environ["ORACLE_INSERT_RECUR_EXPENSE_URL"]
    payload = {
        "p_id": recurExpenses.ID,
        "p_title": recurExpenses.DESCRIPTION,
        "p_amount": recurExpenses.AMOUNT,
        "p_startdate": recurExpenses.START_DATE,
        "p_recursonday": recurExpenses.RECUR_DAY,
        "p_frequency": recurExpenses.FREQUENCY,
        "p_lastgen": recurExpenses.LAST_GEN_DT,
        "p_deleteyn": recurExpenses.DELETEYN,
        "p_category": recurExpenses.BUDGET_CATEGORY_ID,
        "p_card": recurExpenses.CARD_ID,
        "p_cardcategory": recurExpenses.CARD_CATEGORY_ID,
        "p_tosync": recurExpenses.TOSYNC,
        "p_expenseid": recurExpenses.EXPENSE_ID
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    try:
        response = requests.post(ORACLE_INSERT_RECUR_EXPENSE_URL, headers=headers, json=payload)
        if response.status_code == 200:
            return {"status": "success"}
        else:
            logger.error(f"ORDS error: {response.status_code} - {response.text}")
            raise HTTPException(status_code=500, detail=response.text)
    except requests.exceptions.RequestException as e:
        logger.error(f"ORDS request failed: {e}")
        raise HTTPException(status_code=500, detail=f"ORDS communication failed: {e}")

@app.post("/updateCardRewardLimit")
def update_card_reward_limit(
    request: Request,
    cardRewardLimit: CardRewardLimit,
    key: str = Query(None)
):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    ORACLE_INSERT_REWARD_LIMIT_URL = os.environ["ORACLE_INSERT_REWARD_LIMIT_URL"]

    payload = {
        "p_id": cardRewardLimit.ID,
        "p_card": cardRewardLimit.CARD_ID,
        "p_rewardslimit": cardRewardLimit.REWARDSLIMIT,
        "p_rewardlimitused": cardRewardLimit.REWARDLIMITUSED,
        "p_baserewardunlimited": cardRewardLimit.BASEREWARDUNLIMITED,
        "p_tosync": cardRewardLimit.TOSYNC,
        "p_deleteyn": cardRewardLimit.DELETEYN,
        "p_updateddt": cardRewardLimit.UPDATEDDT
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    try:
        response = requests.post(ORACLE_INSERT_REWARD_LIMIT_URL, headers=headers, json=payload)
        if response.status_code == 200:
            return {"status": "success"}
        else:
            logger.error(f"ORDS error: {response.status_code} - {response.text}")
            raise HTTPException(status_code=500, detail=response.text)
    except requests.exceptions.RequestException as e:
        logger.error(f"ORDS request failed: {e}")
        raise HTTPException(status_code=500, detail=f"ORDS communication failed: {e}")

        
@app.get("/ping")
def ping():
    return {"message": "ping success"}
