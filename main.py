import os
import base64
import requests
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Request, HTTPException, Query, Body
from pydantic import BaseModel
from datetime import time, datetime, timedelta
import yfinance as yf
import pytz
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
    CARDID: int
    REWARDSLIMIT: int
    REWARDLIMITUSED: int
    BASEREWARDUNLIMITED: str
    TOSYNC: str 
    UPDATEDDT: str 
    DELETEYN: str

class QueryRewardLimit(BaseModel):
    P1_CARDID: int
    P1_EXPENSEID: int

class QueryRewardByCard(BaseModel):
    P1_CARDID: int

class InvestVehicle(BaseModel):
    ID: int
    NAME: str
    UPDATEDDT: str 
    DELETEYN: str
    TOSYNC: str 

class InvestmentUnit(BaseModel):
    ID: int
    VEH_ID: int
    NAME: str
    TICKER: str
    HOLDINGAMT: float  
    AVGBOUGHTPRICE: float  
    DELETEYN: str   
    UPDATEDDT: str 
    TOSYNC: str

class PricesRequest(BaseModel):
    symbols: List[str]
    key: Optional[str] = None 
    
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
        
from fastapi import Query
################# Get rows ##############################
@app.get("/expenses")
def get_expenses(request: Request, key: str = Query(None), categoryid: str = Query(None), venueFound: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    base_query = os.environ["transactions_query"]
    filters = []

    if categoryid:
        filters.append("cardcategory is not null")
    if venueFound:
        filters.append(f"venuefound = '{venueFound}'")
        
    if filters:
        sql_query = f"{base_query} WHERE " + " AND ".join(filters)
    else:
        sql_query = base_query

    return query_oracle(sql_query)

@app.get("/cards")
def get_cards(
    request: Request,
    key: str = Query(None),
    cardNo: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden") 
    base_query = os.environ["cards_query"]
    if cardNo:
        sql_query = f"{base_query} WHERE CARDNO = '{cardNo}'"
    else:
        sql_query = base_query
    return query_oracle(sql_query)

@app.get("/venueMapping")
def get_venue(
    request: Request,
    key: str = Query(None),
    cardNo: str = Query(None),
    venue: str = Query(None)
):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    base_query = os.environ["venue_mapping_query"]

    filters = []

    if cardNo:
        filters.append(f"CardId = '{cardNo}'")
    if venue:
        filters.append(f"VENUE = '{venue}'")

    if filters:
        sql_query = f"{base_query} WHERE " + " AND ".join(filters)
    else:
        sql_query = base_query

    return query_oracle(sql_query)


@app.get("/categories")
def get_categories(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["category_query"]
    return query_oracle(SQL_QUERY)

@app.get("/cardcategories")
def get_cardcategories(request: Request, key: str = Query(None), categoryid = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    base_query = os.environ["card_category_query"]
    if categoryid:
        sql_query = f"{base_query} WHERE ID = '{categoryid}'"
    else:
        sql_query = base_query
    return query_oracle(sql_query)

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

@app.get("/rewardLimitData")
def get_expenses(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["reward_limit_query"]
    return query_oracle(SQL_QUERY)

@app.get("/rewardCategoryLimitData")
def get_expenses(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["reward_category_limit_usage_query"]
    return query_oracle(SQL_QUERY)

@app.get("/investmentVehData")
def investment_veh_data(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["investment_veh_query"]
    return query_oracle(SQL_QUERY)

@app.get("/investmentUnitData")
def investment_unit_data(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    SQL_QUERY = os.environ["investment_unit_query"]
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

@app.get("/get-invest-veh-id")
def get_invest_veh_id(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    ORDS_NEXT_ID_URL = os.environ["get-invest-veh-id-url"]
    try:
        response = requests.get(ORDS_NEXT_ID_URL)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        data = response.json()

        # return just the ID field
        return {"invest_veh_id": data.get("id")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get-invest-unit-id")
def get_invest_unit_id(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    ORDS_NEXT_ID_URL = os.environ["get-invest-unit-id-url"]
    try:
        response = requests.get(ORDS_NEXT_ID_URL)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        data = response.json()

        # return just the ID field
        return {"invest_unit_id": data.get("id")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get-total-rewards-month")
def get_total_rewards_month(request: Request, key: str = Query(None)):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    GET_REWARD_TOTAL_MONTH_URL = os.environ["GET_REWARD_TOTAL_MONTH_URL"]
    try:
        response = requests.get(GET_REWARD_TOTAL_MONTH_URL)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        data = response.json()

        # return just the ID field
        return {"Reward_total": data.get("reward_total")}

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

@app.post("/updateInvestmentVehicle")
def updateInvestmentVehicle(
    request: Request,
    investVehicle: InvestVehicle,
    key: str = Query(None)
):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    ORACLE_INSERT_INVEST_VEH_URL = os.environ["ORACLE_INSERT_INVEST_VEH_URL"]
    payload = {
        "p_id": investVehicle.ID,
        "p_name": investVehicle.NAME,
        "p_updateddate": investVehicle.UPDATEDDT,
        "p_deleteyn": investVehicle.DELETEYN,
        "p_tosync": investVehicle.TOSYNC
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    try:
        response = requests.post(ORACLE_INSERT_INVEST_VEH_URL, headers=headers, json=payload)
        if response.status_code == 200:
            return {"status": "success"}
        else:
            logger.error(f"ORDS error: {response.status_code} - {response.text}")
            raise HTTPException(status_code=500, detail=response.text)
    except requests.exceptions.RequestException as e:
        logger.error(f"ORDS request failed: {e}")
        raise HTTPException(status_code=500, detail=f"ORDS communication failed: {e}")

@app.post("/updateInvestmentUnit")
def update_investment_unit(
    request: Request,
    investmentUnit: InvestmentUnit,
    key: str = Query(None)
):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    ORACLE_INSERT_INVESTMENT_UNIT_URL = os.environ["ORACLE_INSERT_INVESTMENT_UNIT_URL"]
    payload = {
        "p_id": investmentUnit.ID,
        "p_veh_id": investmentUnit.VEH_ID,
        "p_name": investmentUnit.NAME,
        "p_ticker": investmentUnit.TICKER,
        "p_holdingamt": investmentUnit.HOLDINGAMT,
        "p_avgboughtprice": investmentUnit.AVGBOUGHTPRICE,
        "p_deleteyn": investmentUnit.DELETEYN,
        "p_updateddt": investmentUnit.UPDATEDDT,
        "p_tosync": investmentUnit.TOSYNC
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    try:
        response = requests.post(ORACLE_INSERT_INVESTMENT_UNIT_URL, headers=headers, json=payload)
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
        "p_card": cardRewardLimit.CARDID,
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


@app.post("/queryRewardLimit")
def queryRewardLimit(
    request: Request,
    queryRewardLimit: QueryRewardLimit,
    key: str = Query(None)
):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    ORACLE_QUERY_REWARD_LIMIT_URL = os.environ["ORACLE_QUERY_REWARD_LIMIT_URL"]
    payload = {
        "P1_CARDID": queryRewardLimit.P1_CARDID,
        "P1_EXPENSEID": queryRewardLimit.P1_EXPENSEID
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    try:
        response = requests.post(
            ORACLE_QUERY_REWARD_LIMIT_URL,
            headers=headers,
            json=payload
        )
        if response.status_code == 200:
            if not response.text.strip():
                raise HTTPException(status_code=500, detail="ORDS returned 200 with empty body")
            try:
                data = response.json()
            except ValueError:
                raise HTTPException(status_code=500, detail="Invalid JSON returned from ORDS")

            return {"total_amount": data.get("total_amount")}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)

    except requests.exceptions.RequestException as e:
        print(f"ORDS request failed: {e}")
        raise HTTPException(status_code=500, detail=f"ORDS communication failed: {e}")

@app.post("/queryRewardByCard")
def queryRewardByCard(
    request: Request,
    queryRewardByCard: QueryRewardByCard,
    key: str = Query(None)
):
    client_key = request.headers.get("X-API-Key") or key
    if client_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    ORACLE_QUERY_REWARD_BY_CARD_URL = os.environ["ORACLE_QUERY_REWARD_BY_CARD_URL"]
    payload = {
        "P1_CARDID": queryRewardByCard.P1_CARDID
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    }

    try:
        response = requests.post(
            ORACLE_QUERY_REWARD_BY_CARD_URL,
            headers=headers,
            json=payload
        )
        if response.status_code == 200:
            if not response.text.strip():
                raise HTTPException(status_code=500, detail="ORDS returned 200 with empty body")
            try:
                data = response.json()
            except ValueError:
                raise HTTPException(status_code=500, detail="Invalid JSON returned from ORDS")

            return {"total_miles": data.get("total_miles")}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)

    except requests.exceptions.RequestException as e:
        print(f"ORDS request failed: {e}")
        raise HTTPException(status_code=500, detail=f"ORDS communication failed: {e}")

# @app.get("/tickerPrice")
# def get_price(symbol: str, request: Request, key: str = Query(None)):
#     client_key = request.headers.get("X-API-Key") or key
#     if client_key != API_KEY:
#         raise HTTPException(status_code=403, detail="Forbidden")
        
#     t = yf.Ticker(symbol)
#     info = t.fast_info or {}
#     price = info.get("last_price")
#     currency = info.get("currency") or t.info.get("currency")

#     if price is None:
#         hist = t.history(period="1d")
#         if not hist.empty:
#             price = float(hist["Close"].iloc[-1])

#     if price is None:
#         raise HTTPException(status_code=404, detail=f"No price for {symbol}")

#     return {"symbol": symbol, "price": price, "currency": currency or "USD"}

def session_hours_for_exchange(exchange: str | None):
    """
    Return (tz_name, regular_open, regular_close) for common exchanges.
    Times are local to tz_name.
    """
    ex = (exchange or "").upper()
    # US (NYSE/Nasdaq)
    if "NASDAQ" in ex or "NMS" in ex or "NYSE" in ex or ex in ("NYQ", "NCM", "NMS", "NMQ"):
        return ("America/New_York", time(9, 30), time(16, 0))
    # London Stock Exchange
    if "LSE" in ex or ex == "L" or "LONDON" in ex:
        return ("Europe/London", time(8, 0), time(16, 30))
    # Default to US
    return ("America/New_York", time(9, 30), time(16, 0))

def label_session(ts_local: datetime, open_t: time, close_t: time):
    if ts_local.time() < open_t:
        return "pre"
    if open_t <= ts_local.time() <= close_t:
        return "regular"
    return "post"

# @app.get("/tickerPrice")
# def get_price(symbol: str, request: Request, key: str = Query(None)):
#     client_key = request.headers.get("X-API-Key") or key
#     if client_key != API_KEY:
#         raise HTTPException(status_code=403, detail="Forbidden")

#     t = yf.Ticker(symbol)
#     info = t.fast_info or {}
#     meta = t.info or {}

#     # Figure out exchange + timezone + regular hours
#     tz_name_info = meta.get("exchangeTimezoneName") or info.get("timezone") or "America/New_York"
#     exchange = meta.get("exchange") or ""
#     tz_name_map, reg_open, reg_close = session_hours_for_exchange(exchange)
#     # Prefer Yahoo’s tz if present; fall back to our mapping tz
#     tz_name = tz_name_info or tz_name_map
#     try:
#         tz = pytz.timezone(tz_name)
#     except Exception:
#         tz = pytz.timezone(tz_name_map)

#     now_local = datetime.now(tz)

#     # Pull 1m history with pre/post included — 2 days to cover early premarket/post that cross midnight
#     hist = t.history(period="2d", interval="1m", prepost=True)

#     price = None
#     session = None

#     if not hist.empty:
#         # Use the last bar up to "now" (local to exchange)
#         # Index is tz-aware; convert to exchange tz and filter
#         hist_local = hist.tz_convert(tz)

#         # Keep only rows <= now
#         recent = hist_local[hist_local.index <= now_local]
#         if not recent.empty:
#             last_ts = recent.index[-1]
#             close_px = float(recent["Close"].iloc[-1])
#             price = close_px
#             session = label_session(last_ts, reg_open, reg_close)

#     # If no intraday candle found (weekend/holiday or symbol restrictions), fall back
#     if price is None:
#         price = info.get("last_price")

#     if price is None:
#         # Final fallback: last daily close
#         daily = t.history(period="5d")  # a few days to avoid holidays
#         if not daily.empty:
#             price = float(daily["Close"].iloc[-1])

#     if price is None:
#         raise HTTPException(status_code=404, detail=f"No price for {symbol}")

#     currency = info.get("currency") or meta.get("currency") or "USD"

#     return {
#         "symbol": symbol,
#         "price": price,
#         "currency": currency,
#         "session": session or "unknown",  # pre | regular | post | unknown
#         "exchange": exchange or "unknown",
#         "exchangeTz": str(tz),
#         "asOf": datetime.utcnow().isoformat() + "Z",
#     }  

def fetch_one_symbol(symbol: str) -> Dict[str, Any]:
    """
    EXACT logic from your original /tickerPrice endpoint, factored into a helper.
    Raises HTTPException(404) if no price is found, same as before.
    """
    t = yf.Ticker(symbol)
    info = t.fast_info or {}
    meta = t.info or {}

    # Figure out exchange + timezone + regular hours
    tz_name_info = meta.get("exchangeTimezoneName") or info.get("timezone") or "America/New_York"
    exchange = meta.get("exchange") or ""
    tz_name_map, reg_open, reg_close = session_hours_for_exchange(exchange)
    # Prefer Yahoo’s tz if present; fall back to our mapping tz
    tz_name = tz_name_info or tz_name_map
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = pytz.timezone(tz_name_map)

    now_local = datetime.now(tz)

    # Pull 1m history with pre/post included — 2 days to cover early premarket/post that cross midnight
    hist = t.history(period="2d", interval="1m", prepost=True)

    price = None
    session = None

    if not hist.empty:
        # Index is tz-aware; convert to exchange tz and filter rows <= now
        hist_local = hist.tz_convert(tz)
        recent = hist_local[hist_local.index <= now_local]
        if not recent.empty:
            last_ts = recent.index[-1]
            close_px = float(recent["Close"].iloc[-1])
            price = close_px
            session = label_session(last_ts, reg_open, reg_close)

    # Fallbacks
    if price is None:
        price = info.get("last_price")

    if price is None:
        daily = t.history(period="5d")
        if not daily.empty:
            price = float(daily["Close"].iloc[-1])

    if price is None:
        raise HTTPException(status_code=404, detail=f"No price for {symbol}")

    currency = info.get("currency") or meta.get("currency") or "USD"

    return {
        "symbol": symbol,
        "price": price,
        "currency": currency,
        "session": session or "unknown",  # pre | regular | post | unknown
        "exchange": exchange or "unknown",
        "exchangeTz": str(tz),
        "asOf": datetime.utcnow().isoformat() + "Z",
    }

def auth_ok(request: Request, key_from_query: Optional[str], key_from_body: Optional[str] = None) -> bool:
    client_key = request.headers.get("X-API-Key") or key_from_body or key_from_query
    return client_key == API_KEY

@app.post("/tickerPrices")
def post_prices(
    request: Request,
    body: PricesRequest = Body(...),
    key: str = Query(None)
):
    """
    Batch POST with JSON body: { "symbols": ["TSLA","AAPL","PLTR"] }
    Optional API key can also be in body.key if you prefer.
    """
    if not auth_ok(request, key_from_query=key, key_from_body=body.key):
        raise HTTPException(status_code=403, detail="Forbidden")

    merged = [s.upper() for s in (body.symbols or []) if s and s.strip()]
    if not merged:
        raise HTTPException(status_code=400, detail="Body.symbols must contain at least one ticker")

    results = []
    for sym in merged:
        try:
            results.append(fetch_one_symbol(sym))
        except HTTPException as e:
            results.append({"symbol": sym, "error": {"status": e.status_code, "detail": e.detail}})
        except Exception as e:
            results.append({"symbol": sym, "error": {"status": 500, "detail": str(e)}})

    return {"count": len(results), "results": results}
        
@app.get("/ping")
def ping():
    return {"message": "ping success"}
