from fastapi import FastAPI

app = FastAPI()

@app.get("/transactions")
def get_transactions():
    return [{"id": 1, "amount": 100.0}, {"id": 2, "amount": 50.0}]