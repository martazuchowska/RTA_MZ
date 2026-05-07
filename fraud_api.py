from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np

app = FastAPI(title="Fraud Detection API")

# Model ładowany przy starcie serwera
model = pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    is_electronics: int
    tx_per_minute: int

@app.post("/score")
def score(tx: Transaction):
    # Cechy muszą być w tej samej kolejności co podczas treningu
    X = np.array([[tx.amount, tx.is_electronics, tx.tx_per_minute]])
    
    # predict_proba zwraca prawdopodobieństwo dla obu klas
    proba = model.predict_proba(X)[0, 1] 
    
    return {
        "is_fraud": bool(proba >= 0.5),
        "fraud_probability": round(float(proba), 4)
    }

# do sprawdzania statusu API
@app.get("/health")
def health():
    return {"status": "ok"}
