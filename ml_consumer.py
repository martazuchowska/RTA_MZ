from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json
import requests

consumer = KafkaConsumer(
    'transactions', 
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', 
    group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "http://localhost:8001/score"
print("Konsument ML uruchomiony...\n")

for message in consumer:
    tx = message.value
    
    # 1. Wyciągnięcie cech z transakcji

    is_electronics = 1 if tx.get('category') == 'elektronika' else 0
    
    features = {
        "amount": tx['amount'],
        "is_electronics": is_electronics,
        "tx_per_minute": 5   #stałej 5 jako uproszczenia
    }
    
    try:
        # 2. Wysłanie zapytania do API timeoutem 2 sekundy
        response = requests.post(API_URL, json=features, timeout=2)
        result = response.json()
    except requests.RequestException as e:
        print(f"API niedostępne: {e}")
        continue
        
    # 3. Reakcja na wynik
    if result.get('is_fraud'):
        alert = {
            **tx,  # Kopiuje wszystkie oryginalne pola z transakcji
            'fraud_probability': result['fraud_probability'],
            'alert_source': 'ml_model'
        }
        alert_producer.send('alerts', value=alert)
        print(f"FRAUD [{result['fraud_probability']:.0%}] {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}")
        alert_producer.flush()
