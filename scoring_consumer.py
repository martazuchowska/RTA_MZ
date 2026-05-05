from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json

consumer = KafkaConsumer(
    'transactions', 
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', 
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Funkcja scoringowa 
def score_transaction(tx):
    score = 0
    rules = []
    
    if tx['amount'] > 3000:
        score += 3; rules.append('R1')
    if tx.get('category') == 'elektronika' and tx['amount'] > 1500:
        score += 2; rules.append('R2')
        
    hour = tx.get('hour', datetime.fromisoformat(tx['timestamp']).hour)
    if hour < 6:
        score += 2; rules.append('R3')
        
    return score, rules

print("Konsument scoringowy uruchomiony...\n")

# Rozwiazanie
for message in consumer:
    tx = message.value
    # 1. Scoruj
    score, rules = score_transaction(tx)
    
    # 2. Jeśli >= 3: wyślij do 'alerts' i wypisz ALERT
    if score >= 3:
        alert = {**tx, 'score': score, 'rules': rules, 'alert': True}
        alert_producer.send('alerts', value=alert)
        print(f"ALERT (score {score}) {tx['tx_id']} | {tx['amount']:.2f} PLN | reguły: {rules}")
        alert_producer.flush()
