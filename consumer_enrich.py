from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enrich-group', # INNY group_id!
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def get_risk_level(amount):
    #przypisanie poziomu ryzyka na podstawie kwoty
    if amount > 3000:
        return 'HIGH'
    if amount > 1000:
        return 'MEDIUM'
    return 'LOW'

print("Nasłuchuję i oceniam ryzyko...\n")

for message in consumer:
    tx = message.value
    # Dodanie nowego pola 'risk_level' do słownika transakcji
    tx['risk_level'] = get_risk_level(tx['amount'])
    
    # Wyświetlenie wyniku
    print(f"[{tx['risk_level']:6s}] {tx['tx_id']} | {tx['amount']:.2f} PLN")
