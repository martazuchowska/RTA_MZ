from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()      # domyślna wartość: 0
total_amount = defaultdict(float) # domyślna wartość: 0.0
msg_count = 0

print("Nasłuchuję i zliczam transakcje...\n")

for message in consumer:
    tx = message.value
    store = tx['store']
    
    # 1. Zwiększamy licznik transakcji dla danego sklepu
    store_counts[store] += 1
    
    # 2. Dodajemy kwotę transakcji do sumy danego sklepu[cite: 1]
    total_amount[store] += tx['amount']
    
    msg_count += 1
    
    # 3. Co 10 wiadomości: wyświetlamy tabelę podsumowującą[cite: 1]
    if msg_count % 10 == 0:
        print(f"\n{'='*52}")
        print(f"{'Sklep':<12} {'Liczba':>8} {'Suma':>12} {'Śr.':>10}")
        print(f"{'-'*52}")
        for s in sorted(store_counts):
            n = store_counts[s]
            tot = total_amount[s]
            print(f"{s:<12} {n:>8} {tot:>12.2f} {tot/n:>10.2f}")
        print(f"{'='*52}\nŁącznie: {msg_count} wiadomości\n")
