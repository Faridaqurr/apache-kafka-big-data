from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

gudang_list = ["G1", "G2", "G3"]

while True:
    data = {
        "gudang_id": random.choice(gudang_list),
        "kelembaban": random.randint(65, 80)
    }
    producer.send('sensor-kelembaban-gudang', value=data)
    print("Kirim:", data)
    time.sleep(1)
