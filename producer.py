from kafka import KafkaProducer
from faker import Faker
import json
import random
import time

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_customer():
    return {
        "customer_id": random.randint(1, 10),  # Cố tình dùng id trùng để tạo SCD
        "name": fake.name(),
        "city": fake.city(),
        "gender": random.choice(["Nam", "Nữ", "Khác"]),
        "event_time": fake.date_time_this_year().isoformat()
    }

if __name__ == "__main__":
    while True:
        customer = generate_customer()
        print("Sending:", customer)
        producer.send('scd', value=customer)
        time.sleep(1)
 