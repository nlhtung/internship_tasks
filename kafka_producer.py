from faker import Faker
from kafka import KafkaProducer
import json
import time
import random

fake = Faker()

producer = KafkaProducer(
   bootstrap_servers='localhost:9092',
   value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

actions = ['click', 'view', 'purchase', 'signup', 'login', 'logout']
topic = 'clickstream'

def generate_order():
    action1 = random.choice(actions)
    action2 = random.choice(actions)
    action3 = random.choice(actions)
    return {
        "user_id": fake.random_int(min=1, max=1000),
        "session_id": fake.uuid4(),
        # tỉ lệ None là 25%
        "action": random.choice([action1, action2, action3, None]),
        #tỉ lệ None là 33.33%
        "url": random.choice([fake.uri_path(), fake.uri_path(), None]),
        "timestamp": fake.iso8601()
    }

start_time = time.time()

while time.time()-start_time<=600:
   event = generate_order()
   producer.send(topic, value=event)
   print(f"Sent: {event}")
   if (time.time()-start_time>10 and time.time()-start_time<30):
    time.sleep(0.5)
   else:
    time.sleep(1)