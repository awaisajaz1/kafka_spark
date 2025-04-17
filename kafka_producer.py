from kafka import KafkaProducer
import json
import time
import uuid
from datetime import datetime
import random

producer = KafkaProducer(
    bootstrap_servers=['192.168.1.7:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# List of items and prices
items = [
    {"item_name": "Cake", "item_price": 450, "item_id": "item-1"},
    {"item_name": "Ice Cream", "item_price": 299, "item_id": "item-2"},
    {"item_name": "Soda", "item_price": 120, "item_id": "item-3"},
    {"item_name": "Chips", "item_price": 180, "item_id": "item-4"},
    {"item_name": "Cookies", "item_price": 240, "item_id": "item-5"},
]


while True:
    item1 = random.choice(items)
    item2 = random.choice(items)

    data = {
        "id": f"cust-{uuid.uuid4().hex[:6]}",
        "user_name": "John Doe",
        "user_email": "john@example.com",
         "items": [
      {
        "item_id": item1["item_id"],
        "price": item1["item_price"]
      },
      {
        "item_id": item2["item_id"],
        "price": item2["item_price"]
      }
    ],
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

    producer.send('src_json', value=data)
    print(f"Produced: {data}")
    
    time.sleep(15)