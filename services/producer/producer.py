import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker('en_IN')  # Indian locale for realistic data

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Realistic merchant categories
MERCHANTS = [
    {'id': 'MER001', 'name': 'Amazon India', 'category': 'ecommerce'},
    {'id': 'MER002', 'name': 'Swiggy', 'category': 'food'},
    {'id': 'MER003', 'name': 'Zomato', 'category': 'food'},
    {'id': 'MER004', 'name': 'Flipkart', 'category': 'ecommerce'},
    {'id': 'MER005', 'name': 'BookMyShow', 'category': 'entertainment'},
    {'id': 'MER006', 'name': 'Unknown Vendor XYZ', 'category': 'unknown'},
    {'id': 'MER007', 'name': 'Petrol Pump', 'category': 'fuel'},
]

PAYMENT_METHODS = ['UPI', 'CARD', 'NETBANKING', 'WALLET']

def generate_transaction():
    """Generate a realistic payment transaction"""
    merchant = random.choice(MERCHANTS)
    
    # 90% normal, 10% suspicious
    is_suspicious = random.random() < 0.10
    
    if is_suspicious:
        amount = round(random.uniform(50000, 200000), 2)  # Very high amount
    else:
        amount = round(random.uniform(50, 5000), 2)       # Normal amount

    return {
        "txn_id": fake.uuid4(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"USR{random.randint(1000, 9999)}",
        "merchant_id": merchant['id'],
        "merchant_name": merchant['name'],
        "merchant_category": merchant['category'],
        "amount": amount,
        "currency": "INR",
        "payment_method": random.choice(PAYMENT_METHODS),
        "device_id": f"DEV{random.randint(100, 999)}",
        "location": fake.city(),
        "is_new_merchant": merchant['id'] == 'MER006',  # Unknown vendor = new merchant
    }

def main():
    print("Starting transaction producer...")
    print("Sending to Kafka topic: txn-events")
    print("Press Ctrl+C to stop\n")
    
    count = 0
    while True:
        txn = generate_transaction()
        producer.send('txn-events', value=txn)
        producer.flush()
        count += 1
        print(f"[{count}] txn_id={txn['txn_id'][:8]}... | "
              f"user={txn['user_id']} | "
              f"merchant={txn['merchant_name']} | "
              f"amount=₹{txn['amount']} | "
              f"method={txn['payment_method']}")
        time.sleep(0.5)  # 2 transactions/sec

if __name__ == "__main__":
    main()