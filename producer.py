import json
import random
import time
from kafka import KafkaProducer

# Sample stock symbols
stocks = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

# Initialize a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Function to produce stock prices
def produce_stock_prices():
    while True:
        for stock in stocks:
            price = round(random.uniform(100, 500), 2)
            timestamp = int(round(time.time() * 1000))
            message = {'symbol': stock, 'price': price, 'timestamp': timestamp}
            producer.send('stock-prices', value=message)
            print(f"Sent: {message}")
        time.sleep(1)

produce_stock_prices()
