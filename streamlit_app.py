import streamlit as st
from kafka import KafkaConsumer
import json
import threading
import time

# Function to create Kafka consumer
@st.cache(allow_output_mutation=True)
def get_kafka_consumer():
    return KafkaConsumer(
        'stock-prices',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

# Fetch Kafka consumer
consumer = get_kafka_consumer()

# A thread-safe list to store messages
messages = []

# Function to fetch messages from Kafka
def fetch_messages():
    global messages
    for message in consumer:
        messages.append(message.value)

# Start a thread to fetch messages from Kafka
thread = threading.Thread(target=fetch_messages)
thread.daemon = True
thread.start()

# Streamlit app title
st.title('Real-Time Stock Prices')

# Main Streamlit loop
while True:
    if messages:
        # Display the latest message
        latest_message = messages[-1]
        st.write(f"{latest_message['symbol']}: ${latest_message['price']} (Timestamp: {latest_message['timestamp']})")
    # Sleep briefly to avoid excessive CPU usage
    time.sleep(1)