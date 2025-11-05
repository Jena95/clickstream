import time
import random
import json
from uuid import uuid4
from datetime import datetime
from google.cloud import pubsub_v1  # Install via pip: google-cloud-pubsub

# ----- GCP Pub/Sub Configuration -----
PROJECT_ID = "your-gcp-project-id"
TOPIC_ID = "clickstream-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# ----- Configurable Parameters -----
PAGE_NAMES = ["home", "search", "product", "category", "checkout", "cart"]
EVENT_TYPES = ["page_view", "click", "search", "add_to_cart", "checkout"]
DEVICES = ["desktop", "mobile", "tablet"]
REFERRERS = ["google.com", "bing.com", "facebook.com", "twitter.com", "direct"]

# Probability distribution for event types
EVENT_PROB = [0.6, 0.2, 0.1, 0.05, 0.05]  # Sum = 1.0

def generate_event():
    event = {
        "event_id": str(uuid4()),
        "user_id": f"user_{random.randint(1, 1000)}",
        "session_id": str(uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "page": random.choice(PAGE_NAMES),
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_PROB)[0],
        "device": random.choice(DEVICES),
        "referrer": random.choice(REFERRERS),
        "price": round(random.uniform(5, 500), 2) if random.random() < 0.2 else None
    }
    return event

def stream_to_pubsub(event):
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    # Optional: future.result() blocks until message is confirmed

def main():
    print("Starting clickstream generator...")
    while True:
        event = generate_event()
        stream_to_pubsub(event)
        print(event)  # Optional: for debugging/logging
        # Random delay to simulate real user behavior (50-500 ms)
        time.sleep(random.uniform(0.05, 0.5))

if __name__ == "__main__":
    main()
