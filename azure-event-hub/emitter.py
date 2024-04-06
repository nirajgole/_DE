import json
import random
import time
from datetime import datetime, timedelta, timezone
from azure.eventhub import EventHubProducerClient, EventData

import os
from dotenv import load_dotenv
load_dotenv()

# Replace environment variable with your Azure Event Hub connection string
connection_str = os.getenv('CONNECTION_STR')

# Define data generation functions (customize as needed)
def generate_sensor_data():
    temperature = random.uniform(10.0, 30.0)  # Random temperature between 10 and 30 degrees Celsius
    humidity = random.randint(30, 80)  # Random humidity between 30 and 80 percent
    pressure = random.uniform(980.0, 1020.0)  # Random pressure between 980 and 1020 hPa
    # timestamp = datetime.utcnow().astimezone(datetime.astimezone).strftime("%Y-%m-%dT%H:%M:%SZ")  # UTC timestamp
    timestamp = datetime.utcnow().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


    return {
        "temperature": temperature,
        "humidity": humidity,
        "pressure": pressure,
        "timestamp": timestamp
    }

def generate_user_activity(user_id):
    activity_types = ["login", "search", "view_item", "add_to_cart", "purchase"]
    activity_type = random.choice(activity_types)
    timestamp = datetime.utcnow() - timedelta(seconds=random.randint(0, 60))  # Random timestamp within the last minute

    return {
        "user_id": user_id,
        "activity_type": activity_type,
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

# Main function to generate and send events
def send_fake_data(client, data_type, num_events, event_interval):
    for _ in range(num_events):
        if data_type == "sensor_data":
            event_data = json.dumps(generate_sensor_data())
        elif data_type == "user_activity":
            user_id = random.randint(1, 100)  # Sample user IDs
            event_data = json.dumps(generate_user_activity(user_id))
        else:
            raise ValueError("Invalid data type")

        event_data_bytes = event_data.encode("utf-8")
        # Use the create_batch method to create a batch of events
        event_batch = client.create_batch()

        # Add the event to the batch
        event_batch.add(EventData(event_data_bytes))

        # Send the batch of events to the Event Hub
        client.send_batch(event_batch)
        print(f"Sent event: {event_data}")
        time.sleep(event_interval)  # Control the rate of event generation

if __name__ == "__main__":
    client = EventHubProducerClient.from_connection_string(connection_str)
    data_type = input("Choose data type (sensor_data, user_activity): ")
    num_events = int(input("Enter number of events to generate: "))
    event_interval = float(input("Enter interval between events (seconds): "))

    send_fake_data(client, data_type, num_events, event_interval)

    client.close()
