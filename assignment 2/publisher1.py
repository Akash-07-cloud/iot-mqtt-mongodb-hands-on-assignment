import paho.mqtt.client as mqtt
import time
import json

client = mqtt.Client()
client.connect("localhost", 1883, 60)
client.loop_start()

try:
    while True:
        # Simulate data that exceeds thresholds
        data = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "heart_rate": 105,  # exceeds 100
            "blood_pressure": "150/90"  # systolic exceeds 140
        }
        client.publish("devices/vitals", json.dumps(data))
        print(f"Published: {data}")
        time.sleep(10)  # For test purposes, publish faster
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
