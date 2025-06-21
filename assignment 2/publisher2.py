import paho.mqtt.client as mqtt
import time
import json

client = mqtt.Client()
client.connect("localhost", 1883, 60)
client.loop_start()

try:
    while True:
        data = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "room_temperature": 31.5  # exceeds 30.0
        }
        client.publish("devices/room", json.dumps(data))
        print(f"Published: {data}")
        time.sleep(10)  # Faster for testing
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
