import paho.mqtt.client as mqtt
import json
import pymongo
import time

def on_connect(client, userdata, flags, rc):
    print(f"[Subscriber] Connected to broker with code {rc}")
    client.subscribe("devices/#")

def on_disconnect(client, userdata, rc):
    print(f"[Subscriber] Disconnected with code {rc}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode()
        data = {
            "topic": msg.topic,
            "payload": json.loads(payload_str),
            "timestamp": time.time()
        }
        dbt.insert_one(data)
        print(f"[Subscriber] Inserted message → {data}")
    except Exception as e:
        print(f"[Subscriber] Error inserting: {e}")

with open("config.json") as f:
    config = json.load(f)

dbclient = pymongo.MongoClient(config["db_host"], config["db_port"])
db = dbclient[config["db_name"]]
dbt = db[config["db_collection"]]

# ✅ Quick-fix client
client = mqtt.Client()

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.connect(config["broker_host"], config["broker_port"], keepalive=60)
client.loop_start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    client.disconnect()
    client.loop_stop()
    print("[Subscriber] Stopped gracefully.")
