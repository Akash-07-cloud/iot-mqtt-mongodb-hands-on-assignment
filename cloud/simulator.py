import paho.mqtt.client as mqtt
import time
import json
import numpy as np
import datetime

def on_connect(client, userdata, flags, rc):
    print(f"[Simulator] Connected with result code {rc}")

def on_disconnect(client, userdata, rc):
    print(f"[Simulator] Disconnected with result code {rc}")

client = mqtt.Client()  # Compatible with your current paho-mqtt version

client.on_connect = on_connect
client.on_disconnect = on_disconnect

with open("config.json") as f:
    config = json.load(f)

device_config = []
for device in config["devices"]:
    for n in range(device["device_count"]):
        device_config.append({
            "device_id": f"{device['type']}_{n}",
            "device_type": device["type"],
            "publish_frequency": device["publish_frequency"],
            "std_val": device["std_val"],
            "publish_topic": device["publish_topic"]
        })

client.connect(config["broker_host"], config["broker_port"], keepalive=60)
client.loop_start()

clock = 0
try:
    while True:
        time.sleep(1)
        clock += 1
        for dev in device_config:
            if clock % dev["publish_frequency"] == 0:
                msg = {
                    "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "device_id": dev["device_id"],
                    "device_type": dev["device_type"],
                    "value": round(np.random.normal(dev["std_val"], 2), 2)
                }
                print(f"[Simulator] Published to {dev['publish_topic']} → {msg}")
                client.publish(dev["publish_topic"], json.dumps(msg))
except KeyboardInterrupt:
    client.disconnect()
    client.loop_stop()
    print("[Simulator] Stopped gracefully.")
