import paho.mqtt.client as mqtt
import pymongo
import json

with open("config.json") as f:
    config = json.load(f)
with open("alerts_config.json") as f:
    thresholds = json.load(f)

client_db = pymongo.MongoClient(config["db_host"], config["db_port"])
db = client_db[config["db_name"]]
coll_vitals = db[config["collections"]["device1"]]
coll_temp = db[config["collections"]["device2"]]
alerts_log = db["alerts-log"]

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("devices/#")

def check_alerts(data, topic):
    if topic == "devices/vitals":
        if data["heart_rate"] > thresholds["heart_rate"]:
            log_alert(data, "heart_rate", data["heart_rate"], thresholds["heart_rate"])
        sys_bp = int(data["blood_pressure"].split("/")[0])
        if sys_bp > thresholds["blood_pressure_systolic"]:
            log_alert(data, "blood_pressure_systolic", sys_bp, thresholds["blood_pressure_systolic"])
    elif topic == "devices/room":
        if data["room_temperature"] > thresholds["room_temperature"]:
            log_alert(data, "room_temperature", data["room_temperature"], thresholds["room_temperature"])

def log_alert(data, param, value, threshold):
    alert_doc = {
        "timestamp": data["timestamp"],
        "parameter": param,
        "value": value,
        "threshold": threshold
    }
    alerts_log.insert_one(alert_doc)
    print(f"⚠ ALERT: {param} = {value} exceeded {threshold}")

def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())
    if msg.topic == "devices/vitals":
        coll_vitals.insert_one(data)
        print(f"Inserted vitals: {data}")
    elif msg.topic == "devices/room":
        coll_temp.insert_one(data)
        print(f"Inserted room temp: {data}")
    
    check_alerts(data, msg.topic)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(config["broker_host"], config["broker_port"], 60)
client.loop_forever()
