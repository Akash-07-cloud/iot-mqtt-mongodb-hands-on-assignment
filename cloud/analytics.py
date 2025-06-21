import pymongo
import json
from datetime import datetime, timedelta

# DB connection
client = pymongo.MongoClient("localhost", 27017)
db = client["iot-db"]
collection = db["iot-sensors-data-timestamped"]
summary_coll = db["daily-summary"]

# Get date ranges
today = datetime.utcnow().date()
prev_day = today - timedelta(days=1)
start = datetime(prev_day.year, prev_day.month, prev_day.day)
end = start + timedelta(days=1)

# Fetch previous day data
entries = collection.find({
    "timestamp": {
        "$gte": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "$lt": end.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
})

# Aggregation
devices_data = {}
for entry in entries:
    payload = entry["payload"] if isinstance(entry["payload"], dict) else json.loads(entry["payload"])
    dev_id = payload["device_id"]
    dev_type = payload["device_type"]
    val = payload["value"]

    if dev_id not in devices_data:
        devices_data[dev_id] = {"type": dev_type, "values": []}
    devices_data[dev_id]["values"].append(val)

# Compute summary
summary = {}
for dev_id, data in devices_data.items():
    vals = data["values"]
    summary[dev_id] = {
        "device_type": data["type"],
        "min": min(vals),
        "max": max(vals),
        "avg": sum(vals) / len(vals),
        "count": len(vals)
    }

# Save summary
summary_coll.insert_one({"date": prev_day.strftime("%Y-%m-%d"), "devices": summary})

# Load alert config
with open("alerts_config.json") as f:
    alerts = json.load(f)

# Check alerts
for dev_id, data in summary.items():
    threshold = alerts.get(data["device_type"])
    if threshold and data["max"] > threshold:
        print(f"⚠ ALERT: {dev_id} max {data['max']} exceeded threshold {threshold}")
