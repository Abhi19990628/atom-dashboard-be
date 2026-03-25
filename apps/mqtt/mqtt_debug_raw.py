import paho.mqtt.client as mqtt

BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"

# yaha agar sirf ek topic test karna hai to use karo:
# DEBUG_TOPICS = [("COUNT7", 1)]
DEBUG_TOPICS = [
    ("JJ7", 1),
    # ("COUNT7", 1),
]

def on_connect(client, userdata, flags, rc):
    print(f"🔗 MQTT Connected rc={rc}")
    if rc == 0:
        client.subscribe(DEBUG_TOPICS)
        print(f"✅ Subscribed to: {[t[0] for t in DEBUG_TOPICS]}")

def on_message(client, userdata, msg):
    raw = msg.payload.decode(errors="ignore")
    topic = msg.topic
    print("\n" + "="*60)
    print(f"📩 Topic : {topic}")
    print(f"🧾 Raw   : {raw}")

    # COUNT payload ka parse just debug ke liye
    if topic.startswith("COUNT"):
        parts = raw.strip().split()
        if len(parts) >= 2:
            tool_id_raw = parts[0]
            val_str = parts[1]
            print(f"🔍 tool_id_raw : {tool_id_raw}")
            print(f"🔍 val_str     : {val_str}")
        else:
            print("⚠️ COUNT format unexpected")

    # JJ payload ka simple parse
    if topic.startswith("JJ"):
        print("🔍 JSON payload aaya (JJ topic), direct raw dekh lo.")

    print("="*60)

def start_debug():
    client = mqtt.Client(client_id="mqtt_debug_raw", protocol=mqtt.MQTTv311)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    start_debug()
