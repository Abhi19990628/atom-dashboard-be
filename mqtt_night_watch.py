import time
from datetime import datetime
import paho.mqtt.client as mqtt

BROKER = "192.168.0.35"
PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"

TOPICS = [
    "COUNT", "COUNT1", "COUNT2", "COUNT3", "COUNT4",
    "COUNT52", "COUNT16", "COUNT17", "COUNT18", "COUNT19"
]

LOG_FILE = "mqtt_night_watch.log"

def write_log(text):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {text}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def on_connect(client, userdata, flags, rc):
    write_log(f"MQTT connected rc={rc}")
    for topic in TOPICS:
        client.subscribe(topic, qos=1)
        write_log(f"Subscribed: {topic}")

def on_disconnect(client, userdata, rc):
    write_log(f"MQTT disconnected rc={rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode(errors="ignore")
    write_log(f"{msg.topic} => {payload}")

client = mqtt.Client(client_id=f"plant2_night_watch_{int(time.time())}")
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

write_log("Starting MQTT night watch...")
client.connect(BROKER, PORT, 60)
client.loop_forever()