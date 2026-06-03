import paho.mqtt.client as mqtt
from datetime import datetime
import time
import threading
import json

BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883

USERNAME = "npdAtom"
PASSWORD = "npd@Atom"

TOPICS = [
    "J1", "J2", "J3", "J4", "J5",
    "J6", "J7", "J8", "J9"
]

machine_last_seen = {}


def parse_json_payload(raw_payload):
    try:
        data = json.loads(raw_payload)

        client_id = str(data.get("client_id", ""))

        if len(client_id) < 2:
            return None

        plant_no = int(client_id[0])
        machine_no = int(client_id[1:])

        return {
            "plant_no": plant_no,
            "machine_no": machine_no,
            "card": data.get("card", "N/A"),
            "die_height": data.get("die_height", "0")
        }

    except Exception:
        return None


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected\n")

        for topic in TOPICS:
            client.subscribe(topic)
            print(f"Subscribed: {topic}")
    else:
        print("❌ Connection failed")


def on_message(client, userdata, msg):
    raw = msg.payload.decode(errors="ignore")

    parsed = parse_json_payload(raw)

    if parsed and parsed["plant_no"] == 2:

        machine_no = parsed["machine_no"]

        machine_last_seen[machine_no] = datetime.now()

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"M{machine_no} | "
            f"CARD={parsed['card']} | "
            f"DIE={parsed['die_height']} | "
            f"RAW={raw}"
        )


def monitor():
    while True:
        time.sleep(10)

        now = datetime.now()

        print("\n================ JSON STATUS ================\n")

        for m in range(1, 47):

            if m in machine_last_seen:
                sec = (now - machine_last_seen[m]).total_seconds()

                if sec <= 60:
                    print(f"🟢 M{m} JSON aa raha hai ({int(sec)} sec ago)")
                else:
                    print(f"🔴 M{m} JSON nahi aa raha ({int(sec)} sec old)")
            else:
                print(f"⚫ M{m} Kabhi JSON nahi aaya")

        print("\n===========================================\n")


client = mqtt.Client()

client.username_pw_set(USERNAME, PASSWORD)

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT, 60)

threading.Thread(target=monitor, daemon=True).start()

client.loop_forever()