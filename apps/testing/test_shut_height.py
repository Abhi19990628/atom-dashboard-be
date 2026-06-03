import paho.mqtt.client as mqtt
from datetime import datetime
import time
import threading

BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"

TOPICS = [
    "COUNT", "COUNT1", "COUNT2", "COUNT3",
    "COUNT4", "COUNT52",
    "COUNT16", "COUNT17", "COUNT18", "COUNT19"
]

machine_last_seen = {}


def parse_count_payload(raw_payload):
    try:
        parts = raw_payload.strip().split()

        if len(parts) < 2:
            return None

        tool_id = parts[0]
        val_str = parts[1]

        plant_no = int(val_str[0])

        if len(val_str) > 3:
            if val_str[1].isdigit() and val_str[2].isdigit():
                machine_no = int(val_str[1:3])
            else:
                machine_no = int(val_str[1])
        else:
            machine_no = int(val_str[1])

        return {
            "plant_no": plant_no,
            "machine_no": machine_no,
            "tool_id": tool_id
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
        print("Connection failed")


def on_message(client, userdata, msg):
    raw = msg.payload.decode(errors="ignore")

    parsed = parse_count_payload(raw)

    if parsed and parsed["plant_no"] == 2:
        machine_no = parsed["machine_no"]

        machine_last_seen[machine_no] = datetime.now()

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"M{machine_no} | TOOL={parsed['tool_id']} | RAW={raw}"
        )


def monitor():
    while True:
        time.sleep(10)

        now = datetime.now()

        print("\n================ MACHINE STATUS ================")

        for m in range(1, 47):

            if m in machine_last_seen:
                sec = (now - machine_last_seen[m]).total_seconds()

                if sec <= 60:
                    print(f"🟢 M{m} COUNT aa raha hai ({int(sec)} sec ago)")
                else:
                    print(f"🔴 M{m} COUNT nahi aa raha ({int(sec)} sec old)")
            else:
                print(f"⚫ M{m} Kabhi count nahi aaya")

        print("==============================================\n")


client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT, 60)

threading.Thread(target=monitor, daemon=True).start()

client.loop_forever()