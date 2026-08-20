import paho.mqtt.client as mqtt
from datetime import datetime
import sys
from uuid import uuid4

# Broker Details
BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"
SERIAL_NO = 0
# Plant 1 Topics
PLANT1_TOPICS = [
    ("COUNT5", 1),
    ("COUNT6", 1),
    ("COUNT7", 1),
    ("COUNT8", 1),
    ("COUNT9", 1),
    ("COUNT10", 1),
    ("COUNT11", 1),
    ("COUNT12", 1),
    ("COUNT13", 1),
    ("COUNT14", 1),
    ("COUNT15", 1),
]

print("=" * 60)
# 🎯 USER SE DYNAMIC MACHINE NUMBER POOCHHO
user_input = input(
    "👉 Aapko kaunsi Machine ka count dekhna hai? (Sirf number likhein, jaise 16, 20, 25): "
)

try:
    TARGET_MACHINE = int(user_input.strip())
    print(f"🎯 Done! Ab sirf Machine {TARGET_MACHINE} ka count dikhega...")
except ValueError:
    print(
        "❌ Aapne sahi number nahi daala! Script band ho rahi hai. Phir se chalayein."
    )
    sys.exit()
print("=" * 60)


def on_connect(client, userdata, flags, rc):
    global SERIAL_NO
    SERIAL_NO = 0

    if rc == 0:
        print("✅ Broker se connect ho gaya!\n⏳ Waiting for data...")
        for topic, qos in PLANT1_TOPICS:
            client.subscribe(topic, qos)
        print("-" * 60)
    else:
        print(f"❌ Connection failed! Error code: {rc}")


def on_message(client, userdata, msg):
    global SERIAL_NO
    topic = msg.topic
    payload = msg.payload.decode("utf-8", errors="ignore").strip()
    now = datetime.now().strftime("%H:%M:%S")

    # Sirf COUNT topics par focus karenge
    if topic.startswith("COUNT"):
        try:
            parts = payload.split()
            if len(parts) >= 2:
                val_str = parts[1]

                machine_no = None
                # Plant 1 check
                if len(val_str) > 0 and val_str[0] == "1":
                    if len(val_str) > 3 and val_str[1:3].isdigit():
                        machine_no = val_str[1:3]
                    elif len(val_str) > 2 and val_str[1].isdigit():
                        machine_no = val_str[1]

                # Number convert karke target machine se match karo
                if machine_no and machine_no.isdigit():
                    m_no = int(machine_no)

                    # 🔥 Agar machine number wahi hai jo aapne terminal mein dala tha 🔥
                    if m_no == TARGET_MACHINE:
                        SERIAL_NO += 1
                        print(
                            f"🟢 SNo. {SERIAL_NO} | [{now}] PP: {m_no} | Count {payload}"
                        )

        except Exception:
            pass


def on_disconnect(client, userdata, rc):
    print(f"\n🔴 MQTT disconnected! Return code: {rc}")

    if rc != 0:
        print("⚠️ Unexpected disconnect hua.")
        print("Possible reason: duplicate Client ID or network issue.")


# Client setup aur run
UNIQUE_CLIENT_ID = f"p1m{TARGET_MACHINE}_{uuid4().hex[:8]}"

print(f"🔑 MQTT Client ID: {UNIQUE_CLIENT_ID}")

client = mqtt.Client(
    client_id=UNIQUE_CLIENT_ID, clean_session=True, protocol=mqtt.MQTTv311
)
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

client.reconnect_delay_set(min_delay=2, max_delay=30)

try:
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_forever()
except KeyboardInterrupt:
    print("\n🛑 Tester stopped by user.")
except Exception as e:
    print(f"\n❌ Connection error: {e}")
