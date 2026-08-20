<<<<<<<< HEAD:apps/utils/testing/test_mqtt_p2.py
# import paho.mqtt.client as mqtt
# import json
# from datetime import datetime

# # Broker Details (From your main file)
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1),
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print(f"📥 Sabhi {len(PLANT2_TOPICS)} topics subscribe kar liye hain. Waiting for data...\n")
#         print("-" * 60)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = datetime.now().strftime("%H:%M:%S")

#     # 1️⃣ Agar J topic hai (JSON Data)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))

#             # Plant aur Machine nikalo
#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#                 print(f"🔵 [{now}] J-TOPIC ({topic}) | Machine: {machine_no} | Status: JSON Received")
#                 print(f"   Payload: {payload}")
#                 print("-" * 60)
#         except json.JSONDecodeError:
#             print(f"⚠️ [{now}] Invalid JSON on topic {topic}: {payload}")

#     # 2️⃣ Agar COUNT topic hai
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]

#                 # Plant aur Machine nikalo
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#                     else:
#                         machine_no = "Unknown"

#                     print(f"🟢 [{now}] COUNT-TOPIC ({topic}) | Machine: {machine_no} | Status: Count Received")
#                     print(f"   Payload: {payload}")
#                     print("-" * 60)
#         except Exception as e:
#             print(f"⚠️ [{now}] Error parsing count on topic {topic}: {payload}")

# # Client setup aur run
# client = mqtt.Client(client_id="plant2_quick_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# print("🚀 Starting MQTT Tester for Plant 2...")
# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")


# import paho.mqtt.client as mqtt
# import json
# from datetime import datetime

# # Broker Details
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1),
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# # ✅ Ek Set jisme detect hui machines save hongi taaki baar-baar print na ho
# detected_machines = set()

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print(f"📥 Sabhi {len(PLANT2_TOPICS)} topics subscribe kar liye hain.")
#         print("⏳ Waiting for machine signals... (Terminal spam is OFF)\n")
#         print("-" * 60)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = datetime.now().strftime("%H:%M:%S")

#     machine_no = None

#     # 1️⃣ Agar J topic hai (JSON Data)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))

#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#         except json.JSONDecodeError:
#             pass

#     # 2️⃣ Agar COUNT topic hai
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#         except Exception:
#             pass

#     # 3️⃣ Agar Machine mili aur pehle se Set mein nahi hai, tabhi Print karo
#     if machine_no and machine_no not in detected_machines:
#         detected_machines.add(machine_no)
#         print(f"🟢 [{now}] MACHINE {machine_no} IS ON! (Signal detected via {topic})")

#         # Ek summary print karo ki ab tak kaun kaun si ON ho chuki hain
#         active_list = sorted(list(detected_machines), key=lambda x: int(x) if x.isdigit() else x)
#         print(f"📊 Total Active Machines so far: {', '.join(active_list)}")
#         print("-" * 60)

# # Client setup aur run
# client = mqtt.Client(client_id="plant2_quick_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# print("🚀 Starting MQTT Tester for Plant 2...")
# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")


# import paho.mqtt.client as mqtt
# import json
# import time
# import threading
# from datetime import datetime

# # Broker Details
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1),
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# machines_data = {}
# data_lock = threading.Lock()

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print("⏳ Waiting for machine signals... (3 MINUTE ALGORITHM ACTIVE)\n")
#         print("-" * 70)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = time.time()

#     machine_no = None
#     is_count = False

#     # 1️⃣ JSON Data (Machine ON signal)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))
#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#         except json.JSONDecodeError:
#             pass

#     # 2️⃣ COUNT Data (Die upar-neeche hui stroke laga)
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                         is_count = True
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#                         is_count = True
#         except Exception:
#             pass

#     # 3️⃣ Data Update
#     if machine_no:
#         with data_lock:
#             if machine_no not in machines_data:
#                 machines_data[machine_no] = {
#                     "last_signal": now,
#                     "last_count": now if is_count else 0,
#                     "state": "UNKNOWN"
#                 }
#             else:
#                 machines_data[machine_no]["last_signal"] = now
#                 if is_count:
#                     machines_data[machine_no]["last_count"] = now


# # 🔥 EXACT 3 MINUTE ALGORITHM THREAD 🔥
# def monitor_machines():
#     last_summary_time = time.time()

#     while True:
#         time.sleep(2)
#         now = time.time()

#         idle_machines = [] # Un machines ke liye jo ON hain par 3 min se count nahi diya

#         with data_lock:
#             for m_no, data in machines_data.items():
#                 time_since_signal = now - data["last_signal"]
#                 time_since_count = now - data["last_count"]

#                 current_state = data["state"]
#                 new_state = current_state

#                 # ========================================================
#                 # 3 MINUTE LOGIC (180 Seconds)
#                 # ========================================================
#                 if time_since_signal > 210:
#                     # Agar 3.5 minute se J-topic (JSON) ka signal bhi nahi aaya = OFFLINE
#                     new_state = "OFFLINE"
#                 elif time_since_count <= 180:
#                     # Agar pichle 3 minute ke andar COUNT aaya hai = PRODUCING
#                     new_state = "PRODUCING"
#                 else:
#                     # Signal aa raha hai, par 3 minute se zyada ho gaye COUNT nahi aaya = IDLE
#                     new_state = "IDLE"
#                     idle_machines.append(m_no)

#                 # Sirf tab print karega jab state change hogi
#                 if new_state != current_state:
#                     time_str = datetime.now().strftime("%H:%M:%S")

#                     if new_state == "PRODUCING":
#                         print(f"✅ [{time_str}] MACHINE {m_no} ➔ PRODUCING (Stroke received)")
#                     elif new_state == "IDLE":
#                         print(f"⚠️ [{time_str}] MACHINE {m_no} ➔ IDLE (Machine ON hai, par 3 min se count nahi aaya!)")
#                     elif new_state == "OFFLINE":
#                         print(f"⬛ [{time_str}] MACHINE {m_no} ➔ OFFLINE (Koi signal nahi aa raha)")

#                     data["state"] = new_state

#         # ========================================================
#         # REPORT GENERATOR: Har 1 minute mein IDLE machines batayega
#         # ========================================================
#         if now - last_summary_time >= 60:
#             if idle_machines:
#                 time_str = datetime.now().strftime("%H:%M:%S")
#                 idle_list = ", ".join(sorted(idle_machines, key=lambda x: int(x) if x.isdigit() else x))
#                 print(f"\n📊 [{time_str}] IDLE MACHINES ALERT: Ye machines ON hain par inka count nahi aa raha: {idle_list}\n")
#             last_summary_time = now


# monitor_thread = threading.Thread(target=monitor_machines, daemon=True)
# monitor_thread.start()

# client = mqtt.Client(client_id="plant2_smart_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")


# import paho.mqtt.client as mqtt
# import json
# from datetime import datetime

# # Broker Details (From your main file)
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1),
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print(f"📥 Sabhi {len(PLANT2_TOPICS)} topics subscribe kar liye hain. Waiting for data...\n")
#         print("-" * 60)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = datetime.now().strftime("%H:%M:%S")

#     # 1️⃣ Agar J topic hai (JSON Data)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))

#             # Plant aur Machine nikalo
#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#                 print(f"🔵 [{now}] J-TOPIC ({topic}) | Machine: {machine_no} | Status: JSON Received")
#                 print(f"   Payload: {payload}")
#                 print("-" * 60)
#         except json.JSONDecodeError:
#             print(f"⚠️ [{now}] Invalid JSON on topic {topic}: {payload}")

#     # 2️⃣ Agar COUNT topic hai
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]

#                 # Plant aur Machine nikalo
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#                     else:
#                         machine_no = "Unknown"

#                     print(f"🟢 [{now}] COUNT-TOPIC ({topic}) | Machine: {machine_no} | Status: Count Received")
#                     print(f"   Payload: {payload}")
#                     print("-" * 60)
#         except Exception as e:
#             print(f"⚠️ [{now}] Error parsing count on topic {topic}: {payload}")

# # Client setup aur run
# client = mqtt.Client(client_id="plant2_quick_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# print("🚀 Starting MQTT Tester for Plant 2...")
# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")


# import paho.mqtt.client as mqtt
# import json
# from datetime import datetime

# # Broker Details
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1),
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# # ✅ Ek Set jisme detect hui machines save hongi taaki baar-baar print na ho
# detected_machines = set()

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print(f"📥 Sabhi {len(PLANT2_TOPICS)} topics subscribe kar liye hain.")
#         print("⏳ Waiting for machine signals... (Terminal spam is OFF)\n")
#         print("-" * 60)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = datetime.now().strftime("%H:%M:%S")

#     machine_no = None

#     # 1️⃣ Agar J topic hai (JSON Data)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))

#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#         except json.JSONDecodeError:
#             pass

#     # 2️⃣ Agar COUNT topic hai
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#         except Exception:
#             pass

#     # 3️⃣ Agar Machine mili aur pehle se Set mein nahi hai, tabhi Print karo
#     if machine_no and machine_no not in detected_machines:
#         detected_machines.add(machine_no)
#         print(f"🟢 [{now}] MACHINE {machine_no} IS ON! (Signal detected via {topic})")

#         # Ek summary print karo ki ab tak kaun kaun si ON ho chuki hain
#         active_list = sorted(list(detected_machines), key=lambda x: int(x) if x.isdigit() else x)
#         print(f"📊 Total Active Machines so far: {', '.join(active_list)}")
#         print("-" * 60)

# # Client setup aur run
# client = mqtt.Client(client_id="plant2_quick_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# print("🚀 Starting MQTT Tester for Plant 2...")
# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")

# import paho.mqtt.client as mqtt
# from datetime import datetime
# import sys

# # Broker Details
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"
# SERIAL_NO = 0
# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1),
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1)
# ]

# print("=" * 60)
# # 🎯 USER SE DYNAMIC MACHINE NUMBER POOCHHO
# user_input = input("👉 Aapko kaunsi Machine ka count dekhna hai? (Sirf number likhein, jaise 16, 20, 25): ")

# try:
#     TARGET_MACHINE = int(user_input.strip())
#     print(f"🎯 Done! Ab sirf Machine {TARGET_MACHINE} ka count dikhega...")
# except ValueError:
#     print("❌ Aapne sahi number nahi daala! Script band ho rahi hai. Phir se chalayein.")
#     sys.exit()
# print("=" * 60)


# def on_connect(client, userdata, flags, rc):
#     global SERIAL_NO
#     SERIAL_NO = 0

#     if rc == 0:
#         print("✅ Broker se connect ho gaya!\n⏳ Waiting for data...")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print("-" * 60)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     global SERIAL_NO
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = datetime.now().strftime("%H:%M:%S")

#     # Sirf COUNT topics par focus karenge
#     if topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]

#                 machine_no = None
#                 # Plant 2 check
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]

#                 # Number convert karke target machine se match karo
#                 if machine_no and machine_no.isdigit():
#                     m_no = int(machine_no)

#                     # 🔥 Agar machine number wahi hai jo aapne terminal mein dala tha 🔥
#                     if m_no == TARGET_MACHINE:
#                         SERIAL_NO += 1
#                         print(f"🟢 SNo. {SERIAL_NO} | [{now}] PP: {m_no} | Count ➔ {payload}")

#         except Exception:
#             pass

# # Client setup aur run
# client = mqtt.Client(client_id=f"plant2_monitor_{TARGET_MACHINE}", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")


# by aman


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
# Plant 2 Topics
PLANT2_TOPICS = [
    ("COUNT", 1),
    ("COUNT1", 1),
    ("COUNT2", 1),
    ("COUNT3", 1),
    ("COUNT4", 1),
    ("COUNT52", 1),
    ("COUNT16", 1),
    ("COUNT17", 1),
    ("COUNT18", 1),
    ("COUNT19", 1),
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
        for topic, qos in PLANT2_TOPICS:
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
                # Plant 2 check
                if len(val_str) > 0 and val_str[0] == "2":
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
UNIQUE_CLIENT_ID = f"p2m{TARGET_MACHINE}_{uuid4().hex[:8]}"

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
========
# import paho.mqtt.client as mqtt
# import json
# from datetime import datetime

# # Broker Details (From your main file)
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1), 
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print(f"📥 Sabhi {len(PLANT2_TOPICS)} topics subscribe kar liye hain. Waiting for data...\n")
#         print("-" * 60)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = datetime.now().strftime("%H:%M:%S")

#     # 1️⃣ Agar J topic hai (JSON Data)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))
            
#             # Plant aur Machine nikalo
#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#                 print(f"🔵 [{now}] J-TOPIC ({topic}) | Machine: {machine_no} | Status: JSON Received")
#                 print(f"   Payload: {payload}")
#                 print("-" * 60)
#         except json.JSONDecodeError:
#             print(f"⚠️ [{now}] Invalid JSON on topic {topic}: {payload}")

#     # 2️⃣ Agar COUNT topic hai
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]
                
#                 # Plant aur Machine nikalo
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#                     else:
#                         machine_no = "Unknown"
                    
#                     print(f"🟢 [{now}] COUNT-TOPIC ({topic}) | Machine: {machine_no} | Status: Count Received")
#                     print(f"   Payload: {payload}")
#                     print("-" * 60)
#         except Exception as e:
#             print(f"⚠️ [{now}] Error parsing count on topic {topic}: {payload}")

# # Client setup aur run
# client = mqtt.Client(client_id="plant2_quick_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# print("🚀 Starting MQTT Tester for Plant 2...")
# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")





# import paho.mqtt.client as mqtt
# import json
# from datetime import datetime

# # Broker Details 
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1), 
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# # ✅ Ek Set jisme detect hui machines save hongi taaki baar-baar print na ho
# detected_machines = set()

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print(f"📥 Sabhi {len(PLANT2_TOPICS)} topics subscribe kar liye hain.")
#         print("⏳ Waiting for machine signals... (Terminal spam is OFF)\n")
#         print("-" * 60)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = datetime.now().strftime("%H:%M:%S")
    
#     machine_no = None

#     # 1️⃣ Agar J topic hai (JSON Data)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))
            
#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#         except json.JSONDecodeError:
#             pass

#     # 2️⃣ Agar COUNT topic hai
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#         except Exception:
#             pass

#     # 3️⃣ Agar Machine mili aur pehle se Set mein nahi hai, tabhi Print karo
#     if machine_no and machine_no not in detected_machines:
#         detected_machines.add(machine_no)
#         print(f"🟢 [{now}] MACHINE {machine_no} IS ON! (Signal detected via {topic})")
        
#         # Ek summary print karo ki ab tak kaun kaun si ON ho chuki hain
#         active_list = sorted(list(detected_machines), key=lambda x: int(x) if x.isdigit() else x)
#         print(f"📊 Total Active Machines so far: {', '.join(active_list)}")
#         print("-" * 60)

# # Client setup aur run
# client = mqtt.Client(client_id="plant2_quick_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# print("🚀 Starting MQTT Tester for Plant 2...")
# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")



# import paho.mqtt.client as mqtt
# import json
# import time
# import threading
# from datetime import datetime

# # Broker Details 
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1), 
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# machines_data = {}
# data_lock = threading.Lock()

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print("⏳ Waiting for machine signals... (3 MINUTE ALGORITHM ACTIVE)\n")
#         print("-" * 70)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = time.time()
    
#     machine_no = None
#     is_count = False

#     # 1️⃣ JSON Data (Machine ON signal)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))
#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#         except json.JSONDecodeError:
#             pass

#     # 2️⃣ COUNT Data (Die upar-neeche hui stroke laga)
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                         is_count = True
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#                         is_count = True
#         except Exception:
#             pass

#     # 3️⃣ Data Update
#     if machine_no:
#         with data_lock:
#             if machine_no not in machines_data:
#                 machines_data[machine_no] = {
#                     "last_signal": now,
#                     "last_count": now if is_count else 0,
#                     "state": "UNKNOWN"
#                 }
#             else:
#                 machines_data[machine_no]["last_signal"] = now
#                 if is_count:
#                     machines_data[machine_no]["last_count"] = now


# # 🔥 EXACT 3 MINUTE ALGORITHM THREAD 🔥
# def monitor_machines():
#     last_summary_time = time.time()

#     while True:
#         time.sleep(2)
#         now = time.time()
        
#         idle_machines = [] # Un machines ke liye jo ON hain par 3 min se count nahi diya
        
#         with data_lock:
#             for m_no, data in machines_data.items():
#                 time_since_signal = now - data["last_signal"]
#                 time_since_count = now - data["last_count"]
                
#                 current_state = data["state"]
#                 new_state = current_state
                
#                 # ========================================================
#                 # 3 MINUTE LOGIC (180 Seconds)
#                 # ========================================================
#                 if time_since_signal > 210:  
#                     # Agar 3.5 minute se J-topic (JSON) ka signal bhi nahi aaya = OFFLINE
#                     new_state = "OFFLINE"
#                 elif time_since_count <= 180: 
#                     # Agar pichle 3 minute ke andar COUNT aaya hai = PRODUCING
#                     new_state = "PRODUCING"
#                 else:
#                     # Signal aa raha hai, par 3 minute se zyada ho gaye COUNT nahi aaya = IDLE
#                     new_state = "IDLE"
#                     idle_machines.append(m_no)
                
#                 # Sirf tab print karega jab state change hogi
#                 if new_state != current_state:
#                     time_str = datetime.now().strftime("%H:%M:%S")
                    
#                     if new_state == "PRODUCING":
#                         print(f"✅ [{time_str}] MACHINE {m_no} ➔ PRODUCING (Stroke received)")
#                     elif new_state == "IDLE":
#                         print(f"⚠️ [{time_str}] MACHINE {m_no} ➔ IDLE (Machine ON hai, par 3 min se count nahi aaya!)")
#                     elif new_state == "OFFLINE":
#                         print(f"⬛ [{time_str}] MACHINE {m_no} ➔ OFFLINE (Koi signal nahi aa raha)")
                        
#                     data["state"] = new_state

#         # ========================================================
#         # REPORT GENERATOR: Har 1 minute mein IDLE machines batayega
#         # ========================================================
#         if now - last_summary_time >= 60:
#             if idle_machines:
#                 time_str = datetime.now().strftime("%H:%M:%S")
#                 idle_list = ", ".join(sorted(idle_machines, key=lambda x: int(x) if x.isdigit() else x))
#                 print(f"\n📊 [{time_str}] IDLE MACHINES ALERT: Ye machines ON hain par inka count nahi aa raha: {idle_list}\n")
#             last_summary_time = now


# monitor_thread = threading.Thread(target=monitor_machines, daemon=True)
# monitor_thread.start()

# client = mqtt.Client(client_id="plant2_smart_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")



# import paho.mqtt.client as mqtt
# import json
# from datetime import datetime

# # Broker Details (From your main file)
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1), 
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print(f"📥 Sabhi {len(PLANT2_TOPICS)} topics subscribe kar liye hain. Waiting for data...\n")
#         print("-" * 60)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = datetime.now().strftime("%H:%M:%S")

#     # 1️⃣ Agar J topic hai (JSON Data)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))
            
#             # Plant aur Machine nikalo
#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#                 print(f"🔵 [{now}] J-TOPIC ({topic}) | Machine: {machine_no} | Status: JSON Received")
#                 print(f"   Payload: {payload}")
#                 print("-" * 60)
#         except json.JSONDecodeError:
#             print(f"⚠️ [{now}] Invalid JSON on topic {topic}: {payload}")

#     # 2️⃣ Agar COUNT topic hai
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]
                
#                 # Plant aur Machine nikalo
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#                     else:
#                         machine_no = "Unknown"
                    
#                     print(f"🟢 [{now}] COUNT-TOPIC ({topic}) | Machine: {machine_no} | Status: Count Received")
#                     print(f"   Payload: {payload}")
#                     print("-" * 60)
#         except Exception as e:
#             print(f"⚠️ [{now}] Error parsing count on topic {topic}: {payload}")

# # Client setup aur run
# client = mqtt.Client(client_id="plant2_quick_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# print("🚀 Starting MQTT Tester for Plant 2...")
# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")





# import paho.mqtt.client as mqtt
# import json
# from datetime import datetime

# # Broker Details 
# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # Plant 2 Topics
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1), 
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# # ✅ Ek Set jisme detect hui machines save hongi taaki baar-baar print na ho
# detected_machines = set()

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Broker se connect ho gaya!")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#         print(f"📥 Sabhi {len(PLANT2_TOPICS)} topics subscribe kar liye hain.")
#         print("⏳ Waiting for machine signals... (Terminal spam is OFF)\n")
#         print("-" * 60)
#     else:
#         print(f"❌ Connection failed! Error code: {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8', errors='ignore').strip()
#     now = datetime.now().strftime("%H:%M:%S")
    
#     machine_no = None

#     # 1️⃣ Agar J topic hai (JSON Data)
#     if topic.startswith('J'):
#         try:
#             data = json.loads(payload)
#             client_id = str(data.get('client_id', ''))
            
#             if len(client_id) >= 2 and client_id[0] == '2':
#                 machine_no = client_id[1:]
#         except json.JSONDecodeError:
#             pass

#     # 2️⃣ Agar COUNT topic hai
#     elif topic.startswith('COUNT'):
#         try:
#             parts = payload.split()
#             if len(parts) >= 2:
#                 val_str = parts[1]
#                 if len(val_str) > 0 and val_str[0] == '2':
#                     if len(val_str) > 3 and val_str[1:3].isdigit():
#                         machine_no = val_str[1:3]
#                     elif len(val_str) > 2 and val_str[1].isdigit():
#                         machine_no = val_str[1]
#         except Exception:
#             pass

#     # 3️⃣ Agar Machine mili aur pehle se Set mein nahi hai, tabhi Print karo
#     if machine_no and machine_no not in detected_machines:
#         detected_machines.add(machine_no)
#         print(f"🟢 [{now}] MACHINE {machine_no} IS ON! (Signal detected via {topic})")
        
#         # Ek summary print karo ki ab tak kaun kaun si ON ho chuki hain
#         active_list = sorted(list(detected_machines), key=lambda x: int(x) if x.isdigit() else x)
#         print(f"📊 Total Active Machines so far: {', '.join(active_list)}")
#         print("-" * 60)

# # Client setup aur run
# client = mqtt.Client(client_id="plant2_quick_tester", clean_session=True)
# client.username_pw_set(USERNAME, PASSWORD)
# client.on_connect = on_connect
# client.on_message = on_message

# print("🚀 Starting MQTT Tester for Plant 2...")
# try:
#     client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     client.loop_forever()
# except KeyboardInterrupt:
#     print("\n🛑 Tester stopped by user.")
# except Exception as e:
#     print(f"\n❌ Connection error: {e}")

import paho.mqtt.client as mqtt
from datetime import datetime
import sys

# Broker Details 
BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"

# Plant 2 Topics
PLANT2_TOPICS = [
    ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1), 
    ("COUNT4", 1), ("COUNT52", 1),
    ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1)
]

print("=" * 60)
# 🎯 USER SE DYNAMIC MACHINE NUMBER POOCHHO
user_input = input("👉 Aapko kaunsi Machine ka count dekhna hai? (Sirf number likhein, jaise 16, 20, 25): ")

try:
    TARGET_MACHINE = int(user_input.strip())
    print(f"🎯 Done! Ab sirf Machine {TARGET_MACHINE} ka count dikhega...")
except ValueError:
    print("❌ Aapne sahi number nahi daala! Script band ho rahi hai. Phir se chalayein.")
    sys.exit()
print("=" * 60)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Broker se connect ho gaya!\n⏳ Waiting for data...")
        for topic, qos in PLANT2_TOPICS:
            client.subscribe(topic, qos)
        print("-" * 60)
    else:
        print(f"❌ Connection failed! Error code: {rc}")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode('utf-8', errors='ignore').strip()
    now = datetime.now().strftime("%H:%M:%S")

    # Sirf COUNT topics par focus karenge
    if topic.startswith('COUNT'):
        try:
            parts = payload.split()
            if len(parts) >= 2:
                val_str = parts[1]
                
                machine_no = None
                # Plant 2 check
                if len(val_str) > 0 and val_str[0] == '2':
                    if len(val_str) > 3 and val_str[1:3].isdigit():
                        machine_no = val_str[1:3]
                    elif len(val_str) > 2 and val_str[1].isdigit():
                        machine_no = val_str[1]
                
                # Number convert karke target machine se match karo
                if machine_no and machine_no.isdigit():
                    m_no = int(machine_no)
                    
                    # 🔥 Agar machine number wahi hai jo aapne terminal mein dala tha 🔥
                    if m_no == TARGET_MACHINE:
                        print(f"🟢 [{now}] Machine: {m_no} | Count Aaya ➔ {payload}")
                        
        except Exception:
            pass

# Client setup aur run
client = mqtt.Client(client_id=f"plant2_monitor_{TARGET_MACHINE}", clean_session=True)
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_forever()
except KeyboardInterrupt:
    print("\n🛑 Tester stopped by user.")
except Exception as e:
    print(f"\n❌ Connection error: {e}")
>>>>>>>> main:apps/utils/testing/test_mqtt.py
