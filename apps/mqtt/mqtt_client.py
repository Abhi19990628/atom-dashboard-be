# backend/mqtt_client.py - Exact parsing logic
import re
import paho.mqtt.client as mqtt
from apps.machines.machine_state import MACHINE_STATE
from apps.machines.machine_map import TOPIC_TO_MACHINE, SPECIAL_RULES, rule_count10, COUNT52_GROUP

BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"
KEEPALIVE = 60

PLANT2_TOPICS = [("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT52", 1), ("COUNT3", 1), ("COUNT4", 1)]
PLANT1_TOPICS = [("COUNT5", 1), ("COUNT6", 1), ("COUNT7", 1), ("COUNT8", 1), ("COUNT9", 1), ("COUNT10", 1), ("COUNT11", 1), ("COUNT12", 1), ("COUNT13", 1), ("COUNT14", 1), ("COUNT15", 1)]
ALL_TOPICS = PLANT2_TOPICS + PLANT1_TOPICS

def parse_exact_payload(raw: str):
    """Parse payload exactly as per specification"""
    try:
        # Handle 'Failed' cases
        if 'Failed' in raw:
            raw = raw.replace('Failed', '')
        
        parts = raw.strip().split()
        if len(parts) != 2:
            return None
        
        # Tool ID - exactly 24 characters
        tool_id = parts[0][:24] if len(parts[0]) >= 24 else parts[0]
        if len(tool_id) < 24:
            tool_id = "Unknown"
        
        # Value parsing
        value_str = parts[1]
        
        if len(value_str) >= 5:
            # Plant = first digit
            plant_no = int(value_str[0]) if value_str[0].isdigit() else 2
            
            # Machine = next 2 digits
            machine_str = value_str[1:3] if len(value_str) >= 3 else "15"
            machine_no = int(machine_str) if machine_str.isdigit() else 15
            
            # Count = 1 (fixed)
            count = 1
            
            # Shut Height = last 5 characters as float
            shut_str = value_str[-5:] if len(value_str) >= 5 else value_str
            shut_height = float(shut_str) if shut_str else 0.0
        else:
            plant_no = 2
            machine_no = 15
            count = 1
            shut_height = float(value_str) if value_str else 0.0
        
        return {
            'tool_id': tool_id,
            'plant_no': plant_no,
            'machine_no': machine_no,
            'count': count,
            'shut_height': shut_height
        }
        
    except Exception as e:
        print(f"Parse error for '{raw}': {e}")
        return None

def on_connect(client, userdata, flags, rc):
    print(f"MQTT Connected rc={rc}")
    if rc == 0:
        res = client.subscribe(ALL_TOPICS)
        print(f"Subscribed → {res}")

def on_message(client, userdata, msg):
    raw = msg.payload.decode(errors="ignore")
    topic = msg.topic
    
    if topic == "COUNT52":
        parsed = parse_exact_payload(raw)
        
        if parsed:
            plant_no = parsed['plant_no']
            machine_no = parsed['machine_no']
            tool_id = parsed['tool_id']
            count = parsed['count']
            shut_height = parsed['shut_height']
            
            print(f"COUNT52 '{raw}' → Plant={plant_no} M{machine_no:02d} Tool={tool_id} Count={count} Shut={shut_height}")
        else:
            print(f"Failed to parse COUNT52: {raw}")
            return
    else:
        # Handle other topics
        parts = raw.split()
        tool_id = parts[0] if parts and len(parts[0]) >= 10 else "Unknown"
        value = float(parts[1].replace('Failed', '')) if len(parts) > 1 else 0.0
        plant_no, machine_no = TOPIC_TO_MACHINE.get(topic, (None, None))
        count = 1
        shut_height = value

    if plant_no is None or machine_no is None:
        print(f"⚠️ Unmapped topic {topic}")
        return

    # Store data
    MACHINE_STATE.upsert(plant_no, machine_no, tool_id, count, shut_height)
    print(f"📊 Stored: Plant{plant_no} M{machine_no:02d} Count={count} Shut={shut_height:.2f}")

def start_mqtt():
    print(f"🚀 MQTT → {BROKER_HOST}:{BROKER_PORT}")
    client = mqtt.Client(client_id="django_mqtt", protocol=mqtt.MQTTv311)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
    client.loop_start()
    return client


if __name__ == "__main__":
    start_mqtt()
    import time
    while True:
        time.sleep(5)

