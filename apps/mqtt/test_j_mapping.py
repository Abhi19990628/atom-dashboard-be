# backend/apps/mqtt/test_j_mapping.py - TESTING J-MAPPING JSON MESSAGES

import paho.mqtt.client as mqtt
from datetime import datetime
import pytz
import json

BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"

# 🎯 J-GROUP MAPPING
MACHINE_GROUP_MAPPING = {
    'J4': [1, 2, 3, 4, 5],
    'J3': [6, 7, 8, 9, 10],
    'J2': [11, 12, 13, 14, 15],
    'J1': [16, 17, 18, 19, 20],
    'J5': [41, 42, 43, 44, 45, 46]
}

def get_machine_group(machine_no):
    """Get J-group for machine"""
    for group_name, machines in MACHINE_GROUP_MAPPING.items():
        if machine_no in machines:
            return group_name
    return 'Unknown'


def parse_json_payload(raw_payload):
    """Parse JSON format"""
    try:
        data = json.loads(raw_payload)
        
        if 'client_id' not in data:
            return None
        
        client_id = str(data.get('client_id', ''))
        
        if len(client_id) >= 2:
            plant_no = int(client_id[0]) if client_id[0].isdigit() else None
            machine_no = int(client_id[1:]) if client_id[1:].isdigit() else None
        else:
            return None
        
        card = data.get('card', 'UNKNOWN')
        
        die_height_str = str(data.get('die_height', '0'))
        try:
            die_height = float(die_height_str)
        except:
            die_height = 0.0
        
        return {
            'type': 'json',
            'plant_no': plant_no,
            'machine_no': machine_no,
            'card': card,
            'die_height': die_height
        }
    except:
        return None


def on_connect(client, userdata, flags, rc):
    print("\n" + "="*70)
    print("🔗 MQTT CONNECTED!")
    print("="*70)
    print(f"Return Code: {rc}")
    
    if rc == 0:
        # Subscribe to ALL topics to find JSON messages
        client.subscribe("#")
        print("✅ Subscribed to ALL topics (#)")
        print("\n📡 Waiting for messages...")
        print("="*70 + "\n")


def on_message(client, userdata, msg):
    """Display ALL messages"""
    raw_payload = msg.payload.decode(errors="ignore")
    topic = msg.topic
    
    ist_tz = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist_tz)
    timestamp = now_ist.strftime('%H:%M:%S')
    
    # Try to parse as JSON
    json_parsed = parse_json_payload(raw_payload)
    
    if json_parsed and json_parsed['plant_no'] == 2:
        # ✅ JSON MESSAGE FOUND!
        machine_no = json_parsed['machine_no']
        card = json_parsed['card']
        die_height = json_parsed['die_height']
        group = get_machine_group(machine_no)
        
        print("\n" + "🟢"*35)
        print(f"📡 JSON MESSAGE - {timestamp}")
        print("🟢"*35)
        print(f"📍 Topic: {topic}")
        print(f"🏭 Plant: 2")
        print(f"🎯 Group: {group}")
        print(f"🔧 Machine: {machine_no}")
        print(f"🎫 Card: {card}")
        print(f"📏 Die Height: {die_height}")
        print(f"📦 Raw: {raw_payload[:100]}...")
        print("🟢"*35 + "\n")
    
    else:
        # Regular message (COUNT format or other)
        # Check if it's a COUNT-like message
        if len(raw_payload) > 20 and ' 2' in raw_payload[:30]:
            # Might be COUNT message
            try:
                parts = raw_payload.strip().split()
                if len(parts) >= 2:
                    tool_id = parts[0][:12]
                    val_str = parts[1]
                    
                    if val_str[0] == '2':  # Plant 2
                        print(f"📊 COUNT - {timestamp} | Topic: {topic[:20]} | Tool: {tool_id}... | Data: {val_str}")
            except:
                pass
        else:
            # Other message
            print(f"📨 OTHER - {timestamp} | Topic: {topic[:30]} | Payload: {raw_payload[:50]}...")


def start_test_listener():
    print("\n" + "🎯"*35)   
    print("J-MAPPING JSON MESSAGE TESTER")
    print("🎯"*35)
    print("\n📋 J-GROUP MAPPING:")
    print("   J4: Machines 1-5")
    print("   J3: Machines 6-10")
    print("   J2: Machines 11-15")
    print("   J1: Machines 16-20")
    print("   J5: Machines 41-46")
    print("\n🔍 Looking for JSON messages:")
    print("   Format: {\"client_id\": x217\", \"card\": \"...\", \"die_height\": \"588.01\"}")
    print("\n⏳ Connecting to broker...")
    
    client = mqtt.Client(client_id="j_mapping_tester", protocol=mqtt.MQTTv311)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(BROKER_HOST, BROKER_PORT, 60)
        client.loop_start()
        
        print("\n✅ Connected! Press Ctrl+C to stop\n")
        
        # Keep running
        import time
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\n⛔ STOPPING...")
        client.disconnect()
        print("✅ Disconnected\n")
    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")


if __name__ == "__main__":
    start_test_listener()
