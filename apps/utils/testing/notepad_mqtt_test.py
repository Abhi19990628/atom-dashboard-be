import paho.mqtt.client as mqtt
import time

BROKER = "192.168.0.35"
USER = "npdAtom"
PASS = "npd@Atom"

TOPICS = [
    "COUNT", "COUNT1", "COUNT2", "COUNT3", "COUNT4", "COUNT52",
    "COUNT16", "COUNT17", "COUNT18", "COUNT19",
    "J1", "J2", "J3", "J4", "J5", "J6", "J7", "J8", "J9"
]

def on_connect(client, userdata, flags, rc):
    print("connected rc =", rc)
    for topic in TOPICS:
        client.subscribe(topic)
        print("subscribed:", topic)

def on_message(client, userdata, msg):
    payload = msg.payload.decode(errors="ignore")
    print(time.strftime("%H:%M:%S"), msg.topic, payload[:120])

client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.on_connect = on_connect
client.on_message = on_message

print("Connecting to MQTT broker...")
client.connect(BROKER, 1883, 60)

client.loop_start()
print("Listening for 60 seconds...")
time.sleep(60)
client.loop_stop()
client.disconnect()

print("Done.")