# # backend/operator_app/apps.py - REVERT TO WORKING VERSION
# from django.apps import AppConfig
# import os
# import sys


# class OperatorAppConfig(AppConfig):
#     default_auto_field = 'django.db.models.BigAutoField'
#     name = 'operator_app'
    
#     def ready(self):
#         """Auto-start MQTT clients when Django starts"""
        
#         if 'runserver' not in sys.argv or os.environ.get('RUN_MAIN') != 'true':
#             return
            
#         print("🚀 Django Apps Ready - Starting Plant MQTT clients...")
        
#         # Start Plant 2 MQTT - WORKING
#         try:
#             from apps.mqtt.simple_plant2 import start_plant2_mqtt
#             start_plant2_mqtt()
#             print("✅ Plant 2 MQTT client started")
            
#         except ImportError as e:
#             print(f"⚠️ Plant 2 import error: {e}")
#         except Exception as e:
#             print(f"❌ Plant 2 MQTT error: {e}")
        
#         # Start Plant 1 MQTT - WORKING
#         try:
#             from apps.mqtt.simple_plant1 import start_plant1_mqtt
#             start_plant1_mqtt()
#             print("✅ Plant 1 MQTT client started")
            
#         except ImportError as e:
#             print(f"⚠️ Plant 1 import error: {e}")
#         except Exception as e:
#             print(f"❌ Plant 1 MQTT error: {e}")
            
#         print("🎯 MQTT clients initialization completed")



# backend/operator_app/apps.py

from django.apps import AppConfig
import os
import sys
import threading


_MQTT_STARTED = False
_MQTT_LOCK = threading.Lock()


class OperatorAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'operator_app'

    def ready(self):
        """
        Auto-start MQTT clients safely when Django starts.
        Prevents duplicate MQTT starts caused by Django autoreload.
        """

        global _MQTT_STARTED

        # In commands ke time MQTT start nahi karna
        skip_commands = [
            "makemigrations",
            "migrate",
            "collectstatic",
            "shell",
            "createsuperuser",
            "check",
            "test",
        ]

        if any(cmd in sys.argv for cmd in skip_commands):
            return

        # Sirf runserver ke time MQTT start karo
        if "runserver" not in sys.argv:
            return

        # Normal runserver me parent process ko skip karo
        # --noreload me RUN_MAIN absent hota hai, isliye allow karo
        is_noreload = "--noreload" in sys.argv

        if not is_noreload and os.environ.get("RUN_MAIN") != "true":
            return

        # Same process me duplicate start prevent karo
        with _MQTT_LOCK:
            if _MQTT_STARTED:
                print("⚠️ MQTT already started, skipping duplicate start")
                return

            _MQTT_STARTED = True

        print("🚀 Django Apps Ready - Starting Plant MQTT clients...")

        def start_mqtt_clients():
            # Start Plant 2 MQTT
            try:
                from apps.mqtt.simple_plant2 import start_plant2_mqtt
                start_plant2_mqtt()
                print("✅ Plant 2 MQTT client started")

            except ImportError as e:
                print(f"⚠️ Plant 2 import error: {e}")
            except Exception as e:
                print(f"❌ Plant 2 MQTT error: {e}")

            # Start Plant 1 MQTT
            try:
                from apps.mqtt.simple_plant1 import start_plant1_mqtt
                start_plant1_mqtt()
                print("✅ Plant 1 MQTT client started")

            except ImportError as e:
                print(f"⚠️ Plant 1 import error: {e}")
            except Exception as e:
                print(f"❌ Plant 1 MQTT error: {e}")

            print("🎯 MQTT clients initialization completed")

        threading.Thread(
            target=start_mqtt_clients,
            daemon=True,
            name="plant-mqtt-starter"
        ).start()
