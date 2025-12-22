# backend/operator_app/apps.py - REVERT TO WORKING VERSION
from django.apps import AppConfig
import os
import sys


class OperatorAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'operator_app'
    
    def ready(self):
        """Auto-start MQTT clients when Django starts"""
        
        if 'runserver' not in sys.argv or os.environ.get('RUN_MAIN') != 'true':
            return
            
        print("🚀 Django Apps Ready - Starting Plant MQTT clients...")
        
        # Start Plant 2 MQTT - WORKING
        try:
            from apps.mqtt.simple_plant2 import start_plant2_mqtt
            start_plant2_mqtt()
            print("✅ Plant 2 MQTT client started")
            
        except ImportError as e:
            print(f"⚠️ Plant 2 import error: {e}")
        except Exception as e:
            print(f"❌ Plant 2 MQTT error: {e}")
        
        # Start Plant 1 MQTT - WORKING
        try:
            from apps.mqtt.simple_plant1 import start_plant1_mqtt
            start_plant1_mqtt()
            print("✅ Plant 1 MQTT client started")
            
        except ImportError as e:
            print(f"⚠️ Plant 1 import error: {e}")
        except Exception as e:
            print(f"❌ Plant 1 MQTT error: {e}")
            
        print("🎯 MQTT clients initialization completed")
