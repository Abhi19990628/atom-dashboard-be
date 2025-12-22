# backend/test_force_insert.py - FORCE TEST INSERT
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'operator_app.settings')
django.setup()

from existing_table_saver import EXISTING_TABLE_SAVER

def force_test_insert():
    print("=== FORCE TEST INSERT ===")
    
    # Force insert test data
    EXISTING_TABLE_SAVER.insert_data(
        machine_no=999,
        plant_no=2,
        tool_id="TEST123456789012345678901234",
        shut_height=500.0,
        hourly_count=1,
        idle_minutes=0
    )
    
    print("Test insert completed!")

if __name__ == "__main__":
    force_test_insert()
