# backend/test_db_connection.py - NEW FILE FOR DB TEST
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'operator_app.settings')
django.setup()

from django.db import connection
from django.db.utils import OperationalError

def test_database_connection():
    print("=== DATABASE CONNECTION TEST ===")
    
    try:
        # Test basic connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print("✅ PostgreSQL Connection: SUCCESS")
            
            # Test plant2_data table exists
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name = 'plant2_data'
            """)
            table_exists = cursor.fetchone()
            
            if table_exists:
                print("✅ plant2_data table: EXISTS")
                
                # Show table structure
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'plant2_data'
                    ORDER BY ordinal_position
                """)
                columns = cursor.fetchall()
                print("📋 Table Structure:")
                for col in columns:
                    print(f"   {col[0]} ({col[1]})")
                    
                # Count existing records
                cursor.execute("SELECT COUNT(*) FROM plant2_data")
                count = cursor.fetchone()[0]
                print(f"📊 Existing records: {count}")
                
            else:
                print("❌ plant2_data table: NOT FOUND")
                
                # Show all tables
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                tables = cursor.fetchall()
                print("📋 Available tables:")
                for table in tables:
                    print(f"   {table[0]}")
            
    except OperationalError as e:
        print(f"❌ Database Connection FAILED: {e}")
        print("\n🔧 Possible solutions:")
        print("1. Check if PostgreSQL server is running on 192.168.0.35")
        print("2. Check if database 'Atomone' exists")
        print("3. Check if user 'postgres' has correct password 'atomone'")
        print("4. Check if port 5432 is accessible")
        
    except Exception as e:
        print(f"❌ Other Error: {e}")

if __name__ == "__main__":
    test_database_connection()



# @never_cache
# @api_view(['GET'])
# def plant2_live(request):
#     """
#     Plant 2 - LIVE DASHBOARD DATA
    
#     ✅ SEPARATE logic for:
#       - PRODUCING machines: use RUNNING idle
#       - ON-BUT-IDLE machines: use ON-BUT-IDLE ideal time
#       - Not both!
#     """
#     try:
#         from apps.machines.machine_state import MACHINE_STATE
#         from apps.mqtt.simple_plant2 import (
#             EXACT_REQUIREMENT_STATE as PLANT2_EXACT_REQUIREMENT_STATE,
#             RUNNING_IDLE_CALCULATOR,
#             ON_BUT_IDLE_TRACKER,
#             TOPIC_MACHINE_MAPPING,
#             get_machine_group
#         )
        
#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)
#         all_mapped_machines = sorted(list(all_mapped_machines))
        
#         live_machines = MACHINE_STATE.summarize(plant_filter=2, stale_after_seconds=300)
        
#         enhanced_machines = []
#         problem_machines = []
        
#         for machine_no in all_mapped_machines:
#             machine_data = None
            
#             for m in live_machines:
#                 if m['machine_no'] == machine_no and m.get('plant') == 2:
#                     machine_data = m
#                     break
            
#             try:
#                 exact_data = PLANT2_EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)
                
#                 is_on = exact_data.get('machine_on', False)
#                 is_producing = exact_data.get('is_producing', False)
                
#                 if is_on and not machine_data:
#                     tool_id = exact_data.get('current_tool_id', 'N/A')
#                     shut_height = exact_data.get('current_shut_height', 0.0)
                    
#                     machine_data = {
#                         'plant': 2,
#                         'machine_no': machine_no,
#                         'tool_id': tool_id,
#                         'count': 0,
#                         'shut_height': shut_height if shut_height != 0.0 else 'No data',
#                         'last_seen': 'JSON only',
#                         'status': 'ON (No Count)',
#                         'current_hour_count': 0,
#                         'last_hour_count': 0,
#                         'cumulative_count': 0,
#                         'shift': exact_data.get('shift', 'A'),
#                         'idle_time': 0
#                     }
                
#                 if machine_data:
#                     machine_data.update(exact_data)
                    
#                     problem_detected = is_on and not is_producing
#                     machine_data['problem_detected'] = problem_detected
                    
#                     if problem_detected:
#                         problem_machines.append(machine_no)
                    
#                     # ✅ PERFECT IDLE & COUNT LOGIC
#                     with PLANT2_EXACT_REQUIREMENT_STATE.lock:
#                         ist_tz = pytz.timezone('Asia/Kolkata')
#                         now_ist = datetime.now(ist_tz)
                        
#                         # Last activity time
#                         if machine_no in PLANT2_EXACT_REQUIREMENT_STATE.last_count_time:
#                             last_count = PLANT2_EXACT_REQUIREMENT_STATE.last_count_time[machine_no]
#                             machine_data['last_activity'] = last_count.strftime('%H:%M:%S')
#                         else:
#                             machine_data['last_activity'] = 'Never'
                        
#                         # ===== GET BOTH IDLE TYPES =====
#                         running_idle_status = RUNNING_IDLE_CALCULATOR.get_idle_status(machine_no, now_ist)
#                         on_but_idle_times = ON_BUT_IDLE_TRACKER.get_total_ideal_time(machine_no, now_ist)
                        
#                         # ===== DECIDE WHICH TO USE =====
#                         # KEY: Only ONE should be displayed based on status
                        
#                         if is_producing:
#                             # ✅ PRODUCING MACHINE - USE RUNNING IDLE ONLY
#                             idle_display = max(0, min(60, running_idle_status['total_idle']))
#                             idle_type = 'RUNNING'
#                             live_idle_display = running_idle_status['live_idle']
#                             accumulated_idle_display = running_idle_status['accumulated_idle']
                            
#                             # ✅ Set ON-BUT-IDLE to 0 for producing machines
#                             machine_data.update({
#                                 'running_live_idle': max(0, running_idle_status['live_idle']),
#                                 'running_accumulated_idle': max(0, running_idle_status['accumulated_idle']),
#                                 'running_total_idle': max(0, running_idle_status['total_idle']),
#                                 'running_is_idle': running_idle_status['is_idle'],
#                                 'running_grace_active': running_idle_status['grace_period_active'],
                                
#                                 # ❌ ZERO OUT ON-BUT-IDLE for producing machines
#                                 'on_but_idle_live': 0,
#                                 'on_but_idle_accumulated': 0,
#                                 'on_but_idle_total': 0,
                                
#                                 'display_idle_type': idle_type,
#                                 'display_idle_minutes': idle_display,
#                                 'live_idle_time': f"{live_idle_display}m" if idle_display > 0 else "0m",
#                                 'hourly_idle_total': idle_display,
#                                 'idle_time': idle_display,
#                                 'is_idle': idle_display > 0
#                             })
                        
#                         elif is_on and not is_producing:
#                             # ✅ ON-BUT-IDLE MACHINE - USE ON-BUT-IDLE IDEAL TIME ONLY
#                             idle_display = max(0, min(60, on_but_idle_times['total_ideal']))
#                             idle_type = 'ON_BUT_IDLE'
#                             live_ideal_display = on_but_idle_times['live_ideal']
#                             accumulated_ideal_display = on_but_idle_times['accumulated_ideal']
                            
#                             # ✅ Set RUNNING IDLE to 0 for on-but-idle machines
#                             machine_data.update({
#                                 # ❌ ZERO OUT RUNNING IDLE for on-but-idle machines
#                                 'running_live_idle': 0,
#                                 'running_accumulated_idle': 0,
#                                 'running_total_idle': 0,
#                                 'running_is_idle': False,
#                                 'running_grace_active': False,
                                
#                                 # ✅ USE ON-BUT-IDLE for these machines
#                                 'on_but_idle_live': max(0, on_but_idle_times['live_ideal']),
#                                 'on_but_idle_accumulated': max(0, on_but_idle_times['accumulated_ideal']),
#                                 'on_but_idle_total': max(0, on_but_idle_times['total_ideal']),
                                
#                                 'display_idle_type': idle_type,
#                                 'display_idle_minutes': idle_display,
#                                 'live_idle_time': f"{live_ideal_display}m" if idle_display > 0 else "0m",
#                                 'hourly_idle_total': idle_display,
#                                 'idle_time': idle_display,
#                                 'is_idle': idle_display > 0
#                             })
                        
#                         else:
#                             # ✅ MACHINE OFF - NO IDLE
#                             machine_data.update({
#                                 'running_live_idle': 0,
#                                 'running_accumulated_idle': 0,
#                                 'running_total_idle': 0,
#                                 'running_is_idle': False,
#                                 'running_grace_active': False,
                                
#                                 'on_but_idle_live': 0,
#                                 'on_but_idle_accumulated': 0,
#                                 'on_but_idle_total': 0,
                                
#                                 'display_idle_type': 'OFF',
#                                 'display_idle_minutes': 0,
#                                 'live_idle_time': '0m',
#                                 'hourly_idle_total': 0,
#                                 'idle_time': 0,
#                                 'is_idle': False
#                             })
                        
#             except Exception as e:
#                 print(f"⚠️ M{machine_no} error: {e}")
#                 import traceback
#                 traceback.print_exc()
#                 if not machine_data:
#                     exact_data = {}
            
#             if machine_data:
#                 tool_id = machine_data.get('tool_id', '')
#                 tool_info = get_tool_info_from_tid_map(tool_id)
                
#                 machine_data.update({
#                     'machine_group': get_machine_group(machine_no),
#                     'tool_customer': tool_info.get('customer', 'N/A'),
#                     'tool_model': tool_info.get('model', 'N/A'),
#                     'tool_part_name': tool_info.get('part_name', 'N/A'),
#                     'tool_name': tool_info.get('tool_name', 'N/A'),
#                     'tool_part_number': tool_info.get('part_number', 'N/A'),
#                     'tool_tpm': tool_info.get('tpm', 0),
#                     'tool_epc': tool_info.get('epc', 'N/A')
#                 })
                
#                 machine_data['plant'] = 2
#                 enhanced_machines.append(machine_data)
                
#             else:
#                 enhanced_machines.append({
#                     "plant": 2,
#                     "machine_no": machine_no,
#                     "machine_group": get_machine_group(machine_no),
#                     "tool_id": f"PLANT2_M{machine_no:02d}",
#                     "count": 0,
#                     "shut_height": "Waiting for data",
#                     "last_seen": "Not active",
#                     "status": "Ready",
#                     "current_hour_count": 0,
#                     "last_hour_count": 0,
#                     "cumulative_count": 0,
#                     "shift": "A",
#                     "is_idle": False,
#                     "live_idle_time": "0m",
#                     "hourly_idle_total": 0,
#                     "idle_time": 0,
#                     "last_activity": "Never",
#                     'tool_customer': 'N/A',
#                     'tool_model': 'N/A',
#                     'tool_part_name': 'N/A',
#                     'tool_name': 'N/A',
#                     'tool_part_number': 'N/A',
#                     'tool_tpm': 0,
#                     'tool_epc': 'N/A',
#                     'machine_on': False,
#                     'is_producing': False,
#                     'problem_detected': False,
#                     'on_since': None,
#                     'on_duration_minutes': None,
#                     'first_count_at': None,
#                     'time_to_first_count': None,
#                     'running_live_idle': 0,
#                     'running_accumulated_idle': 0,
#                     'running_total_idle': 0,
#                     'on_but_idle_live': 0,
#                     'on_but_idle_accumulated': 0,
#                     'on_but_idle_total': 0,
#                     'display_idle_type': 'OFF',
#                     'display_idle_minutes': 0
#                 })
        
#         enhanced_machines.sort(key=lambda x: x['machine_no'])
        
#         on_machines = [m for m in enhanced_machines if m.get('machine_on')]
#         producing_machines = [m for m in enhanced_machines if m.get('is_producing')]
        
#         groups_summary = {}
#         for group in ['J1', 'J2', 'J3', 'J4', 'J5']:
#             group_machines = [m for m in enhanced_machines if m.get('machine_group') == group]
#             group_on = [m for m in group_machines if m.get('machine_on')]
#             group_producing = [m for m in group_machines if m.get('is_producing')]
#             group_problems = [m for m in group_machines if m.get('problem_detected')]
            
#             groups_summary[group] = {
#                 'total': len(group_machines),
#                 'on': len(group_on),
#                 'producing': len(group_producing),
#                 'problems': len(group_problems)
#             }
        
#         response = Response({
#             "success": True,
#             "total_machines": len(enhanced_machines),
#             "on_count": len(on_machines),
#             "producing_count": len(producing_machines),
#             "problem_count": len(problem_machines),
#             "problem_machines": problem_machines,
#             "groups_summary": groups_summary,
#             "machines": enhanced_machines,
#             "plant": 2,
#             "message": f"Plant 2 - ON:{len(on_machines)} | Producing:{len(producing_machines)} | Problems:{len(problem_machines)}"
#         })
        
#         response['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
#         response['Pragma'] = 'no-cache'
#         response['Expires'] = '0'
        
#         return response
        
#     except Exception as e:
#         print(f"❌ API ERROR: {e}")
#         import traceback
#         traceback.print_exc()
        
#         error_response = Response({
#             "success": False,
#             "error": str(e),
#             "machines": [],
#             "plant": 2
#         }, status=500)
        
#         error_response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
#         error_response['Pragma'] = 'no-cache'
#         error_response['Expires'] = '0'
        
#         return error_response


# Replace your plant2_live API function