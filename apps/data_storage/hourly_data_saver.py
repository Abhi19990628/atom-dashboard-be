# # apps/datastorage/hourly_data_saver.py
# """
# ✅ PERFECT HOURLY DATA SAVER - UPDATED
# - Saves data EXACTLY at hour boundary (HH:00:00)
# - ONE row per machine per hour - NO duplicates
# - Inserts into Plant2_data table directly
# - count_total = hourly count
# - idle_total_minutes = total idle with 3-min grace
# """

# from datetime import datetime, timedelta
# from threading import RLock, Thread
# import pytz
# import time as timemodule
# from django.db import connection, transaction
# from collections import defaultdict
# import traceback

# IST = pytz.timezone('Asia/Kolkata')


# class HourlyDataSaver:
#     """
#     ✅ Saves complete hourly data for all machines:
#     - count_total (total counts in hour)
#     - idle_total_minutes (total idle with 3-min grace)
#     - Inserts into Plant2_data table
#     """
    
#     def __init__(self):
#         self.lock = RLock()
        
#         # Per-machine data (reset every hour)
#         self.machine_data = {}  # {machine_no: {...}}
        
#         # Scheduler running flag
#         self.scheduler_running = False
    
#     @staticmethod
#     def get_hour_key(dt: datetime) -> str:
#         """Get hour key: 2025-11-01-14 (for 14:00-14:59)"""
#         dt = dt.astimezone(IST)
#         return dt.strftime('%Y-%m-%d-%H')
    
#     @staticmethod
#     def get_hour_boundaries(dt: datetime):
#         """Get hour start and end"""
#         dt = dt.astimezone(IST)
#         hour_start = dt.replace(minute=0, second=0, microsecond=0)
#         hour_end = hour_start.replace(minute=59, second=59, microsecond=999999)
#         return hour_start, hour_end
    
#     def ensure_machine_initialized(self, machine_no: int):
#         """Initialize machine data if not exists"""
#         if machine_no not in self.machine_data:
#             self.machine_data[machine_no] = {
#                 'count_total': 0,
#                 'idle_total_minutes': 0,
#                 'on_since': None,
#                 'first_count_at': None,
#                 'last_count_at': None,
#                 'last_json_at': None,
#                 'status_summary': 'OFF_NO_SIGNAL',
#                 'is_on': False,
#                 'had_count': False,
#                 'tool_id': 'UNKNOWN',
#                 'shut_height': 0.0
#             }
    
#     def record_count(self, machine_no: int, dt: datetime = None):
#         """Record a COUNT event"""
#         if dt is None:
#             dt = datetime.now(IST)
        
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
            
#             # First COUNT in hour
#             if self.machine_data[machine_no]['first_count_at'] is None:
#                 self.machine_data[machine_no]['first_count_at'] = dt
            
#             # Always update last count
#             self.machine_data[machine_no]['last_count_at'] = dt
#             self.machine_data[machine_no]['had_count'] = True
            
#             # Increment count total ✅
#             self.machine_data[machine_no]['count_total'] += 1
            
#             # Update status
#             if self.machine_data[machine_no]['is_on']:
#                 self.machine_data[machine_no]['status_summary'] = 'PRODUCING'
    
#     def record_machine_on(self, machine_no: int, dt: datetime = None):
#         """Record machine turned ON"""
#         if dt is None:
#             dt = datetime.now(IST)
        
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
            
#             # First ON in hour
#             if self.machine_data[machine_no]['on_since'] is None:
#                 self.machine_data[machine_no]['on_since'] = dt
            
#             self.machine_data[machine_no]['is_on'] = True
            
#             # Update status
#             if self.machine_data[machine_no]['had_count']:
#                 self.machine_data[machine_no]['status_summary'] = 'PRODUCING'
#             else:
#                 self.machine_data[machine_no]['status_summary'] = 'ON_NO_COUNT'
    
#     def record_json_event(self, machine_no: int, dt: datetime = None):
#         """Record JSON heartbeat"""
#         if dt is None:
#             dt = datetime.now(IST)
        
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             self.machine_data[machine_no]['last_json_at'] = dt
    
#     def record_machine_off(self, machine_no: int):
#         """Record machine turned OFF"""
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             self.machine_data[machine_no]['is_on'] = False
#             self.machine_data[machine_no]['status_summary'] = 'OFF_NO_SIGNAL'
    
#     def set_idle_for_machine(self, machine_no: int, idle_minutes: int):
#         """Set computed idle minutes for machine (from StrictIdlePolicy)"""
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             self.machine_data[machine_no]['idle_total_minutes'] = idle_minutes
    
#     def set_tool_info(self, machine_no: int, tool_id: str, shut_height: float):
#         """Set tool info for machine"""
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             self.machine_data[machine_no]['tool_id'] = tool_id if tool_id else 'UNKNOWN'
#             self.machine_data[machine_no]['shut_height'] = shut_height
    
#     def get_machine_snapshot(self, machine_no: int) -> dict:
#         """Get current snapshot for a machine"""
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             return dict(self.machine_data[machine_no])
    
#     def get_all_snapshots(self) -> dict:
#         """Get all machines' snapshots"""
#         with self.lock:
#             return {m_no: dict(data) for m_no, data in self.machine_data.items()}
    
#     @transaction.atomic
#     def save_snapshots_to_database(self, plant_id: int, hour_key: str, snapshots: dict) -> dict:
#         """
#         ✅ UPDATED - Save to Plant2_data table
        
#         count = hourly count
#         idle_time = total idle minutes with 3-min grace
#         """
        
#         results = {
#             'inserted': 0,
#             'updated': 0,
#             'errors': 0
#         }
        
#         now = datetime.now(IST)
#         hour_start, hour_end = self.get_hour_boundaries(now)
        
#         try:
#             with connection.cursor() as cursor:
                
#                 for machine_no, data in snapshots.items():
#                     try:
#                         # ✅ Get last cumulative from database
#                         cursor.execute(
#                             """
#                             SELECT cumulative_count FROM Plant2_data 
#                             WHERE machine_no = %s 
#                             ORDER BY timestamp DESC LIMIT 1
#                             """,
#                             [str(machine_no)]
#                         )
#                         result = cursor.fetchone()
#                         last_cumulative = result[0] if result else 0
                        
#                         # ✅ Calculate new cumulative
#                         new_cumulative = last_cumulative + data['count_total']
                        
#                         # ✅ Get tool_id and shut_height
#                         tool_id = data.get('tool_id', 'UNKNOWN')
#                         shut_height = data.get('shut_height', 0.0)
                        
#                         # Clean shut_height
#                         if isinstance(shut_height, str):
#                             try:
#                                 shut_height = float(shut_height)
#                             except:
#                                 shut_height = 0.0
                        
#                         # ✅ Prepare timestamp (hour start)
#                         naive_timestamp = hour_start.replace(tzinfo=None, microsecond=0)
                        
#                         # ✅ INSERT into Plant2_data
#                         cursor.execute(
#                             """
#                             INSERT INTO Plant2_data 
#                             (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#                             """,
#                             [
#                                 naive_timestamp,                # timestamp = hour start (17:00:00)
#                                 str(tool_id),                   # tool_id
#                                 str(machine_no),                # machine_no
#                                 data['count_total'],            # count = ✅ HOURLY COUNT
#                                 new_cumulative,                 # cumulative_count
#                                 0,                              # tpm
#                                 data['idle_total_minutes'],     # idle_time = ✅ TOTAL IDLE
#                                 shut_height,                    # shut_height
#                                 'A'                             # shift
#                             ]
#                         )
#                         results['inserted'] += 1
#                         print(f"✅ M{machine_no}: count={data['count_total']} (hourly), idle={data['idle_total_minutes']}min (total)")
                        
#                     except Exception as e:
#                         print(f"❌ M{machine_no}: {e}")
#                         traceback.print_exc()
#                         results['errors'] += 1
        
#         except Exception as e:
#             print(f"❌ Database error: {e}")
#             traceback.print_exc()
#             raise
        
#         return results
    
#     def reset_for_new_hour(self):
#         """Reset all data for new hour"""
#         with self.lock:
#             self.machine_data.clear()
    
#     def start_hourly_scheduler(self, plant_id: int):
#         """Start automatic hourly save scheduler"""
        
#         if self.scheduler_running:
#             print("⚠️ Scheduler already running!")
#             return
        
#         self.scheduler_running = True
        
#         def scheduler():
#             while self.scheduler_running:
#                 try:
#                     now = datetime.now(IST)
                    
#                     # Calculate next hour boundary
#                     next_hour = (now.replace(minute=0, second=0, microsecond=0) + 
#                                 timedelta(hours=1))
#                     wait_seconds = (next_hour - now).total_seconds()
                    
#                     print(f"⏳ Next hourly save in {wait_seconds:.0f} seconds...")
                    
#                     # Sleep until hour boundary
#                     timemodule.sleep(wait_seconds)
                    
#                     # Now save!
#                     self.execute_hourly_save(plant_id)
                    
#                 except Exception as e:
#                     print(f"❌ Scheduler error: {e}")
#                     traceback.print_exc()
#                     timemodule.sleep(60)
        
#         thread = Thread(target=scheduler, daemon=True)
#         thread.start()
#         print("✅ Hourly save scheduler started!")
    
#     def execute_hourly_save(self, plant_id: int):
#         """Execute hourly save NOW"""
        
#         now = datetime.now(IST)
#         hour_key = self.get_hour_key(now)
        
#         print("\n" + "🔥" * 40)
#         print(f"💾 HOURLY DATA SAVE - {now.strftime('%Y-%m-%d %H:%M:%S')}")
#         print(f"📊 Saving hour: {hour_key}")
#         print("🔥" * 40)
        
#         # Get all snapshots
#         snapshots = self.get_all_snapshots()
        
#         if not snapshots:
#             print("⚠️ No machine data to save")
#             self.reset_for_new_hour()
#             return
        
#         # Save to database
#         results = self.save_snapshots_to_database(plant_id, hour_key, snapshots)
        
#         print(f"\n✅ Inserted: {results['inserted']}")
#         print(f"📝 Updated: {results['updated']}")
#         print(f"❌ Errors: {results['errors']}")
#         print("=" * 80 + "\n")
        
#         # Reset for new hour
#         self.reset_for_new_hour()


# # ✅ Global instance
# HOURLY_DATA_SAVER = HourlyDataSaver()



# # apps/data_storage/hourly_data_saver.py
# """
# ✅ PERFECT HOURLY DATA SAVER - FINAL VERSION
# - timestamp = REAL machine activity time (not hour_start!)
# - idle_time = REAL idle from StrictIdlePolicy (with 3-min grace)
# - count = hourly count
# - Full debug logging for troubleshooting
# """

# from datetime import datetime, timedelta
# from threading import RLock, Thread
# import pytz
# import time as timemodule
# from django.db import connection, transaction
# from collections import defaultdict
# import traceback

# IST = pytz.timezone('Asia/Kolkata')


# class HourlyDataSaver:
#     """
#     ✅ Saves complete hourly data for all machines:
#     - timestamp = first activity time (ON or first COUNT)
#     - count_total (total counts in hour)
#     - idle_total_minutes (REAL idle from StrictIdlePolicy)
#     - Inserts into Plant2_data table
#     """
    
#     def __init__(self):
#         self.lock = RLock()
#         self.machine_data = {}
#         self.scheduler_running = False
#         print("✅ HourlyDataSaver initialized")
    
#     @staticmethod
#     def get_hour_key(dt: datetime) -> str:
#         """Get hour key: 2025-11-03-10"""
#         dt = dt.astimezone(IST)
#         return dt.strftime('%Y-%m-%d-%H')
    
#     @staticmethod
#     def get_hour_boundaries(dt: datetime):
#         """Get hour start and end"""
#         dt = dt.astimezone(IST)
#         hour_start = dt.replace(minute=0, second=0, microsecond=0)
#         hour_end = hour_start.replace(minute=59, second=59, microsecond=999999)
#         return hour_start, hour_end
    
#     def ensure_machine_initialized(self, machine_no: int):
#         """Initialize machine data if not exists"""
#         if machine_no not in self.machine_data:
#             self.machine_data[machine_no] = {
#                 'count_total': 0,
#                 'idle_total_minutes': 0,
#                 'on_since': None,
#                 'first_count_at': None,
#                 'last_count_at': None,
#                 'last_json_at': None,
#                 'status_summary': 'OFF_NO_SIGNAL',
#                 'is_on': False,
#                 'had_count': False,
#                 'tool_id': 'UNKNOWN',
#                 'shut_height': 0.0
#             }
    
#     def record_count(self, machine_no: int, dt: datetime = None):
#         """Record a COUNT event"""
#         if dt is None:
#             dt = datetime.now(IST)
        
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
            
#             # First COUNT in hour
#             if self.machine_data[machine_no]['first_count_at'] is None:
#                 self.machine_data[machine_no]['first_count_at'] = dt
            
#             # Always update last count
#             self.machine_data[machine_no]['last_count_at'] = dt
#             self.machine_data[machine_no]['had_count'] = True
            
#             # Increment count total
#             self.machine_data[machine_no]['count_total'] += 1
            
#             # Update status
#             if self.machine_data[machine_no]['is_on']:
#                 self.machine_data[machine_no]['status_summary'] = 'PRODUCING'
    
#     def record_machine_on(self, machine_no: int, dt: datetime = None):
#         """Record machine turned ON"""
#         if dt is None:
#             dt = datetime.now(IST)
        
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
            
#             # First ON in hour
#             if self.machine_data[machine_no]['on_since'] is None:
#                 self.machine_data[machine_no]['on_since'] = dt
            
#             self.machine_data[machine_no]['is_on'] = True
            
#             # Update status
#             if self.machine_data[machine_no]['had_count']:
#                 self.machine_data[machine_no]['status_summary'] = 'PRODUCING'
#             else:
#                 self.machine_data[machine_no]['status_summary'] = 'ON_NO_COUNT'
    
#     def record_json_event(self, machine_no: int, dt: datetime = None):
#         """Record JSON heartbeat"""
#         if dt is None:
#             dt = datetime.now(IST)
        
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             self.machine_data[machine_no]['last_json_at'] = dt
    
#     def record_machine_off(self, machine_no: int):
#         """Record machine turned OFF"""
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             self.machine_data[machine_no]['is_on'] = False
#             self.machine_data[machine_no]['status_summary'] = 'OFF_NO_SIGNAL'
    
#     def set_idle_for_machine(self, machine_no: int, idle_minutes: int):
#         """
#         ✅ Set computed idle minutes for machine (from StrictIdlePolicy)
#         This is called before saving to database
#         """
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             old_idle = self.machine_data[machine_no]['idle_total_minutes']
#             self.machine_data[machine_no]['idle_total_minutes'] = idle_minutes
            
#             # Debug log
#             if idle_minutes != old_idle:
#                 print(f"  📝 M{machine_no}: idle updated {old_idle} → {idle_minutes}min")
    
#     def set_tool_info(self, machine_no: int, tool_id: str, shut_height: float):
#         """Set tool info for machine"""
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             self.machine_data[machine_no]['tool_id'] = tool_id if tool_id else 'UNKNOWN'
#             self.machine_data[machine_no]['shut_height'] = shut_height
    
#     def get_machine_snapshot(self, machine_no: int) -> dict:
#         """Get current snapshot for a machine"""
#         with self.lock:
#             self.ensure_machine_initialized(machine_no)
#             return dict(self.machine_data[machine_no])
    
#     def get_all_snapshots(self) -> dict:
#         """Get all machines' snapshots"""
#         with self.lock:
#             return {m_no: dict(data) for m_no, data in self.machine_data.items()}
    
#     @transaction.atomic
#     def save_snapshots_to_database(self, plant_id: int, hour_key: str, snapshots: dict) -> dict:
#         """
#         ✅ Save to Plant2_data table with REAL timestamp and idle
        
#         Fields saved:
#         - timestamp: REAL machine activity time (first_count_at > on_since > hour_start)
#         - tool_id: Machine tool ID
#         - machine_no: Machine number
#         - count: Hourly count total
#         - cumulative_count: Running cumulative (last_cumulative + count)
#         - tpm: Always 0
#         - idle_time: REAL idle minutes from StrictIdlePolicy (with 3-min grace)
#         - shut_height: Shut height value
#         - shift: Always 'A'
#         """
        
#         results = {
#             'inserted': 0,
#             'updated': 0,
#             'errors': 0
#         }
        
#         now = datetime.now(IST)
#         hour_start, hour_end = self.get_hour_boundaries(now)
        
#         print(f"\n💾 Saving {len(snapshots)} machines to Plant2_data table...")
#         print(f"   Hour: {hour_key}")
        
#         try:
#             with connection.cursor() as cursor:
                
#                 for machine_no, data in snapshots.items():
#                     try:
#                         # ✅ Get last cumulative count
#                         cursor.execute(
#                             """
#                             SELECT cumulative_count FROM Plant2_data 
#                             WHERE machine_no = %s 
#                             ORDER BY timestamp DESC LIMIT 1
#                             """,
#                             [str(machine_no)]
#                         )
#                         result = cursor.fetchone()
#                         last_cumulative = result[0] if result else 0
                        
#                         # Calculate new cumulative
#                         new_cumulative = last_cumulative + data['count_total']
                        
#                         # Get tool info
#                         tool_id = data.get('tool_id', 'UNKNOWN')
#                         shut_height = data.get('shut_height', 0.0)
                        
#                         # Clean shut_height
#                         if isinstance(shut_height, str):
#                             try:
#                                 shut_height = float(shut_height)
#                             except:
#                                 shut_height = 0.0
                        
#                         # ✅ CRITICAL: Use REAL timestamp
#                         # Priority: first_count_at > on_since > hour_start
#                         real_timestamp = None
#                         timestamp_source = 'hour_start'
                        
#                         if data.get('first_count_at'):
#                             real_timestamp = data['first_count_at']
#                             timestamp_source = 'first_count'
#                         elif data.get('on_since'):
#                             real_timestamp = data['on_since']
#                             timestamp_source = 'on_since'
#                         else:
#                             real_timestamp = hour_start
#                             timestamp_source = 'hour_start (offline)'
                        
#                         # Convert to naive for database
#                         if real_timestamp.tzinfo:
#                             naive_timestamp = real_timestamp.replace(tzinfo=None, microsecond=0)
#                         else:
#                             naive_timestamp = real_timestamp.replace(microsecond=0)
                        
#                         # ✅ Get REAL idle time (already set by StrictIdlePolicy)
#                         idle_minutes = data['idle_total_minutes']
                        
#                         # ✅ Debug log
#                         print(f"  M{machine_no:2d}: ts={naive_timestamp.strftime('%H:%M:%S')} ({timestamp_source}), count={data['count_total']:3d}, idle={idle_minutes:2d}min, cumul={new_cumulative}")
                        
#                         # ✅ INSERT into Plant2_data
#                         cursor.execute(
#                             """
#                             INSERT INTO Plant2_data 
#                             (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#                             """,
#                             [
#                                 naive_timestamp,                # REAL timestamp
#                                 str(tool_id),
#                                 str(machine_no),
#                                 data['count_total'],            # hourly count
#                                 new_cumulative,                 # cumulative
#                                 0,                              # tpm
#                                 idle_minutes,                   # REAL idle from StrictIdlePolicy
#                                 shut_height,
#                                 'A'                             # shift
#                             ]
#                         )
#                         results['inserted'] += 1
                        
#                     except Exception as e:
#                         print(f"  ❌ M{machine_no}: {e}")
#                         traceback.print_exc()
#                         results['errors'] += 1
        
#         except Exception as e:
#             print(f"❌ Database connection error: {e}")
#             traceback.print_exc()
#             raise
        
#         return results
    
#     def reset_for_new_hour(self):
#         """Reset all data for new hour"""
#         with self.lock:
#             machine_count = len(self.machine_data)
#             self.machine_data.clear()
#             print(f"🔄 Reset {machine_count} machines for new hour")
    
#     def start_hourly_scheduler(self, plant_id: int):
#         """Start automatic hourly save scheduler (NOT USED - manual trigger)"""
        
#         if self.scheduler_running:
#             print("⚠️ Scheduler already running!")
#             return
        
#         self.scheduler_running = True
        
#         def scheduler():
#             while self.scheduler_running:
#                 try:
#                     now = datetime.now(IST)
#                     next_hour = (now.replace(minute=0, second=0, microsecond=0) + 
#                                 timedelta(hours=1))
#                     wait_seconds = (next_hour - now).total_seconds()
                    
#                     print(f"⏳ Next hourly save in {wait_seconds:.0f} seconds (at {next_hour.strftime('%H:%M')})")
                    
#                     timemodule.sleep(wait_seconds)
#                     self.execute_hourly_save(plant_id)
                    
#                 except Exception as e:
#                     print(f"❌ Scheduler error: {e}")
#                     traceback.print_exc()
#                     timemodule.sleep(60)
        
#         thread = Thread(target=scheduler, daemon=True)
#         thread.start()
#         print("✅ Hourly save scheduler started!")
    
#     def execute_hourly_save(self, plant_id: int):
#         """Execute hourly save NOW (called manually from save_worker)"""
        
#         now = datetime.now(IST)
#         hour_key = self.get_hour_key(now)
        
#         print("\n" + "🔥" * 40)
#         print(f"💾 HOURLY DATA SAVE - {now.strftime('%Y-%m-%d %H:%M:%S')}")
#         print(f"📊 Saving hour: {hour_key}")
#         print("🔥" * 40)
        
#         snapshots = self.get_all_snapshots()
        
#         if not snapshots:
#             print("⚠️ No machine data to save")
#             self.reset_for_new_hour()
#             return
        
#         print(f"📦 Total machines tracked: {len(snapshots)}")
        
#         # Save to database
#         results = self.save_snapshots_to_database(plant_id, hour_key, snapshots)
        
#         print(f"\n📊 RESULTS:")
#         print(f"  ✅ Inserted: {results['inserted']}")
#         print(f"  📝 Updated: {results['updated']}")
#         print(f"  ❌ Errors: {results['errors']}")
#         print("=" * 80 + "\n")
        
#         # Reset for new hour
#         self.reset_for_new_hour()


# # ✅ Global instance
# HOURLY_DATA_SAVER = HourlyDataSaver()
