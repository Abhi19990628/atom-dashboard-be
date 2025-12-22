# # # backend/existing_table_saver.py - SEPARATE CLASSES FOR EACH PLANT WITH IST FIX
# # from datetime import datetime, time, timedelta
# # from threading import RLock, Timer
# # from collections import defaultdict
# # import pytz
# # from django.db import connection
# # import traceback


# # class BasePlantSaver:
# #     """Base class for plant data saving with IST timezone handling"""
# #     def __init__(self, plant_no, table_name):
# #         self._lock = RLock()
# #         self.ist_tz = pytz.timezone('Asia/Kolkata')
# #         self.plant_no = plant_no
# #         self.table_name = table_name
        
# #         # Track hourly data per machine
# #         self._current_hour_data = {}  # machine_no: latest_hour_data
# #         self._last_processed_date_shift = {}  # machine_no: "YYYY-MM-DD_A"
# #         self._saved_hours = set()  # Prevent duplicate saves
        
# #         # Start automatic saver
# #         self._start_auto_saver()
        
# #         print(f"🚀 Plant {plant_no} Saver - Table: {table_name} with IST timezone fix")

# #     def _start_auto_saver(self):
# #         """Auto save completed hours every 60 seconds"""
# #         def auto_save():
# #             try:
# #                 self._save_completed_hours()
# #             except Exception as e:
# #                 print(f"❌ Plant {self.plant_no} Auto save error: {e}")
# #             finally:
# #                 Timer(60.0, auto_save).start()
        
# #         Timer(60.0, auto_save).start()
# #         print(f"⏰ Plant {self.plant_no} Auto saver started - every 60 seconds")

# #     def get_shift_from_time(self, dt):
# #         """Get shift A (8:30 AM - 8:00 PM) or B (8:00 PM - 8:30 AM)"""
# #         time_only = dt.time()
# #         shift_A_start = time(8, 30)
# #         shift_A_end = time(20, 0)
# #         return 'A' if shift_A_start <= time_only < shift_A_end else 'B'

# #     def get_last_cumulative(self, machine_no):
# #         """Get last cumulative from this plant's table"""
# #         try:
# #             with connection.cursor() as cursor:
# #                 sql = f"""
# #                     SELECT cumulative_count 
# #                     FROM {self.table_name}
# #                     WHERE machine_no = %s 
# #                     ORDER BY timestamp DESC 
# #                     LIMIT 1
# #                 """
# #                 cursor.execute(sql, [str(machine_no)])
# #                 result = cursor.fetchone()
# #                 return result[0] if result else 0
# #         except Exception as e:
# #             print(f"⚠️ Plant {self.plant_no} Machine {machine_no} cumulative error: {e}")
# #             return 0

# #     def record_count(self, machine_no, tool_id, shut_height, hourly_count, idle_minutes):
# #         """Store hourly data with EXPLICIT IST handling"""
# #         with self._lock:
# #             # EXPLICIT IST TIME CREATION - FIXED
# #             utc_now = datetime.utcnow()
# #             ist_now = pytz.utc.localize(utc_now).astimezone(self.ist_tz)
# #             clean_ist_now = ist_now.replace(microsecond=0)
            
# #             current_shift = self.get_shift_from_time(clean_ist_now)
# #             current_date_str = clean_ist_now.strftime('%Y-%m-%d')
# #             current_hour_key = clean_ist_now.strftime('%Y-%m-%d-%H')
            
# #             # Check if first count of this hour
# #             if (machine_no not in self._current_hour_data or 
# #                 self._current_hour_data[machine_no]['hour_key'] != current_hour_key):
                
# #                 first_count_time = clean_ist_now
# #                 ist_display = first_count_time.strftime('%H:%M:%S IST')
# #                 print(f"🕐 Plant {self.plant_no} M{machine_no}: NEW HOUR {current_hour_key} at {ist_display}")
# #             else:
# #                 first_count_time = self._current_hour_data[machine_no]['first_count_time']
            
# #             # Store/update hour data
# #             self._current_hour_data[machine_no] = {
# #                 'hour_key': current_hour_key,
# #                 'first_count_time': first_count_time,  # IST timezone-aware
# #                 'hourly_count': hourly_count,
# #                 'idle_minutes': idle_minutes,
# #                 'shut_height': shut_height,
# #                 'tool_id': tool_id,
# #                 'shift': current_shift,
# #                 'machine_no': machine_no,
# #                 'date_str': current_date_str
# #             }
            
# #             ist_display = first_count_time.strftime('%H:%M:%S IST')
# #             print(f"📝 Plant {self.plant_no} M{machine_no}: Count {hourly_count} → {self.table_name} | Time: {ist_display}")

# #     def _save_completed_hours(self):
# #         """Save completed hours + Save machines with ZERO count if idle"""
# #         with self._lock:
# #             # Use IST time for current hour calculation
# #             utc_now = datetime.utcnow()
# #             ist_now = pytz.utc.localize(utc_now).astimezone(self.ist_tz)
# #             current_hour_key = ist_now.strftime('%Y-%m-%d-%H')
            
# #             saved_count = 0
            
# #             # 1. Save machines that had activity
# #             for machine_no, data in list(self._current_hour_data.items()):
# #                 stored_hour_key = data['hour_key']
# #                 unique_key = f"P{self.plant_no}_M{machine_no}_{stored_hour_key}"
                
# #                 if (stored_hour_key < current_hour_key and 
# #                     unique_key not in self._saved_hours):
                    
# #                     self._save_to_database(machine_no, data)
# #                     self._saved_hours.add(unique_key)
# #                     saved_count += 1
            
# #             # 2. ✅ NEW: Save machines with ZERO count (idle machines)
# #             self._save_idle_machines(current_hour_key)
            
# #             if saved_count > 0:
# #                 print(f"✅ Plant {self.plant_no}: AUTO SAVED {saved_count} hours")

# #     def _save_idle_machines(self, current_hour_key):
# #         """Save machines that had NO activity with count=0 and full idle time"""
# #         # Get list of all possible machines for this plant
# #         all_machines = self._get_all_plant_machines()
        
# #         for machine_no in all_machines:
# #             if machine_no not in self._current_hour_data:
# #                 # Machine had NO activity this hour - save as idle
# #                 utc_previous = datetime.utcnow() - timedelta(hours=1)
# #                 ist_previous = pytz.utc.localize(utc_previous).astimezone(self.ist_tz)
# #                 previous_hour = ist_previous.replace(microsecond=0)
# #                 previous_hour_key = previous_hour.strftime('%Y-%m-%d-%H')
# #                 unique_key = f"P{self.plant_no}_M{machine_no}_{previous_hour_key}"
                
# #                 if unique_key not in self._saved_hours:
# #                     # Create idle machine data
# #                     idle_data = {
# #                         'hour_key': previous_hour_key,
# #                         'first_count_time': previous_hour,
# #                         'hourly_count': 0,  # ✅ ZERO count
# #                         'idle_minutes': 60,  # ✅ Full hour idle (60 minutes)
# #                         'shut_height': 0.00,
# #                         'tool_id': f'IDLE_P{self.plant_no}_M{machine_no:02d}',
# #                         'shift': self.get_shift_from_time(previous_hour),
# #                         'machine_no': machine_no,
# #                         'date_str': previous_hour.strftime('%Y-%m-%d')
# #                     }
                    
# #                     self._save_to_database(machine_no, idle_data)
# #                     self._saved_hours.add(unique_key)
# #                     print(f"💤 Plant {self.plant_no} M{machine_no}: SAVED as IDLE (0 count, 60 min idle)")

# #     def _get_all_plant_machines(self):
# #         """Get all possible machines for this plant"""
# #         if self.plant_no == 1:
# #             return list(range(5, 16))  # Plant 1: Machines 5-15
# #         elif self.plant_no == 2:
# #             return list(range(1, 16))  # Plant 2: Machines 1-15
# #         else:
# #             return []

# #     def _save_to_database(self, machine_no, hour_data):
# #         """Save to database with FORCED IST timestamp"""
# #         try:
# #             # GET RAW TIMESTAMP
# #             raw_timestamp = hour_data['first_count_time']
            
# #             # FORCE IST CONVERSION
# #             if hasattr(raw_timestamp, 'tzinfo') and raw_timestamp.tzinfo:
# #                 # Already timezone-aware - convert to IST
# #                 ist_timestamp = raw_timestamp.astimezone(self.ist_tz)
# #             else:
# #                 # Assume it's already IST but naive - make it timezone-aware
# #                 ist_timestamp = self.ist_tz.localize(raw_timestamp)
            
# #             # FORCE NAIVE IST TIME FOR DATABASE (this is the key!)
# #             clean_timestamp_ist_naive = ist_timestamp.replace(tzinfo=None)
            
# #             # Handle shut height
# #             shut_height = hour_data['shut_height']
# #             if shut_height == 'Unknown' or shut_height is None or shut_height == '':
# #                 shut_height_value = 0.00
# #             else:
# #                 try:
# #                     shut_height_value = float(shut_height)
# #                 except (ValueError, TypeError):
# #                     shut_height_value = 0.00
            
# #             # Cumulative logic
# #             current_date_str = hour_data['date_str']
# #             current_shift = hour_data['shift']
# #             date_shift_key = f"{current_date_str}_{current_shift}"
            
# #             if machine_no not in self._last_processed_date_shift:
# #                 self._last_processed_date_shift[machine_no] = date_shift_key
# #                 cumulative_count = hour_data['hourly_count']
# #                 print(f"🆕 Plant {self.plant_no} M{machine_no}: FIRST → Cumulative {cumulative_count}")
                
# #             elif self._last_processed_date_shift[machine_no] != date_shift_key:
# #                 old_key = self._last_processed_date_shift[machine_no]
# #                 self._last_processed_date_shift[machine_no] = date_shift_key
# #                 cumulative_count = hour_data['hourly_count']
# #                 print(f"♻️ Plant {self.plant_no} M{machine_no}: SHIFT/DATE CHANGED → RESET {cumulative_count}")
                
# #             else:
# #                 last_cumulative = self.get_last_cumulative(machine_no)
# #                 cumulative_count = last_cumulative + hour_data['hourly_count']
# #                 print(f"➕ Plant {self.plant_no} M{machine_no}: CONTINUE → {cumulative_count}")
            
# #             # ✅ FORCE DATABASE TIMEZONE TO IST
# #             with connection.cursor() as cursor:
# #                 # FIRST: Set database session timezone to IST
# #                 cursor.execute("SET TIME ZONE 'Asia/Kolkata';")
                
# #                 insert_sql = f"""
# #                     INSERT INTO {self.table_name}
# #                     (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
# #                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
# #                 """
                
# #                 values = [
# #                     clean_timestamp_ist_naive,  # IST naive datetime
# #                     hour_data['tool_id'],
# #                     str(machine_no),
# #                     hour_data['hourly_count'],
# #                     cumulative_count,
# #                     0,  # tpm
# #                     hour_data['idle_minutes'],
# #                     shut_height_value,
# #                     hour_data['shift']
# #                 ]
                
# #                 cursor.execute(insert_sql, values)
                
# #                 # VERIFICATION LOG WITH IST TIME
# #                 ist_time_str = clean_timestamp_ist_naive.strftime('%Y-%m-%d %H:%M:%S IST')
# #                 print(f"✅ SAVED TO {self.table_name}: M{machine_no} | {ist_time_str} | Cumulative: {cumulative_count}")
                
# #         except Exception as e:
# #             print(f"❌ Plant {self.plant_no} M{machine_no} Save Error: {e}")
# #             traceback.print_exc()

# #     def force_save_all(self):
# #         """Force save all pending data"""
# #         print(f"🔧 Plant {self.plant_no} FORCE SAVING all pending data...")
# #         self._save_completed_hours()


# # # SEPARATE CLASSES FOR EACH PLANT
# # class Plant1TableSaver(BasePlantSaver):
# #     def __init__(self):
# #         super().__init__(plant_no=1, table_name="plant1_data")
# #         print("🏭 Plant 1 Saver → plant1_data table with IST fix")


# # class Plant2TableSaver(BasePlantSaver):
# #     def __init__(self):
# #         super().__init__(plant_no=2, table_name="Plant2_data")  
# #         print("🏭 Plant 2 Saver → Plant2_data table with IST fix")


# # # GLOBAL INSTANCES - SEPARATE FOR EACH PLANT
# # PLANT1_SAVER = Plant1TableSaver()
# # PLANT2_SAVER = Plant2TableSaver()

# # print("✅ SEPARATE PLANT SAVERS INITIALIZED WITH IST TIMEZONE FIX")
# # print("   - PLANT1_SAVER → plant1_data (IST timestamps)")  
# # print("   - PLANT2_SAVER → Plant2_data (IST timestamps)")



# # backend/existing_table_saver.py - GUARANTEED ALL 57 MACHINES SAVE
# from datetime import datetime, time, timedelta
# from threading import RLock, Timer
# from collections import defaultdict
# import pytz
# from django.db import connection
# import traceback


# class BasePlantSaver:
#     """Base class for plant data saving with IST timezone handling"""
#     def __init__(self, plant_no, table_name):
#         self._lock = RLock()
#         self.ist_tz = pytz.timezone('Asia/Kolkata')
#         self.plant_no = plant_no
#         self.table_name = table_name
        
#         # Track hourly data per machine
#         self._current_hour_data = {}  # machine_no: latest_hour_data
#         self._last_processed_date_shift = {}  # machine_no: "YYYY-MM-DD_A"
#         self._saved_hours = set()  # Prevent duplicate saves
        
#         # Start automatic saver
#         self._start_auto_saver()
        
#         print(f"🚀 Plant {plant_no} Saver - Table: {table_name} with GUARANTEED ALL MACHINES SAVE")

#     def _start_auto_saver(self):
#         """Auto save completed hours every 60 seconds"""
#         def auto_save():
#             try:
#                 self._save_completed_hours()
#             except Exception as e:
#                 print(f"❌ Plant {self.plant_no} Auto save error: {e}")
#                 traceback.print_exc()
#             finally:
#                 Timer(60.0, auto_save).start()
        
#         Timer(60.0, auto_save).start()
#         print(f"⏰ Plant {self.plant_no} Auto saver started - every 60 seconds")

#     def get_shift_from_time(self, dt):
#         """Get shift A (8:30 AM - 8:00 PM) or B (8:00 PM - 8:30 AM)"""
#         time_only = dt.time()
#         shift_A_start = time(8, 30)
#         shift_A_end = time(20, 0)
#         return 'A' if shift_A_start <= time_only < shift_A_end else 'B'

#     def get_last_cumulative(self, machine_no):
#         """Get last cumulative from this plant's table"""
#         try:
#             with connection.cursor() as cursor:
#                 sql = f"""
#                     SELECT cumulative_count 
#                     FROM {self.table_name}
#                     WHERE machine_no = %s 
#                     ORDER BY timestamp DESC 
#                     LIMIT 1
#                 """
#                 cursor.execute(sql, [str(machine_no)])
#                 result = cursor.fetchone()
#                 return result[0] if result else 0
#         except Exception as e:
#             print(f"⚠️ Plant {self.plant_no} Machine {machine_no} cumulative error: {e}")
#             return 0

#     def record_count(self, machine_no, tool_id, shut_height, hourly_count, idle_minutes):
#         """Store hourly data with EXPLICIT IST handling"""
#         with self._lock:
#             # EXPLICIT IST TIME CREATION
#             utc_now = datetime.utcnow()
#             ist_now = pytz.utc.localize(utc_now).astimezone(self.ist_tz)
#             clean_ist_now = ist_now.replace(microsecond=0)
            
#             current_shift = self.get_shift_from_time(clean_ist_now)
#             current_date_str = clean_ist_now.strftime('%Y-%m-%d')
#             current_hour_key = clean_ist_now.strftime('%Y-%m-%d-%H')
            
#             # Check if first count of this hour
#             if (machine_no not in self._current_hour_data or 
#                 self._current_hour_data[machine_no]['hour_key'] != current_hour_key):
                
#                 first_count_time = clean_ist_now
#                 ist_display = first_count_time.strftime('%H:%M:%S IST')
#                 print(f"🕐 Plant {self.plant_no} M{machine_no}: NEW HOUR {current_hour_key} at {ist_display}")
#             else:
#                 first_count_time = self._current_hour_data[machine_no]['first_count_time']
            
#             # Store/update hour data
#             self._current_hour_data[machine_no] = {
#                 'hour_key': current_hour_key,
#                 'first_count_time': first_count_time,
#                 'hourly_count': hourly_count,
#                 'idle_minutes': idle_minutes,
#                 'shut_height': shut_height,
#                 'tool_id': tool_id,
#                 'shift': current_shift,
#                 'machine_no': machine_no,
#                 'date_str': current_date_str
#             }
            
#             ist_display = first_count_time.strftime('%H:%M:%S IST')
#             print(f"📝 Plant {self.plant_no} M{machine_no}: Count {hourly_count} → {self.table_name} | Time: {ist_display}")

#     def _save_completed_hours(self):
#         """Save completed hours + GUARANTEE ALL machines saved"""
#         with self._lock:
#             utc_now = datetime.utcnow()
#             ist_now = pytz.utc.localize(utc_now).astimezone(self.ist_tz)
#             current_hour_key = ist_now.strftime('%Y-%m-%d-%H')
            
#             print(f"🔍 Plant {self.plant_no}: Checking for completed hours. Current: {current_hour_key}")
#             print(f"🔍 Current _current_hour_data keys: {list(self._current_hour_data.keys())}")
            
#             saved_count = 0
            
#             # 1. Save machines that had activity
#             for machine_no, data in list(self._current_hour_data.items()):
#                 stored_hour_key = data['hour_key']
#                 unique_key = f"P{self.plant_no}_M{machine_no}_{stored_hour_key}"
                
#                 if (stored_hour_key < current_hour_key and 
#                     unique_key not in self._saved_hours):
                    
#                     self._save_to_database(machine_no, data)
#                     self._saved_hours.add(unique_key)
#                     saved_count += 1
#                     print(f"✅ Plant {self.plant_no} M{machine_no}: ACTIVE machine saved")
            
#             # 2. ✅ CRITICAL: Force save ALL machines
#             idle_saved = self._force_save_all_machines_guaranteed(current_hour_key)
            
#             total_saved = saved_count + idle_saved
#             print(f"🎯 Plant {self.plant_no}: TOTAL SAVED = {total_saved} machines")
            
#             if total_saved > 0:
#                 print(f"✅ Plant {self.plant_no}: AUTO SAVED {saved_count} active + {idle_saved} idle = {total_saved} total")

#     def _force_save_all_machines_guaranteed(self, current_hour_key):
#         """✅ GUARANTEED: Force save ALL machines with detailed logging"""
        
#         # ✅ EXPLICIT: Get all machines for this plant
#         if self.plant_no == 1:
#             all_machines = list(range(1, 58))  # [1, 2, 3, ..., 57]
#         elif self.plant_no == 2:
#             all_machines = list(range(1, 27))  # [1, 2, 3, ..., 26]
#         else:
#             all_machines = []
        
#         print(f"🎯 Plant {self.plant_no}: FORCING SAVE FOR ALL {len(all_machines)} MACHINES")
#         print(f"🔍 All machines list: {all_machines[:10]}...{all_machines[-3:]}")
        
#         # Calculate previous hour
#         utc_previous = datetime.utcnow() - timedelta(hours=1)
#         ist_previous = pytz.utc.localize(utc_previous).astimezone(self.ist_tz)
#         previous_hour = ist_previous.replace(minute=0, second=0, microsecond=0)
#         previous_hour_key = previous_hour.strftime('%Y-%m-%d-%H')
        
#         print(f"🕐 Saving data for previous hour: {previous_hour_key}")
        
#         saved_machines = set()
#         total_saved = 0
        
#         # Step 1: Save active machines (already have data)
#         active_count = 0
#         for machine_no, data in list(self._current_hour_data.items()):
#             if data['hour_key'] == previous_hour_key:
#                 unique_key = f"P{self.plant_no}_M{machine_no}_{previous_hour_key}"
#                 if unique_key not in self._saved_hours:
#                     try:
#                         self._save_to_database(machine_no, data)
#                         self._saved_hours.add(unique_key)
#                         saved_machines.add(machine_no)
#                         total_saved += 1
#                         active_count += 1
#                         print(f"✅ ACTIVE M{machine_no}: Saved with count {data['hourly_count']}")
#                     except Exception as e:
#                         print(f"❌ Failed to save active M{machine_no}: {e}")
        
#         print(f"🟢 Active machines saved: {active_count}")
        
#         # Step 2: ✅ FORCE SAVE ALL REMAINING MACHINES AS IDLE
#         idle_count = 0
#         for machine_no in all_machines:
#             if machine_no not in saved_machines:
#                 unique_key = f"P{self.plant_no}_M{machine_no}_{previous_hour_key}"
                
#                 # Skip if already saved (duplicate check)
#                 if unique_key in self._saved_hours:
#                     saved_machines.add(machine_no)
#                     print(f"⚠️ M{machine_no}: Already saved, skipping")
#                     continue
                
#                 # Create idle machine data
#                 idle_data = {
#                     'hour_key': previous_hour_key,
#                     'first_count_time': previous_hour,
#                     'hourly_count': 0,  # ✅ ZERO count for idle
#                     'idle_minutes': 60,  # ✅ Full hour idle
#                     'shut_height': 0.00,
#                     'tool_id': f'IDLE_P{self.plant_no}_M{machine_no:02d}',
#                     'shift': self.get_shift_from_time(previous_hour),
#                     'machine_no': machine_no,
#                     'date_str': previous_hour.strftime('%Y-%m-%d')
#                 }
                
#                 try:
#                     self._save_to_database(machine_no, idle_data)
#                     self._saved_hours.add(unique_key)
#                     saved_machines.add(machine_no)
#                     total_saved += 1
#                     idle_count += 1
                    
#                     if idle_count <= 5:  # Print first 5 for verification
#                         print(f"💤 IDLE M{machine_no}: Force saved (0 count, 60 min idle)")
#                     elif idle_count == 6:
#                         print(f"💤 ... saving remaining idle machines ...")
                    
#                 except Exception as e:
#                     print(f"❌ Failed to save idle M{machine_no}: {e}")
#                     # ✅ RETRY with different tool_id
#                     try:
#                         idle_data['tool_id'] = f'FORCE_IDLE_P{self.plant_no}_M{machine_no:02d}'
#                         self._save_to_database(machine_no, idle_data)
#                         self._saved_hours.add(unique_key)
#                         saved_machines.add(machine_no)
#                         total_saved += 1
#                         idle_count += 1
#                         print(f"🔄 RETRY SUCCESS M{machine_no}: Saved on second attempt")
#                     except Exception as retry_e:
#                         print(f"💥 RETRY FAILED M{machine_no}: {retry_e}")
        
#         # Step 3: ✅ FINAL VERIFICATION
#         final_count = len(saved_machines)
#         expected_count = len(all_machines)
        
#         print(f"🏁 Plant {self.plant_no} FINAL RESULT:")
#         print(f"   - Active machines: {active_count}")
#         print(f"   - Idle machines: {idle_count}")
#         print(f"   - Total saved: {final_count}/{expected_count}")
#         print(f"   - Success rate: {(final_count/expected_count)*100:.1f}%")
        
#         if final_count != expected_count:
#             missing = set(all_machines) - saved_machines
#             print(f"⚠️ MISSING MACHINES: {sorted(missing)}")
            
#             # ✅ EMERGENCY: Final attempt for missing machines
#             emergency_saved = 0
#             for machine_no in missing:
#                 emergency_data = {
#                     'hour_key': previous_hour_key,
#                     'first_count_time': previous_hour,
#                     'hourly_count': 0,
#                     'idle_minutes': 60,
#                     'shut_height': 0.00,
#                     'tool_id': f'EMERGENCY_P{self.plant_no}_M{machine_no:02d}',
#                     'shift': self.get_shift_from_time(previous_hour),
#                     'machine_no': machine_no,
#                     'date_str': previous_hour.strftime('%Y-%m-%d')
#                 }
                
#                 try:
#                     self._save_to_database(machine_no, emergency_data)
#                     unique_key = f"P{self.plant_no}_M{machine_no}_{previous_hour_key}"
#                     self._saved_hours.add(unique_key)
#                     emergency_saved += 1
#                     print(f"🚨 EMERGENCY SAVED M{machine_no}")
#                 except Exception as e:
#                     print(f"💥 EMERGENCY FAILED M{machine_no}: {e}")
            
#             total_saved += emergency_saved
#             print(f"🚨 Emergency saves: {emergency_saved}")
        
#         else:
#             print(f"✅ PERFECT: All {expected_count} machines saved successfully!")
        
#         # Step 4: ✅ DATABASE VERIFICATION
#         try:
#             self._verify_database_entries(previous_hour_key, expected_count)
#         except Exception as e:
#             print(f"⚠️ Database verification failed: {e}")
        
#         return total_saved

#     def _verify_database_entries(self, hour_key, expected_count):
#         """✅ Verify all machines are actually saved in database"""
#         try:
#             # Convert hour_key back to timestamp range
#             hour_dt = datetime.strptime(hour_key, '%Y-%m-%d-%H')
#             start_time = hour_dt.strftime('%Y-%m-%d %H:00:00')
#             end_time = hour_dt.strftime('%Y-%m-%d %H:59:59')
            
#             with connection.cursor() as cursor:
#                 sql = f"""
#                     SELECT COUNT(DISTINCT machine_no) 
#                     FROM {self.table_name}
#                     WHERE timestamp BETWEEN %s AND %s
#                 """
#                 cursor.execute(sql, [start_time, end_time])
#                 actual_count = cursor.fetchone()[0]
                
#                 print(f"🔍 DB VERIFICATION: {actual_count}/{expected_count} machines in database for hour {hour_key}")
                
#                 if actual_count != expected_count:
#                     # Get missing machine numbers
#                     if self.plant_no == 1:
#                         all_machines = list(range(1, 58))
#                     else:
#                         all_machines = list(range(1, 27))
                    
#                     sql_existing = f"""
#                         SELECT DISTINCT machine_no 
#                         FROM {self.table_name}
#                         WHERE timestamp BETWEEN %s AND %s
#                         ORDER BY CAST(machine_no AS INTEGER)
#                     """
#                     cursor.execute(sql_existing, [start_time, end_time])
#                     existing_machines = [int(row[0]) for row in cursor.fetchall()]
#                     missing_in_db = set(all_machines) - set(existing_machines)
                    
#                     print(f"⚠️ MISSING IN DATABASE: {sorted(missing_in_db)}")
#                 else:
#                     print(f"✅ DATABASE VERIFICATION PASSED: All {actual_count} machines present")
                    
#         except Exception as e:
#             print(f"❌ Database verification error: {e}")

#     def _save_to_database(self, machine_no, hour_data):
#         """Save to database with FORCED IST timestamp"""
#         try:
#             # GET RAW TIMESTAMP
#             raw_timestamp = hour_data['first_count_time']
            
#             # FORCE IST CONVERSION
#             if hasattr(raw_timestamp, 'tzinfo') and raw_timestamp.tzinfo:
#                 ist_timestamp = raw_timestamp.astimezone(self.ist_tz)
#             else:
#                 ist_timestamp = self.ist_tz.localize(raw_timestamp)
            
#             # FORCE NAIVE IST TIME FOR DATABASE
#             clean_timestamp_ist_naive = ist_timestamp.replace(tzinfo=None)
            
#             # Handle shut height
#             shut_height = hour_data['shut_height']
#             if shut_height == 'Unknown' or shut_height is None or shut_height == '':
#                 shut_height_value = 0.00
#             else:
#                 try:
#                     shut_height_value = float(shut_height)
#                 except (ValueError, TypeError):
#                     shut_height_value = 0.00
            
#             # Cumulative logic
#             current_date_str = hour_data['date_str']
#             current_shift = hour_data['shift']
#             date_shift_key = f"{current_date_str}_{current_shift}"
            
#             if machine_no not in self._last_processed_date_shift:
#                 self._last_processed_date_shift[machine_no] = date_shift_key
#                 cumulative_count = hour_data['hourly_count']
#                 print(f"🆕 Plant {self.plant_no} M{machine_no}: FIRST → Cumulative {cumulative_count}")
                
#             elif self._last_processed_date_shift[machine_no] != date_shift_key:
#                 old_key = self._last_processed_date_shift[machine_no]
#                 self._last_processed_date_shift[machine_no] = date_shift_key
#                 cumulative_count = hour_data['hourly_count']
#                 print(f"♻️ Plant {self.plant_no} M{machine_no}: SHIFT/DATE CHANGED → RESET {cumulative_count}")
                
#             else:
#                 last_cumulative = self.get_last_cumulative(machine_no)
#                 cumulative_count = last_cumulative + hour_data['hourly_count']
#                 print(f"➕ Plant {self.plant_no} M{machine_no}: CONTINUE → {cumulative_count}")
            
#             # ✅ FORCE DATABASE TIMEZONE TO IST
#             with connection.cursor() as cursor:
#                 cursor.execute("SET TIME ZONE 'Asia/Kolkata';")
                
#                 insert_sql = f"""
#                     INSERT INTO {self.table_name}
#                     (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#                 """
                
#                 values = [
#                     clean_timestamp_ist_naive,
#                     hour_data['tool_id'],
#                     str(machine_no),
#                     hour_data['hourly_count'],
#                     cumulative_count,
#                     0,  # tpm
#                     hour_data['idle_minutes'],
#                     shut_height_value,
#                     hour_data['shift']
#                 ]
                
#                 cursor.execute(insert_sql, values)
                
#                 ist_time_str = clean_timestamp_ist_naive.strftime('%Y-%m-%d %H:%M:%S IST')
#                 print(f"✅ SAVED TO {self.table_name}: M{machine_no} | {ist_time_str} | Count: {hour_data['hourly_count']} | Cumulative: {cumulative_count}")
                
#         except Exception as e:
#             print(f"❌ Plant {self.plant_no} M{machine_no} Save Error: {e}")
#             traceback.print_exc()
#             raise  # Re-raise to trigger retry mechanism

#     def force_save_all(self):
#         """Force save all pending data"""
#         print(f"🔧 Plant {self.plant_no} FORCE SAVING all pending data...")
#         self._save_completed_hours()

#     def force_save_current_hour_for_all_machines(self):
#         """✅ Emergency function to save all machines for current hour"""
#         utc_now = datetime.utcnow()
#         ist_now = pytz.utc.localize(utc_now).astimezone(self.ist_tz)
#         current_hour = ist_now.replace(minute=0, second=0, microsecond=0)
#         current_hour_key = current_hour.strftime('%Y-%m-%d-%H')
        
#         print(f"🚨 EMERGENCY: Force saving all machines for current hour {current_hour_key}")
        
#         if self.plant_no == 1:
#             all_machines = list(range(1, 58))
#         else:
#             all_machines = list(range(1, 27))
        
#         emergency_saved = 0
        
#         for machine_no in all_machines:
#             unique_key = f"P{self.plant_no}_M{machine_no}_{current_hour_key}"
            
#             if unique_key not in self._saved_hours:
#                 # Create emergency data
#                 emergency_data = {
#                     'hour_key': current_hour_key,
#                     'first_count_time': current_hour,
#                     'hourly_count': 0,
#                     'idle_minutes': 60,
#                     'shut_height': 0.00,
#                     'tool_id': f'EMERGENCY_CURRENT_P{self.plant_no}_M{machine_no:02d}',
#                     'shift': self.get_shift_from_time(current_hour),
#                     'machine_no': machine_no,
#                     'date_str': current_hour.strftime('%Y-%m-%d')
#                 }
                
#                 try:
#                     self._save_to_database(machine_no, emergency_data)
#                     self._saved_hours.add(unique_key)
#                     emergency_saved += 1
#                     print(f"🚨 EMERGENCY CURRENT HOUR M{machine_no}: Saved")
#                 except Exception as e:
#                     print(f"💥 EMERGENCY CURRENT HOUR FAILED M{machine_no}: {e}")
        
#         print(f"🚨 EMERGENCY COMPLETE: {emergency_saved} machines saved for current hour")
#         return emergency_saved


# # SEPARATE CLASSES FOR EACH PLANT
# class Plant1TableSaver(BasePlantSaver):
#     def __init__(self):
#         super().__init__(plant_no=1, table_name="plant1_data")
#         print("🏭 Plant 1 Saver → plant1_data table with GUARANTEED ALL MACHINES SAVE")


# class Plant2TableSaver(BasePlantSaver):
#     def __init__(self):
#         super().__init__(plant_no=2, table_name="Plant2_data")
#         print("🏭 Plant 2 Saver → Plant2_data table with GUARANTEED ALL MACHINES SAVE")


# # GLOBAL INSTANCES - SEPARATE FOR EACH PLANT
# PLANT1_SAVER = Plant1TableSaver()
# PLANT2_SAVER = Plant2TableSaver()

# print("✅ GUARANTEED PLANT SAVERS INITIALIZED")
# print("   - PLANT1_SAVER → plant1_data (GUARANTEED ALL 57 MACHINES)")  
# print("   - PLANT2_SAVER → Plant2_data (GUARANTEED ALL 26 MACHINES)")

# # ✅ EMERGENCY FUNCTION - Call this if needed
# def emergency_save_all_machines():
#     """Emergency function to force save all machines for current hour"""
#     print("🚨🚨 EMERGENCY SAVE TRIGGERED 🚨🚨")
#     plant1_saved = PLANT1_SAVER.force_save_current_hour_for_all_machines()
#     plant2_saved = PLANT2_SAVER.force_save_current_hour_for_all_machines()
#     print(f"🚨 EMERGENCY TOTAL: Plant1={plant1_saved}, Plant2={plant2_saved}")
#     return plant1_saved + plant2_saved



# from datetime import datetime, time, timedelta
# from threading import RLock, Timer
# import pytz
# from django.db import connection
# import traceback

# class BasePlantSaver:
#     """Base class for plant data saving with IST timezone handling"""
#     def __init__(self, plant_no, table_name):
#         self._lock = RLock()
#         self.ist_tz = pytz.timezone('Asia/Kolkata')
#         self.plant_no = plant_no
#         self.table_name = table_name

#         self._current_hour_data = {}  # machine_no: latest hour data dict
#         self._last_processed_date_shift = {}  # machine_no: "YYYY-MM-DD_A" tracking for cumulative reset
#         self._saved_hours = set()  # Prevent duplicate saves

#         self._start_auto_saver()

#         print(f"🚀 Plant {plant_no} Saver - Table: {table_name} with GUARANTEED ALL MACHINES SAVE")

#     def _start_auto_saver(self):
#         def auto_save():
#             try:
#                 self._save_completed_hours()
#             except Exception as e:
#                 print(f"❌ Plant {self.plant_no} Auto save error: {e}")
#                 traceback.print_exc()
#             finally:
#                 Timer(60.0, auto_save).start()

#         Timer(60.0, auto_save).start()
#         print(f"⏰ Plant {self.plant_no} Auto saver started - every 60 seconds")

#     def get_shift_from_time(self, dt):
#         time_only = dt.time()
#         shift_A_start = time(8, 30)
#         shift_A_end = time(20, 0)
#         return 'A' if shift_A_start <= time_only < shift_A_end else 'B'

#     def get_last_cumulative(self, machine_no):
#         try:
#             with connection.cursor() as cursor:
#                 sql = f"""
#                     SELECT cumulative_count 
#                     FROM {self.table_name}
#                     WHERE machine_no = %s 
#                     ORDER BY timestamp DESC 
#                     LIMIT 1
#                 """
#                 cursor.execute(sql, [str(machine_no)])
#                 result = cursor.fetchone()
#                 return result[0] if result else 0
#         except Exception as e:
#             print(f"⚠️ Plant {self.plant_no} Machine {machine_no} cumulative error: {e}")
#             return 0

#     def record_count(self, machine_no, tool_id, shut_height, hourly_count, idle_minutes):
#         with self._lock:
#             utc_now = datetime.utcnow()
#             ist_now = pytz.utc.localize(utc_now).astimezone(self.ist_tz)
#             clean_ist_now = ist_now.replace(microsecond=0)

#             current_shift = self.get_shift_from_time(clean_ist_now)
#             current_date_str = clean_ist_now.strftime('%Y-%m-%d')
#             current_hour_key = clean_ist_now.strftime('%Y-%m-%d-%H')

#             if (machine_no not in self._current_hour_data or
#                 self._current_hour_data[machine_no]['hour_key'] != current_hour_key):
#                 first_count_time = clean_ist_now
#                 ist_display = first_count_time.strftime('%H:%M:%S IST')
#                 print(f"🕐 Plant {self.plant_no} M{machine_no}: NEW HOUR {current_hour_key} at {ist_display}")
#             else:
#                 first_count_time = self._current_hour_data[machine_no]['first_count_time']

#             self._current_hour_data[machine_no] = {
#                 'hour_key': current_hour_key,
#                 'first_count_time': first_count_time,
#                 'hourly_count': hourly_count,
#                 'idle_minutes': idle_minutes,
#                 'shut_height': shut_height,
#                 'tool_id': tool_id,
#                 'shift': current_shift,
#                 'machine_no': machine_no,
#                 'date_str': current_date_str
#             }
#             ist_display = first_count_time.strftime('%H:%M:%S IST')
#             print(f"📝 Plant {self.plant_no} M{machine_no}: Count {hourly_count} → {self.table_name} | Time: {ist_display}")

#     def _save_completed_hours(self):
#         with self._lock:
#             utc_now = datetime.utcnow()
#             ist_now = pytz.utc.localize(utc_now).astimezone(self.ist_tz)
#             current_hour_key = ist_now.strftime('%Y-%m-%d-%H')

#             print(f"🔍 Plant {self.plant_no}: Checking for completed hours. Current: {current_hour_key}")
#             print(f"🔍 Current _current_hour_data keys: {list(self._current_hour_data.keys())}")

#             saved_count = 0

#             for machine_no, data in list(self._current_hour_data.items()):
#                 stored_hour_key = data['hour_key']
#                 unique_key = f"P{self.plant_no}_M{machine_no}_{stored_hour_key}"

#                 if (stored_hour_key < current_hour_key and
#                     unique_key not in self._saved_hours):

#                     self._save_to_database(machine_no, data)
#                     self._saved_hours.add(unique_key)
#                     saved_count += 1
#                     print(f"✅ Plant {self.plant_no} M{machine_no}: ACTIVE machine saved")

#             idle_saved = self._force_save_all_machines_guaranteed(current_hour_key)

#             total_saved = saved_count + idle_saved
#             print(f"🎯 Plant {self.plant_no}: TOTAL SAVED = {total_saved} machines")

#             if total_saved > 0:
#                 print(f"✅ Plant {self.plant_no}: AUTO SAVED {saved_count} active + {idle_saved} idle = {total_saved} total")

#     def _force_save_all_machines_guaranteed(self, current_hour_key):
#         if self.plant_no == 1:
#             all_machines = list(range(1, 58))
#         elif self.plant_no == 2:
#             all_machines = list(range(1, 27))
#         else:
#             all_machines = []

#         print(f"🎯 Plant {self.plant_no}: FORCING SAVE FOR ALL {len(all_machines)} MACHINES")
#         print(f"🔍 All machines list: {all_machines[:10]}...{all_machines[-3:]}")

#         utc_previous = datetime.utcnow() - timedelta(hours=1)
#         ist_previous = pytz.utc.localize(utc_previous).astimezone(self.ist_tz)
#         previous_hour = ist_previous.replace(minute=0, second=0, microsecond=0)
#         previous_hour_key = previous_hour.strftime('%Y-%m-%d-%H')

#         print(f"🕐 Saving data for previous hour: {previous_hour_key}")

#         saved_machines = set()
#         total_saved = 0

#         active_count = 0
#         for machine_no, data in list(self._current_hour_data.items()):
#             if data['hour_key'] == previous_hour_key:
#                 unique_key = f"P{self.plant_no}_M{machine_no}_{previous_hour_key}"
#                 if unique_key not in self._saved_hours:
#                     try:
#                         self._save_to_database(machine_no, data)
#                         self._saved_hours.add(unique_key)
#                         saved_machines.add(machine_no)
#                         total_saved += 1
#                         active_count += 1
#                         print(f"✅ ACTIVE M{machine_no}: Saved with count {data['hourly_count']}")
#                     except Exception as e:
#                         print(f"❌ Failed to save active M{machine_no}: {e}")

#         print(f"🟢 Active machines saved: {active_count}")

#         idle_count = 0
#         for machine_no in all_machines:
#             if machine_no not in saved_machines:
#                 unique_key = f"P{self.plant_no}_M{machine_no}_{previous_hour_key}"

#                 if unique_key in self._saved_hours:
#                     saved_machines.add(machine_no)
#                     print(f"⚠️ M{machine_no}: Already saved, skipping")
#                     continue

#                 idle_data = {
#                     'hour_key': previous_hour_key,
#                     'first_count_time': previous_hour,
#                     'hourly_count': 0,
#                     'idle_minutes': 60,
#                     'shut_height': 0.00,
#                     'tool_id': f'IDLE_P{self.plant_no}_M{machine_no:02d}',
#                     'shift': self.get_shift_from_time(previous_hour),
#                     'machine_no': machine_no,
#                     'date_str': previous_hour.strftime('%Y-%m-%d')
#                 }

#                 try:
#                     self._save_to_database(machine_no, idle_data)
#                     self._saved_hours.add(unique_key)
#                     saved_machines.add(machine_no)
#                     total_saved += 1
#                     idle_count += 1

#                     if idle_count <= 5:
#                         print(f"💤 IDLE M{machine_no}: Force saved (0 count, 60 min idle)")
#                     elif idle_count == 6:
#                         print(f"💤 ... saving remaining idle machines ...")

#                 except Exception as e:
#                     print(f"❌ Failed to save idle M{machine_no}: {e}")

#                     try:
#                         idle_data['tool_id'] = f'FORCE_IDLE_P{self.plant_no}_M{machine_no:02d}'
#                         self._save_to_database(machine_no, idle_data)
#                         self._saved_hours.add(unique_key)
#                         saved_machines.add(machine_no)
#                         total_saved += 1
#                         idle_count += 1
#                         print(f"🔄 RETRY SUCCESS M{machine_no}: Saved on second attempt")
#                     except Exception as retry_e:
#                         print(f"💥 RETRY FAILED M{machine_no}: {retry_e}")

#         final_count = len(saved_machines)
#         expected_count = len(all_machines)

#         print(f"🏁 Plant {self.plant_no} FINAL RESULT:")
#         print(f"   - Active machines: {active_count}")
#         print(f"   - Idle machines: {idle_count}")
#         print(f"   - Total saved: {final_count}/{expected_count}")
#         print(f"   - Success rate: {(final_count/expected_count)*100:.1f}%")

#         if final_count != expected_count:
#             missing = set(all_machines) - saved_machines
#             print(f"⚠️ MISSING MACHINES: {sorted(missing)}")

#             emergency_saved = 0
#             for machine_no in missing:
#                 emergency_data = {
#                     'hour_key': previous_hour_key,
#                     'first_count_time': previous_hour,
#                     'hourly_count': 0,
#                     'idle_minutes': 60,
#                     'shut_height': 0.00,
#                     'tool_id': f'EMERGENCY_P{self.plant_no}_M{machine_no:02d}',
#                     'shift': self.get_shift_from_time(previous_hour),
#                     'machine_no': machine_no,
#                     'date_str': previous_hour.strftime('%Y-%m-%d')
#                 }
#                 try:
#                     self._save_to_database(machine_no, emergency_data)
#                     unique_key = f"P{self.plant_no}_M{machine_no}_{previous_hour_key}"
#                     self._saved_hours.add(unique_key)
#                     emergency_saved += 1
#                     print(f"🚨 EMERGENCY SAVED M{machine_no}")
#                 except Exception as e:
#                     print(f"💥 EMERGENCY FAILED M{machine_no}: {e}")

#             total_saved += emergency_saved
#             print(f"🚨 Emergency saves: {emergency_saved}")

#         else:
#             print(f"✅ PERFECT: All {expected_count} machines saved successfully!")

#         try:
#             self._verify_database_entries(previous_hour_key, expected_count)
#         except Exception as e:
#             print(f"⚠️ Database verification failed: {e}")

#         return total_saved

#     def _verify_database_entries(self, hour_key, expected_count):
#         try:
#             hour_dt = datetime.strptime(hour_key, '%Y-%m-%d-%H')
#             start_time = hour_dt.strftime('%Y-%m-%d %H:00:00')
#             end_time = hour_dt.strftime('%Y-%m-%d %H:59:59')

#             with connection.cursor() as cursor:
#                 sql = f"""
#                     SELECT COUNT(DISTINCT machine_no) 
#                     FROM {self.table_name}
#                     WHERE timestamp BETWEEN %s AND %s
#                 """
#                 cursor.execute(sql, [start_time, end_time])
#                 actual_count = cursor.fetchone()[0]

#                 print(f"🔍 DB VERIFICATION: {actual_count}/{expected_count} machines in database for hour {hour_key}")

#                 if actual_count != expected_count:
#                     if self.plant_no == 1:
#                         all_machines = list(range(1, 58))
#                     else:
#                         all_machines = list(range(1, 27))

#                     sql_existing = f"""
#                         SELECT DISTINCT machine_no 
#                         FROM {self.table_name}
#                         WHERE timestamp BETWEEN %s AND %s
#                         ORDER BY CAST(machine_no AS INTEGER)
#                     """
#                     cursor.execute(sql_existing, [start_time, end_time])
#                     existing_machines = [int(row[0]) for row in cursor.fetchall()]
#                     missing_in_db = set(all_machines) - set(existing_machines)

#                     print(f"⚠️ MISSING IN DATABASE: {sorted(missing_in_db)}")
#                 else:
#                     print(f"✅ DATABASE VERIFICATION PASSED: All {actual_count} machines present")

#         except Exception as e:
#             print(f"❌ Database verification error: {e}")

#     def _save_to_database(self, machine_no, hour_data):
#         try:
#             raw_timestamp = hour_data['first_count_time']

#             if hasattr(raw_timestamp, 'tzinfo') and raw_timestamp.tzinfo:
#                 ist_timestamp = raw_timestamp.astimezone(self.ist_tz)
#             else:
#                 ist_timestamp = self.ist_tz.localize(raw_timestamp)

#             clean_timestamp_ist_naive = ist_timestamp.replace(tzinfo=None)

#             shut_height = hour_data['shut_height']
#             if shut_height == 'Unknown' or shut_height is None or shut_height == '':
#                 shut_height_value = 0.00
#             else:
#                 try:
#                     shut_height_value = float(shut_height)
#                 except (ValueError, TypeError):
#                     shut_height_value = 0.00

#             current_date_str = hour_data['date_str']
#             current_shift = hour_data['shift']
#             date_shift_key = f"{current_date_str}_{current_shift}"

#             if machine_no not in self._last_processed_date_shift:
#                 self._last_processed_date_shift[machine_no] = date_shift_key
#                 cumulative_count = hour_data['hourly_count']
#                 print(f"🆕 Plant {self.plant_no} M{machine_no}: FIRST → Cumulative {cumulative_count}")

#             elif self._last_processed_date_shift[machine_no] != date_shift_key:
#                 old_key = self._last_processed_date_shift[machine_no]
#                 self._last_processed_date_shift[machine_no] = date_shift_key
#                 cumulative_count = hour_data['hourly_count']
#                 print(f"♻️ Plant {self.plant_no} M{machine_no}: SHIFT/DATE CHANGED → RESET {cumulative_count}")

#             else:
#                 last_cumulative = self.get_last_cumulative(machine_no)
#                 cumulative_count = last_cumulative + hour_data['hourly_count']
#                 print(f"➕ Plant {self.plant_no} M{machine_no}: CONTINUE → {cumulative_count}")

#             with connection.cursor() as cursor:
#                 cursor.execute("SET TIME ZONE 'Asia/Kolkata';")

#                 insert_sql = f"""
#                     INSERT INTO {self.table_name}
#                     (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#                 """

#                 values = [
#                     clean_timestamp_ist_naive,
#                     hour_data['tool_id'],
#                     str(machine_no),
#                     hour_data['hourly_count'],
#                     cumulative_count,
#                     0,
#                     hour_data['idle_minutes'],
#                     shut_height_value,
#                     hour_data['shift']
#                 ]

#                 cursor.execute(insert_sql, values)
#                 ist_time_str = clean_timestamp_ist_naive.strftime('%Y-%m-%d %H:%M:%S IST')
#                 print(f"✅ SAVED TO {self.table_name}: M{machine_no} | {ist_time_str} | Count: {hour_data['hourly_count']} | Cumulative: {cumulative_count}")

#         except Exception as e:
#             print(f"❌ Plant {self.plant_no} M{machine_no} Save Error: {e}")
#             traceback.print_exc()
#             raise

#     def force_save_all(self):
#         print(f"🔧 Plant {self.plant_no} FORCE SAVING all pending data...")
#         self._save_completed_hours()

#     def force_save_current_hour_for_all_machines(self):
#         utc_now = datetime.utcnow()
#         ist_now = pytz.utc.localize(utc_now).astimezone(self.ist_tz)
#         current_hour = ist_now.replace(minute=0, second=0, microsecond=0)
#         current_hour_key = current_hour.strftime('%Y-%m-%d-%H')

#         print(f"🚨 EMERGENCY: Force saving all machines for current hour {current_hour_key}")

#         if self.plant_no == 1:
#             all_machines = list(range(1, 58))
#         else:
#             all_machines = list(range(1, 27))

#         emergency_saved = 0

#         for machine_no in all_machines:
#             unique_key = f"P{self.plant_no}_M{machine_no}_{current_hour_key}"

#             if unique_key not in self._saved_hours:
#                 emergency_data = {
#                     'hour_key': current_hour_key,
#                     'first_count_time': current_hour,
#                     'hourly_count': 0,
#                     'idle_minutes': 60,
#                     'shut_height': 0.00,
#                     'tool_id': f'EMERGENCY_CURRENT_P{self.plant_no}_M{machine_no:02d}',
#                     'shift': self.get_shift_from_time(current_hour),
#                     'machine_no': machine_no,
#                     'date_str': current_hour.strftime('%Y-%m-%d')
#                 }

#                 try:
#                     self._save_to_database(machine_no, emergency_data)
#                     self._saved_hours.add(unique_key)
#                     emergency_saved += 1
#                     print(f"🚨 EMERGENCY CURRENT HOUR M{machine_no}: Saved")
#                 except Exception as e:
#                     print(f"💥 EMERGENCY CURRENT HOUR FAILED M{machine_no}: {e}")

#         print(f"🚨 EMERGENCY COMPLETE: {emergency_saved} machines saved for current hour")
#         return emergency_saved


# class Plant1TableSaver(BasePlantSaver):
#     def __init__(self):
#         super().__init__(plant_no=1, table_name="plant1_data")
#         print("🏭 Plant 1 Saver → plant1_data table with GUARANTEED ALL MACHINES SAVE")

# class Plant2TableSaver(BasePlantSaver):
#     def __init__(self):
#         super().__init__(plant_no=2, table_name="Plant2_data")
#         print("🏭 Plant 2 Saver → Plant2_data table with GUARANTEED ALL MACHINES SAVE")


# PLANT1_SAVER = Plant1TableSaver()
# PLANT2_SAVER = Plant2TableSaver()

# print("✅ GUARANTEED PLANT SAVERS INITIALIZED")
# print("   - PLANT1_SAVER → plant1_data (GUARANTEED ALL 57 MACHINES)")
# print("   - PLANT2_SAVER → Plant2_data (GUARANTEED ALL 26 MACHINES)")


# def emergency_save_all_machines():
#     print("🚨🚨 EMERGENCY SAVE TRIGGERED 🚨🚨")
#     plant1_saved = PLANT1_SAVER.force_save_current_hour_for_all_machines()
#     plant2_saved = PLANT2_SAVER.force_save_current_hour_for_all_machines()
#     print(f"🚨 EMERGENCY TOTAL: Plant1={plant1_saved}, Plant2={plant2_saved}")
#     return plant1_saved + plant2_saved



# # backend/apps/data_storage/existing_table_saver.py - DISABLED VERSION
# from datetime import datetime, time, timedelta
# from threading import RLock
# import pytz
# from django.db import connection
# import traceback
# import threading
# import time as time_module

# class Plant1TableSaver:
#     """Plant 1: DISABLED - Using simple_plant1.py counter system instead"""
#     def __init__(self):
#         self._lock = RLock()
#         self.ist_tz = pytz.timezone('Asia/Kolkata')
#         self.plant_no = 1
#         self.table_name = "plant1_data"
#         self.all_machines = list(range(1, 58))
        
#         print(f"🏭 Plant 1 Saver → DISABLED (Using simple_plant1.py counter system)")
#         print(f"⚠️  BACKGROUND THREAD NOT STARTED - simple_plant1.py handles saving")
#         # ✅ NO BACKGROUND THREAD STARTED

#     def _start_background_saver(self):
#         # ✅ DISABLED - No background thread
#         print(f"⚠️  Plant 1: Background saver DISABLED - Using simple_plant1.py instead")
#         pass

#     def _save_all_57_machines(self):
#         # ✅ DISABLED - simple_plant1.py handles this
#         print(f"⚠️  Plant 1: Manual save DISABLED - Using simple_plant1.py counter system")
#         pass

#     def _insert_to_database(self, machine_no, timestamp, count, tool_id, shut_height, idle_time):
#         # ✅ DISABLED
#         print(f"⚠️  Plant 1: Database insert DISABLED - Using simple_plant1.py")
#         pass

#     def _get_last_cumulative(self, machine_no):
#         # ✅ DISABLED
#         return 0

#     def force_save_all(self):
#         # ✅ DISABLED
#         print(f"⚠️  Plant 1: Force save DISABLED - Using simple_plant1.py counter system")
#         pass

# class Plant2TableSaver:
#     """Plant 2: DISABLED - Using simple_plant2.py hour boundary system instead"""
#     def __init__(self):
#         self.plant_no = 2
#         self.table_name = "Plant2_data"
#         print(f"🏭 Plant 2 Saver → DISABLED (Using simple_plant2.py hour boundary system)")

#     def force_save_all(self):
#         print(f"⚠️  Plant 2: Force save DISABLED - Using simple_plant2.py hour boundary system")
#         pass

# # Create instances (but they're disabled)
# PLANT1_SAVER = Plant1TableSaver()
# PLANT2_SAVER = Plant2TableSaver()

# print("⚠️  PLANT SAVERS DISABLED")
# print("   🔧 Plant 1: Using simple_plant1.py counter system with hour boundary save")
# print("   🔧 Plant 2: Using simple_plant2.py hour boundary save")
# print("   ✅ No background threads from this file - cleaner approach")
