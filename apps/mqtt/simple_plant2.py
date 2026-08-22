# # backend/apps/mqtt/simple_plant2.py - ULTIMATE FIXED VERSION

# import paho.mqtt.client as mqtt
# from datetime import datetime, timedelta
# import threading
# from apps.machines.machine_state import MACHINE_STATE
# from apps.data_storage.hourly_idle_tracker import HOURLY_IDLE_TRACKER
# import traceback
# import pytz
# from django.db import connection
# import time as time_module
# from threading import RLock
# from collections import defaultdict
# import json
# from apps.utils.email_alert import send_shut_height_alert
# import os

# # ✅ ULTIMATE FIX 1: Force system timezone to IST
# os.environ['TZ'] = 'Asia/Kolkata'

# IST = pytz.timezone("Asia/Kolkata")


# class IdleType:
#     ON_BUT_NOT_PRODUCING = "ON_BUT_NOT_PRODUCING"
#     NO_SIGNAL_AS_IDLE = "NO_SIGNAL_AS_IDLE"
#     NONE = "NONE"


# class DataSource:
#     COUNT = "COUNT"
#     JSON = "JSON"
#     NONE = "NONE"


# def convert_to_naive_ist(timestamp):
#     """
#     Convert to IST and store as-is (no timezone)
#     Django will treat it as local time
#     """
#     if timestamp.tzinfo is not None:
#         ist_timestamp = timestamp.astimezone(IST)
#     else:
#         ist_timestamp = IST.localize(timestamp)

#     # Create clean datetime (IST time as naive)
#     naive_ist = datetime(
#         ist_timestamp.year,
#         ist_timestamp.month,
#         ist_timestamp.day,
#         ist_timestamp.hour,
#         ist_timestamp.minute,
#         ist_timestamp.second
#     )
#     return naive_ist

# # ✅ NAYA CODE: Event Logger Helper Function
# def log_machine_event(plant_no, machine_no, event_type, timestamp, shift, details=""):
#     """
#     Ye function chup-chaap Machine_Event_Logs table mein data save karega.
#     """
#     try:
#         from django.db import connection
#         import pytz

#         IST = pytz.timezone("Asia/Kolkata")
#         if timestamp.tzinfo is not None:
#             ist_timestamp = timestamp.astimezone(IST)
#         else:
#             ist_timestamp = IST.localize(timestamp)

#         timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S')

#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 INSERT INTO "Machine_Event_Logs"
#                 (plant_no, machine_no, event_type, timestamp, shift, details)
#                 VALUES (%s, %s, %s, %s::timestamp WITHOUT TIME ZONE, %s, %s)
#             """, (plant_no, str(machine_no), event_type, timestamp_str, shift, details))

#         print(f"📝 EVENT SAVED | P{plant_no}-M{machine_no} | {event_type} | {timestamp_str}")
#     except Exception as e:
#         print(f"❌ Event Log Error P{plant_no}-M{machine_no}: {e}")


# class StrictIdlePolicy:
#     def __init__(self, grace_seconds=180, enable_no_signal_as_idle=True):
#         self.lock = RLock()
#         self.grace_seconds = grace_seconds
#         self.enable_no_signal_as_idle = enable_no_signal_as_idle

#         self.on_since = {}
#         self.last_count_time = {}
#         self.last_json_time = {}
#         self.current_hour_start = {}
#         self.completed_segments_minutes = {}
#         self.data_source = {}
#         self.hour_had_activity = {}

#     @staticmethod
#     def _ist(dt: datetime) -> datetime:
#         if dt is None:
#             return None
#         if dt.tzinfo is None:
#             return IST.localize(dt)
#         return dt.astimezone(IST)

#     @staticmethod
#     def _hour_start(dt: datetime) -> datetime:
#         dt = StrictIdlePolicy._ist(dt)
#         return dt.replace(minute=0, second=0, microsecond=0)

#     def _ensure_current_hour(self, m: int, now: datetime):
#         hour = self._hour_start(now)
#         prev = self.current_hour_start.get(m)

#         if prev is None or prev != hour:
#             self.current_hour_start[m] = hour
#             self.completed_segments_minutes[m] = 0
#             self.hour_had_activity[m] = False

#     def mark_json(self, m: int, t: datetime):
#         with self.lock:
#             now = self._ist(t)
#             self.last_json_time[m] = now
#             self.data_source[m] = DataSource.JSON

#             if m not in self.on_since:
#                 self.on_since[m] = now

#             self._ensure_current_hour(m, now)
#             self.hour_had_activity[m] = True

#     def mark_count(self, m: int, t: datetime):
#         with self.lock:
#             now = self._ist(t)
#             prev_count = self.last_count_time.get(m)

#             if prev_count is not None:
#                 live, acc, total = self._compute_live_and_accumulated(m, now)

#                 if live > 0:
#                     self.completed_segments_minutes[m] = self.completed_segments_minutes.get(m, 0) + live

#             self.last_count_time[m] = now
#             self.data_source[m] = DataSource.COUNT

#             if m not in self.on_since:
#                 self.on_since[m] = now

#             self._ensure_current_hour(m, now)
#             self.hour_had_activity[m] = True

#     def mark_off(self, m: int):
#         with self.lock:
#             self.data_source[m] = DataSource.NONE

#     def _compute_base_time(self, m: int, now: datetime) -> datetime:
#         hour_start = self.current_hour_start.get(m, self._hour_start(now))
#         candidates = [hour_start]

#         if m in self.on_since:
#             candidates.append(self.on_since[m])

#         if m in self.last_count_time:
#             candidates.append(self.last_count_time[m])

#         return max(candidates)

#     def _compute_live_and_accumulated(self, m: int, now: datetime):
#         if m not in self.on_since:
#             return (0, 0, 0)

#         base_time = self._compute_base_time(m, now)
#         gap_seconds = (now - base_time).total_seconds()

#         if gap_seconds < self.grace_seconds:
#             live_idle = 0
#             accumulated_idle = 0
#         else:
#             visible_minutes = int(gap_seconds / 60)
#             live_idle = visible_minutes
#             accumulated_idle = visible_minutes

#         completed = self.completed_segments_minutes.get(m, 0)
#         hourly_total = completed + live_idle

#         return (live_idle, accumulated_idle, hourly_total)

#     def get_idle_status(self, m: int, now: datetime = None):
#         with self.lock:
#             if now is None:
#                 now = datetime.now(IST)
#             now = self._ist(now)

#             self._ensure_current_hour(m, now)

#             if self.enable_no_signal_as_idle:
#                 is_never_active = m not in self.on_since and \
#                                 m not in self.last_count_time and \
#                                 m not in self.last_json_time

#                 if is_never_active:
#                     # ✅ FIXED: 8:30 Shift Start logic for OFFLINE IDLE ONLY
#                     hour_start = self._hour_start(now)

#                     shift_a_start = now.replace(hour=8, minute=30, second=0, microsecond=0)
#                     shift_b_start = now.replace(hour=20, minute=30, second=0, microsecond=0)

#                     if shift_a_start <= now < shift_b_start:
#                         actual_start = max(hour_start, shift_a_start)
#                     elif now >= shift_b_start:
#                         actual_start = max(hour_start, shift_b_start)
#                     else:
#                         prev_shift_b = shift_b_start - timedelta(days=1)
#                         actual_start = max(hour_start, prev_shift_b)

#                     elapsed_seconds = max(0, (now - actual_start).total_seconds())
#                     elapsed_mins = int(elapsed_seconds / 60)

#                     return {
#                         'live_idle_time': f'{elapsed_mins}m',
#                         'accumulated_idle_time': f'{elapsed_mins}m',
#                         'hourly_idle_total': elapsed_mins,
#                         'is_idle': True,  # True hona chahiye taaki offline idle track ho
#                         'idle_type': IdleType.NO_SIGNAL_AS_IDLE,
#                         'status': 'No Signal (Offline)',
#                         'data_source': DataSource.NONE,
#                         'on_since': None,
#                         'last_count_time': None,
#                         'count_seconds_ago': None,
#                         'json_seconds_ago': None
#                     }

#             # 👉 Yahan se ONLINE IDLE ka logic shuru hota hai jo bilkul safe aur UNCHANGED hai
#             live, acc, total = self._compute_live_and_accumulated(m, now)

#             has_count = m in self.last_count_time
#             has_json = m in self.last_json_time

#             count_seconds_ago = None
#             json_seconds_ago = None

#             if has_count:
#                 count_seconds_ago = int((now - self.last_count_time[m]).total_seconds())

#             if has_json:
#                 json_seconds_ago = int((now - self.last_json_time[m]).total_seconds())

#             is_on = m in self.on_since
#             is_producing = has_count and count_seconds_ago <= 180

#             if not is_on:
#                 status = "OFF"
#                 idle_type = IdleType.NONE
#             elif is_producing:
#                 if live > 0:
#                     status = "Producing (Idle)"
#                 else:
#                     status = "Producing"
#                 idle_type = IdleType.NONE if live == 0 else IdleType.ON_BUT_NOT_PRODUCING
#             else:
#                 if live > 0:
#                     status = "ON (No Count)"
#                 else:
#                     status = "ON (Grace Period)"
#                 idle_type = IdleType.ON_BUT_NOT_PRODUCING if live > 0 else IdleType.NONE

#             return {
#                 'live_idle_time': f'{live}m' if live > 0 else '0m',
#                 'accumulated_idle_time': f'{acc}m',
#                 'hourly_idle_total': min(60, total),
#                 'is_idle': live > 0,
#                 'idle_type': idle_type,
#                 'status': status,
#                 'data_source': self.data_source.get(m, DataSource.NONE),
#                 'on_since': self.on_since.get(m),
#                 'last_count_time': self.last_count_time.get(m),
#                 'count_seconds_ago': count_seconds_ago,
#                 'json_seconds_ago': json_seconds_ago
#             }

#     def reset_hour(self, m: int = None):
#         with self.lock:
#             if m is None:
#                 self.completed_segments_minutes.clear()
#                 self.current_hour_start.clear()
#                 self.hour_had_activity.clear()
#             else:
#                 self.completed_segments_minutes[m] = 0
#                 self.hour_had_activity[m] = False
#                 if m in self.current_hour_start:
#                     del self.current_hour_start[m]


# class Plant2ExactRequirementState:
#     def __init__(self):
#         self.lock = RLock()
#         self.current_hour_counts = defaultdict(int)
#         self.last_hour_counts = defaultdict(int)
#         self.shift_cumulative = defaultdict(int)
#         self.current_hours = {}
#         self.current_shifts = {}

#         self.last_count_time = {}
#         self.hour_first_count_time = {}

#         self.machine_json_status = {}
#         self.machine_count_status = {}

#         self.machine_on_since = {}
#         self.first_count_time = {}

#         self.machine_segments = defaultdict(lambda: {
#             'shut_height': None,
#             'tool_id': None,
#             'segment_start': None,
#             'segment_count': 0,
#         })

#         self.off_threshold_seconds = 180
#         self.idle_tracker = StrictIdlePolicy(grace_seconds=180, enable_no_signal_as_idle=True)

#     def get_shift_from_time(self, dt):
#         ist_dt = dt.astimezone(pytz.timezone('Asia/Kolkata')) if dt.tzinfo else pytz.timezone('Asia/Kolkata').localize(dt)
#         time_only = ist_dt.time()
#         shift_A_start = datetime.strptime("08:30", "%H:%M").time()
#         shift_A_end = datetime.strptime("20:00", "%H:%M").time()
#         return 'A' if shift_A_start <= time_only < shift_A_end else 'B'

#     def get_shift_start_datetime(self, timestamp):
#         date = timestamp.date()
#         shift = self.get_shift_from_time(timestamp)

#         shift_a_start_time = datetime.strptime("08:30", "%H:%M").time()
#         shift_b_start_time = datetime.strptime("20:30", "%H:%M").time()

#         if shift == 'A':
#             return IST.localize(datetime.combine(date, shift_a_start_time))
#         else:
#             if timestamp.time() < shift_a_start_time:
#                 prev_day = date - timedelta(days=1)
#                 return IST.localize(datetime.combine(prev_day, shift_b_start_time))
#             else:
#                 return IST.localize(datetime.combine(date, shift_b_start_time))

#     def get_shift_idle_from_hourly_table(self, machine_no, shift_start, shift, now):
#         """
#         ✅ Plant2_hourly_idle table se shift ka total idle nikalta hai
#         Date + Shift wise sum karta hai
#         """
#         try:
#             shift_start_naive = convert_to_naive_ist(shift_start)
#             now_naive = convert_to_naive_ist(now)

#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT COALESCE(SUM(idle_time), 0)
#                     FROM "Plant2_hourly_idle"
#                     WHERE machine_no = %s
#                     AND shift = %s
#                     AND DATE(timestamp) = DATE(%s)
#                     AND timestamp >= %s
#                     AND timestamp < %s
#                 """, (str(machine_no), shift, shift_start_naive, shift_start_naive, now_naive))

#                 result = cursor.fetchone()
#                 db_idle = int(result[0]) if result and result[0] else 0

#             current_idle = self.idle_tracker.get_idle_status(machine_no, now)
#             live_idle = current_idle['hourly_idle_total']

#             total_shift_idle = db_idle + live_idle

#             return total_shift_idle

#         except Exception as e:
#             print(f"❌ Error fetching shift idle M{machine_no}: {e}")
#             traceback.print_exc()
#             return 0

#     def reset_shift_state(self, machine_no=None):
#         """✅ FIX: Only called on SHIFT change, not hour change"""
#         with self.lock:
#             if machine_no is None:
#                 self.machine_on_since.clear()
#                 self.first_count_time.clear()
#                 print("🔄 All machines: Shift state reset")
#             else:
#                 self.machine_on_since.pop(machine_no, None)
#                 self.first_count_time.pop(machine_no, None)
#                 print(f"🔄 M{machine_no}: Shift state reset")

#     def update_json_status(self, machine_no, card=None, die_height=0.0):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)

#             if machine_no not in self.machine_on_since:
#                 self.machine_on_since[machine_no] = now_ist

#             self.machine_json_status[machine_no] = {
#                 'last_json_time': now_ist,
#                 'card': card or 'UNKNOWN',
#                 'die_height': die_height
#             }

#             self.idle_tracker.mark_json(machine_no, now_ist)

#     def add_count(self, machine_no, count_increment=1, tool_id=None, shut_height=None):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)
#             current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
#             current_shift = self.get_shift_from_time(now_ist)

#             # ✅ FIX 2: Machine ON tracking (shift level, not hour level)
#             if machine_no not in self.machine_on_since:
#                 self.machine_on_since[machine_no] = now_ist
#                 print(f"🟢 M{machine_no}: Machine ON at {now_ist.strftime('%H:%M:%S')}")

#             if machine_no not in self.first_count_time:
#                 self.first_count_time[machine_no] = now_ist
#                 print(f"🎯 M{machine_no}: First count at {now_ist.strftime('%H:%M:%S')}")

#             if machine_no not in self.hour_first_count_time or \
#                self.hour_first_count_time[machine_no].replace(minute=0, second=0, microsecond=0) != current_hour:
#                 self.hour_first_count_time[machine_no] = now_ist

#             self.last_count_time[machine_no] = now_ist

#             self.machine_count_status[machine_no] = {
#                 'last_count_time': now_ist,
#                 'tool_id': tool_id if tool_id else 'UNKNOWN',
#                 'shut_height': shut_height if shut_height else "No data"
#             }

#             segment = self.machine_segments[machine_no]

#             is_valid_height = False
#             new_height_value = None

#             if shut_height not in ['No data', 'Failed', None, 0, 0.0, '0', '0.0', '']:
#                 try:
#                     new_height_value = float(shut_height)
#                     if new_height_value > 1.0:
#                         is_valid_height = True
#                 except:
#                     is_valid_height = False

#             if is_valid_height:
#                 if segment['shut_height'] is None or segment['shut_height'] == 0.0:
#                     segment['shut_height'] = new_height_value
#                     segment['tool_id'] = tool_id
#                     segment['segment_start'] = now_ist
#                     segment['segment_count'] = count_increment
#                 else:
#                     old_height = segment['shut_height']
#                     height_difference = abs(old_height - new_height_value)
#                     height_changed = height_difference > 1.0

#                     if height_changed:
#                         threading.Thread(
#                             target=send_shut_height_alert,
#                             args=(2, machine_no, old_height, new_height_value, now_ist),
#                             daemon=True
#                         ).start()

#                         # ✅ NAYA CODE: Tool/Height Change record karein
#                         log_machine_event(
#                             plant_no=2,
#                             machine_no=machine_no,
#                             event_type="SHUT_HEIGHT_CHANGE",
#                             timestamp=now_ist,
#                             shift=current_shift,
#                             details=f"Height changed from {old_height} to {new_height_value} | Tool: {tool_id}"
#                         )

#                         if segment['segment_count'] > 0:
#                             self.save_segment_to_db(machine_no, segment)

#                         segment['shut_height'] = new_height_value
#                         segment['tool_id'] = tool_id
#                         segment['segment_start'] = now_ist
#                         segment['segment_count'] = count_increment
#                     else:
#                         segment['segment_count'] += count_increment
#             else:
#                 if segment['shut_height'] and segment['shut_height'] > 0:
#                     segment['segment_count'] += count_increment

#             # ✅ FIX 3: Hour change auto reset (count only, state preserved)
#             if machine_no in self.current_hours:
#                 if self.current_hours[machine_no] != current_hour:
#                     # Save current hour count to last hour
#                     self.last_hour_counts[machine_no] = self.current_hour_counts[machine_no]
#                     # Reset current hour count to 0
#                     old_count = self.current_hour_counts[machine_no]
#                     self.current_hour_counts[machine_no] = 0
#                     self.current_hours[machine_no] = current_hour

#                     print(f"⏰ M{machine_no}: Hour changed | Last={old_count}, New=0")
#             else:
#                 self.current_hours[machine_no] = current_hour

#             # ✅ FIX 4: Shift change pe hi state reset (not hour change)
#             if machine_no in self.current_shifts:
#                 old_shift = self.current_shifts[machine_no]
#                 if old_shift != current_shift:
#                     print(f"🔄 M{machine_no}: Shift changed {old_shift}→{current_shift}")

#                     new_shift_key = (machine_no, current_shift)
#                     self.shift_cumulative[new_shift_key] = 0

#                     # Reset ON-since and first count (shift level only)
#                     self.reset_shift_state(machine_no)

#             self.current_shifts[machine_no] = current_shift
#             self.current_hour_counts[machine_no] += count_increment
#             self.idle_tracker.mark_count(machine_no, now_ist)

#             self._insert_realtime_count(
#                 machine_no=machine_no,
#                 count_increment=count_increment,
#                 tool_id=tool_id,
#                 shut_height=shut_height,
#                 timestamp=now_ist,
#                 shift=current_shift
#             )

#     def _insert_realtime_count(self, machine_no, count_increment, tool_id, shut_height, timestamp, shift):
#         try:
#             shift_start = self.get_shift_start_datetime(timestamp)

#             last_cumulative = 0
#             try:
#                 shift_start_naive = convert_to_naive_ist(shift_start)
#                 with connection.cursor() as cursor:
#                     cursor.execute("""
#                         SELECT cumulative_count FROM Plant2_data
#                         WHERE machine_no = %s AND shift = %s AND timestamp >= %s
#                         ORDER BY timestamp DESC LIMIT 1
#                     """, (str(machine_no), shift, shift_start_naive))
#                     result = cursor.fetchone()
#                     if result and result[0] is not None:
#                         last_cumulative = int(result[0])
#             except Exception:
#                 pass

#             new_cumulative = last_cumulative + int(count_increment)

#             idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
#             idle_time = idle_status['hourly_idle_total']

#             clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

#             if isinstance(shut_height, (int, float)) and shut_height > 0:
#                 clean_shut_height = f"{float(shut_height):.2f}"
#             else:
#                 try:
#                     val = float(shut_height)
#                     clean_shut_height = f"{val:.2f}" if val > 0 else "0.00"
#                 except:
#                     clean_shut_height = "0.00"

#             clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 0

#             # ✅ ULTIMATE FIX 5: Convert to IST string for database
#             if timestamp.tzinfo is not None:
#                 ist_timestamp = timestamp.astimezone(IST)
#             else:
#                 ist_timestamp = IST.localize(timestamp)

#             timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S')

#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     INSERT INTO Plant2_data
#                     (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                     VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
#                 """, (
#                     timestamp_str,
#                     clean_tool_id,
#                     str(machine_no),
#                     int(count_increment),
#                     new_cumulative,
#                     0,
#                     clean_idle_time,
#                     clean_shut_height,
#                     shift
#                 ))

#             print(f"✅ M{machine_no}: {timestamp_str} | cnt={count_increment}, cumul={new_cumulative}, idle={clean_idle_time}m")

#         except Exception as e:
#             print(f"❌ Insert error M{machine_no}: {e}")
#             traceback.print_exc()

#     def save_segment_to_db(self, machine_no, segment):
#         count = segment['segment_count']
#         if count == 0:
#             return

#         timestamp = segment['segment_start']
#         tool_id = segment['tool_id']
#         shut_height = segment['shut_height']

#         shift = self.get_shift_from_time(timestamp)
#         shift_start = self.get_shift_start_datetime(timestamp)

#         last_cumulative = 0
#         try:
#             shift_start_naive = convert_to_naive_ist(shift_start)
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT cumulative_count FROM Plant2_data
#                     WHERE machine_no = %s AND shift = %s AND timestamp >= %s
#                     ORDER BY timestamp DESC LIMIT 1
#                 """, (str(machine_no), shift, shift_start_naive))
#                 result = cursor.fetchone()
#                 if result:
#                     last_cumulative = result[0]
#         except Exception:
#             pass

#         new_cumulative = last_cumulative + count
#         idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
#         idle_time = idle_status['hourly_idle_total']

#         try:
#             clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

#             if isinstance(shut_height, (int, float)) and shut_height > 0:
#                 clean_shut_height = f"{float(shut_height):.2f}"
#             else:
#                clean_shut_height = "0.00"

#             clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 0

#             # ✅ ULTIMATE FIX 6: Convert to IST string for database
#             if timestamp.tzinfo is not None:
#                 ist_timestamp = timestamp.astimezone(IST)
#             else:
#                 ist_timestamp = IST.localize(timestamp)

#             timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S')

#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     INSERT INTO Plant2_data (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                     VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
#                 """, (timestamp_str, clean_tool_id, str(machine_no), count, new_cumulative, 0, clean_idle_time, clean_shut_height, shift))

#         except Exception as e:
#             print(f"❌ Error inserting segment M{machine_no}: {e}")

#         segment['segment_count'] = 0

#     def get_machine_status(self, machine_no):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)

#             has_count = False
#             count_seconds_ago = None
#             count_tool_id = None
#             count_shut_height = None

#             if machine_no in self.machine_count_status:
#                 last_count = self.machine_count_status[machine_no]['last_count_time']
#                 count_seconds_ago = (now_ist - last_count).total_seconds()
#                 count_tool_id = self.machine_count_status[machine_no]['tool_id']
#                 count_shut_height = self.machine_count_status[machine_no]['shut_height']

#                 if count_seconds_ago <= self.off_threshold_seconds:
#                     has_count = True

#             has_json = False
#             json_seconds_ago = None
#             json_card = None
#             json_die_height = None

#             if machine_no in self.machine_json_status:
#                 last_json = self.machine_json_status[machine_no]['last_json_time']
#                 json_seconds_ago = (now_ist - last_json).total_seconds()
#                 json_card = self.machine_json_status[machine_no]['card']
#                 json_die_height = self.machine_json_status[machine_no]['die_height']

#                 if json_seconds_ago <= self.off_threshold_seconds:
#                     has_json = True

#             machine_on = has_count or has_json
#             is_producing = has_count

#             offline_since = None
#             offline_duration_minutes = None

#             if not machine_on:
#                 last_activity_time = None

#                 if machine_no in self.machine_count_status and machine_no in self.machine_json_status:
#                     last_activity_time = max(
#                         self.machine_count_status[machine_no]['last_count_time'],
#                         self.machine_json_status[machine_no]['last_json_time']
#                     )
#                 elif machine_no in self.machine_count_status:
#                     last_activity_time = self.machine_count_status[machine_no]['last_count_time']
#                 elif machine_no in self.machine_json_status:
#                     last_activity_time = self.machine_json_status[machine_no]['last_json_time']

#                 if last_activity_time:
#                     offline_since = last_activity_time
#                     offline_duration_seconds = (now_ist - last_activity_time).total_seconds()
#                     offline_duration_minutes = int(offline_duration_seconds / 60)

#                 self.idle_tracker.mark_off(machine_no)

#             if count_tool_id:
#                 tool_id = count_tool_id
#                 shut_height = count_shut_height
#             elif json_card:
#                 tool_id = json_card
#                 shut_height = json_die_height if json_die_height != 0.0 else "No data"
#             else:
#                 tool_id = 'N/A'
#                 shut_height = "No data"

#             return {
#                 'machine_on': machine_on,
#                 'is_producing': is_producing,
#                 'has_count_data': has_count,
#                 'has_json_data': has_json,
#                 'count_seconds_ago': int(count_seconds_ago) if count_seconds_ago is not None else None,
#                 'json_seconds_ago': int(json_seconds_ago) if json_seconds_ago is not None else None,
#                 'tool_id': tool_id,
#                 'shut_height': shut_height,
#                 'data_source': 'COUNT' if has_count else ('JSON' if has_json else 'NONE'),
#                 'offline_since': offline_since.strftime('%H:%M:%S') if offline_since else None,
#                 'offline_duration_minutes': offline_duration_minutes,
#             }

#     def get_machine_data(self, machine_no):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)
#             current_shift = self.get_shift_from_time(now_ist)
#             current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
#             shift_start = self.get_shift_start_datetime(now_ist)

#         last_hour_count_db = 0
#         try:
#             previous_hour_start = current_hour - timedelta(hours=1)
#             previous_hour_end = current_hour
#             previous_hour_start_naive = convert_to_naive_ist(previous_hour_start)
#             previous_hour_end_naive = convert_to_naive_ist(previous_hour_end)

#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT COALESCE(SUM(count), 0) FROM Plant2_data
#                     WHERE machine_no = %s
#                     AND timestamp >= %s
#                     AND timestamp < %s
#                 """, (str(machine_no), previous_hour_start_naive, previous_hour_end_naive))
#                 result = cursor.fetchone()
#                 if result and result[0] is not None:
#                    last_hour_count_db = int(result[0])
#         except Exception as e:
#             print(f"❌ M{machine_no}: Last hour count error - {e}")

#         cumulative_from_db = 0
#         try:
#             shift_start_naive = convert_to_naive_ist(shift_start)
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT cumulative_count FROM Plant2_data
#                     WHERE machine_no = %s AND shift = %s AND timestamp >= %s
#                     ORDER BY timestamp DESC LIMIT 1
#                 """, (str(machine_no), current_shift, shift_start_naive))
#                 result = cursor.fetchone()
#                 if result and result[0] is not None:
#                     cumulative_from_db = int(result[0])
#         except Exception as e:
#             print(f"⚠️ Error fetching cumulative M{machine_no}: {e}")

#         live_cumulative = cumulative_from_db

#         status_info = self.get_machine_status(machine_no)

#         idle_status = self.idle_tracker.get_idle_status(machine_no, now_ist)
#         hourly_idle_total = idle_status['hourly_idle_total']

#         # ✅ SHIFT TOTAL IDLE from Plant2_hourly_idle table
#         total_shift_idle = self.get_shift_idle_from_hourly_table(
#             machine_no, shift_start, current_shift, now_ist
#         )

#         on_since_str = None
#         first_count_str = None
#         time_to_first_count = None

#         if machine_no in self.machine_on_since and status_info['machine_on']:
#             on_since = self.machine_on_since[machine_no]
#             on_since_str = on_since.strftime('%H:%M:%S')

#             if machine_no in self.first_count_time:
#                 first_count = self.first_count_time[machine_no]
#                 first_count_str = first_count.strftime('%H:%M:%S')
#                 delay = (first_count - on_since).total_seconds()
#                 time_to_first_count = int(delay / 60)

#         if machine_no in self.machine_on_since and not status_info['machine_on']:
#             on_since = self.machine_on_since[machine_no]
#             on_since_str = on_since.strftime('%H:%M:%S')

#             if machine_no in self.first_count_time:
#                 first_count = self.first_count_time[machine_no]
#                 first_count_str = first_count.strftime('%H:%M:%S')

#         return {
#             'machine_no': machine_no,
#             'current_hour_count': self.current_hour_counts.get(machine_no, 0),
#             'last_hour_count': last_hour_count_db,
#             'cumulative_count': live_cumulative,
#             'idle_time': hourly_idle_total,
#             'total_shift_idle_time': total_shift_idle,
#             'shift': current_shift,
#             'machine_on': status_info['machine_on'],
#             'is_producing': status_info['is_producing'],
#             'has_count_data': status_info['has_count_data'],
#             'has_json_data': status_info['has_json_data'],
#             'count_seconds_ago': status_info['count_seconds_ago'],
#             'json_seconds_ago': status_info['json_seconds_ago'],
#             'current_tool_id': status_info['tool_id'],
#             'current_shut_height': status_info['shut_height'],
#             'data_source': status_info['data_source'],
#             'on_since': on_since_str,
#             'first_count_at': first_count_str,
#             'time_to_first_count': time_to_first_count
#         }

#     def force_hour_reset_all_machines(self):
#         """Not used anymore - hour reset is automatic"""
#         pass


# EXACT_REQUIREMENT_STATE = Plant2ExactRequirementState()
# PLANT2_EXACT_REQUIREMENT_STATE = EXACT_REQUIREMENT_STATE

# _messages_lock = threading.Lock()

# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1),
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# TOPIC_MACHINE_MAPPING = {
#     'COUNT3': [1, 2, 3, 4, 5],
#     'COUNT2': [6, 7, 8, 9, 10],
#     'COUNT52': [11, 12, 13, 14, 15],
#     'COUNT1': [16, 17, 18, 19, 20],
#     'COUNT4': [41, 42, 43, 44, 45, 46],
#     'COUNT16': [21, 22, 23, 24, 25],
#     'COUNT17': [26, 27, 28, 29, 30],
#     'COUNT18': [31, 32, 33, 34, 35],
#     'COUNT19': [36, 37, 38, 39, 40],
#     'COUNT': []
# }

# MACHINE_GROUP_MAPPING = {
#     'J4': [1, 2, 3, 4, 5],
#     'J3': [6, 7, 8, 9, 10],
#     'J2': [11, 12, 13, 14, 15],
#     'J1': [16, 17, 18, 19, 20],
#     'J5': [41, 42, 43, 44, 45, 46],
#     'J6': [21, 22, 23, 24, 25],
#     'J7': [26, 27, 28, 29, 30],
#     'J8': [31, 32, 33, 34, 35],
#     'J9': [36, 37, 38, 39, 40]
# }

# def get_machine_group(machine_no):
#     for group_name, machines in MACHINE_GROUP_MAPPING.items():
#         if machine_no in machines:
#             return group_name
#     return 'Unknown'

# ACTIVE_MACHINES_THIS_HOUR = set()
# MACHINE_DATA_CACHE = {}

# def get_machines_for_topic(topic):
#     return TOPIC_MACHINE_MAPPING.get(topic, [])

# def parse_json_payload(raw_payload):
#     try:
#         data = json.loads(raw_payload)
#         if 'client_id' not in data:
#             return None

#         client_id = str(data.get('client_id', ''))

#         if len(client_id) >= 2:
#             plant_no = int(client_id[0]) if client_id[0].isdigit() else None
#             machine_no = int(client_id[1:]) if client_id[1:].isdigit() else None
#         else:
#             return None

#         card = data.get('card', 'UNKNOWN')
#         die_height_str = str(data.get('die_height', '0'))
#         try:
#             die_height = float(die_height_str)
#         except:
#             die_height = 0.0

#         return {
#             'type': 'json',
#             'plant_no': plant_no,
#             'machine_no': machine_no,
#             'card': card,
#             'die_height': die_height
#         }
#     except:
#         return None

# def parse_count_payload(raw_payload):
#     try:
#         parts = raw_payload.strip().split()
#         if len(parts) < 2:
#             return None

#         tool_id = parts[0][:24] if len(parts[0]) >= 24 else parts[0]
#         val_str = parts[1]

#         plant_no = int(val_str[0]) if len(val_str) > 0 and val_str[0].isdigit() else None

#         machine_no = None
#         if len(val_str) > 3:
#             if val_str[1].isdigit() and val_str[2].isdigit():
#                 machine_no = int(val_str[1:3])
#                 shut_height_str = val_str[4:]
#             else:
#                 machine_no = int(val_str[1]) if val_str[1].isdigit() else None
#                 shut_height_str = val_str[3:]
#         elif len(val_str) > 2:
#             machine_no = int(val_str[1]) if val_str[1].isdigit() else None
#             shut_height_str = val_str[3:]

#         if 'Failed' in shut_height_str:
#             shut_height = "Failed"
#         elif shut_height_str:
#             try:
#                 shut_height = float(shut_height_str)
#             except:
#                 shut_height = "No data"
#         else:
#             shut_height = "No data"

#         return {
#             'type': 'count',
#             'plant_no': plant_no,
#             'machine_no': machine_no,
#             'tool_id': tool_id,
#             'shut_height': shut_height
#         }
#     except:
#         return None

# def print_active_machines_summary():
#     def summary_worker():
#         while True:
#             try:
#                 time_module.sleep(30)
#                 ist_tz = pytz.timezone('Asia/Kolkata')
#                 now_ist = datetime.now(ist_tz)

#                 with EXACT_REQUIREMENT_STATE.lock:
#                     producing_machines = []
#                     all_machines = set()
#                     for machines_list in TOPIC_MACHINE_MAPPING.values():
#                         all_machines.update(machines_list)

#                     for machine_no in sorted(all_machines):
#                         if machine_no in EXACT_REQUIREMENT_STATE.last_count_time:
#                             last_count = EXACT_REQUIREMENT_STATE.last_count_time[machine_no]
#                             seconds_ago = (now_ist - last_count).total_seconds()

#                             if seconds_ago <= 60:
#                                 hour_count = EXACT_REQUIREMENT_STATE.current_hour_counts.get(machine_no, 0)
#                                 tool_id = 'N/A'
#                                 if machine_no in EXACT_REQUIREMENT_STATE.machine_count_status:
#                                     tool_id = EXACT_REQUIREMENT_STATE.machine_count_status[machine_no].get('tool_id', 'N/A')

#                                 producing_machines.append({
#                                     'no': machine_no,
#                                     'count': hour_count,
#                                     'tool': tool_id[:8] if tool_id != 'N/A' else 'N/A',
#                                     'last': int(seconds_ago)
#                                 })

#                     if producing_machines:
#                         print("\n" + "=" * 80)
#                         print(f"🏭 ACTIVE MACHINES ({len(producing_machines)} running) - {now_ist.strftime('%H:%M:%S')}")
#                         print("=" * 80)

#                         for i in range(0, len(producing_machines), 4):
#                             chunk = producing_machines[i:i+4]
#                             for m in chunk:
#                                 print(f"M{m['no']:02d}: {m['count']:3d}ct | {m['tool']} | {m['last']:2d}s", end="  |  ")
#                             print()
#                         print("=" * 80 + "\n")
#             except Exception as e:
#                 print(f"❌ Summary error: {e}")

#     thread = threading.Thread(target=summary_worker, daemon=True)
#     thread.start()


# def save_hourly_idle_to_db(machine_no, timestamp, tool_id, shut_height, idle_time, shift):
#     try:
#         clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

#         if isinstance(shut_height, (int, float)) and shut_height > 0:
#             clean_shut_height = f"{float(shut_height):.2f}"
#         else:
#             clean_shut_height = "0.00"

#         clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 60

#         # ✅ ULTIMATE FIX 7: Convert to IST string for database
#         if timestamp.tzinfo is not None:
#             ist_timestamp = timestamp.astimezone(IST)
#         else:
#             ist_timestamp = IST.localize(timestamp)

#         timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S')

#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 INSERT INTO "Plant2_hourly_idle"
#                 (timestamp, tool_id, machine_no, idle_time, shut_height, shift)
#                 VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s)
#             """, (
#                 timestamp_str,
#                 clean_tool_id,
#                 str(machine_no),
#                 clean_idle_time,
#                 clean_shut_height,
#                 shift
#             ))

#         print(f"💾 IDLE M{machine_no}: {timestamp_str} | idle={clean_idle_time}m, shift={shift}")

#     except Exception as e:
#         print(f"❌ Hourly idle DB error M{machine_no}: {e}")
#         traceback.print_exc()


# def save_hourly_idle_time_to_db():
#     def idle_saver_worker():
#         print("\n" + "⏰" * 50)
#         print("⏰ HOURLY IDLE TIME TRACKER STARTED!")
#         print(f"⏰ Snapshot time: XX:59:58")
#         print(f"⏰ Started at: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}")
#         print("⏰" * 50 + "\n")

#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)

#         print(f"✅ Tracking idle time for {len(all_mapped_machines)} machines")
#         print(f"✅ Machines: {sorted(all_mapped_machines)}\n")

#         last_saved_hour = None

#         while True:
#             try:
#                 ist_tz = pytz.timezone('Asia/Kolkata')
#                 now_ist = datetime.now(ist_tz)
#                 current_minute = now_ist.minute
#                 current_second = now_ist.second
#                 current_hour = now_ist.hour

#                 is_snapshot_time = (current_minute == 59 and current_second == 58)

#                 if is_snapshot_time and last_saved_hour != current_hour:
#                     print("\n" + "💾" * 50)
#                     print(f"💾 HOURLY IDLE SNAPSHOT at {now_ist.strftime('%H:%M:%S')}")
#                     print("💾" * 50 + "\n")

#                     last_saved_hour = current_hour
#                     current_hour_start = now_ist.replace(minute=0, second=0, microsecond=0)

#                     saved_count = 0
#                     for machine_no in sorted(all_mapped_machines):
#                         try:
#                             idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                             idle_time = idle_status['hourly_idle_total']

#                             segment = EXACT_REQUIREMENT_STATE.machine_segments[machine_no]
#                             tool_id = segment.get('tool_id', 'NULL')
#                             shut_height = segment.get('shut_height', 0.0)

#                             shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)

#                             save_hourly_idle_to_db(
#                                 machine_no=machine_no,
#                                 timestamp=current_hour_start,
#                                 tool_id=tool_id,
#                                 shut_height=shut_height,
#                                 idle_time=idle_time,
#                                 shift=shift
#                             )
#                             saved_count += 1
#                         except Exception as e:
#                             print(f"❌ M{machine_no} idle save error: {e}")

#                     print(f"\n✅ Saved {saved_count}/{len(all_mapped_machines)} machine idle times\n")

#                 time_module.sleep(1)
#             except Exception as e:
#                 print(f"❌ Idle tracker error: {e}")
#                 traceback.print_exc()
#                 time_module.sleep(5)

#     thread = threading.Thread(target=idle_saver_worker, daemon=True)
#     thread.start()


# # ✅ NAYA CODE: Ye thread lagatar machine ka status dekhega aur database me record karega
# def start_machine_event_monitor():
#     """Ye background thread har 5 second me ON/OFF check karega"""
#     def monitor_worker():
#         import time as time_module
#         print("🔍 Plant 2 - Machine ON/OFF Event Monitor Started!")
#         machine_last_state = {}

#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)

#         while True:
#             try:
#                 time_module.sleep(5)
#                 ist_tz = pytz.timezone('Asia/Kolkata')
#                 now_ist = datetime.now(ist_tz)

#                 for machine_no in all_mapped_machines:
#                     status = EXACT_REQUIREMENT_STATE.get_machine_status(machine_no)
#                     is_currently_on = status['machine_on']

#                     if machine_no not in machine_last_state:
#                         machine_last_state[machine_no] = is_currently_on
#                         continue

#                     was_on_before = machine_last_state[machine_no]

#                     # CONDITION 1: Machine OFF se ON hui
#                     if is_currently_on and not was_on_before:
#                         shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)
#                         log_machine_event(
#                             plant_no=2,
#                             machine_no=machine_no,
#                             event_type="ON",
#                             timestamp=now_ist,
#                             shift=shift,
#                             details="Machine Power/Signal Restored"
#                         )
#                         machine_last_state[machine_no] = True

#                     # CONDITION 2: Machine ON se OFF hui (3 min grace wali)
#                     elif not is_currently_on and was_on_before:
#                         exact_off_time_str = status['offline_since']

#                         if exact_off_time_str:
#                             today = now_ist.date()
#                             time_obj = datetime.strptime(exact_off_time_str, '%H:%M:%S').time()
#                             exact_off_time = IST.localize(datetime.combine(today, time_obj))
#                         else:
#                             exact_off_time = now_ist

#                         shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(exact_off_time)

#                         log_machine_event(
#                             plant_no=2,
#                             machine_no=machine_no,
#                             event_type="OFF",
#                             timestamp=exact_off_time,  # Purana time (jab sach me band hui thi)
#                             shift=shift,
#                             details="Machine Offline (No signal for 3 mins)"
#                         )
#                         machine_last_state[machine_no] = False

#             except Exception as e:
#                 print(f"❌ Event Monitor Error: {e}")
#                 time_module.sleep(5)

#     thread = threading.Thread(target=monitor_worker, daemon=True)
#     thread.start()


# def on_message(client, userdata, msg):
#     try:
#         topic = msg.topic
#         raw_payload = msg.payload.decode('utf-8', errors='ignore').strip()

#         if topic.startswith('J'):
#             parsed = parse_json_payload(raw_payload)
#             if parsed and parsed['plant_no'] == 2:
#                 machine_no = parsed['machine_no']
#                 card = parsed['card']
#                 die_height = parsed['die_height']

#                 EXACT_REQUIREMENT_STATE.update_json_status(
#                     machine_no=machine_no,
#                     card=card,
#                     die_height=die_height
#                 )

#         elif topic.startswith('COUNT'):
#             parsed = parse_count_payload(raw_payload)
#             if parsed and parsed['plant_no'] == 2:
#                 machine_no = parsed['machine_no']
#                 tool_id = parsed['tool_id']
#                 shut_height = parsed['shut_height']

#                 EXACT_REQUIREMENT_STATE.add_count(
#                     machine_no=machine_no,
#                     count_increment=1,
#                     tool_id=tool_id,
#                     shut_height=shut_height
#                 )

#     except Exception as e:
#         print(f"❌ on_message error: {e}")
#         traceback.print_exc()


# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Connected to MQTT Broker (Plant 2)")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#             print(f"📥 Subscribed: {topic}")
#     else:
#         print(f"❌ Connection failed with code {rc}")


# def start_plant2_mqtt():
#     print("\n" + "🚀" * 50)
#     print("🚀 STARTING PLANT 2 MQTT CLIENT")
#     print("🚀" * 50 + "\n")

#     client = mqtt.Client(client_id="plant2_exact_backend", clean_session=True)
#     client.username_pw_set(USERNAME, PASSWORD)
#     client.on_connect = on_connect
#     client.on_message = on_message

#     try:
#         client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     except Exception as e:
#         print(f"❌ MQTT connection error: {e}")
#         return

#     print_active_machines_summary()
#     save_hourly_idle_time_to_db()

#     # ✅ NAYA CODE: Background Monitor start karein
#     start_machine_event_monitor()

#     client.loop_start()
#     print("✅ MQTT Loop Started (Plant 2)\n")

# # backend/apps/mqtt/simple_plant2.py - ULTIMATE FIXED VERSION

# import paho.mqtt.client as mqtt
# from datetime import datetime, timedelta
# import threading
# from apps.machines.machine_state import MACHINE_STATE
# from apps.data_storage.hourly_idle_tracker import HOURLY_IDLE_TRACKER
# import traceback
# import pytz
# from django.db import connection
# import time as time_module
# from threading import RLock
# from collections import defaultdict
# import json
# from apps.utils.email_alert import send_shut_height_alert
# import os

# # ✅ NAYE IMPORTS AUTOMATIC NOTIFICATION KE LIYE
# from api.models import Notification
# from django.contrib.auth.models import Group

# # ✅ ULTIMATE FIX 1: Force system timezone to IST
# os.environ['TZ'] = 'Asia/Kolkata'

# IST = pytz.timezone("Asia/Kolkata")


# class IdleType:
#     ON_BUT_NOT_PRODUCING = "ON_BUT_NOT_PRODUCING"
#     NO_SIGNAL_AS_IDLE = "NO_SIGNAL_AS_IDLE"
#     NONE = "NONE"


# class DataSource:
#     COUNT = "COUNT"
#     JSON = "JSON"
#     NONE = "NONE"


# def convert_to_naive_ist(timestamp):
#     """
#     Convert to IST and store as-is (no timezone)
#     Django will treat it as local time
#     """
#     if timestamp.tzinfo is not None:
#         ist_timestamp = timestamp.astimezone(IST)
#     else:
#         ist_timestamp = IST.localize(timestamp)

#     # Create clean datetime (IST time as naive)
#     naive_ist = datetime(
#         ist_timestamp.year,
#         ist_timestamp.month,
#         ist_timestamp.day,
#         ist_timestamp.hour,
#         ist_timestamp.minute,
#         ist_timestamp.second
#     )
#     return naive_ist


# def log_machine_event(plant_no, machine_no, event_type, timestamp, shift, details=""):
#     try:
#         from django.db import connection
#         import pytz

#         IST = pytz.timezone("Asia/Kolkata")
#         if timestamp.tzinfo is not None:
#             ist_timestamp = timestamp.astimezone(IST)
#         else:
#             ist_timestamp = IST.localize(timestamp)

#         # ✅ FIX: +05:30 force kiya aur WITH TIME ZONE lagaya
#         timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S+05:30')

#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 INSERT INTO "Machine_Event_Logs"
#                 (plant_no, machine_no, event_type, timestamp, shift, details)
#                 VALUES (%s, %s, %s, %s::timestamp WITH TIME ZONE, %s, %s)
#             """, (plant_no, str(machine_no), event_type, timestamp_str, shift, details))

#         print(f"📝 EVENT SAVED | P{plant_no}-M{machine_no} | {event_type} | {timestamp_str}")
#     except Exception as e:
#         print(f"❌ Event Log Error P{plant_no}-M{machine_no}: {e}")


# class StrictIdlePolicy:
#     def __init__(self, grace_seconds=180, enable_no_signal_as_idle=True):
#         self.lock = RLock()
#         self.grace_seconds = grace_seconds
#         self.enable_no_signal_as_idle = enable_no_signal_as_idle

#         self.on_since = {}
#         self.last_count_time = {}
#         self.last_json_time = {}
#         self.current_hour_start = {}
#         self.completed_segments_minutes = {}
#         self.data_source = {}
#         self.hour_had_activity = {}

#     @staticmethod
#     def _ist(dt: datetime) -> datetime:
#         if dt is None:
#             return None
#         if dt.tzinfo is None:
#             return IST.localize(dt)
#         return dt.astimezone(IST)

#     @staticmethod
#     def _hour_start(dt: datetime) -> datetime:
#         dt = StrictIdlePolicy._ist(dt)
#         return dt.replace(minute=0, second=0, microsecond=0)

#     def _ensure_current_hour(self, m: int, now: datetime):
#         hour = self._hour_start(now)
#         prev = self.current_hour_start.get(m)

#         if prev is None or prev != hour:
#             self.current_hour_start[m] = hour
#             self.completed_segments_minutes[m] = 0
#             self.hour_had_activity[m] = False

#     def mark_json(self, m: int, t: datetime):
#         with self.lock:
#             now = self._ist(t)
#             self.last_json_time[m] = now
#             self.data_source[m] = DataSource.JSON

#             if m not in self.on_since:
#                 self.on_since[m] = now

#             self._ensure_current_hour(m, now)
#             self.hour_had_activity[m] = True

#     def mark_count(self, m: int, t: datetime):
#         with self.lock:
#             now = self._ist(t)
#             prev_count = self.last_count_time.get(m)

#             if prev_count is not None:
#                 live, acc, total = self._compute_live_and_accumulated(m, now)

#                 if live > 0:
#                     self.completed_segments_minutes[m] = self.completed_segments_minutes.get(m, 0) + live

#             self.last_count_time[m] = now
#             self.data_source[m] = DataSource.COUNT

#             if m not in self.on_since:
#                 self.on_since[m] = now

#             self._ensure_current_hour(m, now)
#             self.hour_had_activity[m] = True

#     def mark_off(self, m: int):
#         with self.lock:
#             self.data_source[m] = DataSource.NONE

#     def _compute_base_time(self, m: int, now: datetime) -> datetime:
#         hour_start = self.current_hour_start.get(m, self._hour_start(now))
#         candidates = [hour_start]

#         if m in self.on_since:
#             candidates.append(self.on_since[m])

#         if m in self.last_count_time:
#             candidates.append(self.last_count_time[m])

#         return max(candidates)

#     def _compute_live_and_accumulated(self, m: int, now: datetime):
#         if m not in self.on_since:
#             return (0, 0, 0)

#         base_time = self._compute_base_time(m, now)
#         gap_seconds = (now - base_time).total_seconds()

#         if gap_seconds < self.grace_seconds:
#             live_idle = 0
#             accumulated_idle = 0
#         else:
#             visible_minutes = int(gap_seconds / 60)
#             live_idle = visible_minutes
#             accumulated_idle = visible_minutes

#         completed = self.completed_segments_minutes.get(m, 0)
#         hourly_total = completed + live_idle

#         return (live_idle, accumulated_idle, hourly_total)

#     def get_idle_status(self, m: int, now: datetime = None):
#         with self.lock:
#             if now is None:
#                 now = datetime.now(IST)
#             now = self._ist(now)

#             self._ensure_current_hour(m, now)

#             if self.enable_no_signal_as_idle:
#                 is_never_active = m not in self.on_since and \
#                                 m not in self.last_count_time and \
#                                 m not in self.last_json_time

#                 if is_never_active:
#                     hour_start = self._hour_start(now)

#                     shift_a_start = now.replace(hour=8, minute=30, second=0, microsecond=0)
#                     shift_b_start = now.replace(hour=20, minute=30, second=0, microsecond=0)

#                     if shift_a_start <= now < shift_b_start:
#                         actual_start = max(hour_start, shift_a_start)
#                     elif now >= shift_b_start:
#                         actual_start = max(hour_start, shift_b_start)
#                     else:
#                         prev_shift_b = shift_b_start - timedelta(days=1)
#                         actual_start = max(hour_start, prev_shift_b)

#                     elapsed_seconds = max(0, (now - actual_start).total_seconds())
#                     elapsed_mins = int(elapsed_seconds / 60)

#                     return {
#                         'live_idle_time': f'{elapsed_mins}m',
#                         'accumulated_idle_time': f'{elapsed_mins}m',
#                         'hourly_idle_total': elapsed_mins,
#                         'is_idle': True,
#                         'idle_type': IdleType.NO_SIGNAL_AS_IDLE,
#                         'status': 'No Signal (Offline)',
#                         'data_source': DataSource.NONE,
#                         'on_since': None,
#                         'last_count_time': None,
#                         'count_seconds_ago': None,
#                         'json_seconds_ago': None
#                     }

#             live, acc, total = self._compute_live_and_accumulated(m, now)

#             has_count = m in self.last_count_time
#             has_json = m in self.last_json_time

#             count_seconds_ago = None
#             json_seconds_ago = None

#             if has_count:
#                 count_seconds_ago = int((now - self.last_count_time[m]).total_seconds())

#             if has_json:
#                 json_seconds_ago = int((now - self.last_json_time[m]).total_seconds())

#             is_on = m in self.on_since
#             is_producing = has_count and count_seconds_ago <= 180

#             if not is_on:
#                 status = "OFF"
#                 idle_type = IdleType.NONE
#             elif is_producing:
#                 if live > 0:
#                     status = "Producing (Idle)"
#                 else:
#                     status = "Producing"
#                 idle_type = IdleType.NONE if live == 0 else IdleType.ON_BUT_NOT_PRODUCING
#             else:
#                 if live > 0:
#                     status = "ON (No Count)"
#                 else:
#                     status = "ON (Grace Period)"
#                 idle_type = IdleType.ON_BUT_NOT_PRODUCING if live > 0 else IdleType.NONE

#             return {
#                 'live_idle_time': f'{live}m' if live > 0 else '0m',
#                 'accumulated_idle_time': f'{acc}m',
#                 'hourly_idle_total': min(60, total),
#                 'is_idle': live > 0,
#                 'idle_type': idle_type,
#                 'status': status,
#                 'data_source': self.data_source.get(m, DataSource.NONE),
#                 'on_since': self.on_since.get(m),
#                 'last_count_time': self.last_count_time.get(m),
#                 'count_seconds_ago': count_seconds_ago,
#                 'json_seconds_ago': json_seconds_ago
#             }

#     def reset_hour(self, m: int = None):
#         with self.lock:
#             if m is None:
#                 self.completed_segments_minutes.clear()
#                 self.current_hour_start.clear()
#                 self.hour_had_activity.clear()
#             else:
#                 self.completed_segments_minutes[m] = 0
#                 self.hour_had_activity[m] = False
#                 if m in self.current_hour_start:
#                     del self.current_hour_start[m]


# class Plant2ExactRequirementState:
#     def __init__(self):
#         self.lock = RLock()
#         self.current_hour_counts = defaultdict(int)
#         self.last_hour_counts = defaultdict(int)
#         self.shift_cumulative = defaultdict(int)
#         self.current_hours = {}
#         self.current_shifts = {}

#         self.pending_reasons = {}

#         self.last_count_time = {}
#         self.hour_first_count_time = {}

#         self.machine_json_status = {}
#         self.machine_count_status = {}

#         self.machine_on_since = {}
#         self.first_count_time = {}

#         self.machine_segments = defaultdict(lambda: {
#             'shut_height': None,
#             'tool_id': None,
#             'segment_start': None,
#             'segment_count': 0,
#         })

#         self.off_threshold_seconds = 180
#         self.idle_tracker = StrictIdlePolicy(grace_seconds=180, enable_no_signal_as_idle=True)

#     def set_pending_reason(self, machine_no, category, reason, remarks):
#         """Frontend se aaya reason RAM me save karta hai"""
#         with self.lock:
#             self.pending_reasons[machine_no] = {
#                 'category': category,
#                 'reason': reason,
#                 'remarks': remarks,
#                 'timestamp': datetime.now(pytz.timezone('Asia/Kolkata'))
#             }
#             print(f"📝 Buffer Updated for M{machine_no}: {category} -> {reason}")

#     def save_resolved_downtime_to_db(self, machine_no, now_ist, current_shift, idle_mins, machine_status_val, is_hour_change=False):
#         """DB mein final isolated downtime (Reason ke saath ya bina) save karega"""
#         with self.lock:
#             if idle_mins > 0:
#                 category = "Uncategorized"
#                 specific_reason = "Reason Not Provided"

#                 if machine_no in self.pending_reasons:
#                     pending_data = self.pending_reasons[machine_no]
#                     category = pending_data['category']
#                     specific_reason = pending_data['reason']
#                     if pending_data.get('remarks'):
#                         specific_reason += f" - {pending_data['remarks']}"

#                     # ✅ CARRY FORWARD LOGIC:
#                     # Yahan se 'del self.pending_reasons[machine_no]' hata diya hai taaki
#                     # ghanta change hone par reason RAM se na hate aur agle ghante bhi carry forward ho.

#                 try:
#                     timestamp_str = now_ist.strftime('%Y-%m-%d %H:%M:%S+05:30')

#                     with connection.cursor() as cursor:
#                         cursor.execute("""
#                             INSERT INTO "hourly_downtime_logs"
#                             (timestamp, machine_no, idle_time, shift, reason_category, specific_reason, machine_status)
#                             VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s)
#                         """, (
#                             timestamp_str,
#                             str(machine_no),
#                             int(idle_mins),
#                             current_shift,
#                             category,
#                             specific_reason[:255],
#                             machine_status_val
#                         ))
#                     print(f"✅ DOWNTIME LOGGED | M{machine_no} | {machine_status_val} | {category} | Idle: {idle_mins}m")
#                 except Exception as e:
#                     print(f"❌ DB Downtime Save Error M{machine_no}: {e}")

#     def get_shift_from_time(self, dt):
#         ist_dt = dt.astimezone(pytz.timezone('Asia/Kolkata')) if dt.tzinfo else pytz.timezone('Asia/Kolkata').localize(dt)
#         time_only = ist_dt.time()
#         shift_A_start = datetime.strptime("08:30", "%H:%M").time()
#         shift_A_end = datetime.strptime("20:00", "%H:%M").time()
#         return 'A' if shift_A_start <= time_only < shift_A_end else 'B'

#     def get_shift_start_datetime(self, timestamp):
#         date = timestamp.date()
#         shift = self.get_shift_from_time(timestamp)

#         shift_a_start_time = datetime.strptime("08:30", "%H:%M").time()
#         shift_b_start_time = datetime.strptime("20:30", "%H:%M").time()

#         if shift == 'A':
#             return IST.localize(datetime.combine(date, shift_a_start_time))
#         else:
#             if timestamp.time() < shift_a_start_time:
#                 prev_day = date - timedelta(days=1)
#                 return IST.localize(datetime.combine(prev_day, shift_b_start_time))
#             else:
#                 return IST.localize(datetime.combine(date, shift_b_start_time))

#     def get_shift_idle_from_hourly_table(self, machine_no, shift_start, shift, now):
#         try:
#             shift_start_naive = convert_to_naive_ist(shift_start)
#             now_naive = convert_to_naive_ist(now)

#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT COALESCE(SUM(idle_time), 0)
#                     FROM "Plant2_hourly_idle"
#                     WHERE machine_no = %s
#                     AND shift = %s
#                     AND DATE(timestamp) = DATE(%s)
#                     AND timestamp >= %s
#                     AND timestamp < %s
#                 """, (str(machine_no), shift, shift_start_naive, shift_start_naive, now_naive))

#                 result = cursor.fetchone()
#                 db_idle = int(result[0]) if result and result[0] else 0

#             current_idle = self.idle_tracker.get_idle_status(machine_no, now)
#             live_idle = current_idle['hourly_idle_total']

#             total_shift_idle = db_idle + live_idle

#             return total_shift_idle

#         except Exception as e:
#             print(f"❌ Error fetching shift idle M{machine_no}: {e}")
#             traceback.print_exc()
#             return 0

#     def reset_shift_state(self, machine_no=None):
#         with self.lock:
#             if machine_no is None:
#                 self.machine_on_since.clear()
#                 self.first_count_time.clear()
#                 self.pending_reasons.clear() # ✅ SHIFT RESET: Saare purane reasons hata do
#                 print("🔄 All machines: Shift state & reasons reset")
#             else:
#                 self.machine_on_since.pop(machine_no, None)
#                 self.first_count_time.pop(machine_no, None)
#                 self.pending_reasons.pop(machine_no, None) # ✅ SINGLE RESET: Specific machine ka reason hata do
#                 print(f"🔄 M{machine_no}: Shift state & reason reset")

#     def update_json_status(self, machine_no, card=None, die_height=0.0):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)

#             if machine_no not in self.machine_on_since:
#                 self.machine_on_since[machine_no] = now_ist

#             self.machine_json_status[machine_no] = {
#                 'last_json_time': now_ist,
#                 'card': card or 'UNKNOWN',
#                 'die_height': die_height
#             }

#             self.idle_tracker.mark_json(machine_no, now_ist)

#     def add_count(self, machine_no, count_increment=1, tool_id=None, shut_height=None):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)
#             current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
#             current_shift = self.get_shift_from_time(now_ist)

#             # ✅ COUNT AAYA: Machine working state me aa gayi, toh idle reason ko clear kar do!
#             self.pending_reasons.pop(machine_no, None)

#             if machine_no not in self.machine_on_since:
#                 self.machine_on_since[machine_no] = now_ist
#                 print(f"🟢 M{machine_no}: Machine ON at {now_ist.strftime('%H:%M:%S')}")

#             if machine_no not in self.first_count_time:
#                 self.first_count_time[machine_no] = now_ist
#                 print(f"🎯 M{machine_no}: First count at {now_ist.strftime('%H:%M:%S')}")

#             if machine_no not in self.hour_first_count_time or \
#                self.hour_first_count_time[machine_no].replace(minute=0, second=0, microsecond=0) != current_hour:
#                 self.hour_first_count_time[machine_no] = now_ist

#             self.last_count_time[machine_no] = now_ist

#             self.machine_count_status[machine_no] = {
#                 'last_count_time': now_ist,
#                 'tool_id': tool_id if tool_id else 'UNKNOWN',
#                 'shut_height': shut_height if shut_height else "No data"
#             }

#             segment = self.machine_segments[machine_no]

#             is_valid_height = False
#             new_height_value = None

#             if shut_height not in ['No data', 'Failed', None, 0, 0.0, '0', '0.0', '']:
#                 try:
#                     new_height_value = float(shut_height)
#                     if new_height_value > 1.0:
#                         is_valid_height = True
#                 except:
#                     is_valid_height = False

#             if is_valid_height:
#                 if segment['shut_height'] is None or segment['shut_height'] == 0.0:
#                     segment['shut_height'] = new_height_value
#                     segment['tool_id'] = tool_id
#                     segment['segment_start'] = now_ist
#                     segment['segment_count'] = count_increment
#                 else:
#                     old_height = segment['shut_height']
#                     height_difference = abs(old_height - new_height_value)
#                     height_changed = height_difference > 1.0

#                     if height_changed:
#                         threading.Thread(
#                             target=send_shut_height_alert,
#                             args=(2, machine_no, old_height, new_height_value, now_ist),
#                             daemon=True
#                         ).start()

#                         log_machine_event(
#                             plant_no=2,
#                             machine_no=machine_no,
#                             event_type="SHUT_HEIGHT_CHANGE",
#                             timestamp=now_ist,
#                             shift=current_shift,
#                             details=f"Height changed from {old_height} to {new_height_value} | Tool: {tool_id}"
#                         )

#                         segment['shut_height'] = new_height_value
#                         segment['tool_id'] = tool_id
#                         segment['segment_start'] = now_ist
#                         segment['segment_count'] = count_increment
#                     else:
#                         segment['segment_count'] += count_increment
#             else:
#                 if segment['shut_height'] and segment['shut_height'] > 0:
#                     segment['segment_count'] += count_increment

#             if machine_no in self.current_hours:
#                 if self.current_hours[machine_no] != current_hour:
#                     self.last_hour_counts[machine_no] = self.current_hour_counts[machine_no]
#                     old_count = self.current_hour_counts[machine_no]
#                     self.current_hour_counts[machine_no] = 0
#                     self.current_hours[machine_no] = current_hour

#                     print(f"⏰ M{machine_no}: Hour changed | Last={old_count}, New=0")
#             else:
#                 self.current_hours[machine_no] = current_hour

#             if machine_no in self.current_shifts:
#                 old_shift = self.current_shifts[machine_no]
#                 if old_shift != current_shift:
#                     print(f"🔄 M{machine_no}: Shift changed {old_shift}→{current_shift}")
#                     new_shift_key = (machine_no, current_shift)
#                     self.shift_cumulative[new_shift_key] = 0
#                     self.reset_shift_state(machine_no)

#             self.current_shifts[machine_no] = current_shift
#             self.current_hour_counts[machine_no] += count_increment

#             idle_status = self.idle_tracker.get_idle_status(machine_no, now_ist)
#             live_idle_str = idle_status.get('live_idle_time', '0m')
#             live_idle_mins = int(live_idle_str.replace('m', ''))

#             if live_idle_mins > 0:
#                 # ✅ Count aya hai matlab machine ON ho chuki hai
#                 self.save_resolved_downtime_to_db(machine_no, now_ist, current_shift, live_idle_mins, "ONLINE", is_hour_change=False)

#             self.idle_tracker.mark_count(machine_no, now_ist)

#             self._insert_realtime_count(
#                 machine_no=machine_no,
#                 count_increment=count_increment,
#                 tool_id=tool_id,
#                 shut_height=shut_height,
#                 timestamp=now_ist,
#                 shift=current_shift
#             )

#     def _insert_realtime_count(self, machine_no, count_increment, tool_id, shut_height, timestamp, shift):
#         try:
#             shift_start = self.get_shift_start_datetime(timestamp)

#             last_cumulative = 0
#             try:
#                 shift_start_naive = convert_to_naive_ist(shift_start)
#                 with connection.cursor() as cursor:
#                     cursor.execute("""
#                         SELECT cumulative_count FROM Plant2_data
#                         WHERE machine_no = %s AND shift = %s AND timestamp >= %s
#                         ORDER BY timestamp DESC LIMIT 1
#                     """, (str(machine_no), shift, shift_start_naive))
#                     result = cursor.fetchone()
#                     if result and result[0] is not None:
#                         last_cumulative = int(result[0])
#             except Exception:
#                 pass

#             new_cumulative = last_cumulative + int(count_increment)

#             idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
#             idle_time = idle_status['hourly_idle_total']

#             clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

#             if isinstance(shut_height, (int, float)) and shut_height > 0:
#                 clean_shut_height = f"{float(shut_height):.2f}"
#             else:
#                 try:
#                     val = float(shut_height)
#                     clean_shut_height = f"{val:.2f}" if val > 0 else "0.00"
#                 except:
#                     clean_shut_height = "0.00"

#             clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 0

#             if timestamp.tzinfo is not None:
#                 ist_timestamp = timestamp.astimezone(IST)
#             else:
#                 ist_timestamp = IST.localize(timestamp)

#             timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S+05:30')

#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     INSERT INTO Plant2_data
#                     (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                     VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
#                 """, (
#                     timestamp_str,
#                     clean_tool_id,
#                     str(machine_no),
#                     int(count_increment),
#                     new_cumulative,
#                     0,
#                     clean_idle_time,
#                     clean_shut_height,
#                     shift
#                 ))

#         except Exception as e:
#             print(f"❌ Insert error M{machine_no}: {e}")
#             traceback.print_exc()

#     def save_segment_to_db(self, machine_no, segment):
#         count = segment['segment_count']
#         if count == 0:
#             return

#         timestamp = segment['segment_start']
#         tool_id = segment['tool_id']
#         shut_height = segment['shut_height']

#         shift = self.get_shift_from_time(timestamp)
#         shift_start = self.get_shift_start_datetime(timestamp)

#         last_cumulative = 0
#         try:
#             shift_start_naive = convert_to_naive_ist(shift_start)
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT cumulative_count FROM Plant2_data
#                     WHERE machine_no = %s AND shift = %s AND timestamp >= %s
#                     ORDER BY timestamp DESC LIMIT 1
#                 """, (str(machine_no), shift, shift_start_naive))
#                 result = cursor.fetchone()
#                 if result:
#                     last_cumulative = result[0]
#         except Exception:
#             pass

#         new_cumulative = last_cumulative + count
#         idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
#         idle_time = idle_status['hourly_idle_total']

#         try:
#             clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

#             if isinstance(shut_height, (int, float)) and shut_height > 0:
#                 clean_shut_height = f"{float(shut_height):.2f}"
#             else:
#                clean_shut_height = "0.00"

#             clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 0

#             if timestamp.tzinfo is not None:
#                 ist_timestamp = timestamp.astimezone(IST)
#             else:
#                 ist_timestamp = IST.localize(timestamp)

#             timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S+05:30')

#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     INSERT INTO Plant2_data (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                     VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
#                 """, (timestamp_str, clean_tool_id, str(machine_no), count, new_cumulative, 0, clean_idle_time, clean_shut_height, shift))

#         except Exception as e:
#             print(f"❌ Error inserting segment M{machine_no}: {e}")

#         segment['segment_count'] = 0

#     def get_machine_status(self, machine_no):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)

#             has_count = False
#             count_seconds_ago = None
#             count_tool_id = None
#             count_shut_height = None

#             if machine_no in self.machine_count_status:
#                 last_count = self.machine_count_status[machine_no]['last_count_time']
#                 count_seconds_ago = (now_ist - last_count).total_seconds()
#                 count_tool_id = self.machine_count_status[machine_no]['tool_id']
#                 count_shut_height = self.machine_count_status[machine_no]['shut_height']

#                 if count_seconds_ago <= self.off_threshold_seconds:
#                     has_count = True

#             has_json = False
#             json_seconds_ago = None
#             json_card = None
#             json_die_height = None

#             if machine_no in self.machine_json_status:
#                 last_json = self.machine_json_status[machine_no]['last_json_time']
#                 json_seconds_ago = (now_ist - last_json).total_seconds()
#                 json_card = self.machine_json_status[machine_no]['card']
#                 json_die_height = self.machine_json_status[machine_no]['die_height']

#                 if json_seconds_ago <= self.off_threshold_seconds:
#                     has_json = True

#             machine_on = has_count or has_json
#             is_producing = has_count

#             offline_since = None
#             offline_duration_minutes = None

#             if not machine_on:
#                 last_activity_time = None

#                 if machine_no in self.machine_count_status and machine_no in self.machine_json_status:
#                     last_activity_time = max(
#                         self.machine_count_status[machine_no]['last_count_time'],
#                         self.machine_json_status[machine_no]['last_json_time']
#                     )
#                 elif machine_no in self.machine_count_status:
#                     last_activity_time = self.machine_count_status[machine_no]['last_count_time']
#                 elif machine_no in self.machine_json_status:
#                     last_activity_time = self.machine_json_status[machine_no]['last_json_time']

#                 if last_activity_time:
#                     offline_since = last_activity_time
#                     offline_duration_seconds = (now_ist - last_activity_time).total_seconds()
#                     offline_duration_minutes = int(offline_duration_seconds / 60)

#                 self.idle_tracker.mark_off(machine_no)

#             if count_tool_id:
#                 tool_id = count_tool_id
#                 shut_height = count_shut_height
#             elif json_card:
#                 tool_id = json_card
#                 shut_height = json_die_height if json_die_height != 0.0 else "No data"
#             else:
#                 tool_id = 'N/A'
#                 shut_height = "No data"

#             return {
#                 'machine_on': machine_on,
#                 'is_producing': is_producing,
#                 'has_count_data': has_count,
#                 'has_json_data': has_json,
#                 'count_seconds_ago': int(count_seconds_ago) if count_seconds_ago is not None else None,
#                 'json_seconds_ago': int(json_seconds_ago) if json_seconds_ago is not None else None,
#                 'tool_id': tool_id,
#                 'shut_height': shut_height,
#                 'data_source': 'COUNT' if has_count else ('JSON' if has_json else 'NONE'),
#                 'offline_since': offline_since.strftime('%H:%M:%S') if offline_since else None,
#                 'offline_duration_minutes': offline_duration_minutes,
#             }

#     def get_machine_data(self, machine_no):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)
#             current_shift = self.get_shift_from_time(now_ist)
#             current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
#             shift_start = self.get_shift_start_datetime(now_ist)

#         last_hour_count_db = 0
#         try:
#             previous_hour_start = current_hour - timedelta(hours=1)
#             previous_hour_end = current_hour
#             previous_hour_start_naive = convert_to_naive_ist(previous_hour_start)
#             previous_hour_end_naive = convert_to_naive_ist(previous_hour_end)

#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT COALESCE(SUM(count), 0) FROM Plant2_data
#                     WHERE machine_no = %s
#                     AND timestamp >= %s
#                     AND timestamp < %s
#                 """, (str(machine_no), previous_hour_start_naive, previous_hour_end_naive))
#                 result = cursor.fetchone()
#                 if result and result[0] is not None:
#                    last_hour_count_db = int(result[0])
#         except Exception as e:
#             pass

#         cumulative_from_db = 0
#         try:
#             shift_start_naive = convert_to_naive_ist(shift_start)
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT cumulative_count FROM Plant2_data
#                     WHERE machine_no = %s AND shift = %s AND timestamp >= %s
#                     ORDER BY timestamp DESC LIMIT 1
#                 """, (str(machine_no), current_shift, shift_start_naive))
#                 result = cursor.fetchone()
#                 if result and result[0] is not None:
#                     cumulative_from_db = int(result[0])
#         except Exception as e:
#             pass

#         live_cumulative = cumulative_from_db

#         status_info = self.get_machine_status(machine_no)

#         idle_status = self.idle_tracker.get_idle_status(machine_no, now_ist)
#         hourly_idle_total = idle_status['hourly_idle_total']

#         total_shift_idle = self.get_shift_idle_from_hourly_table(
#             machine_no, shift_start, current_shift, now_ist
#         )

#         on_since_str = None
#         first_count_str = None
#         time_to_first_count = None

#         if machine_no in self.machine_on_since and status_info['machine_on']:
#             on_since = self.machine_on_since[machine_no]
#             on_since_str = on_since.strftime('%H:%M:%S')

#             if machine_no in self.first_count_time:
#                 first_count = self.first_count_time[machine_no]
#                 first_count_str = first_count.strftime('%H:%M:%S')
#                 delay = (first_count - on_since).total_seconds()
#                 time_to_first_count = int(delay / 60)

#         if machine_no in self.machine_on_since and not status_info['machine_on']:
#             on_since = self.machine_on_since[machine_no]
#             on_since_str = on_since.strftime('%H:%M:%S')

#             if machine_no in self.first_count_time:
#                 first_count = self.first_count_time[machine_no]
#                 first_count_str = first_count.strftime('%H:%M:%S')

#         return {
#             'machine_no': machine_no,
#             'current_hour_count': self.current_hour_counts.get(machine_no, 0),
#             'last_hour_count': last_hour_count_db,
#             'cumulative_count': live_cumulative,
#             'idle_time': hourly_idle_total,
#             'total_shift_idle_time': total_shift_idle,
#             'shift': current_shift,
#             'machine_on': status_info['machine_on'],
#             'is_producing': status_info['is_producing'],
#             'has_count_data': status_info['has_count_data'],
#             'has_json_data': status_info['has_json_data'],
#             'count_seconds_ago': status_info['count_seconds_ago'],
#             'json_seconds_ago': status_info['json_seconds_ago'],
#             'current_tool_id': status_info['tool_id'],
#             'current_shut_height': status_info['shut_height'],
#             'data_source': status_info['data_source'],
#             'on_since': on_since_str,
#             'first_count_at': first_count_str,
#             'time_to_first_count': time_to_first_count,
#             'has_pending_reason': machine_no in self.pending_reasons
#         }

#     def force_hour_reset_all_machines(self):
#         pass


# EXACT_REQUIREMENT_STATE = Plant2ExactRequirementState()
# PLANT2_EXACT_REQUIREMENT_STATE = EXACT_REQUIREMENT_STATE

# _messages_lock = threading.Lock()

# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1),
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1)
# ]

# TOPIC_MACHINE_MAPPING = {
#     'COUNT3': [1, 2, 3, 4, 5],
#     'COUNT2': [6, 7, 8, 9, 10],
#     'COUNT52': [11, 12, 13, 14, 15],
#     'COUNT1': [16, 17, 18, 19, 20],
#     'COUNT4': [41, 42, 43, 44, 45, 46],
#     'COUNT16': [21, 22, 23, 24, 25],
#     'COUNT17': [26, 27, 28, 29, 30],
#     'COUNT18': [31, 32, 33, 34, 35],
#     'COUNT19': [36, 37, 38, 39, 40],
#     'COUNT': []
# }

# MACHINE_GROUP_MAPPING = {
#     'J4': [1, 2, 3, 4, 5],
#     'J3': [6, 7, 8, 9, 10],
#     'J2': [11, 12, 13, 14, 15],
#     'J1': [16, 17, 18, 19, 20],
#     'J5': [41, 42, 43, 44, 45, 46],
#     'J6': [21, 22, 23, 24, 25],
#     'J7': [26, 27, 28, 29, 30],
#     'J8': [31, 32, 33, 34, 35],
#     'J9': [36, 37, 38, 39, 40]
# }

# def get_machine_group(machine_no):
#     for group_name, machines in MACHINE_GROUP_MAPPING.items():
#         if machine_no in machines:
#             return group_name
#     return 'Unknown'

# ACTIVE_MACHINES_THIS_HOUR = set()
# MACHINE_DATA_CACHE = {}

# def get_machines_for_topic(topic):
#     return TOPIC_MACHINE_MAPPING.get(topic, [])

# def parse_json_payload(raw_payload):
#     try:
#         data = json.loads(raw_payload)
#         if 'client_id' not in data:
#             return None

#         client_id = str(data.get('client_id', ''))

#         if len(client_id) >= 2:
#             plant_no = int(client_id[0]) if client_id[0].isdigit() else None
#             machine_no = int(client_id[1:]) if client_id[1:].isdigit() else None
#         else:
#             return None

#         card = data.get('card', 'UNKNOWN')
#         die_height_str = str(data.get('die_height', '0'))
#         try:
#             die_height = float(die_height_str)
#         except:
#             die_height = 0.0

#         return {
#             'type': 'json',
#             'plant_no': plant_no,
#             'machine_no': machine_no,
#             'card': card,
#             'die_height': die_height
#         }
#     except:
#         return None

# def parse_count_payload(raw_payload):
#     try:
#         parts = raw_payload.strip().split()
#         if len(parts) < 2:
#             return None

#         tool_id = parts[0][:24] if len(parts[0]) >= 24 else parts[0]
#         val_str = parts[1]

#         plant_no = int(val_str[0]) if len(val_str) > 0 and val_str[0].isdigit() else None

#         machine_no = None
#         if len(val_str) > 3:
#             if val_str[1].isdigit() and val_str[2].isdigit():
#                 machine_no = int(val_str[1:3])
#                 shut_height_str = val_str[4:]
#             else:
#                 machine_no = int(val_str[1]) if val_str[1].isdigit() else None
#                 shut_height_str = val_str[3:]
#         elif len(val_str) > 2:
#             machine_no = int(val_str[1]) if val_str[1].isdigit() else None
#             shut_height_str = val_str[3:]

#         if 'Failed' in shut_height_str:
#             shut_height = "Failed"
#         elif shut_height_str:
#             try:
#                 shut_height = float(shut_height_str)
#             except:
#                 shut_height = "No data"
#         else:
#             shut_height = "No data"

#         return {
#             'type': 'count',
#             'plant_no': plant_no,
#             'machine_no': machine_no,
#             'tool_id': tool_id,
#             'shut_height': shut_height
#         }
#     except:
#         return None

# def print_active_machines_summary():
#     def summary_worker():
#         while True:
#             try:
#                 time_module.sleep(30)
#                 ist_tz = pytz.timezone('Asia/Kolkata')
#                 now_ist = datetime.now(ist_tz)

#                 with EXACT_REQUIREMENT_STATE.lock:
#                     producing_machines = []
#                     all_machines = set()
#                     for machines_list in TOPIC_MACHINE_MAPPING.values():
#                         all_machines.update(machines_list)

#                     for machine_no in sorted(all_machines):
#                         if machine_no in EXACT_REQUIREMENT_STATE.last_count_time:
#                             last_count = EXACT_REQUIREMENT_STATE.last_count_time[machine_no]
#                             seconds_ago = (now_ist - last_count).total_seconds()

#                             if seconds_ago <= 60:
#                                 hour_count = EXACT_REQUIREMENT_STATE.current_hour_counts.get(machine_no, 0)
#                                 tool_id = 'N/A'
#                                 if machine_no in EXACT_REQUIREMENT_STATE.machine_count_status:
#                                     tool_id = EXACT_REQUIREMENT_STATE.machine_count_status[machine_no].get('tool_id', 'N/A')

#                                 producing_machines.append({
#                                     'no': machine_no,
#                                     'count': hour_count,
#                                     'tool': tool_id[:8] if tool_id != 'N/A' else 'N/A',
#                                     'last': int(seconds_ago)
#                                 })

#                     if producing_machines:
#                         print("\n" + "=" * 80)
#                         print(f"🏭 ACTIVE MACHINES ({len(producing_machines)} running) - {now_ist.strftime('%H:%M:%S')}")
#                         print("=" * 80)

#                         for i in range(0, len(producing_machines), 4):
#                             chunk = producing_machines[i:i+4]
#                             for m in chunk:
#                                 print(f"M{m['no']:02d}: {m['count']:3d}ct | {m['tool']} | {m['last']:2d}s", end="  |  ")
#                             print()
#                         print("=" * 80 + "\n")
#             except Exception as e:
#                 print(f"❌ Summary error: {e}")

#     thread = threading.Thread(target=summary_worker, daemon=True)
#     thread.start()


# def save_hourly_idle_to_db(machine_no, timestamp, tool_id, shut_height, idle_time, shift):
#     try:
#         clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

#         if isinstance(shut_height, (int, float)) and shut_height > 0:
#             clean_shut_height = f"{float(shut_height):.2f}"
#         else:
#             clean_shut_height = "0.00"

#         clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 60

#         if timestamp.tzinfo is not None:
#             ist_timestamp = timestamp.astimezone(IST)
#         else:
#             ist_timestamp = IST.localize(timestamp)

#         # ✅ FIX: +05:30 force kiya
#         timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S+05:30')

#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 INSERT INTO "Plant2_hourly_idle"
#                 (timestamp, tool_id, machine_no, idle_time, shut_height, shift)
#                 VALUES (%s::timestamp WITH TIME ZONE, %s, %s, %s, %s, %s)
#             """, (
#                 timestamp_str, clean_tool_id, str(machine_no), clean_idle_time, clean_shut_height, shift
#             ))
#     except Exception as e:
#         pass


# def save_hourly_idle_time_to_db():
#     def idle_saver_worker():
#         print("\n" + "⏰" * 50)
#         print("⏰ HOURLY IDLE TIME TRACKER STARTED!")
#         print(f"⏰ Snapshot time: XX:59:58")
#         print(f"⏰ Started at: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}")
#         print("⏰" * 50 + "\n")

#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)

#         last_saved_hour = None

#         while True:
#             try:
#                 ist_tz = pytz.timezone('Asia/Kolkata')
#                 now_ist = datetime.now(ist_tz)
#                 current_minute = now_ist.minute
#                 current_second = now_ist.second
#                 current_hour = now_ist.hour

#                 is_snapshot_time = (current_minute == 59 and current_second >= 58)

#                 if is_snapshot_time and last_saved_hour != current_hour:
#                     print("\n" + "💾" * 50)
#                     print(f"💾 HOURLY IDLE SNAPSHOT at {now_ist.strftime('%H:%M:%S')}")
#                     print("💾" * 50 + "\n")

#                     last_saved_hour = current_hour
#                     current_hour_start = now_ist.replace(minute=0, second=0, microsecond=0)

#                     saved_count = 0
#                     for machine_no in sorted(all_mapped_machines):
#                         try:
#                             idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                             idle_time = idle_status['hourly_idle_total']

#                             segment = EXACT_REQUIREMENT_STATE.machine_segments[machine_no]
#                             tool_id = segment.get('tool_id', 'NULL')
#                             shut_height = segment.get('shut_height', 0.0)

#                             shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)

#                             save_hourly_idle_to_db(
#                                 machine_no=machine_no,
#                                 timestamp=current_hour_start,
#                                 tool_id=tool_id,
#                                 shut_height=shut_height,
#                                 idle_time=idle_time,
#                                 shift=shift
#                             )

#                             live_idle_str = idle_status.get('live_idle_time', '0m')
#                             live_idle_mins = int(live_idle_str.replace('m', ''))

#                             machine_current_status = EXACT_REQUIREMENT_STATE.get_machine_status(machine_no)
#                             is_offline_now = not machine_current_status['machine_on']

#                             # ✅ Status depend karega ki abhi offline hai ya online
#                             machine_status_val = "OFFLINE" if is_offline_now else "ONLINE"

#                             if live_idle_mins > 0:
#                                 EXACT_REQUIREMENT_STATE.save_resolved_downtime_to_db(
#                                     machine_no, now_ist, shift, live_idle_mins, machine_status_val, is_hour_change=True
#                                 )

#                             saved_count += 1
#                         except Exception as e:
#                             print(f"❌ M{machine_no} idle save error: {e}")

#                     print(f"\n✅ Saved {saved_count}/{len(all_mapped_machines)} machine idle times\n")

#                 time_module.sleep(1)
#             except Exception as e:
#                 print(f"❌ Idle tracker error: {e}")
#                 traceback.print_exc()
#                 time_module.sleep(5)

#     thread = threading.Thread(target=idle_saver_worker, daemon=True)
#     thread.start()


# # ==============================================================
# # ✅ NAYA FUNCTION: AUTO IDLE NOTIFICATION SENDER
# # ==============================================================
# def auto_generate_idle_notification(machine_no, idle_mins):
#     """Ye function tab chalega jab machine 3 minute se idle hogi. Ye direct DB mein notification dalega."""
#     try:
#         target_group = Group.objects.filter(name='Supervisor').first()
#         if target_group:
#             users = target_group.user_set.all()
#             if users.exists():
#                 message = f"Machine M-{machine_no:02d} is idle for {idle_mins} mins. Please fill the downtime reason!"

#                 notifications_to_create = [
#                     Notification(user=user, machine_no=str(machine_no), message=message)
#                     for user in users
#                 ]
#                 Notification.objects.bulk_create(notifications_to_create)
#                 print(f"🔔 AUTO-ALERT: Notification created for M-{machine_no} (Idle {idle_mins}m)")
#     except Exception as e:
#         print(f"❌ Auto Alert Error M{machine_no}: {e}")
# # ==============================================================


# def start_machine_event_monitor():
#     """Ye background thread har 5 second me ON/OFF check karega"""
#     def monitor_worker():
#         import time as time_module
#         print("🔍 Plant 2 - Machine ON/OFF Event Monitor Started!")
#         machine_last_state = {}

#         # ✅ NAYA: Track karta hai ki kis machine ke liye alert bhej diya gaya hai
#         machine_alert_state = {}

#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)

#         while True:
#             try:
#                 time_module.sleep(5)
#                 ist_tz = pytz.timezone('Asia/Kolkata')
#                 now_ist = datetime.now(ist_tz)

#                 for machine_no in all_mapped_machines:
#                     status = EXACT_REQUIREMENT_STATE.get_machine_status(machine_no)
#                     is_currently_on = status['machine_on']

#                     # ==============================================================
#                     # ✅ NAYA LOGIC: CHECK AND SEND AUTO NOTIFICATIONS
#                     # ==============================================================
#                     idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                     live_idle_str = idle_status.get('live_idle_time', '0m')
#                     live_idle_mins = int(live_idle_str.replace('m', ''))

#                     if live_idle_mins >= 3:
#                         if not machine_alert_state.get(machine_no, False):
#                             auto_generate_idle_notification(machine_no, live_idle_mins)
#                             machine_alert_state[machine_no] = True # Mark that alert is sent

#                     elif live_idle_mins == 0:
#                         machine_alert_state[machine_no] = False # Reset if machine is producing again
#                     # ==============================================================

#                     if machine_no not in machine_last_state:
#                         machine_last_state[machine_no] = is_currently_on
#                         continue

#                     was_on_before = machine_last_state[machine_no]

#                     # ✅ OFFLINE TO ONLINE: Machine mein wapas signal aaya
#                     if is_currently_on and not was_on_before:
#                         shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)

#                         # ✅ OFFLINE to ONLINE aane par RAM se pending reason hata do (Naya idle reason mangega)
#                         EXACT_REQUIREMENT_STATE.pending_reasons.pop(machine_no, None)

#                         # Pehle offline wala gap DB mein save karo
#                         idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                         live_idle_str = idle_status.get('live_idle_time', '0m')
#                         live_idle_mins = int(live_idle_str.replace('m', ''))

#                         if live_idle_mins > 0:
#                             EXACT_REQUIREMENT_STATE.save_resolved_downtime_to_db(
#                                 machine_no, now_ist, shift, live_idle_mins, "OFFLINE to ONLINE", is_hour_change=False
#                             )

#                         log_machine_event(
#                             plant_no=2,
#                             machine_no=machine_no,
#                             event_type="ON",
#                             timestamp=now_ist,
#                             shift=shift,
#                             details="Machine Power/Signal Restored"
#                         )
#                         machine_last_state[machine_no] = True

#                     # ✅ ONLINE TO OFFLINE: Machine ka signal toote hue 3 minute se zyada ho gaya
#                     elif not is_currently_on and was_on_before:
#                         exact_off_time_str = status['offline_since']

#                         if exact_off_time_str:
#                             today = now_ist.date()
#                             time_obj = datetime.strptime(exact_off_time_str, '%H:%M:%S').time()
#                             exact_off_time = IST.localize(datetime.combine(today, time_obj))
#                         else:
#                             exact_off_time = now_ist

#                         shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(exact_off_time)

#                         # Machine offline ho gayi (3 min grace ke baad), abhi tak ka gap DB mein daalo
#                         idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                         live_idle_str = idle_status.get('live_idle_time', '0m')
#                         live_idle_mins = int(live_idle_str.replace('m', ''))

#                         if live_idle_mins > 0:
#                             EXACT_REQUIREMENT_STATE.save_resolved_downtime_to_db(
#                                 machine_no, now_ist, shift, live_idle_mins, "ONLINE to OFFLINE", is_hour_change=False
#                             )

#                         log_machine_event(
#                             plant_no=2,
#                             machine_no=machine_no,
#                             event_type="OFF",
#                             timestamp=exact_off_time,
#                             shift=shift,
#                             details="Machine Offline (No signal for 3 mins)"
#                         )
#                         machine_last_state[machine_no] = False

#             except Exception as e:
#                 print(f"❌ Event Monitor Error: {e}")
#                 time_module.sleep(5)

#     thread = threading.Thread(target=monitor_worker, daemon=True)
#     thread.start()


# def on_message(client, userdata, msg):
#     try:
#         topic = msg.topic
#         raw_payload = msg.payload.decode('utf-8', errors='ignore').strip()

#         if topic.startswith('J'):
#             parsed = parse_json_payload(raw_payload)
#             if parsed and parsed['plant_no'] == 2:
#                 machine_no = parsed['machine_no']
#                 card = parsed['card']
#                 die_height = parsed['die_height']

#                 EXACT_REQUIREMENT_STATE.update_json_status(
#                     machine_no=machine_no,
#                     card=card,
#                     die_height=die_height
#                 )

#         elif topic.startswith('COUNT'):
#             parsed = parse_count_payload(raw_payload)
#             if parsed and parsed['plant_no'] == 2:
#                 machine_no = parsed['machine_no']
#                 tool_id = parsed['tool_id']
#                 shut_height = parsed['shut_height']

#                 EXACT_REQUIREMENT_STATE.add_count(
#                     machine_no=machine_no,
#                     count_increment=1,
#                     tool_id=tool_id,
#                     shut_height=shut_height
#                 )

#     except Exception as e:
#         print(f"❌ on_message error: {e}")
#         traceback.print_exc()


# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Connected to MQTT Broker (Plant 2)")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#             print(f"📥 Subscribed: {topic}")
#     else:
#         print(f"❌ Connection failed with code {rc}")


# def start_plant2_mqtt():
#     print("\n" + "🚀" * 50)
#     print("🚀 STARTING PLANT 2 MQTT CLIENT")
#     print("🚀" * 50 + "\n")

#     client = mqtt.Client(client_id="plant2_exact_backend", clean_session=True)
#     client.username_pw_set(USERNAME, PASSWORD)
#     client.on_connect = on_connect
#     client.on_message = on_message

#     try:
#         client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     except Exception as e:
#         print(f"❌ MQTT connection error: {e}")
#         return

#     print_active_machines_summary()
#     save_hourly_idle_time_to_db()

#     start_machine_event_monitor()

#     client.loop_start()
#     print("✅ MQTT Loop Started (Plant 2)\n")


# # backend/apps/mqtt/simple_plant2.py - ULTIMATE FIXED VERSION DB CONNECTION FIX V2

# import paho.mqtt.client as mqtt
# from datetime import datetime, timedelta
# import threading
# from apps.machines.machine_state import MACHINE_STATE
# from apps.data_storage.hourly_idle_tracker import HOURLY_IDLE_TRACKER
# import traceback
# import pytz
# from django.db import connection, transaction, close_old_connections
# import time as time_module
# from threading import RLock
# from collections import defaultdict
# import json
# from apps.utils.email_alert import send_shut_height_alert
# import os
# import redis  # ✅ NAYA IMPORT REDIS KE LIYE
# import queue
# import socket

# # ✅ NAYE IMPORTS AUTOMATIC NOTIFICATION KE LIYE
# from api.models import Notification
# from django.contrib.auth.models import Group

# # ✅ WEBSOCKET BROADCAST KE LIYE IMPORTS (NAYA ADD KIYA HAI)
# from channels.layers import get_channel_layer
# from asgiref.sync import async_to_sync

# # ✅ ULTIMATE FIX 1: Force system timezone to IST
# os.environ['TZ'] = 'Asia/Kolkata'

# IST = pytz.timezone("Asia/Kolkata")


# def refresh_db_connection():
#     """
#     Background threads (MQTT/Redis/summary/notification) me Django DB connection
#     stale/closed ho sakta hai. Is helper se old/closed connection close hota hai
#     aur fresh connection open hota hai.
#     """
#     try:
#         close_old_connections()
#         try:
#             # psycopg2 closed: 0 means open, non-zero means closed/broken
#             if connection.connection is not None and getattr(connection.connection, "closed", 0):
#                 connection.close()
#         except Exception:
#             try:
#                 connection.close()
#             except Exception:
#                 pass
#         connection.ensure_connection()
#     except Exception:
#         try:
#             connection.close()
#         except Exception:
#             pass
#         close_old_connections()
#         connection.ensure_connection()

# # ==============================================================
# # ✅ REDIS CONNECTION SETUP
# # ==============================================================
# REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
# REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
# try:
#     redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
# except Exception as e:
#     print(f"❌ Initial Redis Setup Error: {e}")
# # ==============================================================


# class IdleType:
#     ON_BUT_NOT_PRODUCING = "ON_BUT_NOT_PRODUCING"
#     NO_SIGNAL_AS_IDLE = "NO_SIGNAL_AS_IDLE"
#     NONE = "NONE"


# class DataSource:
#     COUNT = "COUNT"
#     JSON = "JSON"
#     NONE = "NONE"


# def convert_to_naive_ist(timestamp):
#     """
#     Convert to IST and store as-is (no timezone)
#     Django will treat it as local time
#     """
#     if timestamp.tzinfo is not None:
#         ist_timestamp = timestamp.astimezone(IST)
#     else:
#         ist_timestamp = IST.localize(timestamp)

#     # Create clean datetime (IST time as naive)
#     naive_ist = datetime(
#         ist_timestamp.year,
#         ist_timestamp.month,
#         ist_timestamp.day,
#         ist_timestamp.hour,
#         ist_timestamp.minute,
#         ist_timestamp.second
#     )
#     return naive_ist


# def log_machine_event(plant_no, machine_no, event_type, timestamp, shift, details=""):
#     try:
#         from django.db import connection, close_old_connections
#         import pytz

#         IST = pytz.timezone("Asia/Kolkata")
#         if timestamp.tzinfo is not None:
#             ist_timestamp = timestamp.astimezone(IST)
#         else:
#             ist_timestamp = IST.localize(timestamp)

#         # ✅ FIX: +05:30 force kiya aur WITH TIME ZONE lagaya
#         timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S+05:30')

#         refresh_db_connection()
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 INSERT INTO "Machine_Event_Logs"
#                 (plant_no, machine_no, event_type, timestamp, shift, details)
#                 VALUES (%s, %s, %s, %s::timestamp WITH TIME ZONE, %s, %s)
#             """, (plant_no, str(machine_no), event_type, timestamp_str, shift, details))

#         print(f"📝 EVENT SAVED | P{plant_no}-M{machine_no} | {event_type} | {timestamp_str}")
#     except Exception as e:
#         print(f"❌ Event Log Error P{plant_no}-M{machine_no}: {e}")


# class StrictIdlePolicy:
#     def __init__(self, grace_seconds=180, enable_no_signal_as_idle=True):
#         self.lock = RLock()
#         self.grace_seconds = grace_seconds
#         self.enable_no_signal_as_idle = enable_no_signal_as_idle

#         self.on_since = {}
#         self.last_count_time = {}
#         self.last_json_time = {}
#         self.current_hour_start = {}
#         self.completed_segments_minutes = {}
#         self.data_source = {}
#         self.hour_had_activity = {}

#     @staticmethod
#     def _ist(dt: datetime) -> datetime:
#         if dt is None:
#             return None
#         if dt.tzinfo is None:
#             return IST.localize(dt)
#         return dt.astimezone(IST)

#     @staticmethod
#     def _hour_start(dt: datetime) -> datetime:
#         dt = StrictIdlePolicy._ist(dt)
#         return dt.replace(minute=0, second=0, microsecond=0)

#     def _ensure_current_hour(self, m: int, now: datetime):
#         hour = self._hour_start(now)
#         prev = self.current_hour_start.get(m)

#         if prev is None or prev != hour:
#             self.current_hour_start[m] = hour
#             self.completed_segments_minutes[m] = 0
#             self.hour_had_activity[m] = False

#     def mark_json(self, m: int, t: datetime):
#         with self.lock:
#             now = self._ist(t)
#             self.last_json_time[m] = now
#             self.data_source[m] = DataSource.JSON

#             if m not in self.on_since:
#                 self.on_since[m] = now

#             self._ensure_current_hour(m, now)
#             self.hour_had_activity[m] = True

#     def mark_count(self, m: int, t: datetime):
#         with self.lock:
#             now = self._ist(t)
#             prev_count = self.last_count_time.get(m)

#             if prev_count is not None:
#                 live, acc, total = self._compute_live_and_accumulated(m, now)

#                 if live > 0:
#                     self.completed_segments_minutes[m] = self.completed_segments_minutes.get(m, 0) + live

#             self.last_count_time[m] = now
#             self.data_source[m] = DataSource.COUNT

#             if m not in self.on_since:
#                 self.on_since[m] = now

#             self._ensure_current_hour(m, now)
#             self.hour_had_activity[m] = True

#     def mark_off(self, m: int):
#         with self.lock:
#             self.data_source[m] = DataSource.NONE

#     def _compute_base_time(self, m: int, now: datetime) -> datetime:
#         hour_start = self.current_hour_start.get(m, self._hour_start(now))
#         candidates = [hour_start]

#         if m in self.on_since:
#             candidates.append(self.on_since[m])

#         if m in self.last_count_time:
#             candidates.append(self.last_count_time[m])

#         return max(candidates)

#     def _compute_live_and_accumulated(self, m: int, now: datetime):
#         if m not in self.on_since:
#             return (0, 0, 0)

#         base_time = self._compute_base_time(m, now)
#         gap_seconds = (now - base_time).total_seconds()

#         if gap_seconds < self.grace_seconds:
#             live_idle = 0
#             accumulated_idle = 0
#         else:
#             visible_minutes = int(gap_seconds / 60)
#             live_idle = visible_minutes
#             accumulated_idle = visible_minutes

#         completed = self.completed_segments_minutes.get(m, 0)
#         hourly_total = completed + live_idle

#         return (live_idle, accumulated_idle, hourly_total)

#     def get_idle_status(self, m: int, now: datetime = None):
#         with self.lock:
#             if now is None:
#                 now = datetime.now(IST)
#             now = self._ist(now)

#             self._ensure_current_hour(m, now)

#             if self.enable_no_signal_as_idle:
#                 is_never_active = m not in self.on_since and \
#                                 m not in self.last_count_time and \
#                                 m not in self.last_json_time

#                 if is_never_active:
#                     hour_start = self._hour_start(now)

#                     shift_a_start = now.replace(hour=8, minute=30, second=0, microsecond=0)
#                     shift_b_start = now.replace(hour=20, minute=30, second=0, microsecond=0)

#                     if shift_a_start <= now < shift_b_start:
#                         actual_start = max(hour_start, shift_a_start)
#                     elif now >= shift_b_start:
#                         actual_start = max(hour_start, shift_b_start)
#                     else:
#                         prev_shift_b = shift_b_start - timedelta(days=1)
#                         actual_start = max(hour_start, prev_shift_b)

#                     elapsed_seconds = max(0, (now - actual_start).total_seconds())
#                     elapsed_mins = int(elapsed_seconds / 60)

#                     return {
#                         'live_idle_time': f'{elapsed_mins}m',
#                         'accumulated_idle_time': f'{elapsed_mins}m',
#                         'hourly_idle_total': elapsed_mins,
#                         'is_idle': True,
#                         'idle_type': IdleType.NO_SIGNAL_AS_IDLE,
#                         'status': 'No Signal (Offline)',
#                         'data_source': DataSource.NONE,
#                         'on_since': None,
#                         'last_count_time': None,
#                         'count_seconds_ago': None,
#                         'json_seconds_ago': None
#                     }

#             live, acc, total = self._compute_live_and_accumulated(m, now)

#             has_count = m in self.last_count_time
#             has_json = m in self.last_json_time

#             count_seconds_ago = None
#             json_seconds_ago = None

#             if has_count:
#                 count_seconds_ago = int((now - self.last_count_time[m]).total_seconds())

#             if has_json:
#                 json_seconds_ago = int((now - self.last_json_time[m]).total_seconds())

#             is_on = m in self.on_since
#             is_producing = has_count and count_seconds_ago <= 180

#             if not is_on:
#                 status = "OFF"
#                 idle_type = IdleType.NONE
#             elif is_producing:
#                 if live > 0:
#                     status = "Producing (Idle)"
#                 else:
#                     status = "Producing"
#                 idle_type = IdleType.NONE if live == 0 else IdleType.ON_BUT_NOT_PRODUCING
#             else:
#                 if live > 0:
#                     status = "ON (No Count)"
#                 else:
#                     status = "ON (Grace Period)"
#                 idle_type = IdleType.ON_BUT_NOT_PRODUCING if live > 0 else IdleType.NONE

#             return {
#                 'live_idle_time': f'{live}m' if live > 0 else '0m',
#                 'accumulated_idle_time': f'{acc}m',
#                 'hourly_idle_total': min(60, total),
#                 'is_idle': live > 0,
#                 'idle_type': idle_type,
#                 'status': status,
#                 'data_source': self.data_source.get(m, DataSource.NONE),
#                 'on_since': self.on_since.get(m),
#                 'last_count_time': self.last_count_time.get(m),
#                 'count_seconds_ago': count_seconds_ago,
#                 'json_seconds_ago': json_seconds_ago
#             }

#     def reset_hour(self, m: int = None):
#         with self.lock:
#             if m is None:
#                 self.completed_segments_minutes.clear()
#                 self.current_hour_start.clear()
#                 self.hour_had_activity.clear()
#             else:
#                 self.completed_segments_minutes[m] = 0
#                 self.hour_had_activity[m] = False
#                 if m in self.current_hour_start:
#                     del self.current_hour_start[m]


# class Plant2ExactRequirementState:
#     def __init__(self):
#         self.lock = RLock()
#         self.current_hour_counts = defaultdict(int)
#         self.last_hour_counts = defaultdict(int)
#         self.shift_cumulative = defaultdict(int)
#         self.current_hours = {}
#         self.current_shifts = {}

#         self.pending_reasons = {}

#         self.last_count_time = {}
#         self.hour_first_count_time = {}

#         self.machine_json_status = {}
#         self.machine_count_status = {}

#         self.machine_on_since = {}
#         self.first_count_time = {}

#         self.machine_segments = defaultdict(lambda: {
#             'shut_height': None,
#             'tool_id': None,
#             'segment_start': None,
#             'segment_count': 0,
#         })

#         self.off_threshold_seconds = 180
#         self.idle_tracker = StrictIdlePolicy(grace_seconds=180, enable_no_signal_as_idle=True)

#     def set_pending_reason(self, machine_no, category, reason, remarks):
#         """Frontend se aaya reason RAM me save karta hai"""
#         with self.lock:
#             self.pending_reasons[machine_no] = {
#                 'category': category,
#                 'reason': reason,
#                 'remarks': remarks,
#                 'timestamp': datetime.now(pytz.timezone('Asia/Kolkata'))
#             }
#             print(f"📝 Buffer Updated for M{machine_no}: {category} -> {reason}")

#     def save_resolved_downtime_to_db(self, machine_no, now_ist, current_shift, idle_mins, machine_status_val, is_hour_change=False):
#         """DB mein final isolated downtime (Reason ke saath ya bina) save karega"""
#         with self.lock:
#             if idle_mins > 0:
#                 category = "Uncategorized"
#                 specific_reason = "Reason Not Provided"

#                 if machine_no in self.pending_reasons:
#                     pending_data = self.pending_reasons[machine_no]
#                     category = pending_data['category']
#                     specific_reason = pending_data['reason']
#                     if pending_data.get('remarks'):
#                         specific_reason += f" - {pending_data['remarks']}"

#                     # ✅ CARRY FORWARD LOGIC:
#                     # Yahan se 'del self.pending_reasons[machine_no]' hata diya hai taaki
#                     # ghanta change hone par reason RAM se na hate aur agle ghante bhi carry forward ho.

#                 try:
#                     timestamp_str = now_ist.strftime('%Y-%m-%d %H:%M:%S+05:30')

#                     refresh_db_connection()
#                     with connection.cursor() as cursor:
#                         cursor.execute("""
#                             INSERT INTO "hourly_downtime_logs"
#                             (timestamp, machine_no, idle_time, shift, reason_category, specific_reason, machine_status)
#                             VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s)
#                         """, (
#                             timestamp_str,
#                             str(machine_no),
#                             int(idle_mins),
#                             current_shift,
#                             category,
#                             specific_reason[:255],
#                             machine_status_val
#                         ))
#                     print(f"✅ DOWNTIME LOGGED | M{machine_no} | {machine_status_val} | {category} | Idle: {idle_mins}m")
#                 except Exception as e:
#                     print(f"❌ DB Downtime Save Error M{machine_no}: {e}")

#     def get_shift_from_time(self, dt):
#         ist_dt = dt.astimezone(pytz.timezone('Asia/Kolkata')) if dt.tzinfo else pytz.timezone('Asia/Kolkata').localize(dt)
#         time_only = ist_dt.time()
#         shift_A_start = datetime.strptime("08:30", "%H:%M").time()
#         shift_A_end = datetime.strptime("20:00", "%H:%M").time()
#         return 'A' if shift_A_start <= time_only < shift_A_end else 'B'

#     def get_shift_start_datetime(self, timestamp):
#         date = timestamp.date()
#         shift = self.get_shift_from_time(timestamp)

#         shift_a_start_time = datetime.strptime("08:30", "%H:%M").time()
#         shift_b_start_time = datetime.strptime("20:30", "%H:%M").time()

#         if shift == 'A':
#             return IST.localize(datetime.combine(date, shift_a_start_time))
#         else:
#             if timestamp.time() < shift_a_start_time:
#                 prev_day = date - timedelta(days=1)
#                 return IST.localize(datetime.combine(prev_day, shift_b_start_time))
#             else:
#                 return IST.localize(datetime.combine(date, shift_b_start_time))

#     def get_shift_idle_from_hourly_table(self, machine_no, shift_start, shift, now):
#         try:
#             shift_start_naive = convert_to_naive_ist(shift_start)
#             now_naive = convert_to_naive_ist(now)

#             refresh_db_connection()
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT COALESCE(SUM(idle_time), 0)
#                     FROM "Plant2_hourly_idle"
#                     WHERE machine_no = %s
#                     AND shift = %s
#                     AND DATE(timestamp) = DATE(%s)
#                     AND timestamp >= %s
#                     AND timestamp < %s
#                 """, (str(machine_no), shift, shift_start_naive, shift_start_naive, now_naive))

#                 result = cursor.fetchone()
#                 db_idle = int(result[0]) if result and result[0] else 0

#             current_idle = self.idle_tracker.get_idle_status(machine_no, now)
#             live_idle = current_idle['hourly_idle_total']

#             total_shift_idle = db_idle + live_idle

#             return total_shift_idle

#         except Exception as e:
#             print(f"❌ Error fetching shift idle M{machine_no}: {e}")
#             traceback.print_exc()
#             return 0

#     def get_current_hour_count_from_db(self, machine_no, timestamp):
#         """
#         Current hour ka exact count DB se nikalega.
#         Dashboard/WebSocket ko RAM count nahi, DB count milega.
#         """
#         try:
#             if timestamp.tzinfo is not None:
#                 timestamp = timestamp.astimezone(IST)
#             else:
#                 timestamp = IST.localize(timestamp)

#             current_hour = timestamp.replace(minute=0, second=0, microsecond=0)
#             next_hour = current_hour + timedelta(hours=1)

#             current_hour_naive = convert_to_naive_ist(current_hour)
#             next_hour_naive = convert_to_naive_ist(next_hour)

#             refresh_db_connection()
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT COALESCE(SUM(count), 0)
#                     FROM Plant2_data
#                     WHERE machine_no = %s
#                     AND timestamp >= %s
#                     AND timestamp < %s
#                 """, (
#                     str(machine_no),
#                     current_hour_naive,
#                     next_hour_naive
#                 ))

#                 result = cursor.fetchone()
#                 return int(result[0]) if result and result[0] is not None else 0

#         except Exception as e:
#             print(f"❌ Current hour count DB error M{machine_no}: {e}")
#             return 0

#     def reset_shift_state(self, machine_no=None):
#         with self.lock:
#             if machine_no is None:
#                 self.machine_on_since.clear()
#                 self.first_count_time.clear()
#                 self.pending_reasons.clear() # ✅ SHIFT RESET: Saare purane reasons hata do
#                 print("🔄 All machines: Shift state & reasons reset")
#             else:
#                 self.machine_on_since.pop(machine_no, None)
#                 self.first_count_time.pop(machine_no, None)
#                 self.pending_reasons.pop(machine_no, None) # ✅ SINGLE RESET: Specific machine ka reason hata do
#                 print(f"🔄 M{machine_no}: Shift state & reason reset")

#     def update_json_status(self, machine_no, card=None, die_height=0.0):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)

#             if machine_no not in self.machine_on_since:
#                 self.machine_on_since[machine_no] = now_ist

#             self.machine_json_status[machine_no] = {
#                 'last_json_time': now_ist,
#                 'card': card or 'UNKNOWN',
#                 'die_height': die_height
#             }

#             self.idle_tracker.mark_json(machine_no, now_ist)

#     def add_count(self, machine_no, count_increment=1, tool_id=None, shut_height=None):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)
#             current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
#             current_shift = self.get_shift_from_time(now_ist)

#             # ✅ COUNT AAYA: Machine working state me aa gayi, toh idle reason ko clear kar do!
#             self.pending_reasons.pop(machine_no, None)

#             if machine_no not in self.machine_on_since:
#                 self.machine_on_since[machine_no] = now_ist
#                 print(f"🟢 M{machine_no}: Machine ON at {now_ist.strftime('%H:%M:%S')}")

#             if machine_no not in self.first_count_time:
#                 self.first_count_time[machine_no] = now_ist
#                 print(f"🎯 M{machine_no}: First count at {now_ist.strftime('%H:%M:%S')}")

#             if machine_no not in self.hour_first_count_time or \
#                self.hour_first_count_time[machine_no].replace(minute=0, second=0, microsecond=0) != current_hour:
#                 self.hour_first_count_time[machine_no] = now_ist

#             self.last_count_time[machine_no] = now_ist

#             self.machine_count_status[machine_no] = {
#                 'last_count_time': now_ist,
#                 'tool_id': tool_id if tool_id else 'UNKNOWN',
#                 'shut_height': shut_height if shut_height else "No data"
#             }

#             segment = self.machine_segments[machine_no]

#             is_valid_height = False
#             new_height_value = None

#             if shut_height not in ['No data', 'Failed', None, 0, 0.0, '0', '0.0', '']:
#                 try:
#                     new_height_value = float(shut_height)
#                     if new_height_value > 1.0:
#                         is_valid_height = True
#                 except:
#                     is_valid_height = False

#             if is_valid_height:
#                 if segment['shut_height'] is None or segment['shut_height'] == 0.0:
#                     segment['shut_height'] = new_height_value
#                     segment['tool_id'] = tool_id
#                     segment['segment_start'] = now_ist
#                     segment['segment_count'] = count_increment
#                 else:
#                     old_height = segment['shut_height']
#                     height_difference = abs(old_height - new_height_value)
#                     height_changed = height_difference > 1.0

#                     if height_changed:
#                         threading.Thread(
#                             target=send_shut_height_alert,
#                             args=(2, machine_no, old_height, new_height_value, now_ist),
#                             daemon=True
#                         ).start()

#                         log_machine_event(
#                             plant_no=2,
#                             machine_no=machine_no,
#                             event_type="SHUT_HEIGHT_CHANGE",
#                             timestamp=now_ist,
#                             shift=current_shift,
#                             details=f"Height changed from {old_height} to {new_height_value} | Tool: {tool_id}"
#                         )

#                         segment['shut_height'] = new_height_value
#                         segment['tool_id'] = tool_id
#                         segment['segment_start'] = now_ist
#                         segment['segment_count'] = count_increment
#             else:
#                 if segment['shut_height'] and segment['shut_height'] > 0:
#                     segment['segment_count'] += count_increment

#             if machine_no in self.current_hours:
#                 if self.current_hours[machine_no] != current_hour:
#                     self.last_hour_counts[machine_no] = self.current_hour_counts[machine_no]
#                     old_count = self.current_hour_counts[machine_no]
#                     self.current_hour_counts[machine_no] = 0
#                     self.current_hours[machine_no] = current_hour

#                     print(f"⏰ M{machine_no}: Hour changed | Last={old_count}, New=0")
#             else:
#                 self.current_hours[machine_no] = current_hour

#             if machine_no in self.current_shifts:
#                 old_shift = self.current_shifts[machine_no]
#                 if old_shift != current_shift:
#                     print(f"🔄 M{machine_no}: Shift changed {old_shift}→{current_shift}")
#                     new_shift_key = (machine_no, current_shift)
#                     self.shift_cumulative[new_shift_key] = 0
#                     self.reset_shift_state(machine_no)

#             self.current_shifts[machine_no] = current_shift
#             self.current_hour_counts[machine_no] += count_increment

#             idle_status = self.idle_tracker.get_idle_status(machine_no, now_ist)
#             live_idle_str = idle_status.get('live_idle_time', '0m')
#             live_idle_mins = int(live_idle_str.replace('m', ''))

#             if live_idle_mins > 0:
#                 # ✅ Count aya hai matlab machine ON ho chuki hai
#                 self.save_resolved_downtime_to_db(machine_no, now_ist, current_shift, live_idle_mins, "ONLINE", is_hour_change=False)

#             self.idle_tracker.mark_count(machine_no, now_ist)

#         # ✅ DB insert/WebSocket ko global RAM lock ke bahar rakha hai.
#         # Isse Redis queue worker count messages parallel process kar sakte hain.
#         # Cumulative count DB advisory lock se safe rahega.
#         self._insert_realtime_count(
#             machine_no=machine_no,
#             count_increment=count_increment,
#             tool_id=tool_id,
#             shut_height=shut_height,
#             timestamp=now_ist,
#             shift=current_shift
#         )

#     def _insert_realtime_count(self, machine_no, count_increment, tool_id, shut_height, timestamp, shift):
#         """
#         Count ko DB me insert karta hai, phir DB se exact current-hour count nikal kar
#         WebSocket par bhejta hai. Isse UI RAM count se mismatch nahi hota.
#         """
#         try:
#             shift_start = self.get_shift_start_datetime(timestamp)
#             shift_start_naive = convert_to_naive_ist(shift_start)

#             idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
#             idle_time = idle_status['hourly_idle_total']

#             clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

#             if isinstance(shut_height, (int, float)) and shut_height > 0:
#                 clean_shut_height = f"{float(shut_height):.2f}"
#             else:
#                 try:
#                     val = float(shut_height)
#                     clean_shut_height = f"{val:.2f}" if val > 0 else "0.00"
#                 except:
#                     clean_shut_height = "0.00"

#             clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 0

#             if timestamp.tzinfo is not None:
#                 ist_timestamp = timestamp.astimezone(IST)
#             else:
#                 ist_timestamp = IST.localize(timestamp)

#             timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S+05:30')

#             refresh_db_connection()
#             with transaction.atomic():
#                 with connection.cursor() as cursor:
#                     # Same machine + shift par cumulative calculation safe rahegi
#                     cursor.execute(
#                         "SELECT pg_advisory_xact_lock(hashtext(%s))",
#                         (f"plant2:{machine_no}:{shift}",)
#                     )

#                     cursor.execute("""
#                         SELECT cumulative_count
#                         FROM Plant2_data
#                         WHERE machine_no = %s
#                         AND shift = %s
#                         AND timestamp >= %s
#                         ORDER BY timestamp DESC
#                         LIMIT 1
#                     """, (
#                         str(machine_no),
#                         shift,
#                         shift_start_naive
#                     ))

#                     result = cursor.fetchone()
#                     last_cumulative = int(result[0]) if result and result[0] is not None else 0
#                     new_cumulative = last_cumulative + int(count_increment)

#                     cursor.execute("""
#                         INSERT INTO Plant2_data
#                         (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                         VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
#                     """, (
#                         timestamp_str,
#                         clean_tool_id,
#                         str(machine_no),
#                         int(count_increment),
#                         new_cumulative,
#                         0,
#                         clean_idle_time,
#                         clean_shut_height,
#                         shift
#                     ))

#             # ✅ DB insert ke baad exact current-hour count DB se nikalo
#             current_hour_count_db = self.get_current_hour_count_from_db(machine_no, timestamp)

#             try:
#                 channel_layer = get_channel_layer()
#                 if channel_layer:
#                     live_data = {
#                         "machine_no": machine_no,
#                         "count": int(count_increment),
#                         "current_hour_count": current_hour_count_db,
#                         "cumulative_count": new_cumulative,
#                         "shift": shift,
#                         "status": "ONLINE"
#                     }
#                     async_to_sync(channel_layer.group_send)(
#                         "plant2_live_updates",
#                         {
#                             "type": "send_machine_update",
#                             "message": live_data
#                         }
#                     )
#                     print(f"📡 LIVE DB COUNT SENT | M{machine_no} | Hour={current_hour_count_db} | Cum={new_cumulative}")
#             except Exception as ws_err:
#                 print(f"❌ WebSocket Broadcast Error M{machine_no}: {ws_err}")

#         except Exception as e:
#             print(f"❌ Insert error M{machine_no}: {e}")
#             traceback.print_exc()
#             raise

#     def save_segment_to_db(self, machine_no, segment):
#         count = segment['segment_count']
#         if count == 0:
#             return

#         timestamp = segment['segment_start']
#         tool_id = segment['tool_id']
#         shut_height = segment['shut_height']

#         shift = self.get_shift_from_time(timestamp)
#         shift_start = self.get_shift_start_datetime(timestamp)

#         last_cumulative = 0
#         try:
#             shift_start_naive = convert_to_naive_ist(shift_start)
#             refresh_db_connection()
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT cumulative_count FROM Plant2_data
#                     WHERE machine_no = %s AND shift = %s AND timestamp >= %s
#                     ORDER BY timestamp DESC LIMIT 1
#                 """, (str(machine_no), shift, shift_start_naive))
#                 result = cursor.fetchone()
#                 if result:
#                     last_cumulative = result[0]
#         except Exception:
#             pass

#         new_cumulative = last_cumulative + count
#         idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
#         idle_time = idle_status['hourly_idle_total']

#         try:
#             clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

#             if isinstance(shut_height, (int, float)) and shut_height > 0:
#                 clean_shut_height = f"{float(shut_height):.2f}"
#             else:
#                clean_shut_height = "0.00"

#             clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 0

#             if timestamp.tzinfo is not None:
#                 ist_timestamp = timestamp.astimezone(IST)
#             else:
#                 ist_timestamp = IST.localize(timestamp)

#             timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S+05:30')

#             refresh_db_connection()
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     INSERT INTO Plant2_data (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
#                     VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
#                 """, (timestamp_str, clean_tool_id, str(machine_no), count, new_cumulative, 0, clean_idle_time, clean_shut_height, shift))

#         except Exception as e:
#             print(f"❌ Error inserting segment M{machine_no}: {e}")

#         segment['segment_count'] = 0

#     def get_machine_status(self, machine_no):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)

#             has_count = False
#             count_seconds_ago = None
#             count_tool_id = None
#             count_shut_height = None

#             if machine_no in self.machine_count_status:
#                 last_count = self.machine_count_status[machine_no]['last_count_time']
#                 count_seconds_ago = (now_ist - last_count).total_seconds()
#                 count_tool_id = self.machine_count_status[machine_no]['tool_id']
#                 count_shut_height = self.machine_count_status[machine_no]['shut_height']

#                 if count_seconds_ago <= self.off_threshold_seconds:
#                     has_count = True

#             has_json = False
#             json_seconds_ago = None
#             json_card = None
#             json_die_height = None

#             if machine_no in self.machine_json_status:
#                 last_json = self.machine_json_status[machine_no]['last_json_time']
#                 json_seconds_ago = (now_ist - last_json).total_seconds()
#                 json_card = self.machine_json_status[machine_no]['card']
#                 json_die_height = self.machine_json_status[machine_no]['die_height']

#                 if json_seconds_ago <= self.off_threshold_seconds:
#                     has_json = True

#             machine_on = has_count or has_json
#             is_producing = has_count

#             offline_since = None
#             offline_duration_minutes = None

#             if not machine_on:
#                 last_activity_time = None

#                 if machine_no in self.machine_count_status and machine_no in self.machine_json_status:
#                     last_activity_time = max(
#                         self.machine_count_status[machine_no]['last_count_time'],
#                         self.machine_json_status[machine_no]['last_json_time']
#                     )
#                 elif machine_no in self.machine_count_status:
#                     last_activity_time = self.machine_count_status[machine_no]['last_count_time']
#                 elif machine_no in self.machine_json_status:
#                     last_activity_time = self.machine_json_status[machine_no]['last_json_time']

#                 if last_activity_time:
#                     offline_since = last_activity_time
#                     offline_duration_seconds = (now_ist - last_activity_time).total_seconds()
#                     offline_duration_minutes = int(offline_duration_seconds / 60)

#                 self.idle_tracker.mark_off(machine_no)

#             if count_tool_id:
#                 tool_id = count_tool_id
#                 shut_height = count_shut_height
#             elif json_card:
#                 tool_id = json_card
#                 shut_height = json_die_height if json_die_height != 0.0 else "No data"
#             else:
#                 tool_id = 'N/A'
#                 shut_height = "No data"

#             return {
#                 'machine_on': machine_on,
#                 'is_producing': is_producing,
#                 'has_count_data': has_count,
#                 'has_json_data': has_json,
#                 'count_seconds_ago': int(count_seconds_ago) if count_seconds_ago is not None else None,
#                 'json_seconds_ago': int(json_seconds_ago) if json_seconds_ago is not None else None,
#                 'tool_id': tool_id,
#                 'shut_height': shut_height,
#                 'data_source': 'COUNT' if has_count else ('JSON' if has_json else 'NONE'),
#                 'offline_since': offline_since.strftime('%H:%M:%S') if offline_since else None,
#                 'offline_duration_minutes': offline_duration_minutes,
#             }

#     def get_machine_data(self, machine_no):
#         with self.lock:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             now_ist = datetime.now(ist_tz)
#             current_shift = self.get_shift_from_time(now_ist)
#             current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
#             shift_start = self.get_shift_start_datetime(now_ist)

#         last_hour_count_db = 0
#         try:
#             previous_hour_start = current_hour - timedelta(hours=1)
#             previous_hour_end = current_hour
#             previous_hour_start_naive = convert_to_naive_ist(previous_hour_start)
#             previous_hour_end_naive = convert_to_naive_ist(previous_hour_end)

#             refresh_db_connection()
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT COALESCE(SUM(count), 0) FROM Plant2_data
#                     WHERE machine_no = %s
#                     AND timestamp >= %s
#                     AND timestamp < %s
#                 """, (str(machine_no), previous_hour_start_naive, previous_hour_end_naive))
#                 result = cursor.fetchone()
#                 if result and result[0] is not None:
#                    last_hour_count_db = int(result[0])
#         except Exception as e:
#             pass

#         cumulative_from_db = 0
#         try:
#             shift_start_naive = convert_to_naive_ist(shift_start)
#             refresh_db_connection()
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT cumulative_count FROM Plant2_data
#                     WHERE machine_no = %s AND shift = %s AND timestamp >= %s
#                     ORDER BY timestamp DESC LIMIT 1
#                 """, (str(machine_no), current_shift, shift_start_naive))
#                 result = cursor.fetchone()
#                 if result and result[0] is not None:
#                     cumulative_from_db = int(result[0])
#         except Exception as e:
#             pass

#         live_cumulative = cumulative_from_db
#         current_hour_count_db = self.get_current_hour_count_from_db(machine_no, now_ist)

#         status_info = self.get_machine_status(machine_no)

#         idle_status = self.idle_tracker.get_idle_status(machine_no, now_ist)
#         hourly_idle_total = idle_status['hourly_idle_total']

#         total_shift_idle = self.get_shift_idle_from_hourly_table(
#             machine_no, shift_start, current_shift, now_ist
#         )

#         on_since_str = None
#         first_count_str = None
#         time_to_first_count = None

#         if machine_no in self.machine_on_since and status_info['machine_on']:
#             on_since = self.machine_on_since[machine_no]
#             on_since_str = on_since.strftime('%H:%M:%S')

#             if machine_no in self.first_count_time:
#                 first_count = self.first_count_time[machine_no]
#                 first_count_str = first_count.strftime('%H:%M:%S')
#                 delay = (first_count - on_since).total_seconds()
#                 time_to_first_count = int(delay / 60)

#         if machine_no in self.machine_on_since and not status_info['machine_on']:
#             on_since = self.machine_on_since[machine_no]
#             on_since_str = on_since.strftime('%H:%M:%S')

#             if machine_no in self.first_count_time:
#                 first_count = self.first_count_time[machine_no]
#                 first_count_str = first_count.strftime('%H:%M:%S')

#         return {
#             'machine_no': machine_no,
#             'current_hour_count': current_hour_count_db,
#             'last_hour_count': last_hour_count_db,
#             'cumulative_count': live_cumulative,
#             'idle_time': hourly_idle_total,
#             'total_shift_idle_time': total_shift_idle,
#             'shift': current_shift,
#             'machine_on': status_info['machine_on'],
#             'is_producing': status_info['is_producing'],
#             'has_count_data': status_info['has_count_data'],
#             'has_json_data': status_info['has_json_data'],
#             'count_seconds_ago': status_info['count_seconds_ago'],
#             'json_seconds_ago': status_info['json_seconds_ago'],
#             'current_tool_id': status_info['tool_id'],
#             'current_shut_height': status_info['shut_height'],
#             'data_source': status_info['data_source'],
#             'on_since': on_since_str,
#             'first_count_at': first_count_str,
#             'time_to_first_count': time_to_first_count,
#             'has_pending_reason': machine_no in self.pending_reasons
#         }

#     def force_hour_reset_all_machines(self):
#         pass


# EXACT_REQUIREMENT_STATE = Plant2ExactRequirementState()
# PLANT2_EXACT_REQUIREMENT_STATE = EXACT_REQUIREMENT_STATE

# _messages_lock = threading.Lock()

# BROKER_HOST = "192.168.0.35"
# BROKER_PORT = 1883
# USERNAME = "npdAtom"
# PASSWORD = "npd@Atom"

# # ✅ FINAL TOPICS: COUNT + J DONO SUBSCRIBE HONGE
# # COUNT topics -> Redis queue -> DB insert -> WebSocket -> UI count
# # J1-J9 topics -> Redis me nahi jayenge; sirf RAM me machine ON/OFF/status update hoga
# PLANT2_TOPICS = [
#     ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1),
#     ("COUNT4", 1), ("COUNT52", 1),
#     ("COUNT16", 1), ("COUNT17", 1), ("COUNT18", 1), ("COUNT19", 1),
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1),
#     ("J6", 1), ("J7", 1), ("J8", 1), ("J9", 1),
# ]

# TOPIC_MACHINE_MAPPING = {
#     'COUNT3': [1, 2, 3, 4, 5],
#     'COUNT2': [6, 7, 8, 9, 10],
#     'COUNT52': [11, 12, 13, 14, 15],
#     'COUNT1': [16, 17, 18, 19, 20],
#     'COUNT4': [41, 42, 43, 44, 45, 46],
#     'COUNT16': [21, 22, 23, 24, 25],
#     'COUNT17': [26, 27, 28, 29, 30],
#     'COUNT18': [31, 32, 33, 34, 35],
#     'COUNT19': [36, 37, 38, 39, 40],
#     'COUNT': []
# }

# MACHINE_GROUP_MAPPING = {
#     'J4': [1, 2, 3, 4, 5],
#     'J3': [6, 7, 8, 9, 10],
#     'J2': [11, 12, 13, 14, 15],
#     'J1': [16, 17, 18, 19, 20],
#     'J5': [41, 42, 43, 44, 45, 46],
#     'J6': [21, 22, 23, 24, 25],
#     'J7': [26, 27, 28, 29, 30],
#     'J8': [31, 32, 33, 34, 35],
#     'J9': [36, 37, 38, 39, 40]
# }

# # ✅ J TOPIC STATUS THROTTLE
# # Same machine ka J status bahut fast aata hai (1 sec me 3-4 messages).
# # Isliye J ko Redis queue me nahi bhejenge; sirf RAM status ko throttle ke saath update karenge.
# JSON_STATUS_THROTTLE_SECONDS = float(os.getenv("PLANT2_JSON_STATUS_THROTTLE_SECONDS", "3"))
# _json_status_lock = threading.Lock()
# _last_json_status_update = {}

# def get_machine_group(machine_no):
#     for group_name, machines in MACHINE_GROUP_MAPPING.items():
#         if machine_no in machines:
#             return group_name
#     return 'Unknown'

# ACTIVE_MACHINES_THIS_HOUR = set()
# MACHINE_DATA_CACHE = {}

# def get_machines_for_topic(topic):
#     return TOPIC_MACHINE_MAPPING.get(topic, [])

# def parse_json_payload(raw_payload):
#     try:
#         data = json.loads(raw_payload)
#         if 'client_id' not in data:
#             return None

#         client_id = str(data.get('client_id', ''))

#         if len(client_id) >= 2:
#             plant_no = int(client_id[0]) if client_id[0].isdigit() else None
#             machine_no = int(client_id[1:]) if client_id[1:].isdigit() else None
#         else:
#             return None

#         card = data.get('card', 'UNKNOWN')
#         die_height_str = str(data.get('die_height', '0'))
#         try:
#             die_height = float(die_height_str)
#         except:
#             die_height = 0.0

#         return {
#             'type': 'json',
#             'plant_no': plant_no,
#             'machine_no': machine_no,
#             'card': card,
#             'die_height': die_height
#         }
#     except:
#         return None


# def handle_json_status_direct(raw_payload):
#     """
#     J topic ko Redis queue me nahi daalte.
#     Sirf machine ON/OFF/status ke liye RAM state update karte hain.
#     Throttle se J topic ka high-frequency load control hota hai.
#     """
#     try:
#         parsed = parse_json_payload(raw_payload)
#         if not parsed or parsed.get('plant_no') != 2:
#             return

#         machine_no = parsed['machine_no']
#         now_epoch = time_module.time()

#         with _json_status_lock:
#             last_update = _last_json_status_update.get(machine_no, 0)
#             if now_epoch - last_update < JSON_STATUS_THROTTLE_SECONDS:
#                 return
#             _last_json_status_update[machine_no] = now_epoch

#         EXACT_REQUIREMENT_STATE.update_json_status(
#             machine_no=machine_no,
#             card=parsed.get('card'),
#             die_height=parsed.get('die_height', 0.0)
#         )

#     except Exception as e:
#         print(f"❌ J topic status update error: {e}")

# def parse_count_payload(raw_payload):
#     try:
#         parts = raw_payload.strip().split()
#         if len(parts) < 2:
#             return None

#         tool_id = parts[0][:24] if len(parts[0]) >= 24 else parts[0]
#         val_str = parts[1]

#         plant_no = int(val_str[0]) if len(val_str) > 0 and val_str[0].isdigit() else None

#         machine_no = None
#         if len(val_str) > 3:
#             if val_str[1].isdigit() and val_str[2].isdigit():
#                 machine_no = int(val_str[1:3])
#                 shut_height_str = val_str[4:]
#             else:
#                 machine_no = int(val_str[1]) if val_str[1].isdigit() else None
#                 shut_height_str = val_str[3:]
#         elif len(val_str) > 2:
#             machine_no = int(val_str[1]) if val_str[1].isdigit() else None
#             shut_height_str = val_str[3:]

#         if 'Failed' in shut_height_str:
#             shut_height = "Failed"
#         elif shut_height_str:
#             try:
#                 shut_height = float(shut_height_str)
#             except:
#                 shut_height = "No data"
#         else:
#             shut_height = "No data"

#         return {
#             'type': 'count',
#             'plant_no': plant_no,
#             'machine_no': machine_no,
#             'tool_id': tool_id,
#             'shut_height': shut_height
#         }
#     except:
#         return None

# def print_active_machines_summary():
#     def summary_worker():
#         while True:
#             try:
#                 time_module.sleep(30)
#                 ist_tz = pytz.timezone('Asia/Kolkata')
#                 now_ist = datetime.now(ist_tz)

#                 with EXACT_REQUIREMENT_STATE.lock:
#                     producing_machines = []
#                     all_machines = set()
#                     for machines_list in TOPIC_MACHINE_MAPPING.values():
#                         all_machines.update(machines_list)

#                     for machine_no in sorted(all_machines):
#                         if machine_no in EXACT_REQUIREMENT_STATE.last_count_time:
#                             last_count = EXACT_REQUIREMENT_STATE.last_count_time[machine_no]
#                             seconds_ago = (now_ist - last_count).total_seconds()

#                             if seconds_ago <= 60:
#                                 hour_count = EXACT_REQUIREMENT_STATE.get_current_hour_count_from_db(machine_no, now_ist)
#                                 tool_id = 'N/A'
#                                 if machine_no in EXACT_REQUIREMENT_STATE.machine_count_status:
#                                     tool_id = EXACT_REQUIREMENT_STATE.machine_count_status[machine_no].get('tool_id', 'N/A')

#                                 producing_machines.append({
#                                     'no': machine_no,
#                                     'count': hour_count,
#                                     'tool': tool_id[:8] if tool_id != 'N/A' else 'N/A',
#                                     'last': int(seconds_ago)
#                                 })

#                     if producing_machines:
#                         print("\n" + "=" * 80)
#                         print(f"🏭 ACTIVE MACHINES ({len(producing_machines)} running) - {now_ist.strftime('%H:%M:%S')}")
#                         print("=" * 80)

#                         for i in range(0, len(producing_machines), 4):
#                             chunk = producing_machines[i:i+4]
#                             for m in chunk:
#                                 print(f"M{m['no']:02d}: {m['count']:3d}ct | {m['tool']} | {m['last']:2d}s", end="  |  ")
#                             print()
#                         print("=" * 80 + "\n")
#             except Exception as e:
#                 print(f"❌ Summary error: {e}")

#     thread = threading.Thread(target=summary_worker, daemon=True)
#     thread.start()


# def save_hourly_idle_to_db(machine_no, timestamp, tool_id, shut_height, idle_time, shift):
#     try:
#         clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

#         if isinstance(shut_height, (int, float)) and shut_height > 0:
#             clean_shut_height = f"{float(shut_height):.2f}"
#         else:
#             clean_shut_height = "0.00"

#         clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 60

#         if timestamp.tzinfo is not None:
#             ist_timestamp = timestamp.astimezone(IST)
#         else:
#             ist_timestamp = IST.localize(timestamp)

#         # ✅ FIX: +05:30 force kiya
#         timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S+05:30')

#         refresh_db_connection()
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 INSERT INTO "Plant2_hourly_idle"
#                 (timestamp, tool_id, machine_no, idle_time, shut_height, shift)
#                 VALUES (%s::timestamp WITH TIME ZONE, %s, %s, %s, %s, %s)
#             """, (
#                 timestamp_str, clean_tool_id, str(machine_no), clean_idle_time, clean_shut_height, shift
#             ))
#     except Exception as e:
#         pass


# def save_hourly_idle_time_to_db():
#     def idle_saver_worker():
#         print("\n" + "⏰" * 50)
#         print("⏰ HOURLY IDLE TIME TRACKER STARTED!")
#         print(f"⏰ Snapshot time: XX:59:58")
#         print(f"⏰ Started at: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}")
#         print("⏰" * 50 + "\n")

#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)

#         last_saved_hour = None

#         while True:
#             try:
#                 ist_tz = pytz.timezone('Asia/Kolkata')
#                 now_ist = datetime.now(ist_tz)
#                 current_minute = now_ist.minute
#                 current_second = now_ist.second
#                 current_hour = now_ist.hour

#                 is_snapshot_time = (current_minute == 59 and current_second >= 58)

#                 if is_snapshot_time and last_saved_hour != current_hour:
#                     print("\n" + "💾" * 50)
#                     print(f"💾 HOURLY IDLE SNAPSHOT at {now_ist.strftime('%H:%M:%S')}")
#                     print("💾" * 50 + "\n")

#                     last_saved_hour = current_hour
#                     current_hour_start = now_ist.replace(minute=0, second=0, microsecond=0)

#                     saved_count = 0
#                     for machine_no in sorted(all_mapped_machines):
#                         try:
#                             idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                             idle_time = idle_status['hourly_idle_total']

#                             segment = EXACT_REQUIREMENT_STATE.machine_segments[machine_no]
#                             tool_id = segment.get('tool_id', 'NULL')
#                             shut_height = segment.get('shut_height', 0.0)

#                             shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)

#                             save_hourly_idle_to_db(
#                                 machine_no=machine_no,
#                                 timestamp=current_hour_start,
#                                 tool_id=tool_id,
#                                 shut_height=shut_height,
#                                 idle_time=idle_time,
#                                 shift=shift
#                             )

#                             live_idle_str = idle_status.get('live_idle_time', '0m')
#                             live_idle_mins = int(live_idle_str.replace('m', ''))

#                             machine_current_status = EXACT_REQUIREMENT_STATE.get_machine_status(machine_no)
#                             is_offline_now = not machine_current_status['machine_on']

#                             # ✅ Status depend karega ki abhi offline hai ya online
#                             machine_status_val = "OFFLINE" if is_offline_now else "ONLINE"

#                             if live_idle_mins > 0:
#                                 EXACT_REQUIREMENT_STATE.save_resolved_downtime_to_db(
#                                     machine_no, now_ist, shift, live_idle_mins, machine_status_val, is_hour_change=True
#                                 )

#                             saved_count += 1
#                         except Exception as e:
#                             print(f"❌ M{machine_no} idle save error: {e}")

#                     print(f"\n✅ Saved {saved_count}/{len(all_mapped_machines)} machine idle times\n")

#                 time_module.sleep(1)
#             except Exception as e:
#                 print(f"❌ Idle tracker error: {e}")
#                 traceback.print_exc()
#                 time_module.sleep(5)

#     thread = threading.Thread(target=idle_saver_worker, daemon=True)
#     thread.start()


# # ==============================================================
# # ✅ NAYA FUNCTION: AUTO IDLE NOTIFICATION SENDER
# # ==============================================================
# def auto_generate_idle_notification(machine_no, idle_mins):
#     """Ye function tab chalega jab machine 3 minute se idle hogi. Ye direct DB mein notification dalega."""
#     try:
#         refresh_db_connection()
#         target_group = Group.objects.filter(name='Supervisor').first()
#         if target_group:
#             users = target_group.user_set.all()
#             if users.exists():
#                 message = f"Machine M-{machine_no:02d} is idle for {idle_mins} mins. Please fill the downtime reason!"

#                 notifications_to_create = [
#                     Notification(user=user, machine_no=str(machine_no), message=message)
#                     for user in users
#                 ]
#                 Notification.objects.bulk_create(notifications_to_create)
#                 print(f"🔔 AUTO-ALERT: Notification created for M-{machine_no} (Idle {idle_mins}m)")
#     except Exception as e:
#         print(f"❌ Auto Alert Error M{machine_no}: {e}")
# # ==============================================================


# def start_machine_event_monitor():
#     """Ye background thread har 5 second me ON/OFF check karega"""
#     def monitor_worker():
#         import time as time_module
#         print("🔍 Plant 2 - Machine ON/OFF Event Monitor Started!")
#         machine_last_state = {}

#         # ✅ NAYA: Track karta hai ki kis machine ke liye alert bhej diya gaya hai
#         machine_alert_state = {}

#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)

#         while True:
#             try:
#                 time_module.sleep(5)
#                 ist_tz = pytz.timezone('Asia/Kolkata')
#                 now_ist = datetime.now(ist_tz)

#                 for machine_no in all_mapped_machines:
#                     status = EXACT_REQUIREMENT_STATE.get_machine_status(machine_no)
#                     is_currently_on = status['machine_on']

#                     # ==============================================================
#                     # ✅ NAYA LOGIC: CHECK AND SEND AUTO NOTIFICATIONS
#                     # ==============================================================
#                     idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                     live_idle_str = idle_status.get('live_idle_time', '0m')
#                     live_idle_mins = int(live_idle_str.replace('m', ''))

#                     if live_idle_mins >= 3:
#                         if not machine_alert_state.get(machine_no, False):
#                             auto_generate_idle_notification(machine_no, live_idle_mins)
#                             machine_alert_state[machine_no] = True # Mark that alert is sent

#                     elif live_idle_mins == 0:
#                         machine_alert_state[machine_no] = False # Reset if machine is producing again
#                     # ==============================================================

#                     if machine_no not in machine_last_state:
#                         machine_last_state[machine_no] = is_currently_on
#                         continue

#                     was_on_before = machine_last_state[machine_no]

#                     # ✅ OFFLINE TO ONLINE: Machine mein wapas signal aaya
#                     if is_currently_on and not was_on_before:
#                         shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)

#                         # ✅ OFFLINE to ONLINE aane par RAM se pending reason hata do (Naya idle reason mangega)
#                         EXACT_REQUIREMENT_STATE.pending_reasons.pop(machine_no, None)

#                         # Pehle offline wala gap DB mein save karo
#                         idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                         live_idle_str = idle_status.get('live_idle_time', '0m')
#                         live_idle_mins = int(live_idle_str.replace('m', ''))

#                         if live_idle_mins > 0:
#                             EXACT_REQUIREMENT_STATE.save_resolved_downtime_to_db(
#                                 machine_no, now_ist, shift, live_idle_mins, "OFFLINE to ONLINE", is_hour_change=False
#                             )

#                         log_machine_event(
#                             plant_no=2,
#                             machine_no=machine_no,
#                             event_type="ON",
#                             timestamp=now_ist,
#                             shift=shift,
#                             details="Machine Power/Signal Restored"
#                         )
#                         machine_last_state[machine_no] = True

#                     # ✅ ONLINE TO OFFLINE: Machine ka signal toote hue 3 minute se zyada ho gaya
#                     elif not is_currently_on and was_on_before:
#                         exact_off_time_str = status['offline_since']

#                         if exact_off_time_str:
#                             today = now_ist.date()
#                             time_obj = datetime.strptime(exact_off_time_str, '%H:%M:%S').time()
#                             exact_off_time = IST.localize(datetime.combine(today, time_obj))
#                         else:
#                             exact_off_time = now_ist

#                         shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(exact_off_time)

#                         # Machine offline ho gayi (3 min grace ke baad), abhi tak ka gap DB mein daalo
#                         idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                         live_idle_str = idle_status.get('live_idle_time', '0m')
#                         live_idle_mins = int(live_idle_str.replace('m', ''))

#                         if live_idle_mins > 0:
#                             EXACT_REQUIREMENT_STATE.save_resolved_downtime_to_db(
#                                 machine_no, now_ist, shift, live_idle_mins, "ONLINE to OFFLINE", is_hour_change=False
#                             )

#                         log_machine_event(
#                             plant_no=2,
#                             machine_no=machine_no,
#                             event_type="OFF",
#                             timestamp=exact_off_time,
#                             shift=shift,
#                             details="Machine Offline (No signal for 3 mins)"
#                         )
#                         machine_last_state[machine_no] = False

#             except Exception as e:
#                 print(f"❌ Event Monitor Error: {e}")
#                 time_module.sleep(5)

#     thread = threading.Thread(target=monitor_worker, daemon=True)
#     thread.start()

# # ==============================================================
# # ✅ NAYA THREAD: REDIS QUEUE WORKER
# # ==============================================================
# def recover_processing_queue():
#     """
#     Agar backend crash hua aur message processing queue me reh gaya,
#     startup par usko wapas main queue me daal do.
#     """
#     try:
#         recovered = 0
#         while True:
#             item = redis_client.rpoplpush("plant2_processing_queue", "plant2_mqtt_queue")
#             if not item:
#                 break
#             recovered += 1
#         if recovered:
#             print(f"♻️ Redis recovered {recovered} pending Plant 2 MQTT messages")
#     except Exception as e:
#         print(f"❌ Redis recovery error: {e}")


# def start_redis_queue_worker():
#     """
#     Redis reliable queue worker.
#     Sirf COUNT messages DB/WebSocket pipeline me process honge.
#     J topics Redis queue me nahi jayenge; agar old J queue me mil bhi gaya to ignore hoga.
#     """
#     def process_queue_data(data):
#         topic = data.get("topic")
#         raw_payload = data.get("payload")

#         if not topic or raw_payload is None:
#             return

#         # ✅ Redis worker sirf COUNT process karega
#         if not topic.startswith('COUNT'):
#             return

#         parsed = parse_count_payload(raw_payload)
#         if parsed and parsed.get('plant_no') == 2:
#             EXACT_REQUIREMENT_STATE.add_count(
#                 machine_no=parsed['machine_no'],
#                 count_increment=1,
#                 tool_id=parsed['tool_id'],
#                 shut_height=parsed['shut_height']
#             )

#     def worker(worker_no):
#         print(f"🚀 Redis Reliable Queue Worker #{worker_no} Started! COUNT -> DB/WebSocket")

#         while True:
#             data_str = None
#             try:
#                 data_str = redis_client.brpoplpush(
#                     "plant2_mqtt_queue",
#                     "plant2_processing_queue",
#                     timeout=1
#                 )

#                 if not data_str:
#                     continue

#                 data = json.loads(data_str)
#                 refresh_db_connection()
#                 process_queue_data(data)

#                 # ✅ DB insert/update success ke baad hi processing queue se remove karo
#                 redis_client.lrem("plant2_processing_queue", 1, data_str)
#                 close_old_connections()

#             except Exception as e:
#                 print(f"❌ Redis Queue Worker #{worker_no} Error: {e}")
#                 traceback.print_exc()
#                 # data_str remove nahi karna. Restart/recovery me retry hoga.
#                 close_old_connections()
#                 time_module.sleep(1)

#     # ✅ Recovery sirf ek baar karo, har worker me nahi
#     recover_processing_queue()

#     worker_count = int(os.getenv("PLANT2_QUEUE_WORKERS", "4"))
#     if worker_count < 1:
#         worker_count = 1

#     for i in range(worker_count):
#         thread = threading.Thread(
#             target=worker,
#             args=(i + 1,),
#             daemon=True,
#             name=f"plant2-redis-worker-{i+1}"
#         )
#         thread.start()

#     print(f"🚀 Plant 2 Redis workers started: {worker_count}")
# # ==============================================================


# # ==============================================================
# # ✅ ON_MESSAGE (Updated to use Redis only)
# # ==============================================================
# def on_message(client, userdata, msg):
#     try:
#         topic = msg.topic
#         raw_payload = msg.payload.decode('utf-8', errors='ignore').strip()

#         # ✅ COUNT topics: reliable Redis queue -> DB insert -> WebSocket
#         if topic.startswith("COUNT"):
#             queue_data = {
#                 "topic": topic,
#                 "payload": raw_payload,
#                 "received_at": datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S.%f')
#             }
#             redis_client.lpush("plant2_mqtt_queue", json.dumps(queue_data))
#             return

#         # ✅ J topics: Redis/DB/WebSocket count pipeline me nahi jayenge.
#         # Sirf RAM status update hoga, jisse machine ON/OFF, idle, notification logic chalta rahe.
#         if topic.startswith("J"):
#             handle_json_status_direct(raw_payload)
#             return

#         return

#     except Exception as e:
#         print(f"❌ on_message error: {e}")
#         traceback.print_exc()
# # ==============================================================


# def build_mqtt_client_id(plant_name):
#     """
#     Har backend/process ka MQTT client id unique banata hai.
#     .env me MQTT_CLIENT_PREFIX set karo, example:
#     MQTT_CLIENT_PREFIX=production_server
#     """
#     prefix = os.getenv("MQTT_CLIENT_PREFIX", "local")
#     hostname = socket.gethostname()
#     pid = os.getpid()

#     raw_client_id = f"{plant_name}_{prefix}_{hostname}_{pid}"

#     # MQTT client id me safe characters rakho
#     safe_client_id = "".join(
#         ch if ch.isalnum() or ch in ["_", "-"] else "_"
#         for ch in raw_client_id
#     )

#     # Kuch brokers long client ids allow karte hain, but safe side par limit rakhte hain
#     return safe_client_id[:100]


# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print("✅ Connected to MQTT Broker (Plant 2)")
#         for topic, qos in PLANT2_TOPICS:
#             client.subscribe(topic, qos)
#             print(f"📥 Subscribed: {topic}")
#     else:
#         print(f"❌ Connection failed with code {rc}")


# def on_disconnect(client, userdata, rc):
#     if rc != 0:
#         print(f"⚠️ Plant 2 MQTT disconnected unexpectedly. rc={rc}. Client will auto-reconnect.")
#     else:
#         print("ℹ️ Plant 2 MQTT disconnected cleanly.")


# def start_plant2_mqtt():
#     print("\n" + "🚀" * 50)
#     print("🚀 STARTING PLANT 2 MQTT CLIENT")
#     print("🚀" * 50 + "\n")

#     # ==============================================================
#     # ✅ REDIS CONNECTION TEST ON STARTUP
#     # ==============================================================
#     try:
#         redis_client.ping()
#         print("✅ Redis Connection Successful! Queue is active.")
#     except Exception as e:
#         print(f"❌ Redis Connection FAILED! Error: {e}")
#     # ==============================================================

#     client_id = build_mqtt_client_id("plant2")
#     client = mqtt.Client(client_id=client_id, clean_session=True)
#     print(f"🆔 Plant 2 MQTT Client ID: {client_id}")
#     print(f"⚙️ MQTT_CLIENT_PREFIX={os.getenv('MQTT_CLIENT_PREFIX', 'local')}")

#     client.username_pw_set(USERNAME, PASSWORD)
#     client.on_connect = on_connect
#     client.on_message = on_message
#     client.on_disconnect = on_disconnect

#     try:
#         client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
#     except Exception as e:
#         print(f"❌ MQTT connection error: {e}")
#         return

#     print_active_machines_summary()
#     save_hourly_idle_time_to_db()

#     start_machine_event_monitor()

#     # ✅ NAYA: Background worker start karo
#     start_redis_queue_worker()

#     client.loop_start()
#     print("✅ MQTT Loop Started (Plant 2)\n")


# backend/apps/mqtt/simple_plant2.py - FINAL VERSION: DBFIX V2 + IDEAL SEGMENTS + NAIVE IST TIME

import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import threading
from apps.machines.machine_state import MACHINE_STATE
from apps.data_storage.hourly_idle_tracker import HOURLY_IDLE_TRACKER
import traceback
import pytz
from django.db import connection, transaction, close_old_connections
import time as time_module
from threading import RLock
from collections import defaultdict
import json
from apps.utils.email_alert import send_shut_height_alert
import os
import redis  # ✅ NAYA IMPORT REDIS KE LIYE
import queue
import socket

# ✅ NAYE IMPORTS AUTOMATIC NOTIFICATION KE LIYE
from api.models import Notification
from django.contrib.auth.models import Group

# ✅ WEBSOCKET BROADCAST KE LIYE IMPORTS (NAYA ADD KIYA HAI)
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

# ✅ ULTIMATE FIX 1: Force system timezone to IST
os.environ["TZ"] = "Asia/Kolkata"

IST = pytz.timezone("Asia/Kolkata")


def refresh_db_connection():
    """
    Background threads (MQTT/Redis/summary/notification) me Django DB connection
    stale/closed ho sakta hai. Is helper se old/closed connection close hota hai
    aur fresh connection open hota hai.
    """
    try:
        close_old_connections()
        try:
            # psycopg2 closed: 0 means open, non-zero means closed/broken
            if connection.connection is not None and getattr(
                connection.connection, "closed", 0
            ):
                connection.close()
        except Exception:
            try:
                connection.close()
            except Exception:
                pass
        connection.ensure_connection()
    except Exception:
        try:
            connection.close()
        except Exception:
            pass
        close_old_connections()
        connection.ensure_connection()


# ==============================================================
# ✅ REDIS CONNECTION SETUP
# ==============================================================
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
try:
    redis_client = redis.StrictRedis(
        host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True
    )
except Exception as e:
    print(f"❌ Initial Redis Setup Error: {e}")
# ==============================================================


class IdleType:
    ON_BUT_NOT_PRODUCING = "ON_BUT_NOT_PRODUCING"
    NO_SIGNAL_AS_IDLE = "NO_SIGNAL_AS_IDLE"
    NONE = "NONE"


class DataSource:
    COUNT = "COUNT"
    JSON = "JSON"
    NONE = "NONE"


def convert_to_naive_ist(timestamp):
    """
    FINAL IDEAL TIME DB FIX:
    PostgreSQL column ideal_start_at / ideal_end_at should be:
        timestamp without time zone

    This function stores simple Indian local time in DB.
    Example saved value:
        2026-07-10 12:47:00
    Not:
        2026-07-10 12:47:00+05:30

    Important:
    - If datetime is aware, convert to Asia/Kolkata first, then remove tzinfo.
    - If datetime is already naive, treat it as already Indian local time.
    """
    if timestamp is None:
        return None

    if timestamp.tzinfo is not None:
        return timestamp.astimezone(IST).replace(tzinfo=None, microsecond=0)

    return timestamp.replace(microsecond=0)


# Clear alias name for ideal segment saving
# Both names do same thing; keep convert_to_naive_ist because old code calls it.
def to_db_ist_naive(timestamp):
    return convert_to_naive_ist(timestamp)


def log_machine_event(plant_no, machine_no, event_type, timestamp, shift, details=""):
    try:
        from django.db import connection, close_old_connections
        import pytz

        IST = pytz.timezone("Asia/Kolkata")
        if timestamp.tzinfo is not None:
            ist_timestamp = timestamp.astimezone(IST)
        else:
            ist_timestamp = IST.localize(timestamp)

        # ✅ FIX: +05:30 force kiya aur WITH TIME ZONE lagaya
        timestamp_str = ist_timestamp.strftime("%Y-%m-%d %H:%M:%S+05:30")

        refresh_db_connection()
        with connection.cursor() as cursor:
            # ✅ HISTORY DUPLICATE GUARD:
            # Same machine + same event_type + same timestamp duplicate save nahi hoga.
            # Isse ON/OFF duplicate entries avoid hoti hain, but real next event block nahi hota.
            cursor.execute(
                """
                INSERT INTO live_data."Machine_Event_Logs" 
                (plant_no, machine_no, event_type, timestamp, shift, details)
                SELECT %s, %s, %s, %s::timestamp WITH TIME ZONE, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM live_data."Machine_Event_Logs"
                    WHERE plant_no = %s
                      AND machine_no = %s
                      AND event_type = %s
                      AND timestamp = %s::timestamp WITH TIME ZONE
                )
            """,
                (
                    plant_no,
                    str(machine_no),
                    event_type,
                    timestamp_str,
                    shift,
                    details,
                    plant_no,
                    str(machine_no),
                    event_type,
                    timestamp_str,
                ),
            )

            if cursor.rowcount == 0:
                print(
                    f"⏭️ EVENT DUPLICATE SKIPPED | P{plant_no}-M{machine_no} | {event_type} | {timestamp_str}"
                )
                return

        print(
            f"📝 EVENT SAVED | P{plant_no}-M{machine_no} | {event_type} | {timestamp_str}"
        )
    except Exception as e:
        print(f"❌ Event Log Error P{plant_no}-M{machine_no}: {e}")


class StrictIdlePolicy:
    def __init__(self, grace_seconds=180, enable_no_signal_as_idle=True):
        self.lock = RLock()
        self.grace_seconds = grace_seconds
        self.enable_no_signal_as_idle = enable_no_signal_as_idle

        self.on_since = {}
        self.last_count_time = {}
        self.last_json_time = {}
        self.current_hour_start = {}
        self.completed_segments_minutes = {}
        self.data_source = {}
        self.hour_had_activity = {}

    @staticmethod
    def _ist(dt: datetime) -> datetime:
        if dt is None:
            return None
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)

    @staticmethod
    def _hour_start(dt: datetime) -> datetime:
        dt = StrictIdlePolicy._ist(dt)
        return dt.replace(minute=0, second=0, microsecond=0)

    def _ensure_current_hour(self, m: int, now: datetime):
        hour = self._hour_start(now)
        prev = self.current_hour_start.get(m)

        if prev is None or prev != hour:
            self.current_hour_start[m] = hour
            self.completed_segments_minutes[m] = 0
            self.hour_had_activity[m] = False

    def mark_json(self, m: int, t: datetime):
        with self.lock:
            now = self._ist(t)
            self.last_json_time[m] = now
            self.data_source[m] = DataSource.JSON

            if m not in self.on_since:
                self.on_since[m] = now

            self._ensure_current_hour(m, now)
            self.hour_had_activity[m] = True

    def mark_count(self, m: int, t: datetime):
        with self.lock:
            now = self._ist(t)
            prev_count = self.last_count_time.get(m)

            if prev_count is not None:
                live, acc, total = self._compute_live_and_accumulated(m, now)

                if live > 0:
                    self.completed_segments_minutes[m] = (
                        self.completed_segments_minutes.get(m, 0) + live
                    )

            self.last_count_time[m] = now
            self.data_source[m] = DataSource.COUNT

            if m not in self.on_since:
                self.on_since[m] = now

            self._ensure_current_hour(m, now)
            self.hour_had_activity[m] = True

    def mark_off(self, m: int):
        with self.lock:
            self.data_source[m] = DataSource.NONE

    def _compute_base_time(self, m: int, now: datetime) -> datetime:
        hour_start = self.current_hour_start.get(m, self._hour_start(now))
        candidates = [hour_start]

        if m in self.on_since:
            candidates.append(self.on_since[m])

        if m in self.last_count_time:
            candidates.append(self.last_count_time[m])

        return max(candidates)

    def _compute_live_and_accumulated(self, m: int, now: datetime):
        if m not in self.on_since:
            return (0, 0, 0)

        base_time = self._compute_base_time(m, now)
        gap_seconds = (now - base_time).total_seconds()

        if gap_seconds < self.grace_seconds:
            live_idle = 0
            accumulated_idle = 0
        else:
            visible_minutes = int(gap_seconds / 60)
            live_idle = visible_minutes
            accumulated_idle = visible_minutes

        completed = self.completed_segments_minutes.get(m, 0)
        hourly_total = completed + live_idle

        return (live_idle, accumulated_idle, hourly_total)

    def get_idle_status(self, m: int, now: datetime = None):
        with self.lock:
            if now is None:
                now = datetime.now(IST)
            now = self._ist(now)

            self._ensure_current_hour(m, now)

            if self.enable_no_signal_as_idle:
                is_never_active = (
                    m not in self.on_since
                    and m not in self.last_count_time
                    and m not in self.last_json_time
                )

                if is_never_active:
                    hour_start = self._hour_start(now)

                    shift_a_start = now.replace(
                        hour=8, minute=30, second=0, microsecond=0
                    )
                    shift_b_start = now.replace(
                        hour=20, minute=30, second=0, microsecond=0
                    )

                    if shift_a_start <= now < shift_b_start:
                        actual_start = max(hour_start, shift_a_start)
                    elif now >= shift_b_start:
                        actual_start = max(hour_start, shift_b_start)
                    else:
                        prev_shift_b = shift_b_start - timedelta(days=1)
                        actual_start = max(hour_start, prev_shift_b)

                    elapsed_seconds = max(0, (now - actual_start).total_seconds())
                    elapsed_mins = int(elapsed_seconds / 60)

                    return {
                        "live_idle_time": f"{elapsed_mins}m",
                        "accumulated_idle_time": f"{elapsed_mins}m",
                        "hourly_idle_total": elapsed_mins,
                        "is_idle": True,
                        "idle_type": IdleType.NO_SIGNAL_AS_IDLE,
                        "status": "No Signal (Offline)",
                        "data_source": DataSource.NONE,
                        "on_since": None,
                        "last_count_time": None,
                        "count_seconds_ago": None,
                        "json_seconds_ago": None,
                    }

            live, acc, total = self._compute_live_and_accumulated(m, now)

            has_count = m in self.last_count_time
            has_json = m in self.last_json_time

            count_seconds_ago = None
            json_seconds_ago = None

            if has_count:
                count_seconds_ago = int((now - self.last_count_time[m]).total_seconds())

            if has_json:
                json_seconds_ago = int((now - self.last_json_time[m]).total_seconds())

            is_on = m in self.on_since
            is_producing = has_count and count_seconds_ago <= 180

            if not is_on:
                status = "OFF"
                idle_type = IdleType.NONE
            elif is_producing:
                if live > 0:
                    status = "Producing (Idle)"
                else:
                    status = "Producing"
                idle_type = (
                    IdleType.NONE if live == 0 else IdleType.ON_BUT_NOT_PRODUCING
                )
            else:
                if live > 0:
                    status = "ON (No Count)"
                else:
                    status = "ON (Grace Period)"
                idle_type = IdleType.ON_BUT_NOT_PRODUCING if live > 0 else IdleType.NONE

            return {
                "live_idle_time": f"{live}m" if live > 0 else "0m",
                "accumulated_idle_time": f"{acc}m",
                "hourly_idle_total": min(60, total),
                "is_idle": live > 0,
                "idle_type": idle_type,
                "status": status,
                "data_source": self.data_source.get(m, DataSource.NONE),
                "on_since": self.on_since.get(m),
                "last_count_time": self.last_count_time.get(m),
                "count_seconds_ago": count_seconds_ago,
                "json_seconds_ago": json_seconds_ago,
            }

    def reset_hour(self, m: int = None):
        with self.lock:
            if m is None:
                self.completed_segments_minutes.clear()
                self.current_hour_start.clear()
                self.hour_had_activity.clear()
            else:
                self.completed_segments_minutes[m] = 0
                self.hour_had_activity[m] = False
                if m in self.current_hour_start:
                    del self.current_hour_start[m]


class Plant2ExactRequirementState:
    def __init__(self):
        self.lock = RLock()
        self.current_hour_counts = defaultdict(int)
        self.last_hour_counts = defaultdict(int)
        self.shift_cumulative = defaultdict(int)
        self.current_hours = {}
        self.current_shifts = {}

        self.pending_reasons = {}

        # Separate lightweight lock only for reason buffer.
        # Do not make form submission wait for heavy MQTT processing.
        self.reason_lock = RLock()

        self.last_count_time = {}
        self.hour_first_count_time = {}

        self.machine_json_status = {}
        self.machine_count_status = {}

        self.machine_on_since = {}
        self.first_count_time = {}

        self.machine_segments = defaultdict(
            lambda: {
                "shut_height": None,
                "tool_id": None,
                "segment_start": None,
                "segment_count": 0,
            }
        )

        self.off_threshold_seconds = 180
        self.idle_tracker = StrictIdlePolicy(
            grace_seconds=180, enable_no_signal_as_idle=True
        )

        # ✅ NEW: Ideal time segment tracker for single table
        # Table: live_data.ideal_time_segments_reason
        # Data is inserted only when a segment closes / hour changes.
        self.active_ideal_segments = {}
        # ✅ NEW FIX: har machine ka last valid mode boundary rakhenge.
        # Isse old last_count_time se fake ONLINE row generate nahi hogi
        # jab beech me machine OFFLINE ho chuki ho.
        self.last_ideal_transition_time = {}
        self.backend_started_at = datetime.now(IST)

    def set_pending_reason(self, machine_no, category, reason, remarks):
        """Frontend se aaya reason RAM me save karta hai"""
        with self.reason_lock:
            self.pending_reasons[machine_no] = {
                "category": category,
                "reason": reason,
                "remarks": remarks,
                "timestamp": datetime.now(pytz.timezone("Asia/Kolkata")),
            }
            print(f"📝 Buffer Updated for M{machine_no}: {category} -> {reason}")

    # ==============================================================
    # ✅ NEW: Ideal Time Segment Logic
    # Table: live_data.ideal_time_segments_reason
    # This logic does NOT touch Redis count queue or WebSocket count flow.
    # ==============================================================
    def _get_ideal_reason_data(self, machine_no, ideal_mode):
        """Reason/specification/remark pending buffer se nikalta hai."""
        with self.reason_lock:
             data = dict(
                 self.pending_reasons.get(machine_no) or {}
             )
        if data:
            reason = data.get("category") or "Uncategorized"
            specific_reason = data.get("reason") or "Reason Not Provided"
            remark = data.get("remarks") or ""
        else:
            if ideal_mode == "OFFLINE":
                reason = "Machine Off"
                specific_reason = "Machine offline / no signal"
            else:
                reason = "Uncategorized"
                specific_reason = "Reason Not Provided"
            remark = ""

        return reason, specific_reason, remark

    def _as_ist(self, dt):
        """Datetime ko safe IST-aware banata hai."""
        if dt is None:
            return None
        return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)

    def _current_shift_start_for(self, reference_time):
        """Reference time ke hisaab se current shift start nikalta hai."""
        reference_time = self._as_ist(reference_time)
        return self.get_shift_start_datetime(reference_time)

    def _clamp_start_to_reference_shift(self, start_at, reference_time):
        """
        Agar start current shift se pehle aa raha hai, to current shift start par clamp karo.
        Example: Shift A 08:30 hai, offline_since 08:25 aa gaya -> save 08:30 se hoga.
        """
        start_at = self._as_ist(start_at)
        reference_time = self._as_ist(reference_time)
        shift_start = self._current_shift_start_for(reference_time)
        if start_at < shift_start <= reference_time:
            return shift_start
        return start_at

    def _safe_online_start_time(self, machine_no, candidate_start, now_ist):
        """
        ONLINE ideal ke start ko safe banata hai.
        Old last_count_time ko tab ignore karega jab beech me OFFLINE boundary aa chuki ho.
        """
        now_ist = self._as_ist(now_ist)
        start_candidates = []

        if candidate_start is not None:
            start_candidates.append(self._as_ist(candidate_start))

        if machine_no in self.machine_on_since:
            start_candidates.append(self._as_ist(self.machine_on_since[machine_no]))

        if machine_no in self.last_ideal_transition_time:
            start_candidates.append(
                self._as_ist(self.last_ideal_transition_time[machine_no])
            )

        shift_start = self._current_shift_start_for(now_ist)
        start_candidates.append(shift_start)

        if not start_candidates:
            return now_ist

        start_at = max(start_candidates)
        if start_at > now_ist:
            start_at = now_ist
        return start_at

    def _db_has_ideal_overlap(self, machine_no, start_naive, end_naive):
        """
        Same machine par already saved ONLINE/OFFLINE segment ke saath overlap ho to
        new row skip karenge. Ye fake duplicate/overlap entries rokta hai.
        """
        try:
            refresh_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, ideal_mode, ideal_start_at, ideal_end_at
                    FROM "live_data"."ideal_time_segments_reason"
                    WHERE plant_location = %s
                      AND machine_no = %s
                      AND ideal_start_at < %s
                      AND ideal_end_at > %s
                    ORDER BY ideal_start_at
                    LIMIT 1
                """,
                    ("Plant 2", int(machine_no), end_naive, start_naive),
                )
                return cursor.fetchone()
        except Exception as e:
            print(f"⚠️ Ideal overlap check error M{machine_no}: {e}")
            # DB check fail ho to insert ko block nahi karenge.
            return None

    def _save_ideal_piece_to_db(
        self, machine_no, ideal_mode, start_at, end_at, closed_by
    ):
        """One already-split ideal piece DB me save karta hai."""
        try:
            start_at = self._as_ist(start_at)
            end_at = self._as_ist(end_at)

            # ✅ Shift boundary protection:
            # Agar start current shift start se pehle aa gaya, to reference/end time ke current shift se clamp karo.
            # Isse 08:25 wali fake OFFLINE entry 08:30 se save hogi.
            start_at = self._clamp_start_to_reference_shift(start_at, end_at)

            ideal_seconds = int((end_at - start_at).total_seconds())

            # ✅ FINAL RULE:
            # 3 minute se kam koi bhi ideal row DB me save nahi hogi.
            # Isse 4 sec / 11 sec / 28 sec wali HOUR_CHANGE/COUNT_RESUME pieces bhi stop hongi.
            if ideal_seconds < self.off_threshold_seconds:
                print(
                    f"⏭️ IDEAL PIECE IGNORED | M{machine_no} | {ideal_mode} | "
                    f"{start_at.strftime('%H:%M:%S')}→{end_at.strftime('%H:%M:%S')} | "
                    f"{ideal_seconds}s < {self.off_threshold_seconds}s | {closed_by}"
                )
                return False

            shift = self.get_shift_from_time(start_at)
            reason, specific_reason, remark = self._get_ideal_reason_data(
                machine_no, ideal_mode
            )

            # ✅ FINAL TIMEZONE FIX:
            # DB columns are timestamp WITHOUT time zone.
            # Save simple Indian local time: 2026-07-10 12:47:00
            # No +05:30 should be stored/shown.
            start_naive = to_db_ist_naive(start_at)
            end_naive = to_db_ist_naive(end_at)

            # ✅ FINAL OVERLAP GUARD:
            # Same machine me same time par ONLINE/OFFLINE dono rows nahi banengi.
            overlap = self._db_has_ideal_overlap(machine_no, start_naive, end_naive)
            if overlap:
                print(
                    f"⏭️ IDEAL OVERLAP SKIPPED | M{machine_no} | {ideal_mode} | "
                    f"{start_at.strftime('%H:%M:%S')}→{end_at.strftime('%H:%M:%S')} | "
                    f"existing_id={overlap[0]} {overlap[1]} {overlap[2]}→{overlap[3]}"
                )
                return False

            refresh_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
    INSERT INTO "live_data"."ideal_time_segments_reason"
    (plant_location, machine_no, ideal_mode, ideal_start_at, ideal_end_at,
     ideal_time, closed_by, reason, specific_reason, remark, shift, report_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
""",
                    (
                        "Plant 2",
                        int(machine_no),
                        ideal_mode,
                        start_naive,
                        end_naive,
                        int(ideal_seconds),
                        closed_by,
                        reason[:100] if reason else "Uncategorized",
                        (
                            specific_reason[:255]
                            if specific_reason
                            else "Reason Not Provided"
                        ),
                        remark,
                        shift,
                        "PENDING",
                    ),
                )

            print(
                f"✅ IDEAL SAVED | Plant 2 | M{machine_no} | {ideal_mode} | "
                f"{start_at.strftime('%H:%M:%S')}→{end_at.strftime('%H:%M:%S')} | "
                f"{ideal_seconds}s | {closed_by}"
            )
            return True

        except Exception as e:
            print(f"❌ Ideal segment save error M{machine_no}: {e}")
            traceback.print_exc()
            return False

    def _save_ideal_range_split_by_hour(
        self, machine_no, ideal_mode, start_at, end_at, closed_by, min_total_seconds=180
    ):
        """
        Full ideal range ko hour-wise split karke save karta hai.
        Final rule: total range bhi >= 3 min hona chahiye aur har saved piece bhi >= 3 min hona chahiye.
        Isse HOUR_CHANGE par 4 sec / 11 sec / 28 sec wali rows save nahi hoti.
        """
        try:
            start_at = self._as_ist(start_at)
            end_at = self._as_ist(end_at)

            # ✅ Shift-start fix:
            # Current shift A start 08:30 hai, to 08:25/08:31 backend-start confusion nahi rahega.
            # Agar range current shift se pehle start ho gayi hai, to shift start par clamp.
            start_at = self._clamp_start_to_reference_shift(start_at, end_at)

            total_seconds = int((end_at - start_at).total_seconds())
            if total_seconds < min_total_seconds:
                print(
                    f"⏭️ IDEAL IGNORED | M{machine_no} | {ideal_mode} | "
                    f"{total_seconds}s < {min_total_seconds}s"
                )
                return 0

            saved_count = 0
            piece_start = start_at

            while piece_start < end_at:
                next_hour = piece_start.replace(
                    minute=0, second=0, microsecond=0
                ) + timedelta(hours=1)
                piece_end = min(end_at, next_hour)

                if self._save_ideal_piece_to_db(
                    machine_no, ideal_mode, piece_start, piece_end, closed_by
                ):
                    saved_count += 1

                piece_start = piece_end

            return saved_count

        except Exception as e:
            print(f"❌ Ideal split error M{machine_no}: {e}")
            traceback.print_exc()
            return 0

    def _start_ideal_segment(self, machine_no, ideal_mode, start_at):
        """Active ideal segment RAM me start karta hai."""
        start_at = self._as_ist(start_at)

        active = self.active_ideal_segments.get(machine_no)

        if active and active.get("mode") == ideal_mode:
            # ✅ Agar same mode already active hai aur new start shift-start ke karib earlier hai,
            # to start time ko safely earlier adjust kar do. Old offline/online overlap se pehle nahi le jayenge.
            current_start = active["start_at"]
            boundary = self.last_ideal_transition_time.get(machine_no)
            shift_start = self._current_shift_start_for(current_start)
            safe_min = max([x for x in [shift_start, boundary] if x is not None])
            if safe_min <= start_at < current_start:
                active["start_at"] = start_at
                print(
                    f"↩️ IDEAL START ADJUSTED | M{machine_no} | {ideal_mode} | {start_at.strftime('%H:%M:%S')}"
                )
            return

        # ✅ Safety: different active mode ko overwrite nahi karna.
        # Pehle purana mode close hoga, phir naya start hoga.
        if active and active.get("mode") != ideal_mode:
            close_at = max(start_at, active["start_at"])
            close_reason = "MACHINE_ON" if ideal_mode == "ONLINE" else "MACHINE_OFF"
            self._close_ideal_segment(machine_no, close_at, close_reason)

        self.active_ideal_segments[machine_no] = {
            "mode": ideal_mode,
            "start_at": start_at,
        }
        print(
            f"▶️ IDEAL START | M{machine_no} | {ideal_mode} | {start_at.strftime('%H:%M:%S')}"
        )

    def _close_ideal_segment(self, machine_no, end_at, closed_by):
        """Active ideal segment close karke DB me save karta hai."""
        active = self.active_ideal_segments.get(machine_no)
        if not active:
            return 0

        end_at = self._as_ist(end_at)
        start_at = active["start_at"]
        ideal_mode = active["mode"]

        if end_at <= start_at:
            self.active_ideal_segments.pop(machine_no, None)
            self.last_ideal_transition_time[machine_no] = end_at
            return 0

        saved = self._save_ideal_range_split_by_hour(
            machine_no=machine_no,
            ideal_mode=ideal_mode,
            start_at=start_at,
            end_at=end_at,
            closed_by=closed_by,
            min_total_seconds=180,
        )
        self.active_ideal_segments.pop(machine_no, None)

        # ✅ Boundary save karo. Future ONLINE ideal old last_count_time se isse pehle start nahi hoga.
        self.last_ideal_transition_time[machine_no] = end_at
        return saved

    def _infer_offline_start_for_new_signal(self, now_ist):
        """
        FINAL REQUIREMENT:
        Shift A 08:30 start hai, to offline/ideal calculation 08:30:00 se hi hoga.
        Backend 08:31:24 par start hua to bhi 08:31:24 se start nahi karenge.
        """
        shift_start = self.get_shift_start_datetime(now_ist)
        return shift_start

    def _start_online_ideal_if_needed(self, machine_no, now_ist):
        """Machine ON hai but recent count nahi hai, to ONLINE ideal start karta hai."""
        now_ist = self._as_ist(now_ist)
        recent_count = False
        if machine_no in self.machine_count_status:
            last_count = self.machine_count_status[machine_no]["last_count_time"]
            recent_count = (
                now_ist - last_count
            ).total_seconds() <= self.off_threshold_seconds

        if recent_count:
            return

        if machine_no in self.machine_count_status:
            candidate_start = self.machine_count_status[machine_no]["last_count_time"]
        elif machine_no in self.machine_on_since:
            candidate_start = self.machine_on_since[machine_no]
        else:
            candidate_start = now_ist

        start_at = self._safe_online_start_time(machine_no, candidate_start, now_ist)
        self._start_ideal_segment(machine_no, "ONLINE", start_at)

    def close_ideal_on_count_resume(self, machine_no, now_ist):
        """Count aate hi active ONLINE/OFFLINE ideal close karta hai."""
        now_ist = self._as_ist(now_ist)
        active = self.active_ideal_segments.get(machine_no)

        # ✅ Agar machine OFFLINE thi aur count directly aa gaya, to naya ON session start samjho.
        # Isse old first_count/on_since values se fake ONLINE ideal nahi banega.
        if active and active.get("mode") == "OFFLINE":
            self.machine_on_since[machine_no] = now_ist
            self.first_count_time.pop(machine_no, None)

        if machine_no not in self.active_ideal_segments:
            # Agar monitor ne segment start nahi kiya, phir bhi gap > 3 min hai to save miss na ho.
            if machine_no in self.machine_count_status:
                last_count = self.machine_count_status[machine_no]["last_count_time"]
                start_at = self._safe_online_start_time(machine_no, last_count, now_ist)
                if (now_ist - start_at).total_seconds() >= self.off_threshold_seconds:
                    self._start_ideal_segment(machine_no, "ONLINE", start_at)
            elif machine_no in self.machine_on_since:
                on_since = self.machine_on_since[machine_no]
                start_at = self._safe_online_start_time(machine_no, on_since, now_ist)
                if (now_ist - start_at).total_seconds() >= self.off_threshold_seconds:
                    self._start_ideal_segment(machine_no, "ONLINE", start_at)

        self._close_ideal_segment(machine_no, now_ist, "COUNT_RESUME")

    def split_active_ideal_segment_at_hour(self, machine_no, now_ist):
        """Hour boundary cross ho gaya to current active segment ka hour piece save karta hai."""
        active = self.active_ideal_segments.get(machine_no)
        if not active:
            return

        start_at = active["start_at"]
        now_ist = now_ist.astimezone(IST) if now_ist.tzinfo else IST.localize(now_ist)

        # Jab active segment previous hour se current hour me aa gaya ho
        current_hour_start = now_ist.replace(minute=0, second=0, microsecond=0)
        if start_at < current_hour_start:
            ideal_mode = active["mode"]
            self._save_ideal_range_split_by_hour(
                machine_no=machine_no,
                ideal_mode=ideal_mode,
                start_at=start_at,
                end_at=current_hour_start,
                closed_by="HOUR_CHANGE",
                min_total_seconds=180,
            )
            # Current hour ke liye same mode ka segment continue rahega
            self.active_ideal_segments[machine_no] = {
                "mode": ideal_mode,
                "start_at": current_hour_start,
            }

    def track_ideal_segment_from_status(self, machine_no, status, now_ist):
        """
        Monitor thread se call hoga. Isse ONLINE/OFFLINE ideal start/end hota hai.
        """
        try:
            now_ist = self._as_ist(now_ist)

            # Pehle hour split check karo
            self.split_active_ideal_segment_at_hour(machine_no, now_ist)

            machine_on = status.get("machine_on", False)
            is_producing = status.get("is_producing", False)

            if is_producing:
                self._close_ideal_segment(machine_no, now_ist, "COUNT_RESUME")
                return

            if machine_on:
                # Machine ON hai, but count nahi aa raha => ONLINE ideal
                if machine_no in self.machine_count_status:
                    candidate_start = self.machine_count_status[machine_no][
                        "last_count_time"
                    ]
                elif machine_no in self.machine_on_since:
                    candidate_start = self.machine_on_since[machine_no]
                else:
                    candidate_start = now_ist

                active = self.active_ideal_segments.get(machine_no)
                if active and active.get("mode") == "OFFLINE":
                    # ✅ OFFLINE -> ONLINE boundary actual now par close karo.
                    # Old last_count_time se close/start karne par overlap/fake entry banti thi.
                    self._close_ideal_segment(machine_no, now_ist, "MACHINE_ON")
                    self.machine_on_since[machine_no] = now_ist
                    self.first_count_time.pop(machine_no, None)

                if machine_no not in self.active_ideal_segments:
                    start_at = self._safe_online_start_time(
                        machine_no, candidate_start, now_ist
                    )
                    self._start_ideal_segment(machine_no, "ONLINE", start_at)
                return

            # Machine OFF / no signal => OFFLINE ideal
            offline_start = None
            offline_since_str = status.get("offline_since")
            if offline_since_str:
                try:
                    time_obj = datetime.strptime(offline_since_str, "%H:%M:%S").time()
                    offline_start = IST.localize(
                        datetime.combine(now_ist.date(), time_obj)
                    )
                    # Agar midnight crossing me future ban gaya to previous day lo
                    if offline_start > now_ist:
                        offline_start = offline_start - timedelta(days=1)
                except Exception:
                    offline_start = None

            if offline_start is None:
                offline_start = self._infer_offline_start_for_new_signal(now_ist)

            # ✅ 08:30 shift-start clamp: 08:25 jaise old signal ko current shift me 08:30 se count karo.
            offline_start = self._clamp_start_to_reference_shift(offline_start, now_ist)

            active = self.active_ideal_segments.get(machine_no)
            if active and active.get("mode") == "ONLINE":
                close_time = max(offline_start, active["start_at"])
                self._close_ideal_segment(machine_no, close_time, "MACHINE_OFF")

            if machine_no not in self.active_ideal_segments:
                self._start_ideal_segment(machine_no, "OFFLINE", offline_start)

        except Exception as e:
            print(f"❌ Ideal tracker error M{machine_no}: {e}")
            traceback.print_exc()

    def save_resolved_downtime_to_db(
        self,
        machine_no,
        now_ist,
        current_shift,
        idle_mins,
        machine_status_val,
        is_hour_change=False,
    ):
        """DB mein final isolated downtime (Reason ke saath ya bina) save karega"""
        with self.lock:
            if idle_mins > 0:
                category = "Uncategorized"
                specific_reason = "Reason Not Provided"

                with self.reason_lock:
                    pending_data = dict(
                        self.pending_reasons.get(machine_no) or {}
                    )

                if pending_data:
                    category = pending_data['category']
                    specific_reason = pending_data['reason']
                
                    if pending_data.get('remarks'):
                        specific_reason += (
                            f" - {pending_data['remarks']}"
                        )

                    # ✅ CARRY FORWARD LOGIC:
                    # Yahan se 'del self.pending_reasons[machine_no]' hata diya hai taaki
                    # ghanta change hone par reason RAM se na hate aur agle ghante bhi carry forward ho.

                try:
                    timestamp_str = now_ist.strftime("%Y-%m-%d %H:%M:%S+05:30")

                    refresh_db_connection()
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            INSERT INTO "hourly_downtime_logs" 
                            (timestamp, machine_no, idle_time, shift, reason_category, specific_reason, machine_status)
                            VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s)
                        """,
                            (
                                timestamp_str,
                                str(machine_no),
                                int(idle_mins),
                                current_shift,
                                category,
                                specific_reason[:255],
                                machine_status_val,
                            ),
                        )
                    print(
                        f"✅ DOWNTIME LOGGED | M{machine_no} | {machine_status_val} | {category} | Idle: {idle_mins}m"
                    )
                except Exception as e:
                    print(f"❌ DB Downtime Save Error M{machine_no}: {e}")

    def get_shift_from_time(self, dt):
        ist_dt = (
            dt.astimezone(pytz.timezone("Asia/Kolkata"))
            if dt.tzinfo
            else pytz.timezone("Asia/Kolkata").localize(dt)
        )
        time_only = ist_dt.time()
        shift_A_start = datetime.strptime("08:30", "%H:%M").time()
        shift_A_end = datetime.strptime("20:00", "%H:%M").time()
        return "A" if shift_A_start <= time_only < shift_A_end else "B"

    def get_shift_start_datetime(self, timestamp):
        date = timestamp.date()
        shift = self.get_shift_from_time(timestamp)

        shift_a_start_time = datetime.strptime("08:30", "%H:%M").time()
        shift_b_start_time = datetime.strptime("20:30", "%H:%M").time()

        if shift == "A":
            return IST.localize(datetime.combine(date, shift_a_start_time))
        else:
            if timestamp.time() < shift_a_start_time:
                prev_day = date - timedelta(days=1)
                return IST.localize(datetime.combine(prev_day, shift_b_start_time))
            else:
                return IST.localize(datetime.combine(date, shift_b_start_time))

    def get_shift_idle_from_hourly_table(self, machine_no, shift_start, shift, now):
        try:
            shift_start_naive = convert_to_naive_ist(shift_start)
            now_naive = convert_to_naive_ist(now)

            refresh_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(idle_time), 0) 
                    FROM "Plant2_hourly_idle"
                    WHERE machine_no = %s 
                    AND shift = %s
                    AND DATE(timestamp) = DATE(%s)
                    AND timestamp >= %s
                    AND timestamp < %s
                """,
                    (
                        str(machine_no),
                        shift,
                        shift_start_naive,
                        shift_start_naive,
                        now_naive,
                    ),
                )

                result = cursor.fetchone()
                db_idle = int(result[0]) if result and result[0] else 0

            current_idle = self.idle_tracker.get_idle_status(machine_no, now)
            live_idle = current_idle["hourly_idle_total"]

            total_shift_idle = db_idle + live_idle

            return total_shift_idle

        except Exception as e:
            print(f"❌ Error fetching shift idle M{machine_no}: {e}")
            traceback.print_exc()
            return 0

    def get_current_hour_count_from_db(self, machine_no, timestamp):
        """
        Current hour ka exact count DB se nikalega.
        Dashboard/WebSocket ko RAM count nahi, DB count milega.
        """
        try:
            if timestamp.tzinfo is not None:
                timestamp = timestamp.astimezone(IST)
            else:
                timestamp = IST.localize(timestamp)

            current_hour = timestamp.replace(minute=0, second=0, microsecond=0)
            next_hour = current_hour + timedelta(hours=1)

            current_hour_naive = convert_to_naive_ist(current_hour)
            next_hour_naive = convert_to_naive_ist(next_hour)

            refresh_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(count), 0)
                    FROM Plant2_data
                    WHERE machine_no = %s
                    AND timestamp >= %s
                    AND timestamp < %s
                """,
                    (str(machine_no), current_hour_naive, next_hour_naive),
                )

                result = cursor.fetchone()
                return int(result[0]) if result and result[0] is not None else 0

        except Exception as e:
            print(f"❌ Current hour count DB error M{machine_no}: {e}")
            return 0

    def reset_shift_state(self, machine_no=None):
        with self.lock:
            if machine_no is None:
                self.machine_on_since.clear()
                self.first_count_time.clear()
                self.pending_reasons.clear()  # ✅ SHIFT RESET: Saare purane reasons hata do
                print("🔄 All machines: Shift state & reasons reset")
            else:
                self.machine_on_since.pop(machine_no, None)
                self.first_count_time.pop(machine_no, None)
                self.pending_reasons.pop(
                    machine_no, None
                )  # ✅ SINGLE RESET: Specific machine ka reason hata do
                print(f"🔄 M{machine_no}: Shift state & reason reset")

    def update_json_status(self, machine_no, card=None, die_height=0.0):
        with self.lock:
            ist_tz = pytz.timezone("Asia/Kolkata")
            now_ist = datetime.now(ist_tz)

            had_any_signal_before = (
                machine_no in self.machine_on_since
                or machine_no in self.machine_json_status
                or machine_no in self.machine_count_status
            )

            # ✅ First J signal: agar pehle koi signal nahi tha, to SHIFT START se OFFLINE ideal close karo
            if not had_any_signal_before:
                offline_start = self._infer_offline_start_for_new_signal(now_ist)
                self._save_ideal_range_split_by_hour(
                    machine_no=machine_no,
                    ideal_mode="OFFLINE",
                    start_at=offline_start,
                    end_at=now_ist,
                    closed_by="MACHINE_ON",
                    min_total_seconds=180,
                )
                # New ON session start
                self.machine_on_since[machine_no] = now_ist
                self.first_count_time.pop(machine_no, None)

            # ✅ Agar active OFFLINE ideal chal raha tha, J signal aate hi close karo
            active = self.active_ideal_segments.get(machine_no)
            if active and active.get("mode") == "OFFLINE":
                self._close_ideal_segment(machine_no, now_ist, "MACHINE_ON")
                # OFFLINE -> ONLINE boundary par machine_on_since reset karo, old count time reuse nahi hoga
                self.machine_on_since[machine_no] = now_ist
                self.first_count_time.pop(machine_no, None)

            if machine_no not in self.machine_on_since:
                self.machine_on_since[machine_no] = now_ist

            self.machine_json_status[machine_no] = {
                "last_json_time": now_ist,
                "card": card or "UNKNOWN",
                "die_height": die_height,
            }

            self.idle_tracker.mark_json(machine_no, now_ist)

            # ✅ Machine ON hai, agar recent count nahi hai to ONLINE ideal start karo
            self._start_online_ideal_if_needed(machine_no, now_ist)

    def _normalize_tool_id(self, tool_id):
        """
        COUNT/J payload se aayi tool id ko safe/valid banata hai.

        Valid RFID/EPC example:
            e2004716aad06821a4aa0113

        Invalid/cache-miss examples:
            e00000000000000000000002
            000000000000000000000000
            N/A / UNKNOWN / Failed

        Rule:
        - History/TOOL_CHANGE me sirf real EPC save hoga.
        - e000... fake EPC ko kabhi tool change nahi maana jayega.
        """
        if tool_id in [None, "", "NULL", "UNKNOWN", "N/A", "No data", "Failed"]:
            return None

        clean_tool_id = str(tool_id).strip().lower()[:24]
        if not clean_tool_id or clean_tool_id.upper() in [
            "NULL",
            "UNKNOWN",
            "N/A",
            "NO DATA",
            "FAILED",
        ]:
            return None

        # EPC normally 24 hex chars hota hai.
        if len(clean_tool_id) != 24:
            return None

        if any(ch not in "0123456789abcdef" for ch in clean_tool_id):
            return None

        # Fake/cache miss condition: e00000000000000000000002 jaisi id ignore.
        if clean_tool_id.startswith("e000"):
            return None

        # Atom/Plant RFID EPC usually e2... se start hota hai.
        # e0/e1 fake/misread ko tool change nahi maanenge.
        if not clean_tool_id.startswith("e2"):
            return None

        return clean_tool_id

    def _parse_valid_shut_height(self, shut_height):
        """
        RFID/cache miss case me node kabhi 1.01 / 0 / Failed / No data bhejta hai.
        Aise value ko valid shut height nahi maanenge, DB/history state corrupt nahi hogi.

        Valid examples: 408.00, 321.10, 208.00
        Invalid examples: 1.01, 1.00, 0.00, Failed, No data
        """
        if shut_height in ["No data", "Failed", None, 0, 0.0, "0", "0.0", "0.00", ""]:
            return None
        try:
            value = float(shut_height)
        except Exception:
            return None

        # ✅ 1.01 / 0.01 cache/RFID miss value ko valid height nahi maana jayega.
        # Actual press shut height hamesha is range se kaafi upar hoti hai.
        if value <= 10.0:
            return None

        return value

    def _is_failed_shut_height_reading(self, shut_height):
        """
        UI requirement:
        Agar MQTT/cache miss se 0.01 / 1.01 / Failed jaisi current reading aaye,
        to live UI me previous valid height carry-forward nahi karni; sirf 'Failed' show karna hai.

        Note: 0 / 0.00 / blank ko no-data maanenge, failed reading nahi.
        """
        if shut_height in ["Failed", "failed", "FAILED"]:
            return True
        if shut_height in [
            None,
            "",
            "No data",
            "None",
            "N/A",
            "UNKNOWN",
            0,
            0.0,
            "0",
            "0.0",
            "0.00",
        ]:
            return False
        try:
            value = float(shut_height)
        except Exception:
            return False
        return 0 < value <= 10.0

    def add_count(self, machine_no, count_increment=1, tool_id=None, shut_height=None):
        with self.lock:
            ist_tz = pytz.timezone("Asia/Kolkata")
            now_ist = datetime.now(ist_tz)
            current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
            current_shift = self.get_shift_from_time(now_ist)

            # ✅ COUNT AAYA: agar ONLINE/OFFLINE ideal chal raha tha to pehle close/save karo
            self.close_ideal_on_count_resume(machine_no, now_ist)

            # ✅ Ideal close hone ke baad reason clear karo, taaki saved row me reason miss na ho
            with self.reason_lock:
                 self.pending_reasons.pop(
                     machine_no,
                     None
                 )

            if machine_no not in self.machine_on_since:
                self.machine_on_since[machine_no] = now_ist
                print(f"🟢 M{machine_no}: Machine ON at {now_ist.strftime('%H:%M:%S')}")

            if machine_no not in self.first_count_time:
                self.first_count_time[machine_no] = now_ist
                print(
                    f"🎯 M{machine_no}: First count at {now_ist.strftime('%H:%M:%S')}"
                )

            if (
                machine_no not in self.hour_first_count_time
                or self.hour_first_count_time[machine_no].replace(
                    minute=0, second=0, microsecond=0
                )
                != current_hour
            ):
                self.hour_first_count_time[machine_no] = now_ist

            self.last_count_time[machine_no] = now_ist

            segment = self.machine_segments[machine_no]

            # ==========================================================
            # ✅ MACHINE HISTORY FIX: TOOL_CHANGE + VALID SHUT HEIGHT
            # ==========================================================
            # MQTT COUNT payload example:
            #   e20047154ab068218ea8010f 2161588.00
            # Here tool_id = e20047154ab068218ea8010f, shut_height = 588.00
            #
            # Requirement:
            # - Same tool id repeat ho to history me duplicate TOOL_CHANGE nahi save hoga.
            # - Tool id change ho to Machine_Event_Logs me TOOL_CHANGE save hoga.
            # - Shut height 1.01 / 0 / Failed / No data valid nahi hai, ignore hoga.
            # - Actual shut height change like 408.00 -> 321.10 save hoga.
            clean_tool_id = self._normalize_tool_id(tool_id)
            new_height_value = self._parse_valid_shut_height(shut_height)
            is_valid_height = new_height_value is not None

            old_tool_id = segment.get("tool_id")
            old_height = segment.get("shut_height")

            # DB/count row me bhi fake e000... tool id store nahi karenge.
            # Agar current message ka tool invalid hai to previous valid tool id carry forward hoga.
            db_tool_id_for_insert = clean_tool_id or old_tool_id or "UNKNOWN"
            db_shut_height_for_insert = (
                old_height
                if old_height
                else (new_height_value if is_valid_height else "0.00")
            )

            # ✅ TOOL ID CHANGE EVENT
            # First valid tool id ko baseline maanenge; event tabhi save hoga jab old tool id already ho.
            if clean_tool_id:
                if old_tool_id and old_tool_id != clean_tool_id:
                    log_machine_event(
                        plant_no=2,
                        machine_no=machine_no,
                        event_type="TOOL_CHANGE",
                        timestamp=now_ist,
                        shift=current_shift,
                        details=(
                            f"Tool changed from {old_tool_id} to {clean_tool_id}"
                            f" | Shut Height: {old_height if old_height else 'N/A'} -> "
                            f"{new_height_value if is_valid_height else 'Invalid/Not Read'}"
                        ),
                    )
                    segment["tool_id"] = clean_tool_id
                    segment["segment_start"] = now_ist
                    segment["segment_count"] = count_increment
                elif not old_tool_id:
                    segment["tool_id"] = clean_tool_id

            # ✅ SHUT HEIGHT CHANGE EVENT
            # 1.01 cache/RFID miss value ko yahan ignore karenge; wo DB/history state update nahi karegi.
            if is_valid_height:
                if old_height is None or old_height == 0.0:
                    segment["shut_height"] = new_height_value
                    segment["segment_start"] = now_ist
                    segment["segment_count"] = count_increment
                    db_shut_height_for_insert = new_height_value
                else:
                    height_difference = abs(float(old_height) - float(new_height_value))
                    height_changed = height_difference > 1.0

                    if height_changed:
                        threading.Thread(
                            target=send_shut_height_alert,
                            args=(2, machine_no, old_height, new_height_value, now_ist),
                            daemon=True,
                        ).start()

                        log_machine_event(
                            plant_no=2,
                            machine_no=machine_no,
                            event_type="SHUT_HEIGHT_CHANGE",
                            timestamp=now_ist,
                            shift=current_shift,
                            details=(
                                f"Height changed from {old_height} to {new_height_value}"
                                f" | Tool: {clean_tool_id or old_tool_id or tool_id}"
                            ),
                        )

                        segment["shut_height"] = new_height_value
                        segment["segment_start"] = now_ist
                        segment["segment_count"] = count_increment
                        db_shut_height_for_insert = new_height_value
                    else:
                        segment["segment_count"] = (
                            segment.get("segment_count", 0) + count_increment
                        )
                        db_shut_height_for_insert = old_height
            else:
                # Invalid height (0.01 / 1.01 etc.) se history/shut height state update nahi hogi.
                # Live UI me is current reading ke liye 'Failed' show hoga, but DB/count insert safe rahega.
                if old_height and old_height > 0:
                    segment["segment_count"] = (
                        segment.get("segment_count", 0) + count_increment
                    )
                    db_shut_height_for_insert = old_height
                    print(
                        f"⏭️ INVALID SHUT HEIGHT READING | M{machine_no} | "
                        f"raw={shut_height} | UI=Failed | state_kept={old_height}"
                    )

            # ✅ UI requirement:
            # Current MQTT/cache miss me 0.01 / 1.01 / Failed aaye to UI me 'Failed' show hoga.
            # Segment/history state me previous valid height safe rahegi, but live card carry-forward nahi karega.
            display_shut_height_for_status = (
                "Failed"
                if self._is_failed_shut_height_reading(shut_height)
                else (
                    db_shut_height_for_insert
                    if db_shut_height_for_insert
                    else "No data"
                )
            )

            self.machine_count_status[machine_no] = {
                "last_count_time": now_ist,
                "tool_id": (
                    db_tool_id_for_insert if db_tool_id_for_insert else "UNKNOWN"
                ),
                "shut_height": display_shut_height_for_status,
            }

            if machine_no in self.current_hours:
                if self.current_hours[machine_no] != current_hour:
                    self.last_hour_counts[machine_no] = self.current_hour_counts[
                        machine_no
                    ]
                    old_count = self.current_hour_counts[machine_no]
                    self.current_hour_counts[machine_no] = 0
                    self.current_hours[machine_no] = current_hour

                    print(f"⏰ M{machine_no}: Hour changed | Last={old_count}, New=0")
            else:
                self.current_hours[machine_no] = current_hour

            if machine_no in self.current_shifts:
                old_shift = self.current_shifts[machine_no]
                if old_shift != current_shift:
                    print(
                        f"🔄 M{machine_no}: Shift changed {old_shift}→{current_shift}"
                    )
                    new_shift_key = (machine_no, current_shift)
                    self.shift_cumulative[new_shift_key] = 0
                    self.reset_shift_state(machine_no)

            self.current_shifts[machine_no] = current_shift
            self.current_hour_counts[machine_no] += count_increment

            idle_status = self.idle_tracker.get_idle_status(machine_no, now_ist)
            live_idle_str = idle_status.get("live_idle_time", "0m")
            live_idle_mins = int(live_idle_str.replace("m", ""))

            if live_idle_mins > 0:
                # ✅ Count aya hai matlab machine ON ho chuki hai
                self.save_resolved_downtime_to_db(
                    machine_no,
                    now_ist,
                    current_shift,
                    live_idle_mins,
                    "ONLINE",
                    is_hour_change=False,
                )

            self.idle_tracker.mark_count(machine_no, now_ist)

        # ✅ DB insert/WebSocket ko global RAM lock ke bahar rakha hai.
        # Isse Redis queue worker count messages parallel process kar sakte hain.
        # Cumulative count DB advisory lock se safe rahega.
        self._insert_realtime_count(
            machine_no=machine_no,
            count_increment=count_increment,
            tool_id=db_tool_id_for_insert,
            shut_height=db_shut_height_for_insert,
            timestamp=now_ist,
            shift=current_shift,
        )

    def _insert_realtime_count(
        self, machine_no, count_increment, tool_id, shut_height, timestamp, shift
    ):
        """
        Count ko DB me insert karta hai, phir DB se exact current-hour count nikal kar
        WebSocket par bhejta hai. Isse UI RAM count se mismatch nahi hota.
        """
        try:
            shift_start = self.get_shift_start_datetime(timestamp)
            shift_start_naive = convert_to_naive_ist(shift_start)

            idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
            idle_time = idle_status["hourly_idle_total"]

            clean_tool_id = (
                str(tool_id)[:50] if tool_id not in ["NULL", None] else "NULL"
            )

            if isinstance(shut_height, (int, float)) and shut_height > 0:
                clean_shut_height = f"{float(shut_height):.2f}"
            else:
                try:
                    val = float(shut_height)
                    clean_shut_height = f"{val:.2f}" if val > 0 else "0.00"
                except:
                    clean_shut_height = "0.00"

            clean_idle_time = (
                int(idle_time) if isinstance(idle_time, (int, float)) else 0
            )

            if timestamp.tzinfo is not None:
                ist_timestamp = timestamp.astimezone(IST)
            else:
                ist_timestamp = IST.localize(timestamp)

            timestamp_str = ist_timestamp.strftime("%Y-%m-%d %H:%M:%S+05:30")

            refresh_db_connection()
            with transaction.atomic():
                with connection.cursor() as cursor:
                    # Same machine + shift par cumulative calculation safe rahegi
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s))",
                        (f"plant2:{machine_no}:{shift}",),
                    )

                    cursor.execute(
                        """
                        SELECT cumulative_count
                        FROM Plant2_data
                        WHERE machine_no = %s
                        AND shift = %s
                        AND timestamp >= %s
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """,
                        (str(machine_no), shift, shift_start_naive),
                    )

                    result = cursor.fetchone()
                    last_cumulative = (
                        int(result[0]) if result and result[0] is not None else 0
                    )
                    new_cumulative = last_cumulative + int(count_increment)

                    cursor.execute(
                        """
                        INSERT INTO Plant2_data
                        (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
                        VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            timestamp_str,
                            clean_tool_id,
                            str(machine_no),
                            int(count_increment),
                            new_cumulative,
                            0,
                            clean_idle_time,
                            clean_shut_height,
                            shift,
                        ),
                    )

            # ✅ DB insert ke baad exact current-hour count DB se nikalo
            current_hour_count_db = self.get_current_hour_count_from_db(
                machine_no, timestamp
            )

            try:
                channel_layer = get_channel_layer()
                if channel_layer:
                    live_data = {
                        "machine_no": machine_no,
                        "count": int(count_increment),
                        "current_hour_count": current_hour_count_db,
                        "cumulative_count": new_cumulative,
                        "shift": shift,
                        "status": "ONLINE",
                    }
                    async_to_sync(channel_layer.group_send)(
                        "plant2_live_updates",
                        {"type": "send_machine_update", "message": live_data},
                    )
                    print(
                        f"📡 LIVE DB COUNT SENT | M{machine_no} | Hour={current_hour_count_db} | Cum={new_cumulative}"
                    )
            except Exception as ws_err:
                print(f"❌ WebSocket Broadcast Error M{machine_no}: {ws_err}")

        except Exception as e:
            print(f"❌ Insert error M{machine_no}: {e}")
            traceback.print_exc()
            raise

    def save_segment_to_db(self, machine_no, segment):
        count = segment["segment_count"]
        if count == 0:
            return

        timestamp = segment["segment_start"]
        tool_id = segment["tool_id"]
        shut_height = segment["shut_height"]

        shift = self.get_shift_from_time(timestamp)
        shift_start = self.get_shift_start_datetime(timestamp)

        last_cumulative = 0
        try:
            shift_start_naive = convert_to_naive_ist(shift_start)
            refresh_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT cumulative_count FROM Plant2_data 
                    WHERE machine_no = %s AND shift = %s AND timestamp >= %s
                    ORDER BY timestamp DESC LIMIT 1
                """,
                    (str(machine_no), shift, shift_start_naive),
                )
                result = cursor.fetchone()
                if result:
                    last_cumulative = result[0]
        except Exception:
            pass

        new_cumulative = last_cumulative + count
        idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
        idle_time = idle_status["hourly_idle_total"]

        try:
            clean_tool_id = (
                str(tool_id)[:50] if tool_id not in ["NULL", None] else "NULL"
            )

            if isinstance(shut_height, (int, float)) and shut_height > 0:
                clean_shut_height = f"{float(shut_height):.2f}"
            else:
                clean_shut_height = "0.00"

            clean_idle_time = (
                int(idle_time) if isinstance(idle_time, (int, float)) else 0
            )

            if timestamp.tzinfo is not None:
                ist_timestamp = timestamp.astimezone(IST)
            else:
                ist_timestamp = IST.localize(timestamp)

            timestamp_str = ist_timestamp.strftime("%Y-%m-%d %H:%M:%S+05:30")

            refresh_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO Plant2_data (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
                    VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        timestamp_str,
                        clean_tool_id,
                        str(machine_no),
                        count,
                        new_cumulative,
                        0,
                        clean_idle_time,
                        clean_shut_height,
                        shift,
                    ),
                )

        except Exception as e:
            print(f"❌ Error inserting segment M{machine_no}: {e}")

        segment["segment_count"] = 0

    def get_machine_status(self, machine_no):
        with self.lock:
            ist_tz = pytz.timezone("Asia/Kolkata")
            now_ist = datetime.now(ist_tz)

            has_count = False
            count_seconds_ago = None
            count_tool_id = None
            count_shut_height = None

            if machine_no in self.machine_count_status:
                last_count = self.machine_count_status[machine_no]["last_count_time"]
                count_seconds_ago = (now_ist - last_count).total_seconds()
                count_tool_id = self.machine_count_status[machine_no]["tool_id"]
                count_shut_height = self.machine_count_status[machine_no]["shut_height"]

                if count_seconds_ago <= self.off_threshold_seconds:
                    has_count = True

            has_json = False
            json_seconds_ago = None
            json_card = None
            json_die_height = None

            if machine_no in self.machine_json_status:
                last_json = self.machine_json_status[machine_no]["last_json_time"]
                json_seconds_ago = (now_ist - last_json).total_seconds()
                json_card = self.machine_json_status[machine_no]["card"]
                json_die_height = self.machine_json_status[machine_no]["die_height"]

                if json_seconds_ago <= self.off_threshold_seconds:
                    has_json = True

            machine_on = has_count or has_json
            is_producing = has_count

            offline_since = None
            offline_duration_minutes = None

            if not machine_on:
                last_activity_time = None

                if (
                    machine_no in self.machine_count_status
                    and machine_no in self.machine_json_status
                ):
                    last_activity_time = max(
                        self.machine_count_status[machine_no]["last_count_time"],
                        self.machine_json_status[machine_no]["last_json_time"],
                    )
                elif machine_no in self.machine_count_status:
                    last_activity_time = self.machine_count_status[machine_no][
                        "last_count_time"
                    ]
                elif machine_no in self.machine_json_status:
                    last_activity_time = self.machine_json_status[machine_no][
                        "last_json_time"
                    ]

                if last_activity_time:
                    offline_since = last_activity_time
                    offline_duration_seconds = (
                        now_ist - last_activity_time
                    ).total_seconds()
                    offline_duration_minutes = int(offline_duration_seconds / 60)

                self.idle_tracker.mark_off(machine_no)

            # ✅ Current UI/status me fake tool id and invalid shut height show nahi karenge.
            # Priority: recent COUNT valid tool/height -> last valid segment -> valid JSON card/height -> N/A.
            segment_info = self.machine_segments.get(machine_no, {})
            segment_tool_id = segment_info.get("tool_id")
            segment_shut_height = segment_info.get("shut_height")

            valid_count_tool = self._normalize_tool_id(count_tool_id)
            valid_json_tool = self._normalize_tool_id(json_card)

            valid_count_height = self._parse_valid_shut_height(count_shut_height)
            valid_json_height = self._parse_valid_shut_height(json_die_height)
            valid_segment_height = self._parse_valid_shut_height(segment_shut_height)

            if valid_count_tool:
                tool_id = valid_count_tool
            elif segment_tool_id:
                tool_id = segment_tool_id
            elif valid_json_tool:
                tool_id = valid_json_tool
            else:
                tool_id = "N/A"

            if self._is_failed_shut_height_reading(
                count_shut_height
            ) or self._is_failed_shut_height_reading(json_die_height):
                # Current sensor/RFID reading failed. Live UI should show Failed, not old height.
                shut_height = "Failed"
            elif valid_count_height is not None:
                shut_height = valid_count_height
            elif valid_segment_height is not None:
                shut_height = valid_segment_height
            elif valid_json_height is not None:
                shut_height = valid_json_height
            else:
                shut_height = "No data"

            return {
                "machine_on": machine_on,
                "is_producing": is_producing,
                "has_count_data": has_count,
                "has_json_data": has_json,
                "count_seconds_ago": (
                    int(count_seconds_ago) if count_seconds_ago is not None else None
                ),
                "json_seconds_ago": (
                    int(json_seconds_ago) if json_seconds_ago is not None else None
                ),
                "tool_id": tool_id,
                "shut_height": shut_height,
                "data_source": (
                    "COUNT" if has_count else ("JSON" if has_json else "NONE")
                ),
                "offline_since": (
                    offline_since.strftime("%H:%M:%S") if offline_since else None
                ),
                "offline_duration_minutes": offline_duration_minutes,
            }

    def get_machine_data(self, machine_no):
        with self.lock:
            ist_tz = pytz.timezone("Asia/Kolkata")
            now_ist = datetime.now(ist_tz)
            current_shift = self.get_shift_from_time(now_ist)
            current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
            shift_start = self.get_shift_start_datetime(now_ist)

        last_hour_count_db = 0
        try:
            previous_hour_start = current_hour - timedelta(hours=1)
            previous_hour_end = current_hour
            previous_hour_start_naive = convert_to_naive_ist(previous_hour_start)
            previous_hour_end_naive = convert_to_naive_ist(previous_hour_end)

            refresh_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(count), 0) FROM Plant2_data 
                    WHERE machine_no = %s 
                    AND timestamp >= %s 
                    AND timestamp < %s
                """,
                    (
                        str(machine_no),
                        previous_hour_start_naive,
                        previous_hour_end_naive,
                    ),
                )
                result = cursor.fetchone()
                if result and result[0] is not None:
                    last_hour_count_db = int(result[0])
        except Exception as e:
            pass

        cumulative_from_db = 0
        try:
            shift_start_naive = convert_to_naive_ist(shift_start)
            refresh_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT cumulative_count FROM Plant2_data 
                    WHERE machine_no = %s AND shift = %s AND timestamp >= %s
                    ORDER BY timestamp DESC LIMIT 1
                """,
                    (str(machine_no), current_shift, shift_start_naive),
                )
                result = cursor.fetchone()
                if result and result[0] is not None:
                    cumulative_from_db = int(result[0])
        except Exception as e:
            pass

        live_cumulative = cumulative_from_db
        current_hour_count_db = self.get_current_hour_count_from_db(machine_no, now_ist)

        status_info = self.get_machine_status(machine_no)

        idle_status = self.idle_tracker.get_idle_status(machine_no, now_ist)
        hourly_idle_total = idle_status["hourly_idle_total"]

        total_shift_idle = self.get_shift_idle_from_hourly_table(
            machine_no, shift_start, current_shift, now_ist
        )

        on_since_str = None
        first_count_str = None
        time_to_first_count = None

        if machine_no in self.machine_on_since and status_info["machine_on"]:
            on_since = self.machine_on_since[machine_no]
            on_since_str = on_since.strftime("%H:%M:%S")

            if machine_no in self.first_count_time:
                first_count = self.first_count_time[machine_no]
                first_count_str = first_count.strftime("%H:%M:%S")
                delay = (first_count - on_since).total_seconds()
                time_to_first_count = int(delay / 60)

        if machine_no in self.machine_on_since and not status_info["machine_on"]:
            on_since = self.machine_on_since[machine_no]
            on_since_str = on_since.strftime("%H:%M:%S")

            if machine_no in self.first_count_time:
                first_count = self.first_count_time[machine_no]
                first_count_str = first_count.strftime("%H:%M:%S")

        return {
            "machine_no": machine_no,
            "current_hour_count": current_hour_count_db,
            "last_hour_count": last_hour_count_db,
            "cumulative_count": live_cumulative,
            "idle_time": hourly_idle_total,
            "total_shift_idle_time": total_shift_idle,
            "shift": current_shift,
            "machine_on": status_info["machine_on"],
            "is_producing": status_info["is_producing"],
            "has_count_data": status_info["has_count_data"],
            "has_json_data": status_info["has_json_data"],
            "count_seconds_ago": status_info["count_seconds_ago"],
            "json_seconds_ago": status_info["json_seconds_ago"],
            "current_tool_id": status_info["tool_id"],
            "current_shut_height": status_info["shut_height"],
            "data_source": status_info["data_source"],
            "on_since": on_since_str,
            "first_count_at": first_count_str,
            "time_to_first_count": time_to_first_count,
            "has_pending_reason": machine_no in self.pending_reasons,
        }

    def force_hour_reset_all_machines(self):
        pass


EXACT_REQUIREMENT_STATE = Plant2ExactRequirementState()
PLANT2_EXACT_REQUIREMENT_STATE = EXACT_REQUIREMENT_STATE

_messages_lock = threading.Lock()

BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"

# ✅ FINAL TOPICS: COUNT + J DONO SUBSCRIBE HONGE
# COUNT topics -> Redis queue -> DB insert -> WebSocket -> UI count
# J1-J9 topics -> Redis me nahi jayenge; sirf RAM me machine ON/OFF/status update hoga
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
    ("J1", 1),
    ("J2", 1),
    ("J3", 1),
    ("J4", 1),
    ("J5", 1),
    ("J6", 1),
    ("J7", 1),
    ("J8", 1),
    ("J9", 1),
]

TOPIC_MACHINE_MAPPING = {
    "COUNT3": [1, 2, 3, 4, 5],
    "COUNT2": [6, 7, 8, 9, 10],
    "COUNT52": [11, 12, 13, 14, 15],
    "COUNT1": [16, 17, 18, 19, 20],
    "COUNT4": [41, 42, 43, 44, 45, 46],
    "COUNT16": [21, 22, 23, 24, 25],
    "COUNT17": [26, 27, 28, 29, 30],
    "COUNT18": [31, 32, 33, 34, 35],
    "COUNT19": [36, 37, 38, 39, 40],
    "COUNT": [],
}

MACHINE_GROUP_MAPPING = {
    "J4": [1, 2, 3, 4, 5],
    "J3": [6, 7, 8, 9, 10],
    "J2": [11, 12, 13, 14, 15],
    "J1": [16, 17, 18, 19, 20],
    "J5": [41, 42, 43, 44, 45, 46],
    "J6": [21, 22, 23, 24, 25],
    "J7": [26, 27, 28, 29, 30],
    "J8": [31, 32, 33, 34, 35],
    "J9": [36, 37, 38, 39, 40],
}

# ✅ J TOPIC STATUS THROTTLE
# Same machine ka J status bahut fast aata hai (1 sec me 3-4 messages).
# Isliye J ko Redis queue me nahi bhejenge; sirf RAM status ko throttle ke saath update karenge.
JSON_STATUS_THROTTLE_SECONDS = float(
    os.getenv("PLANT2_JSON_STATUS_THROTTLE_SECONDS", "3")
)
_json_status_lock = threading.Lock()
_last_json_status_update = {}


def get_machine_group(machine_no):
    for group_name, machines in MACHINE_GROUP_MAPPING.items():
        if machine_no in machines:
            return group_name
    return "Unknown"


ACTIVE_MACHINES_THIS_HOUR = set()
MACHINE_DATA_CACHE = {}


def get_machines_for_topic(topic):
    return TOPIC_MACHINE_MAPPING.get(topic, [])


def parse_json_payload(raw_payload):
    try:
        data = json.loads(raw_payload)
        if "client_id" not in data:
            return None

        client_id = str(data.get("client_id", ""))

        if len(client_id) >= 2:
            plant_no = int(client_id[0]) if client_id[0].isdigit() else None
            machine_no = int(client_id[1:]) if client_id[1:].isdigit() else None
        else:
            return None

        card = data.get("card", "UNKNOWN")
        die_height_str = str(data.get("die_height", "0"))
        try:
            die_height = float(die_height_str)
        except:
            die_height = 0.0

        return {
            "type": "json",
            "plant_no": plant_no,
            "machine_no": machine_no,
            "card": card,
            "die_height": die_height,
        }
    except:
        return None


def handle_json_status_direct(raw_payload):
    """
    J topic ko Redis queue me nahi daalte.
    Sirf machine ON/OFF/status ke liye RAM state update karte hain.
    Throttle se J topic ka high-frequency load control hota hai.
    """
    try:
        parsed = parse_json_payload(raw_payload)
        if not parsed or parsed.get("plant_no") != 2:
            return

        machine_no = parsed["machine_no"]
        now_epoch = time_module.time()

        with _json_status_lock:
            last_update = _last_json_status_update.get(machine_no, 0)
            if now_epoch - last_update < JSON_STATUS_THROTTLE_SECONDS:
                return
            _last_json_status_update[machine_no] = now_epoch

        EXACT_REQUIREMENT_STATE.update_json_status(
            machine_no=machine_no,
            card=parsed.get("card"),
            die_height=parsed.get("die_height", 0.0),
        )

    except Exception as e:
        print(f"❌ J topic status update error: {e}")


def parse_count_payload(raw_payload):
    try:
        parts = raw_payload.strip().split()
        if len(parts) < 2:
            return None

        tool_id = parts[0][:24] if len(parts[0]) >= 24 else parts[0]
        val_str = parts[1]

        plant_no = (
            int(val_str[0]) if len(val_str) > 0 and val_str[0].isdigit() else None
        )

        machine_no = None
        if len(val_str) > 3:
            if val_str[1].isdigit() and val_str[2].isdigit():
                machine_no = int(val_str[1:3])
                shut_height_str = val_str[4:]
            else:
                machine_no = int(val_str[1]) if val_str[1].isdigit() else None
                shut_height_str = val_str[3:]
        elif len(val_str) > 2:
            machine_no = int(val_str[1]) if val_str[1].isdigit() else None
            shut_height_str = val_str[3:]

        if "Failed" in shut_height_str:
            shut_height = "Failed"
        elif shut_height_str:
            try:
                shut_height = float(shut_height_str)
            except:
                shut_height = "No data"
        else:
            shut_height = "No data"

        return {
            "type": "count",
            "plant_no": plant_no,
            "machine_no": machine_no,
            "tool_id": tool_id,
            "shut_height": shut_height,
        }
    except:
        return None


def print_active_machines_summary():
    def summary_worker():
        while True:
            try:
                time_module.sleep(30)
                ist_tz = pytz.timezone("Asia/Kolkata")
                now_ist = datetime.now(ist_tz)

                with EXACT_REQUIREMENT_STATE.lock:
                    producing_machines = []
                    all_machines = set()
                    for machines_list in TOPIC_MACHINE_MAPPING.values():
                        all_machines.update(machines_list)

                    for machine_no in sorted(all_machines):
                        if machine_no in EXACT_REQUIREMENT_STATE.last_count_time:
                            last_count = EXACT_REQUIREMENT_STATE.last_count_time[
                                machine_no
                            ]
                            seconds_ago = (now_ist - last_count).total_seconds()

                            if seconds_ago <= 60:
                                hour_count = EXACT_REQUIREMENT_STATE.get_current_hour_count_from_db(
                                    machine_no, now_ist
                                )
                                tool_id = "N/A"
                                if (
                                    machine_no
                                    in EXACT_REQUIREMENT_STATE.machine_count_status
                                ):
                                    tool_id = (
                                        EXACT_REQUIREMENT_STATE.machine_count_status[
                                            machine_no
                                        ].get("tool_id", "N/A")
                                    )

                                producing_machines.append(
                                    {
                                        "no": machine_no,
                                        "count": hour_count,
                                        "tool": (
                                            tool_id[:8] if tool_id != "N/A" else "N/A"
                                        ),
                                        "last": int(seconds_ago),
                                    }
                                )

                    if producing_machines:
                        print("\n" + "=" * 80)
                        print(
                            f"🏭 ACTIVE MACHINES ({len(producing_machines)} running) - {now_ist.strftime('%H:%M:%S')}"
                        )
                        print("=" * 80)

                        for i in range(0, len(producing_machines), 4):
                            chunk = producing_machines[i : i + 4]
                            for m in chunk:
                                print(
                                    f"M{m['no']:02d}: {m['count']:3d}ct | {m['tool']} | {m['last']:2d}s",
                                    end="  |  ",
                                )
                            print()
                        print("=" * 80 + "\n")
            except Exception as e:
                print(f"❌ Summary error: {e}")

    thread = threading.Thread(target=summary_worker, daemon=True)
    thread.start()


def save_hourly_idle_to_db(
    machine_no, timestamp, tool_id, shut_height, idle_time, shift
):
    try:
        clean_tool_id = str(tool_id)[:50] if tool_id not in ["NULL", None] else "NULL"

        if isinstance(shut_height, (int, float)) and shut_height > 0:
            clean_shut_height = f"{float(shut_height):.2f}"
        else:
            clean_shut_height = "0.00"

        clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 60

        if timestamp.tzinfo is not None:
            ist_timestamp = timestamp.astimezone(IST)
        else:
            ist_timestamp = IST.localize(timestamp)

        # ✅ FIX: +05:30 force kiya
        timestamp_str = ist_timestamp.strftime("%Y-%m-%d %H:%M:%S+05:30")

        refresh_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO "Plant2_hourly_idle"
                (timestamp, tool_id, machine_no, idle_time, shut_height, shift)
                VALUES (%s::timestamp WITH TIME ZONE, %s, %s, %s, %s, %s)
            """,
                (
                    timestamp_str,
                    clean_tool_id,
                    str(machine_no),
                    clean_idle_time,
                    clean_shut_height,
                    shift,
                ),
            )
    except Exception as e:
        pass


def save_hourly_idle_time_to_db():
    def idle_saver_worker():
        print("\n" + "⏰" * 50)
        print("⏰ HOURLY IDLE TIME TRACKER STARTED!")
        print(f"⏰ Snapshot time: XX:59:58")
        print(
            f"⏰ Started at: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print("⏰" * 50 + "\n")

        all_mapped_machines = set()
        for machines_list in TOPIC_MACHINE_MAPPING.values():
            all_mapped_machines.update(machines_list)

        last_saved_hour = None

        while True:
            try:
                ist_tz = pytz.timezone("Asia/Kolkata")
                now_ist = datetime.now(ist_tz)
                current_minute = now_ist.minute
                current_second = now_ist.second
                current_hour = now_ist.hour

                is_snapshot_time = current_minute == 59 and current_second >= 58

                if is_snapshot_time and last_saved_hour != current_hour:
                    print("\n" + "💾" * 50)
                    print(f"💾 HOURLY IDLE SNAPSHOT at {now_ist.strftime('%H:%M:%S')}")
                    print("💾" * 50 + "\n")

                    last_saved_hour = current_hour
                    current_hour_start = now_ist.replace(
                        minute=0, second=0, microsecond=0
                    )

                    saved_count = 0
                    for machine_no in sorted(all_mapped_machines):
                        try:
                            idle_status = (
                                EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(
                                    machine_no, now_ist
                                )
                            )
                            idle_time = idle_status["hourly_idle_total"]

                            segment = EXACT_REQUIREMENT_STATE.machine_segments[
                                machine_no
                            ]
                            tool_id = segment.get("tool_id", "NULL")
                            shut_height = segment.get("shut_height", 0.0)

                            shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)

                            save_hourly_idle_to_db(
                                machine_no=machine_no,
                                timestamp=current_hour_start,
                                tool_id=tool_id,
                                shut_height=shut_height,
                                idle_time=idle_time,
                                shift=shift,
                            )

                            live_idle_str = idle_status.get("live_idle_time", "0m")
                            live_idle_mins = int(live_idle_str.replace("m", ""))

                            machine_current_status = (
                                EXACT_REQUIREMENT_STATE.get_machine_status(machine_no)
                            )
                            is_offline_now = not machine_current_status["machine_on"]

                            # ✅ Status depend karega ki abhi offline hai ya online
                            machine_status_val = (
                                "OFFLINE" if is_offline_now else "ONLINE"
                            )

                            if live_idle_mins > 0:
                                EXACT_REQUIREMENT_STATE.save_resolved_downtime_to_db(
                                    machine_no,
                                    now_ist,
                                    shift,
                                    live_idle_mins,
                                    machine_status_val,
                                    is_hour_change=True,
                                )

                            saved_count += 1
                        except Exception as e:
                            print(f"❌ M{machine_no} idle save error: {e}")

                    print(
                        f"\n✅ Saved {saved_count}/{len(all_mapped_machines)} machine idle times\n"
                    )

                time_module.sleep(1)
            except Exception as e:
                print(f"❌ Idle tracker error: {e}")
                traceback.print_exc()
                time_module.sleep(5)

    thread = threading.Thread(target=idle_saver_worker, daemon=True)
    thread.start()


# ==============================================================
# ✅ NAYA FUNCTION: AUTO IDLE NOTIFICATION SENDER
# ==============================================================
def auto_generate_idle_notification(machine_no, idle_mins):
    """Ye function tab chalega jab machine 3 minute se idle hogi. Ye direct DB mein notification dalega."""
    try:
        refresh_db_connection()
        target_group = Group.objects.filter(name="Supervisor").first()
        if target_group:
            users = target_group.user_set.all()
            if users.exists():
                message = f"Machine M-{machine_no:02d} is idle for {idle_mins} mins. Please fill the downtime reason!"

                notifications_to_create = [
                    Notification(user=user, machine_no=str(machine_no), message=message)
                    for user in users
                ]
                Notification.objects.bulk_create(notifications_to_create)
                print(
                    f"🔔 AUTO-ALERT: Notification created for M-{machine_no} (Idle {idle_mins}m)"
                )
    except Exception as e:
        print(f"❌ Auto Alert Error M{machine_no}: {e}")


# ==============================================================


def start_machine_event_monitor():
    """Ye background thread har 5 second me ON/OFF check karega"""

    def monitor_worker():
        import time as time_module

        print("🔍 Plant 2 - Machine ON/OFF Event Monitor Started!")
        machine_last_state = {}

        # ✅ NAYA: Track karta hai ki kis machine ke liye alert bhej diya gaya hai
        machine_alert_state = {}

        all_mapped_machines = set()
        for machines_list in TOPIC_MACHINE_MAPPING.values():
            all_mapped_machines.update(machines_list)

        while True:
            try:
                time_module.sleep(5)
                ist_tz = pytz.timezone("Asia/Kolkata")
                now_ist = datetime.now(ist_tz)

                for machine_no in all_mapped_machines:
                    status = EXACT_REQUIREMENT_STATE.get_machine_status(machine_no)
                    is_currently_on = status["machine_on"]

                    # ✅ NEW: ONLINE/OFFLINE ideal segment tracking for ideal_time_segments_reason
                    EXACT_REQUIREMENT_STATE.track_ideal_segment_from_status(
                        machine_no, status, now_ist
                    )

                    # ==============================================================
                    # ✅ NAYA LOGIC: CHECK AND SEND AUTO NOTIFICATIONS
                    # ==============================================================
                    idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(
                        machine_no, now_ist
                    )
                    live_idle_str = idle_status.get("live_idle_time", "0m")
                    live_idle_mins = int(live_idle_str.replace("m", ""))

                    if live_idle_mins >= 3:
                        if not machine_alert_state.get(machine_no, False):
                            auto_generate_idle_notification(machine_no, live_idle_mins)
                            machine_alert_state[machine_no] = (
                                True  # Mark that alert is sent
                            )

                    elif live_idle_mins == 0:
                        machine_alert_state[machine_no] = (
                            False  # Reset if machine is producing again
                        )
                    # ==============================================================

                    if machine_no not in machine_last_state:
                        machine_last_state[machine_no] = is_currently_on
                        continue

                    was_on_before = machine_last_state[machine_no]

                    # ✅ OFFLINE TO ONLINE: Machine mein wapas signal aaya
                    if is_currently_on and not was_on_before:
                        shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)

                        # ✅ OFFLINE to ONLINE aane par RAM se pending reason hata do (Naya idle reason mangega)
                        EXACT_REQUIREMENT_STATE.pending_reasons.pop(machine_no, None)

                        # Pehle offline wala gap DB mein save karo
                        idle_status = (
                            EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(
                                machine_no, now_ist
                            )
                        )
                        live_idle_str = idle_status.get("live_idle_time", "0m")
                        live_idle_mins = int(live_idle_str.replace("m", ""))

                        if live_idle_mins > 0:
                            EXACT_REQUIREMENT_STATE.save_resolved_downtime_to_db(
                                machine_no,
                                now_ist,
                                shift,
                                live_idle_mins,
                                "OFFLINE to ONLINE",
                                is_hour_change=False,
                            )

                        log_machine_event(
                            plant_no=2,
                            machine_no=machine_no,
                            event_type="ON",
                            timestamp=now_ist,
                            shift=shift,
                            details="Machine Power/Signal Restored",
                        )
                        machine_last_state[machine_no] = True

                    # ✅ ONLINE TO OFFLINE: Machine ka signal toote hue 3 minute se zyada ho gaya
                    elif not is_currently_on and was_on_before:
                        exact_off_time_str = status["offline_since"]

                        if exact_off_time_str:
                            today = now_ist.date()
                            time_obj = datetime.strptime(
                                exact_off_time_str, "%H:%M:%S"
                            ).time()
                            exact_off_time = IST.localize(
                                datetime.combine(today, time_obj)
                            )
                        else:
                            exact_off_time = now_ist

                        shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(
                            exact_off_time
                        )

                        # Machine offline ho gayi (3 min grace ke baad), abhi tak ka gap DB mein daalo
                        idle_status = (
                            EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(
                                machine_no, now_ist
                            )
                        )
                        live_idle_str = idle_status.get("live_idle_time", "0m")
                        live_idle_mins = int(live_idle_str.replace("m", ""))

                        if live_idle_mins > 0:
                            EXACT_REQUIREMENT_STATE.save_resolved_downtime_to_db(
                                machine_no,
                                now_ist,
                                shift,
                                live_idle_mins,
                                "ONLINE to OFFLINE",
                                is_hour_change=False,
                            )

                        log_machine_event(
                            plant_no=2,
                            machine_no=machine_no,
                            event_type="OFF",
                            timestamp=exact_off_time,
                            shift=shift,
                            details="Machine Offline (No signal for 3 mins)",
                        )
                        machine_last_state[machine_no] = False

            except Exception as e:
                print(f"❌ Event Monitor Error: {e}")
                time_module.sleep(5)

    thread = threading.Thread(target=monitor_worker, daemon=True)
    thread.start()


# ==============================================================
# ✅ NAYA THREAD: REDIS QUEUE WORKER
# ==============================================================
def recover_processing_queue():
    """
    Agar backend crash hua aur message processing queue me reh gaya,
    startup par usko wapas main queue me daal do.
    """
    try:
        recovered = 0
        while True:
            item = redis_client.rpoplpush(
                "plant2_processing_queue", "plant2_mqtt_queue"
            )
            if not item:
                break
            recovered += 1
        if recovered:
            print(f"♻️ Redis recovered {recovered} pending Plant 2 MQTT messages")
    except Exception as e:
        print(f"❌ Redis recovery error: {e}")


def start_redis_queue_worker():
    """
    Redis reliable queue worker.
    Sirf COUNT messages DB/WebSocket pipeline me process honge.
    J topics Redis queue me nahi jayenge; agar old J queue me mil bhi gaya to ignore hoga.
    """

    def process_queue_data(data):
        topic = data.get("topic")
        raw_payload = data.get("payload")

        if not topic or raw_payload is None:
            return

        # ✅ Redis worker sirf COUNT process karega
        if not topic.startswith("COUNT"):
            return

        parsed = parse_count_payload(raw_payload)
        if parsed and parsed.get("plant_no") == 2:
            EXACT_REQUIREMENT_STATE.add_count(
                machine_no=parsed["machine_no"],
                count_increment=1,
                tool_id=parsed["tool_id"],
                shut_height=parsed["shut_height"],
            )

    def worker(worker_no):
        print(
            f"🚀 Redis Reliable Queue Worker #{worker_no} Started! COUNT -> DB/WebSocket"
        )

        while True:
            data_str = None
            try:
                data_str = redis_client.brpoplpush(
                    "plant2_mqtt_queue", "plant2_processing_queue", timeout=1
                )

                if not data_str:
                    continue

                data = json.loads(data_str)
                refresh_db_connection()
                process_queue_data(data)

                # ✅ DB insert/update success ke baad hi processing queue se remove karo
                redis_client.lrem("plant2_processing_queue", 1, data_str)
                close_old_connections()

            except Exception as e:
                print(f"❌ Redis Queue Worker #{worker_no} Error: {e}")
                traceback.print_exc()
                # data_str remove nahi karna. Restart/recovery me retry hoga.
                close_old_connections()
                time_module.sleep(1)

    # ✅ Recovery sirf ek baar karo, har worker me nahi
    recover_processing_queue()

    worker_count = int(os.getenv("PLANT2_QUEUE_WORKERS", "4"))
    if worker_count < 1:
        worker_count = 1

    for i in range(worker_count):
        thread = threading.Thread(
            target=worker, args=(i + 1,), daemon=True, name=f"plant2-redis-worker-{i+1}"
        )
        thread.start()

    print(f"🚀 Plant 2 Redis workers started: {worker_count}")


# ==============================================================


# ==============================================================
# ✅ ON_MESSAGE (Updated to use Redis only)
# ==============================================================
def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        raw_payload = msg.payload.decode("utf-8", errors="ignore").strip()

        # ✅ COUNT topics: reliable Redis queue -> DB insert -> WebSocket
        if topic.startswith("COUNT"):
            queue_data = {
                "topic": topic,
                "payload": raw_payload,
                "received_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S.%f"),
            }
            redis_client.lpush("plant2_mqtt_queue", json.dumps(queue_data))
            return

        # ✅ J topics: Redis/DB/WebSocket count pipeline me nahi jayenge.
        # Sirf RAM status update hoga, jisse machine ON/OFF, idle, notification logic chalta rahe.
        if topic.startswith("J"):
            handle_json_status_direct(raw_payload)
            return

        return

    except Exception as e:
        print(f"❌ on_message error: {e}")
        traceback.print_exc()


# ==============================================================


def build_mqtt_client_id(plant_name):
    """
    Har backend/process ka MQTT client id unique banata hai.
    .env me MQTT_CLIENT_PREFIX set karo, example:
    MQTT_CLIENT_PREFIX=production_server
    """
    prefix = os.getenv("MQTT_CLIENT_PREFIX", "local")
    hostname = socket.gethostname()
    pid = os.getpid()

    raw_client_id = f"{plant_name}_{prefix}_{hostname}_{pid}"

    # MQTT client id me safe characters rakho
    safe_client_id = "".join(
        ch if ch.isalnum() or ch in ["_", "-"] else "_" for ch in raw_client_id
    )

    # Kuch brokers long client ids allow karte hain, but safe side par limit rakhte hain
    return safe_client_id[:100]


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker (Plant 2)")
        for topic, qos in PLANT2_TOPICS:
            client.subscribe(topic, qos)
            print(f"📥 Subscribed: {topic}")
    else:
        print(f"❌ Connection failed with code {rc}")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(
            f"⚠️ Plant 2 MQTT disconnected unexpectedly. rc={rc}. Client will auto-reconnect."
        )
    else:
        print("ℹ️ Plant 2 MQTT disconnected cleanly.")


def start_plant2_mqtt():
    print("\n" + "🚀" * 50)
    print("🚀 STARTING PLANT 2 MQTT CLIENT")
    print("🚀" * 50 + "\n")

    # ==============================================================
    # ✅ REDIS CONNECTION TEST ON STARTUP
    # ==============================================================
    try:
        redis_client.ping()
        print("✅ Redis Connection Successful! Queue is active.")
    except Exception as e:
        print(f"❌ Redis Connection FAILED! Error: {e}")
    # ==============================================================

    client_id = build_mqtt_client_id("plant2")
    client = mqtt.Client(client_id=client_id, clean_session=True)
    print(f"🆔 Plant 2 MQTT Client ID: {client_id}")
    print(f"⚙️ MQTT_CLIENT_PREFIX={os.getenv('MQTT_CLIENT_PREFIX', 'local')}")

    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    except Exception as e:
        print(f"❌ MQTT connection error: {e}")
        return

    print_active_machines_summary()
    save_hourly_idle_time_to_db()

    start_machine_event_monitor()

    # ✅ NAYA: Background worker start karo
    start_redis_queue_worker()

    client.loop_start()
    print("✅ MQTT Loop Started (Plant 2)\n")
