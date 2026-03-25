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
# import threading
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
#                     return {
#                         'live_idle_time': '0m',
#                         'accumulated_idle_time': '0m',
#                         'hourly_idle_total': 60,
#                         'is_idle': False,
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
#                     FROM Plant2_hourly_idle
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
#     ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1)
# ]

# TOPIC_MACHINE_MAPPING = {
#     'COUNT3': [1, 2, 3, 4, 5],
#     'COUNT2': [6, 7, 8, 9, 10],
#     'COUNT52': [11, 12, 13, 14, 15],
#     'COUNT1': [16, 17, 18, 19, 20],
#     'COUNT4': [41, 42, 43, 44, 45, 46],
#     'COUNT': []
# }

# MACHINE_GROUP_MAPPING = {
#     'J4': [1, 2, 3, 4, 5],
#     'J3': [6, 7, 8, 9, 10],
#     'J2': [11, 12, 13, 14, 15],
#     'J1': [16, 17, 18, 19, 20],
#     'J5': [41, 42, 43, 44, 45, 46]
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

#     client.loop_start()
#     print("✅ MQTT Loop Started (Plant 2)\n")




# backend/apps/mqtt/simple_plant2.py - ULTIMATE FIXED VERSION

import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import threading
from apps.machines.machine_state import MACHINE_STATE
from apps.data_storage.hourly_idle_tracker import HOURLY_IDLE_TRACKER
import traceback
import pytz
from django.db import connection
import time as time_module
from threading import RLock
from collections import defaultdict
import json
from apps.utils.email_alert import send_shut_height_alert
import threading
import os

# ✅ ULTIMATE FIX 1: Force system timezone to IST
os.environ['TZ'] = 'Asia/Kolkata'

IST = pytz.timezone("Asia/Kolkata")


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
    Convert to IST and store as-is (no timezone)
    Django will treat it as local time
    """
    if timestamp.tzinfo is not None:
        ist_timestamp = timestamp.astimezone(IST)
    else:
        ist_timestamp = IST.localize(timestamp)

    # Create clean datetime (IST time as naive)
    naive_ist = datetime(
        ist_timestamp.year,
        ist_timestamp.month,
        ist_timestamp.day,
        ist_timestamp.hour,
        ist_timestamp.minute,
        ist_timestamp.second
    )
    return naive_ist


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
                    self.completed_segments_minutes[m] = self.completed_segments_minutes.get(m, 0) + live

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
                is_never_active = m not in self.on_since and \
                                m not in self.last_count_time and \
                                m not in self.last_json_time

                if is_never_active:
                    return {
                        'live_idle_time': '0m',
                        'accumulated_idle_time': '0m',
                        'hourly_idle_total': 60,
                        'is_idle': False,
                        'idle_type': IdleType.NO_SIGNAL_AS_IDLE,
                        'status': 'No Signal (Offline)',
                        'data_source': DataSource.NONE,
                        'on_since': None,
                        'last_count_time': None,
                        'count_seconds_ago': None,
                        'json_seconds_ago': None
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
                idle_type = IdleType.NONE if live == 0 else IdleType.ON_BUT_NOT_PRODUCING
            else:
                if live > 0:
                    status = "ON (No Count)"
                else:
                    status = "ON (Grace Period)"
                idle_type = IdleType.ON_BUT_NOT_PRODUCING if live > 0 else IdleType.NONE

            return {
                'live_idle_time': f'{live}m' if live > 0 else '0m',
                'accumulated_idle_time': f'{acc}m',
                'hourly_idle_total': min(60, total),
                'is_idle': live > 0,
                'idle_type': idle_type,
                'status': status,
                'data_source': self.data_source.get(m, DataSource.NONE),
                'on_since': self.on_since.get(m),
                'last_count_time': self.last_count_time.get(m),
                'count_seconds_ago': count_seconds_ago,
                'json_seconds_ago': json_seconds_ago
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

        self.last_count_time = {}
        self.hour_first_count_time = {}

        self.machine_json_status = {}
        self.machine_count_status = {}

        self.machine_on_since = {}
        self.first_count_time = {}

        self.machine_segments = defaultdict(lambda: {
            'shut_height': None,
            'tool_id': None,
            'segment_start': None,
            'segment_count': 0,
        })

        self.off_threshold_seconds = 180
        self.idle_tracker = StrictIdlePolicy(grace_seconds=180, enable_no_signal_as_idle=True)

    def get_shift_from_time(self, dt):
        ist_dt = dt.astimezone(pytz.timezone('Asia/Kolkata')) if dt.tzinfo else pytz.timezone('Asia/Kolkata').localize(dt)
        time_only = ist_dt.time()
        shift_A_start = datetime.strptime("08:30", "%H:%M").time()
        shift_A_end = datetime.strptime("20:00", "%H:%M").time()
        return 'A' if shift_A_start <= time_only < shift_A_end else 'B'

    def get_shift_start_datetime(self, timestamp):
        date = timestamp.date()
        shift = self.get_shift_from_time(timestamp)

        shift_a_start_time = datetime.strptime("08:30", "%H:%M").time()
        shift_b_start_time = datetime.strptime("20:30", "%H:%M").time()

        if shift == 'A':
            return IST.localize(datetime.combine(date, shift_a_start_time))
        else:
            if timestamp.time() < shift_a_start_time:
                prev_day = date - timedelta(days=1)
                return IST.localize(datetime.combine(prev_day, shift_b_start_time))
            else:
                return IST.localize(datetime.combine(date, shift_b_start_time))

    def get_shift_idle_from_hourly_table(self, machine_no, shift_start, shift, now):
        """
        ✅ Plant2_hourly_idle table se shift ka total idle nikalta hai
        Date + Shift wise sum karta hai
        """
        try:
            shift_start_naive = convert_to_naive_ist(shift_start)
            now_naive = convert_to_naive_ist(now)

            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COALESCE(SUM(idle_time), 0) 
                    FROM "Plant2_hourly_idle"
                    WHERE machine_no = %s 
                    AND shift = %s
                    AND DATE(timestamp) = DATE(%s)
                    AND timestamp >= %s
                    AND timestamp < %s
                """, (str(machine_no), shift, shift_start_naive, shift_start_naive, now_naive))

                result = cursor.fetchone()
                db_idle = int(result[0]) if result and result[0] else 0

            current_idle = self.idle_tracker.get_idle_status(machine_no, now)
            live_idle = current_idle['hourly_idle_total']

            total_shift_idle = db_idle + live_idle

            return total_shift_idle

        except Exception as e:
            print(f"❌ Error fetching shift idle M{machine_no}: {e}")
            traceback.print_exc()
            return 0

    def reset_shift_state(self, machine_no=None):
        """✅ FIX: Only called on SHIFT change, not hour change"""
        with self.lock:
            if machine_no is None:
                self.machine_on_since.clear()
                self.first_count_time.clear()
                print("🔄 All machines: Shift state reset")
            else:
                self.machine_on_since.pop(machine_no, None)
                self.first_count_time.pop(machine_no, None)
                print(f"🔄 M{machine_no}: Shift state reset")

    def update_json_status(self, machine_no, card=None, die_height=0.0):
        with self.lock:
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)

            if machine_no not in self.machine_on_since:
                self.machine_on_since[machine_no] = now_ist

            self.machine_json_status[machine_no] = {
                'last_json_time': now_ist,
                'card': card or 'UNKNOWN',
                'die_height': die_height
            }

            self.idle_tracker.mark_json(machine_no, now_ist)

    def add_count(self, machine_no, count_increment=1, tool_id=None, shut_height=None):
        with self.lock:
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)
            current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
            current_shift = self.get_shift_from_time(now_ist)

            # ✅ FIX 2: Machine ON tracking (shift level, not hour level)
            if machine_no not in self.machine_on_since:
                self.machine_on_since[machine_no] = now_ist
                print(f"🟢 M{machine_no}: Machine ON at {now_ist.strftime('%H:%M:%S')}")

            if machine_no not in self.first_count_time:
                self.first_count_time[machine_no] = now_ist
                print(f"🎯 M{machine_no}: First count at {now_ist.strftime('%H:%M:%S')}")

            if machine_no not in self.hour_first_count_time or \
               self.hour_first_count_time[machine_no].replace(minute=0, second=0, microsecond=0) != current_hour:
                self.hour_first_count_time[machine_no] = now_ist

            self.last_count_time[machine_no] = now_ist

            self.machine_count_status[machine_no] = {
                'last_count_time': now_ist,
                'tool_id': tool_id if tool_id else 'UNKNOWN',
                'shut_height': shut_height if shut_height else "No data"
            }

            segment = self.machine_segments[machine_no]

            is_valid_height = False
            new_height_value = None

            if shut_height not in ['No data', 'Failed', None, 0, 0.0, '0', '0.0', '']:
                try:
                    new_height_value = float(shut_height)
                    if new_height_value > 1.0:
                        is_valid_height = True
                except:
                    is_valid_height = False

            if is_valid_height:
                if segment['shut_height'] is None or segment['shut_height'] == 0.0:
                    segment['shut_height'] = new_height_value
                    segment['tool_id'] = tool_id
                    segment['segment_start'] = now_ist
                    segment['segment_count'] = count_increment
                else:
                    old_height = segment['shut_height']
                    height_difference = abs(old_height - new_height_value)
                    height_changed = height_difference > 1.0

                    if height_changed:
                        threading.Thread(
                            target=send_shut_height_alert,
                            args=(2, machine_no, old_height, new_height_value, now_ist),
                            daemon=True
                        ).start()

                        if segment['segment_count'] > 0:
                            self.save_segment_to_db(machine_no, segment)

                        segment['shut_height'] = new_height_value
                        segment['tool_id'] = tool_id
                        segment['segment_start'] = now_ist
                        segment['segment_count'] = count_increment
                    else:
                        segment['segment_count'] += count_increment
            else:
                if segment['shut_height'] and segment['shut_height'] > 0:
                    segment['segment_count'] += count_increment

            # ✅ FIX 3: Hour change auto reset (count only, state preserved)
            if machine_no in self.current_hours:
                if self.current_hours[machine_no] != current_hour:
                    # Save current hour count to last hour
                    self.last_hour_counts[machine_no] = self.current_hour_counts[machine_no]
                    # Reset current hour count to 0
                    old_count = self.current_hour_counts[machine_no]
                    self.current_hour_counts[machine_no] = 0
                    self.current_hours[machine_no] = current_hour

                    print(f"⏰ M{machine_no}: Hour changed | Last={old_count}, New=0")
            else:
                self.current_hours[machine_no] = current_hour

            # ✅ FIX 4: Shift change pe hi state reset (not hour change)
            if machine_no in self.current_shifts:
                old_shift = self.current_shifts[machine_no]
                if old_shift != current_shift:
                    print(f"🔄 M{machine_no}: Shift changed {old_shift}→{current_shift}")

                    new_shift_key = (machine_no, current_shift)
                    self.shift_cumulative[new_shift_key] = 0

                    # Reset ON-since and first count (shift level only)
                    self.reset_shift_state(machine_no)

            self.current_shifts[machine_no] = current_shift
            self.current_hour_counts[machine_no] += count_increment
            self.idle_tracker.mark_count(machine_no, now_ist)

            self._insert_realtime_count(
                machine_no=machine_no,
                count_increment=count_increment,
                tool_id=tool_id,
                shut_height=shut_height,
                timestamp=now_ist,
                shift=current_shift
            )

    def _insert_realtime_count(self, machine_no, count_increment, tool_id, shut_height, timestamp, shift):
        try:
            shift_start = self.get_shift_start_datetime(timestamp)

            last_cumulative = 0
            try:
                shift_start_naive = convert_to_naive_ist(shift_start)
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT cumulative_count FROM Plant2_data 
                        WHERE machine_no = %s AND shift = %s AND timestamp >= %s
                        ORDER BY timestamp DESC LIMIT 1
                    """, (str(machine_no), shift, shift_start_naive))
                    result = cursor.fetchone()
                    if result and result[0] is not None:
                        last_cumulative = int(result[0])
            except Exception:
                pass

            new_cumulative = last_cumulative + int(count_increment)

            idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
            idle_time = idle_status['hourly_idle_total']

            clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

            if isinstance(shut_height, (int, float)) and shut_height > 0:
                clean_shut_height = f"{float(shut_height):.2f}"
            else:
                try:
                    val = float(shut_height)
                    clean_shut_height = f"{val:.2f}" if val > 0 else "0.00"
                except:
                    clean_shut_height = "0.00"

            clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 0

            # ✅ ULTIMATE FIX 5: Convert to IST string for database
            if timestamp.tzinfo is not None:
                ist_timestamp = timestamp.astimezone(IST)
            else:
                ist_timestamp = IST.localize(timestamp)

            timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S')

            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO Plant2_data 
                    (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
                    VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    timestamp_str,
                    clean_tool_id,
                    str(machine_no),
                    int(count_increment),
                    new_cumulative,
                    0,
                    clean_idle_time,
                    clean_shut_height,
                    shift
                ))

            print(f"✅ M{machine_no}: {timestamp_str} | cnt={count_increment}, cumul={new_cumulative}, idle={clean_idle_time}m")

        except Exception as e:
            print(f"❌ Insert error M{machine_no}: {e}")
            traceback.print_exc()

    def save_segment_to_db(self, machine_no, segment):
        count = segment['segment_count']
        if count == 0:
            return

        timestamp = segment['segment_start']
        tool_id = segment['tool_id']
        shut_height = segment['shut_height']

        shift = self.get_shift_from_time(timestamp)
        shift_start = self.get_shift_start_datetime(timestamp)

        last_cumulative = 0
        try:
            shift_start_naive = convert_to_naive_ist(shift_start)
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT cumulative_count FROM Plant2_data 
                    WHERE machine_no = %s AND shift = %s AND timestamp >= %s
                    ORDER BY timestamp DESC LIMIT 1
                """, (str(machine_no), shift, shift_start_naive))
                result = cursor.fetchone()
                if result:
                    last_cumulative = result[0]
        except Exception:
            pass

        new_cumulative = last_cumulative + count
        idle_status = self.idle_tracker.get_idle_status(machine_no, timestamp)
        idle_time = idle_status['hourly_idle_total']

        try:
            clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

            if isinstance(shut_height, (int, float)) and shut_height > 0:
                clean_shut_height = f"{float(shut_height):.2f}"
            else:
               clean_shut_height = "0.00"

            clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 0

            # ✅ ULTIMATE FIX 6: Convert to IST string for database
            if timestamp.tzinfo is not None:
                ist_timestamp = timestamp.astimezone(IST)
            else:
                ist_timestamp = IST.localize(timestamp)

            timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S')

            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO Plant2_data (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
                    VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (timestamp_str, clean_tool_id, str(machine_no), count, new_cumulative, 0, clean_idle_time, clean_shut_height, shift))

        except Exception as e:
            print(f"❌ Error inserting segment M{machine_no}: {e}")

        segment['segment_count'] = 0

    def get_machine_status(self, machine_no):
        with self.lock:
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)

            has_count = False
            count_seconds_ago = None
            count_tool_id = None
            count_shut_height = None

            if machine_no in self.machine_count_status:
                last_count = self.machine_count_status[machine_no]['last_count_time']
                count_seconds_ago = (now_ist - last_count).total_seconds()
                count_tool_id = self.machine_count_status[machine_no]['tool_id']
                count_shut_height = self.machine_count_status[machine_no]['shut_height']

                if count_seconds_ago <= self.off_threshold_seconds:
                    has_count = True

            has_json = False
            json_seconds_ago = None
            json_card = None
            json_die_height = None

            if machine_no in self.machine_json_status:
                last_json = self.machine_json_status[machine_no]['last_json_time']
                json_seconds_ago = (now_ist - last_json).total_seconds()
                json_card = self.machine_json_status[machine_no]['card']
                json_die_height = self.machine_json_status[machine_no]['die_height']

                if json_seconds_ago <= self.off_threshold_seconds:
                    has_json = True

            machine_on = has_count or has_json
            is_producing = has_count

            offline_since = None
            offline_duration_minutes = None

            if not machine_on:
                last_activity_time = None

                if machine_no in self.machine_count_status and machine_no in self.machine_json_status:
                    last_activity_time = max(
                        self.machine_count_status[machine_no]['last_count_time'],
                        self.machine_json_status[machine_no]['last_json_time']
                    )
                elif machine_no in self.machine_count_status:
                    last_activity_time = self.machine_count_status[machine_no]['last_count_time']
                elif machine_no in self.machine_json_status:
                    last_activity_time = self.machine_json_status[machine_no]['last_json_time']

                if last_activity_time:
                    offline_since = last_activity_time
                    offline_duration_seconds = (now_ist - last_activity_time).total_seconds()
                    offline_duration_minutes = int(offline_duration_seconds / 60)

                self.idle_tracker.mark_off(machine_no)

            if count_tool_id:
                tool_id = count_tool_id
                shut_height = count_shut_height
            elif json_card:
                tool_id = json_card
                shut_height = json_die_height if json_die_height != 0.0 else "No data"
            else:
                tool_id = 'N/A'
                shut_height = "No data"

            return {
                'machine_on': machine_on,
                'is_producing': is_producing,
                'has_count_data': has_count,
                'has_json_data': has_json,
                'count_seconds_ago': int(count_seconds_ago) if count_seconds_ago is not None else None,
                'json_seconds_ago': int(json_seconds_ago) if json_seconds_ago is not None else None,
                'tool_id': tool_id,
                'shut_height': shut_height,
                'data_source': 'COUNT' if has_count else ('JSON' if has_json else 'NONE'),
                'offline_since': offline_since.strftime('%H:%M:%S') if offline_since else None,
                'offline_duration_minutes': offline_duration_minutes,
            }

    def get_machine_data(self, machine_no):
        with self.lock:
            ist_tz = pytz.timezone('Asia/Kolkata')
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

            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COALESCE(SUM(count), 0) FROM Plant2_data 
                    WHERE machine_no = %s 
                    AND timestamp >= %s 
                    AND timestamp < %s
                """, (str(machine_no), previous_hour_start_naive, previous_hour_end_naive))
                result = cursor.fetchone()
                if result and result[0] is not None:
                   last_hour_count_db = int(result[0])
        except Exception as e:
            print(f"❌ M{machine_no}: Last hour count error - {e}")

        cumulative_from_db = 0
        try:
            shift_start_naive = convert_to_naive_ist(shift_start)
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT cumulative_count FROM Plant2_data 
                    WHERE machine_no = %s AND shift = %s AND timestamp >= %s
                    ORDER BY timestamp DESC LIMIT 1
                """, (str(machine_no), current_shift, shift_start_naive))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    cumulative_from_db = int(result[0])
        except Exception as e:
            print(f"⚠️ Error fetching cumulative M{machine_no}: {e}")

        live_cumulative = cumulative_from_db

        status_info = self.get_machine_status(machine_no)

        idle_status = self.idle_tracker.get_idle_status(machine_no, now_ist)
        hourly_idle_total = idle_status['hourly_idle_total']

        # ✅ SHIFT TOTAL IDLE from Plant2_hourly_idle table
        total_shift_idle = self.get_shift_idle_from_hourly_table(
            machine_no, shift_start, current_shift, now_ist
        )

        on_since_str = None
        first_count_str = None
        time_to_first_count = None

        if machine_no in self.machine_on_since and status_info['machine_on']:
            on_since = self.machine_on_since[machine_no]
            on_since_str = on_since.strftime('%H:%M:%S')

            if machine_no in self.first_count_time:
                first_count = self.first_count_time[machine_no]
                first_count_str = first_count.strftime('%H:%M:%S')
                delay = (first_count - on_since).total_seconds()
                time_to_first_count = int(delay / 60)

        if machine_no in self.machine_on_since and not status_info['machine_on']:
            on_since = self.machine_on_since[machine_no]
            on_since_str = on_since.strftime('%H:%M:%S')

            if machine_no in self.first_count_time:
                first_count = self.first_count_time[machine_no]
                first_count_str = first_count.strftime('%H:%M:%S')

        return {
            'machine_no': machine_no,
            'current_hour_count': self.current_hour_counts.get(machine_no, 0),
            'last_hour_count': last_hour_count_db,
            'cumulative_count': live_cumulative,
            'idle_time': hourly_idle_total,
            'total_shift_idle_time': total_shift_idle,
            'shift': current_shift,
            'machine_on': status_info['machine_on'],
            'is_producing': status_info['is_producing'],
            'has_count_data': status_info['has_count_data'],
            'has_json_data': status_info['has_json_data'],
            'count_seconds_ago': status_info['count_seconds_ago'],
            'json_seconds_ago': status_info['json_seconds_ago'],
            'current_tool_id': status_info['tool_id'],
            'current_shut_height': status_info['shut_height'],
            'data_source': status_info['data_source'],
            'on_since': on_since_str,
            'first_count_at': first_count_str,
            'time_to_first_count': time_to_first_count
        }

    def force_hour_reset_all_machines(self):
        """Not used anymore - hour reset is automatic"""
        pass


EXACT_REQUIREMENT_STATE = Plant2ExactRequirementState()
PLANT2_EXACT_REQUIREMENT_STATE = EXACT_REQUIREMENT_STATE

_messages_lock = threading.Lock()

BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"

PLANT2_TOPICS = [
    ("COUNT", 1), ("COUNT1", 1), ("COUNT2", 1), ("COUNT3", 1), 
    ("COUNT4", 1), ("COUNT52", 1),
    ("J1", 1), ("J2", 1), ("J3", 1), ("J4", 1), ("J5", 1)
]

TOPIC_MACHINE_MAPPING = {
    'COUNT3': [1, 2, 3, 4, 5],
    'COUNT2': [6, 7, 8, 9, 10],
    'COUNT52': [11, 12, 13, 14, 15],
    'COUNT1': [16, 17, 18, 19, 20],
    'COUNT4': [41, 42, 43, 44, 45, 46],
    'COUNT': []
}

MACHINE_GROUP_MAPPING = {
    'J4': [1, 2, 3, 4, 5],
    'J3': [6, 7, 8, 9, 10],
    'J2': [11, 12, 13, 14, 15],
    'J1': [16, 17, 18, 19, 20],
    'J5': [41, 42, 43, 44, 45, 46]
}

def get_machine_group(machine_no):
    for group_name, machines in MACHINE_GROUP_MAPPING.items():
        if machine_no in machines:
            return group_name
    return 'Unknown'

ACTIVE_MACHINES_THIS_HOUR = set()
MACHINE_DATA_CACHE = {}

def get_machines_for_topic(topic):
    return TOPIC_MACHINE_MAPPING.get(topic, [])

def parse_json_payload(raw_payload):
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

def parse_count_payload(raw_payload):
    try:
        parts = raw_payload.strip().split()
        if len(parts) < 2:
            return None

        tool_id = parts[0][:24] if len(parts[0]) >= 24 else parts[0]
        val_str = parts[1]

        plant_no = int(val_str[0]) if len(val_str) > 0 and val_str[0].isdigit() else None

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

        if 'Failed' in shut_height_str:
            shut_height = "Failed"
        elif shut_height_str:
            try:
                shut_height = float(shut_height_str)
            except:
                shut_height = "No data"
        else:
            shut_height = "No data"

        return {
            'type': 'count',
            'plant_no': plant_no,
            'machine_no': machine_no,
            'tool_id': tool_id,
            'shut_height': shut_height
        }
    except:
        return None

def print_active_machines_summary():
    def summary_worker():
        while True:
            try:
                time_module.sleep(30)
                ist_tz = pytz.timezone('Asia/Kolkata')
                now_ist = datetime.now(ist_tz)

                with EXACT_REQUIREMENT_STATE.lock:
                    producing_machines = []
                    all_machines = set()
                    for machines_list in TOPIC_MACHINE_MAPPING.values():
                        all_machines.update(machines_list)

                    for machine_no in sorted(all_machines):
                        if machine_no in EXACT_REQUIREMENT_STATE.last_count_time:
                            last_count = EXACT_REQUIREMENT_STATE.last_count_time[machine_no]
                            seconds_ago = (now_ist - last_count).total_seconds()

                            if seconds_ago <= 60:
                                hour_count = EXACT_REQUIREMENT_STATE.current_hour_counts.get(machine_no, 0)
                                tool_id = 'N/A'
                                if machine_no in EXACT_REQUIREMENT_STATE.machine_count_status:
                                    tool_id = EXACT_REQUIREMENT_STATE.machine_count_status[machine_no].get('tool_id', 'N/A')

                                producing_machines.append({
                                    'no': machine_no,
                                    'count': hour_count,
                                    'tool': tool_id[:8] if tool_id != 'N/A' else 'N/A',
                                    'last': int(seconds_ago)
                                })

                    if producing_machines:
                        print("\n" + "=" * 80)
                        print(f"🏭 ACTIVE MACHINES ({len(producing_machines)} running) - {now_ist.strftime('%H:%M:%S')}")
                        print("=" * 80)

                        for i in range(0, len(producing_machines), 4):
                            chunk = producing_machines[i:i+4]
                            for m in chunk:
                                print(f"M{m['no']:02d}: {m['count']:3d}ct | {m['tool']} | {m['last']:2d}s", end="  |  ")
                            print()
                        print("=" * 80 + "\n")
            except Exception as e:
                print(f"❌ Summary error: {e}")

    thread = threading.Thread(target=summary_worker, daemon=True)
    thread.start()


def save_hourly_idle_to_db(machine_no, timestamp, tool_id, shut_height, idle_time, shift):
    try:
        clean_tool_id = str(tool_id)[:50] if tool_id not in ['NULL', None] else 'NULL'

        if isinstance(shut_height, (int, float)) and shut_height > 0:
            clean_shut_height = f"{float(shut_height):.2f}"
        else:
            clean_shut_height = "0.00"

        clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 60

        # ✅ ULTIMATE FIX 7: Convert to IST string for database
        if timestamp.tzinfo is not None:
            ist_timestamp = timestamp.astimezone(IST)
        else:
            ist_timestamp = IST.localize(timestamp)

        timestamp_str = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO "Plant2_hourly_idle"
                (timestamp, tool_id, machine_no, idle_time, shut_height, shift)
                VALUES (%s::timestamp WITHOUT TIME ZONE, %s, %s, %s, %s, %s)
            """, (
                timestamp_str,
                clean_tool_id,
                str(machine_no),
                clean_idle_time,
                clean_shut_height,
                shift
            ))

        print(f"💾 IDLE M{machine_no}: {timestamp_str} | idle={clean_idle_time}m, shift={shift}")

    except Exception as e:
        print(f"❌ Hourly idle DB error M{machine_no}: {e}")
        traceback.print_exc()


def save_hourly_idle_time_to_db():
    def idle_saver_worker():
        print("\n" + "⏰" * 50)
        print("⏰ HOURLY IDLE TIME TRACKER STARTED!")
        print(f"⏰ Snapshot time: XX:59:58")
        print(f"⏰ Started at: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}")
        print("⏰" * 50 + "\n")

        all_mapped_machines = set()
        for machines_list in TOPIC_MACHINE_MAPPING.values():
            all_mapped_machines.update(machines_list)

        print(f"✅ Tracking idle time for {len(all_mapped_machines)} machines")
        print(f"✅ Machines: {sorted(all_mapped_machines)}\n")

        last_saved_hour = None

        while True:
            try:
                ist_tz = pytz.timezone('Asia/Kolkata')
                now_ist = datetime.now(ist_tz)
                current_minute = now_ist.minute
                current_second = now_ist.second
                current_hour = now_ist.hour

                is_snapshot_time = (current_minute == 59 and current_second == 58)

                if is_snapshot_time and last_saved_hour != current_hour:
                    print("\n" + "💾" * 50)
                    print(f"💾 HOURLY IDLE SNAPSHOT at {now_ist.strftime('%H:%M:%S')}")
                    print("💾" * 50 + "\n")

                    last_saved_hour = current_hour
                    current_hour_start = now_ist.replace(minute=0, second=0, microsecond=0)

                    saved_count = 0
                    for machine_no in sorted(all_mapped_machines):
                        try:
                            idle_status = EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
                            idle_time = idle_status['hourly_idle_total']

                            segment = EXACT_REQUIREMENT_STATE.machine_segments[machine_no]
                            tool_id = segment.get('tool_id', 'NULL')
                            shut_height = segment.get('shut_height', 0.0)

                            shift = EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)

                            save_hourly_idle_to_db(
                                machine_no=machine_no,
                                timestamp=current_hour_start,
                                tool_id=tool_id,
                                shut_height=shut_height,
                                idle_time=idle_time,
                                shift=shift
                            )
                            saved_count += 1
                        except Exception as e:
                            print(f"❌ M{machine_no} idle save error: {e}")

                    print(f"\n✅ Saved {saved_count}/{len(all_mapped_machines)} machine idle times\n")

                time_module.sleep(1)
            except Exception as e:
                print(f"❌ Idle tracker error: {e}")
                traceback.print_exc()
                time_module.sleep(5)

    thread = threading.Thread(target=idle_saver_worker, daemon=True)
    thread.start()


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        raw_payload = msg.payload.decode('utf-8', errors='ignore').strip()

        if topic.startswith('J'):
            parsed = parse_json_payload(raw_payload)
            if parsed and parsed['plant_no'] == 2:
                machine_no = parsed['machine_no']
                card = parsed['card']
                die_height = parsed['die_height']

                EXACT_REQUIREMENT_STATE.update_json_status(
                    machine_no=machine_no,
                    card=card,
                    die_height=die_height
                )

        elif topic.startswith('COUNT'):
            parsed = parse_count_payload(raw_payload)
            if parsed and parsed['plant_no'] == 2:
                machine_no = parsed['machine_no']
                tool_id = parsed['tool_id']
                shut_height = parsed['shut_height']

                EXACT_REQUIREMENT_STATE.add_count(
                    machine_no=machine_no,
                    count_increment=1,
                    tool_id=tool_id,
                    shut_height=shut_height
                )

    except Exception as e:
        print(f"❌ on_message error: {e}")
        traceback.print_exc()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker (Plant 2)")
        for topic, qos in PLANT2_TOPICS:
            client.subscribe(topic, qos)
            print(f"📥 Subscribed: {topic}")
    else:
        print(f"❌ Connection failed with code {rc}")


def start_plant2_mqtt():
    print("\n" + "🚀" * 50)
    print("🚀 STARTING PLANT 2 MQTT CLIENT")
    print("🚀" * 50 + "\n")

    client = mqtt.Client(client_id="plant2_exact_backend", clean_session=True)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    except Exception as e:
        print(f"❌ MQTT connection error: {e}")
        return

    print_active_machines_summary()
    save_hourly_idle_time_to_db()

    client.loop_start()
    print("✅ MQTT Loop Started (Plant 2)\n")