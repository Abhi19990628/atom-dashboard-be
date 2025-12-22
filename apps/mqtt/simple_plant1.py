
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


IST = pytz.timezone("Asia/Kolkata")


class IdleType:
    ON_BUT_NOT_PRODUCING = "ON_BUT_NOT_PRODUCING"
    NO_SIGNAL_AS_IDLE = "NO_SIGNAL_AS_IDLE"
    NONE = "NONE"


class DataSource:
    COUNT = "COUNT"
    JSON = "JSON"
    NONE = "NONE"


class StrictIdlePolicy:
    """Same idle tracker as Plant 2"""
    
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
            if m in self.on_since:
                del self.on_since[m]
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
                is_never_active = m not in self.on_since and m not in self.last_count_time and m not in self.last_json_time
                
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
                status = "Producing" if live == 0 else "Producing (Idle)"
                idle_type = IdleType.NONE if live == 0 else IdleType.ON_BUT_NOT_PRODUCING
            else:
                status = "ON (Grace Period)" if live == 0 else "ON (No Count)"
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


class Plant1ExactRequirementState:
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

            if machine_no not in self.machine_on_since:
                self.machine_on_since[machine_no] = now_ist
            
            if machine_no not in self.first_count_time:
                self.first_count_time[machine_no] = now_ist
            
            if machine_no not in self.hour_first_count_time or \
               self.hour_first_count_time[machine_no].replace(minute=0, second=0, microsecond=0) != current_hour:
                self.hour_first_count_time[machine_no] = now_ist
            
            self.last_count_time[machine_no] = now_ist
            
            self.machine_count_status[machine_no] = {
                'last_count_time': now_ist,
                'tool_id': tool_id if tool_id else 'UNKNOWN',
                'shut_height': shut_height if shut_height else "No data"
            }

            if machine_no not in self.current_hours:
                self.current_hours[machine_no] = current_hour

            if machine_no in self.current_shifts:
                old_shift = self.current_shifts[machine_no]
                if old_shift != current_shift:
                    new_shift_key = (machine_no, current_shift)
                    self.shift_cumulative[new_shift_key] = 0

            self.current_shifts[machine_no] = current_shift
            
            self.current_hour_counts[machine_no] += count_increment
            
            self.idle_tracker.mark_count(machine_no, now_ist)

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
            
            if not machine_on:
                if machine_no in self.machine_on_since:
                    del self.machine_on_since[machine_no]
                if machine_no in self.first_count_time:
                    del self.first_count_time[machine_no]
            
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
                'count_seconds_ago': int(count_seconds_ago) if count_seconds_ago else None,
                'json_seconds_ago': int(json_seconds_ago) if json_seconds_ago else None,
                'tool_id': tool_id,
                'shut_height': shut_height,
                'data_source': 'COUNT' if has_count else ('JSON' if has_json else 'NONE')
            }

    def get_machine_data(self, machine_no):
        with self.lock:
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)
            current_shift = self.get_shift_from_time(now_ist)
            current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
            shift_start = self.get_shift_start_datetime(now_ist)
        
        # ✅ 1. FETCH LAST HOUR COUNT FROM DATABASE (Previous completed hour)
        last_hour_count_db = 0
        try:
            previous_hour_start = current_hour - timedelta(hours=1)
            previous_hour_end = current_hour
            previous_hour_start_naive = previous_hour_start.replace(tzinfo=None)
            previous_hour_end_naive = previous_hour_end.replace(tzinfo=None)
        
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT count FROM Plant1_data 
                    WHERE machine_no = %s 
                    AND timestamp >= %s 
                    AND timestamp < %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (str(machine_no), previous_hour_start_naive, previous_hour_end_naive))
                result = cursor.fetchone()
                if result:
                   last_hour_count_db = int(result[0])
        except Exception as e:
            print(f"❌ M{machine_no}: ERROR - {e}")
        
        # ✅ 2. FETCH CUMULATIVE COUNT FROM DATABASE (shift-based)
        cumulative_from_db = 0
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT cumulative_count FROM Plant1_data 
                    WHERE machine_no = %s AND shift = %s AND timestamp >= %s
                    ORDER BY timestamp DESC LIMIT 1
                """, (str(machine_no), current_shift, shift_start))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    cumulative_from_db = int(result[0])
        except Exception as e:
            print(f"⚠️ Error fetching cumulative M{machine_no}: {e}")
        
        # Add current hour live count
        live_cumulative = cumulative_from_db + self.current_hour_counts.get(machine_no, 0)
        
        # ✅ 3. FETCH TOTAL SHIFT IDLE TIME FROM DATABASE
        total_shift_idle_time = 0
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COALESCE(SUM(idle_time), 0) FROM Plant1_data 
                    WHERE machine_no = %s AND shift = %s AND timestamp >= %s
                """, (str(machine_no), current_shift, shift_start))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    total_shift_idle_time = int(result[0])
        except Exception as e:
            print(f"⚠️ Error fetching total shift idle M{machine_no}: {e}")
        
        # Get current hour idle (live)
        idle_status = self.idle_tracker.get_idle_status(machine_no)
        current_hour_idle = idle_status['hourly_idle_total']
        
        # Total shift idle = DB sum + current hour live
        total_shift_idle = total_shift_idle_time + current_hour_idle
        
        status_info = self.get_machine_status(machine_no)

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
        
        return {
            'machine_no': machine_no,
            'current_hour_count': self.current_hour_counts.get(machine_no, 0),
            'last_hour_count': last_hour_count_db,
            'cumulative_count': live_cumulative,
            'idle_time': current_hour_idle,
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
        with self.lock:
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)
            current_shift = self.get_shift_from_time(now_ist)
            
            all_machines = list(range(1, 58))
            
            for machine_no in all_machines:
                current_count = self.current_hour_counts.get(machine_no, 0)
                self.last_hour_counts[machine_no] = current_count
                
                if machine_no in self.current_shifts:
                    old_shift = self.current_shifts[machine_no]
                    if old_shift != current_shift:
                        new_shift_key = (machine_no, current_shift)
                        self.shift_cumulative[new_shift_key] = 0
                
                self.current_shifts[machine_no] = current_shift
            
            self.current_hour_counts.clear()
            self.idle_tracker.reset_hour()


PLANT1_EXACT_REQUIREMENT_STATE = Plant1ExactRequirementState()
EXACT_REQUIREMENT_STATE = PLANT1_EXACT_REQUIREMENT_STATE

_messages_lock = threading.Lock()

BROKER_HOST = "192.168.0.35"
BROKER_PORT = 1883
USERNAME = "npdAtom"
PASSWORD = "npd@Atom"

# ✅ Plant 1 Topics: JJ = JSON (ON/OFF), COUNT = Production count
PLANT1_TOPICS = [
    ("JJ5", 1), ("JJ6", 1), ("JJ7", 1), ("JJ8", 1), ("JJ9", 1),
    ("JJ10", 1), ("JJ11", 1), ("JJ12", 1), ("JJ13", 1), ("JJ14", 1), ("JJ15", 1),
    ("COUNT5", 1), ("COUNT6", 1), ("COUNT7", 1), ("COUNT8", 1), ("COUNT9", 1),
    ("COUNT10", 1), ("COUNT11", 1), ("COUNT12", 1), ("COUNT13", 1), ("COUNT14", 1), ("COUNT15", 1)
]

# ✅ J Topic Mapping (JSON - Machine ON/OFF signals)
J_TOPIC_MACHINE_MAPPING = {
    'JJ5': [31, 32, 33, 34, 35],
    'JJ6': [26, 27, 28, 29, 30],
    'JJ7': [40, 41, 42, 43, 44, 45],
    'JJ8': [46, 47, 48, 49, 50, 51, 52],
    'JJ9': [54, 55, 56, 57],
    'JJ10': [36, 37, 38, 39],
    'JJ11': [5, 6, 21, 22, 23, 53],
    'JJ12': [4, 7, 13, 14, 16],
    'JJ13': [3, 8, 12, 15, 17],
    'JJ14': [2, 9, 11, 18, 25],
    'JJ15': [1, 10, 20, 19, 24]
}

# ✅ COUNT Topic Mapping (Production count messages)
COUNT_TOPIC_MACHINE_MAPPING = {
    'COUNT5': [31, 32, 33, 34, 35],
    'COUNT6': [26, 27, 28, 29, 30],
    'COUNT7': [40, 41, 42, 43, 44, 45],
    'COUNT8': [46, 47, 48, 49, 50, 51, 52],
    'COUNT9': [54, 55, 56, 57],
    'COUNT10': [36, 37, 38, 39],
    'COUNT11': [5, 6, 21, 22, 23, 53],
    'COUNT12': [4, 7, 13, 14, 16],
    'COUNT13': [3, 8, 12, 15, 17],
    'COUNT14': [2, 9, 11, 18, 25],
    'COUNT15': [1, 10, 20, 19, 24]
}

# Combined for easy access
TOPIC_MACHINE_MAPPING = {**J_TOPIC_MACHINE_MAPPING, **COUNT_TOPIC_MACHINE_MAPPING}

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
    
def save_all_machines_on_hour_boundary():
    def save_worker():
        print("\n" + "🚀" * 50)
        print("🚀 PLANT 1 WORKER THREAD STARTED!")
        print(f"🚀 Started at: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}")
        print("🚀" * 50 + "\n")
        
        all_mapped_machines = list(range(1, 58))
        print(f"✅ Total machines: {len(all_mapped_machines)}")
        print(f"✅ Machines: {sorted(all_mapped_machines)}\n")
        
        last_saved_hour = None
        
        while True:
            try:
                ist_tz = pytz.timezone('Asia/Kolkata')
                now_ist = datetime.now(ist_tz)
                
                current_minute = now_ist.minute
                current_second = now_ist.second
                current_hour = now_ist.hour
                
                is_snapshot_time = (current_minute == 59 and current_second >= 40)
                
                if is_snapshot_time and last_saved_hour != current_hour:
                    print("\n" + "=" * 50)
                    print(f"📸 SNAPSHOT TRIGGER! {now_ist.strftime('%H:%M:%S')}")
                    print("=" * 50)
                    
                    if current_second < 50:
                        wait = 50 - current_second
                        print(f"⏳ Waiting {wait}s till 59:50...")
                        time_module.sleep(wait)
                        now_ist = datetime.now(ist_tz)
                    
                    print("=" * 40)
                    print(f"📸 PLANT 1 SNAPSHOT - {now_ist.strftime('%H:%M:%S')}")
                    print("=" * 40)
                    
                    captured_data = {}
                    with PLANT1_EXACT_REQUIREMENT_STATE.lock:
                        for machine_no in all_mapped_machines:
                            hour_count = PLANT1_EXACT_REQUIREMENT_STATE.current_hour_counts.get(machine_no, 0)
                            first_count_time = PLANT1_EXACT_REQUIREMENT_STATE.hour_first_count_time.get(machine_no)
                            
                            tool_id = 'NULL'
                            shut_height = "No data"
                            if machine_no in PLANT1_EXACT_REQUIREMENT_STATE.machine_count_status:
                                tool_id = PLANT1_EXACT_REQUIREMENT_STATE.machine_count_status[machine_no].get('tool_id', 'NULL')
                                shut_height = PLANT1_EXACT_REQUIREMENT_STATE.machine_count_status[machine_no].get('shut_height', "No data")
                            
                            idle_status = PLANT1_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
                            idle_time = idle_status['hourly_idle_total']
                            
                            captured_data[machine_no] = {
                                'hour_count': hour_count,
                                'first_count_time': first_count_time,
                                'tool_id': tool_id,
                                'shut_height': shut_height,
                                'idle_time': idle_time
                            }
                    
                    machine_data_snapshot = {}
                    current_hour_start = now_ist.replace(minute=0, second=0, microsecond=0)
                    next_hour_start = current_hour_start + timedelta(hours=1)
                    
                    for machine_no in all_mapped_machines:
                        try:
                            data = captured_data[machine_no]
                            first_count_time = data['first_count_time']
                            total_idle = data['idle_time']
                            
                            if machine_no in PLANT1_EXACT_REQUIREMENT_STATE.machine_on_since:
                                on_time = PLANT1_EXACT_REQUIREMENT_STATE.machine_on_since[machine_no]
                                
                                if on_time >= current_hour_start and on_time < next_hour_start:
                                    if first_count_time and first_count_time >= current_hour_start:
                                        save_timestamp = first_count_time
                                    else:
                                        save_timestamp = on_time
                                elif on_time < current_hour_start:
                                    if first_count_time and first_count_time >= current_hour_start and first_count_time < next_hour_start:
                                        save_timestamp = first_count_time
                                    else:
                                        save_timestamp = current_hour_start
                                else:
                                    save_timestamp = current_hour_start
                            else:
                                save_timestamp = current_hour_start
                            
                            machine_data_snapshot[machine_no] = {
                                'timestamp': save_timestamp,
                                'count': data['hour_count'],
                                'tool_id': data['tool_id'],
                                'shut_height': data['shut_height'],
                                'idle_time': total_idle
                            }
                        
                        except Exception as e:
                            machine_data_snapshot[machine_no] = {
                                'timestamp': current_hour_start,
                                'count': 0,
                                'tool_id': 'NULL',
                                'shut_height': 0.0,
                                'idle_time': 60
                            }
                    
                    now_ist = datetime.now(ist_tz)
                    seconds_to_next_hour = 60 - now_ist.second + (60 - now_ist.minute - 1) * 60
                    
                    if seconds_to_next_hour > 0 and seconds_to_next_hour < 12:
                        print(f"\n⏰ Waiting {seconds_to_next_hour}s for 00:00...")
                        time_module.sleep(seconds_to_next_hour)
                    
                    PLANT1_EXACT_REQUIREMENT_STATE.force_hour_reset_all_machines()
                    with PLANT1_EXACT_REQUIREMENT_STATE.lock:
                        PLANT1_EXACT_REQUIREMENT_STATE.hour_first_count_time.clear()
                    
                    saved_count = 0
                    error_count = 0
                    for machine_no in sorted(all_mapped_machines):
                        try:
                            data = machine_data_snapshot[machine_no]
                            save_machine_to_database(
                                machine_no,
                                data['timestamp'],
                                data['count'],
                                data['tool_id'],
                                data['shut_height'],
                                data['idle_time']
                            )
                            saved_count += 1
                        except Exception as e:
                            error_count += 1
                    
                    print("=" * 80)
                    print(f"✅ PLANT 1: SAVED {saved_count}, ERRORS: {error_count}")
                    print("=" * 80)
                    
                    with _messages_lock:
                        ACTIVE_MACHINES_THIS_HOUR.clear()
                        MACHINE_DATA_CACHE.clear()
                    
                    last_saved_hour = current_hour
                    time_module.sleep(5)
                
                else:
                    time_module.sleep(5)
                
            except Exception as e:
                print(f"❌ PLANT 1 ERROR: {e}")
                traceback.print_exc()
                time_module.sleep(30)
    
    thread = threading.Thread(target=save_worker, daemon=True, name="Plant1-Hourly")
    thread.start()

def save_machine_to_database(machine_no, timestamp, count, tool_id, shut_height, idle_time):
    try:
        ist_tz = pytz.timezone('Asia/Kolkata')
        
        if timestamp.tzinfo is None:
            timestamp = ist_tz.localize(timestamp)
        elif timestamp.tzinfo != ist_tz:
            timestamp = timestamp.astimezone(ist_tz)
        
        time_only = timestamp.time()
        shift_A_start = datetime.strptime("08:30", "%H:%M").time()
        shift_A_end = datetime.strptime("20:00", "%H:%M").time()
        shift = 'A' if shift_A_start <= time_only < shift_A_end else 'B'
        
        shift_start = PLANT1_EXACT_REQUIREMENT_STATE.get_shift_start_datetime(timestamp)
        
        last_cumulative = 0
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT cumulative_count FROM Plant1_data 
                    WHERE machine_no = %s AND shift = %s AND timestamp >= %s
                    ORDER BY timestamp DESC LIMIT 1
                """, [str(machine_no), shift, shift_start])
                result = cursor.fetchone()
                if result:
                    last_cumulative = result[0]
        except:
            pass
        
        new_cumulative = last_cumulative + count
        
        clean_tool_id = str(tool_id)[:50] if tool_id != 'NULL' else 'NULL'
        
        if isinstance(shut_height, str):
            clean_shut_height = 0.0
        else:
            clean_shut_height = float(shut_height) if isinstance(shut_height, (int, float)) else 0.0
        
        clean_idle_time = int(idle_time) if isinstance(idle_time, (int, float)) else 60
        naive_timestamp = timestamp.replace(tzinfo=None, microsecond=0)
        
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO Plant1_data 
                (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [naive_timestamp, clean_tool_id, str(machine_no), count, new_cumulative, 0, clean_idle_time, clean_shut_height, shift])
        
        print(f"💾 M{machine_no}: count={count}, cumul={new_cumulative}")
        
    except Exception as e:
        print(f"❌ DB M{machine_no}: {e}")

def on_connect(client, userdata, flags, rc):
    print(f"🔗 Plant 1 MQTT Connected (rc={rc})")
    if rc == 0:
        client.subscribe(PLANT1_TOPICS)
        print("✅ Plant 1 Subscribed!")

def on_message(client, userdata, msg):
    raw_payload = msg.payload.decode(errors="ignore")
    topic = msg.topic
    
    if topic.startswith('JJ'):
        json_parsed = parse_json_payload(raw_payload)
        if json_parsed and json_parsed['plant_no'] == 1 and json_parsed['machine_no']:
            machine_no = json_parsed['machine_no']
            card = json_parsed['card']
            die_height = json_parsed['die_height']
            
            PLANT1_EXACT_REQUIREMENT_STATE.update_json_status(machine_no, card=card, die_height=die_height)
        return
    
    count_parsed = parse_count_payload(raw_payload)
    if not count_parsed or count_parsed['plant_no'] != 1:
        return
    
    if count_parsed['machine_no']:
        machine_no = count_parsed['machine_no']
        tool_id = count_parsed['tool_id']
        shut_height = count_parsed['shut_height']
        
        MACHINE_STATE.upsert(1, machine_no, tool_id, 1, shut_height)
        PLANT1_EXACT_REQUIREMENT_STATE.add_count(machine_no, count_increment=1, tool_id=tool_id, shut_height=shut_height)
        HOURLY_IDLE_TRACKER.record_activity(machine_no)
        
        with _messages_lock:
            ACTIVE_MACHINES_THIS_HOUR.add(machine_no)
            MACHINE_DATA_CACHE[machine_no] = {
                'tool_id': tool_id,
                'shut_height': shut_height,
                'last_updated': datetime.now()
            }
    else:
        machines_for_topic = get_machines_for_topic(topic)
        if machines_for_topic:
            tool_id = count_parsed['tool_id']
            shut_height = count_parsed['shut_height']
            
            with _messages_lock:
                for machine_no in machines_for_topic:
                    MACHINE_STATE.upsert(1, machine_no, tool_id, 1, shut_height)
                    PLANT1_EXACT_REQUIREMENT_STATE.add_count(machine_no, count_increment=1, tool_id=tool_id, shut_height=shut_height)
                    HOURLY_IDLE_TRACKER.record_activity(machine_no)
                    
                    ACTIVE_MACHINES_THIS_HOUR.add(machine_no)
                    MACHINE_DATA_CACHE[machine_no] = {
                        'tool_id': tool_id,
                        'shut_height': shut_height,
                        'last_updated': datetime.now()
                    }

def start_plant1_mqtt():
    print("\n" + "=" * 70)
    print("🚀 PLANT 1 MQTT CLIENT")
    print("=" * 70 + "\n")
    
    save_all_machines_on_hour_boundary()
    
    client = mqtt.Client(client_id="plant1_mqtt_client", protocol=mqtt.MQTTv311)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    return client

if __name__ == "__main__":
    print("\n" + "🚀" * 40)
    print("🚀 PLANT 1 MQTT - STARTING")
    print("🚀" * 40 + "\n")
    
    client = start_plant1_mqtt()
    print("\n✅ MQTT client started!\n")
    time_module.sleep(2)
    
    print("=" * 60)
    print("🔄 Service running...")
    print("=" * 60 + "\n")
    
    try:
        while True:
            time_module.sleep(1)
    except KeyboardInterrupt:
        print("\n⛔ Stopping...")
        client.disconnect()
        print("✅ Stopped!\n")
