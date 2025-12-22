# backend/hourly_idle_tracker.py - CORRECTED IDLE LOGIC
from datetime import datetime, timedelta
from threading import RLock
from collections import defaultdict
import pytz

class HourlyIdleTracker:
    def __init__(self, idle_threshold_minutes=3):
        self._lock = RLock()
        self.idle_threshold = idle_threshold_minutes
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self._last_activity = {}
        self._completed_hourly_idle = defaultdict(lambda: defaultdict(int))
        self._currently_idle = {}

    def _get_hour_key(self, dt):
        return dt.replace(minute=0, second=0, microsecond=0).strftime('%Y-%m-%d-%H')

    def _calculate_idle_periods_by_hour(self, idle_start, idle_end):
        hour_minutes = {}
        current_hour = idle_start.replace(minute=0, second=0, microsecond=0)
        
        while current_hour < idle_end:
            next_hour = current_hour + timedelta(hours=1)
            period_start = max(idle_start, current_hour)
            period_end = min(idle_end, next_hour)
            
            if period_start < period_end:
                idle_minutes = (period_end - period_start).total_seconds() / 60
                hour_key = self._get_hour_key(current_hour)
                hour_minutes[hour_key] = int(idle_minutes)
            
            current_hour = next_hour
        return hour_minutes

    def record_activity(self, machine_no):
        with self._lock:
            now_ist = datetime.now(self.ist_tz)
            
            if machine_no in self._last_activity:
                last_activity = self._last_activity[machine_no]
                time_since_last = now_ist - last_activity
                
                if time_since_last.total_seconds() >= (self.idle_threshold * 60):
                    idle_start = last_activity  # Include 3-min threshold
                    idle_end = now_ist
                    
                    hour_minutes = self._calculate_idle_periods_by_hour(idle_start, idle_end)
                    
                    for hour_key, minutes in hour_minutes.items():
                        self._completed_hourly_idle[machine_no][hour_key] += minutes
                    
                    self._currently_idle[machine_no] = False
                    print(f"🟢 Machine {machine_no}: ACTIVE - Was idle {time_since_last.total_seconds()/60:.1f} min")
            
            self._last_activity[machine_no] = now_ist

    def get_live_idle_status(self, machine_no):
        with self._lock:
            now_ist = datetime.now(self.ist_tz)
            
            if machine_no not in self._last_activity:
                return False, "0:00"
            
            last_activity = self._last_activity[machine_no]
            time_since_last = now_ist - last_activity
            
            if time_since_last.total_seconds() >= (self.idle_threshold * 60):
                self._currently_idle[machine_no] = True
                total_seconds = int(time_since_last.total_seconds())
                minutes = total_seconds // 60
                seconds = total_seconds % 60
                return True, f"{minutes}:{seconds:02d}"
            else:
                self._currently_idle[machine_no] = False
                return False, "0:00"

    def get_current_hour_idle(self, machine_no):
        with self._lock:
            now_ist = datetime.now(self.ist_tz)
            current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
            hour_key = self._get_hour_key(current_hour)
            
            completed_idle = self._completed_hourly_idle[machine_no][hour_key]
            
            ongoing_idle = 0
            if machine_no in self._last_activity and self._currently_idle.get(machine_no, False):
                last_activity = self._last_activity[machine_no]
                time_since_last = now_ist - last_activity
                
                if time_since_last.total_seconds() >= (self.idle_threshold * 60):
                    idle_start_in_hour = max(last_activity, current_hour)
                    if idle_start_in_hour < now_ist:
                        ongoing_idle = (now_ist - idle_start_in_hour).total_seconds() / 60
            
            return completed_idle + int(ongoing_idle)

    def get_all_machine_status(self):
        with self._lock:
            status = {}
            for machine_no in self._last_activity.keys():
                is_idle, live_idle = self.get_live_idle_status(machine_no)
                current_hour_idle = self.get_current_hour_idle(machine_no)
                
                status[machine_no] = {
                    'is_idle': is_idle,
                    'live_idle_time': live_idle,
                    'hourly_idle_total': current_hour_idle,
                    'last_activity': self._last_activity[machine_no].strftime('%H:%M:%S')
                }
            return status

HOURLY_IDLE_TRACKER = HourlyIdleTracker(idle_threshold_minutes=3)
