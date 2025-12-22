# backend/enhanced_hourly_state.py - NEW FILE
from threading import RLock
from datetime import datetime, time
from collections import defaultdict
import pytz

class EnhancedHourlyMachineState:
    def __init__(self):
        self._lock = RLock()
        # Hourly data: {machine_no: {current_hour: count}} - RESETS every hour
        self._hourly_data = defaultdict(lambda: defaultdict(int))
        # Shift data: {(machine_no, shift, date): cumulative_count} - CUMULATIVE
        self._shift_data = defaultdict(int)
        self._last_hour = {}  # Track last hour per machine for reset

    def get_shift_from_time(self, dt):
        """Get shift A (8:30 AM - 8:00 PM) or B (8:00 PM - 8:30 AM)"""
        ist_dt = dt.astimezone(pytz.timezone('Asia/Kolkata')) if dt.tzinfo else pytz.timezone('Asia/Kolkata').localize(dt)
        time_only = ist_dt.time()
        
        shift_A_start = time(8, 30)  # 8:30 AM
        shift_A_end = time(20, 0)    # 8:00 PM
        
        if shift_A_start <= time_only < shift_A_end:
            return 'A'
        else:
            return 'B'

    def add_count(self, machine_no, count_increment=1):
        """Add count for machine - hourly resets, shift cumulative"""
        with self._lock:
            # Get IST time
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)
            
            # Current hour (for reset logic)
            current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
            current_hour_key = current_hour.strftime('%Y-%m-%d-%H')
            
            # Check if hour changed - RESET hourly count
            last_hour = self._last_hour.get(machine_no)
            if last_hour and last_hour != current_hour:
                # Hour changed - reset this machine's hourly count
                self._hourly_data[machine_no].clear()
                print(f"🔄 Machine {machine_no}: Hour reset at {current_hour_key}")
            
            # Add to CURRENT hour count (resets every hour)
            self._hourly_data[machine_no][current_hour_key] += count_increment
            self._last_hour[machine_no] = current_hour
            
            # Add to CUMULATIVE shift count (never resets unless shift changes)
            shift = self.get_shift_from_time(now_ist)
            date_key = now_ist.strftime('%Y-%m-%d')
            shift_key = (machine_no, shift, date_key)
            
            self._shift_data[shift_key] += count_increment
            
            print(f"📊 Machine {machine_no}: Hourly +{count_increment} (Total: {self._hourly_data[machine_no][current_hour_key]}), Shift {shift} +{count_increment} (Total: {self._shift_data[shift_key]})")

    def get_current_hour_count(self, machine_no):
        """Get current hour count for machine"""
        with self._lock:
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)
            current_hour_key = now_ist.replace(minute=0, second=0, microsecond=0).strftime('%Y-%m-%d-%H')
            return self._hourly_data[machine_no].get(current_hour_key, 0)

    def get_shift_count(self, machine_no, shift=None, date=None):
        """Get cumulative shift count"""
        with self._lock:
            if not shift:
                ist_tz = pytz.timezone('Asia/Kolkata')
                shift = self.get_shift_from_time(datetime.now(ist_tz))
            
            if not date:
                date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')
            
            shift_key = (machine_no, shift, date)
            return self._shift_data.get(shift_key, 0)

    def get_all_data_for_machine(self, machine_no):
        """Get complete data for a machine"""
        with self._lock:
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)
            current_shift = self.get_shift_from_time(now_ist)
            today = now_ist.strftime('%Y-%m-%d')
            
            return {
                'machine_no': machine_no,
                'current_hour_count': self.get_current_hour_count(machine_no),
                'shift_A_count': self.get_shift_count(machine_no, 'A', today),
                'shift_B_count': self.get_shift_count(machine_no, 'B', today),
                'current_shift': current_shift,
                'current_shift_count': self.get_shift_count(machine_no, current_shift, today)
            }

# Global instance
ENHANCED_HOURLY_STATE = EnhancedHourlyMachineState()
