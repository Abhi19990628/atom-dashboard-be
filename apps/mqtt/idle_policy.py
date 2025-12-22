from datetime import datetime, timedelta
from threading import RLock
import pytz

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
    """
    Enforces EXACT requirement:
    - Grace 3: idle visible only when gap >= 180s; first visible = 3
    - base_time = max(on_since, last_count_time, hour_start)
    - live_idle == accumulated_idle for current segment
    - hourly_total = completed + live; resets on hour change
    - COUNT resets live/accumulated to 0
    - Cross-hour: re-base at hour_start, display starts at 3 again
    - No-signal hour: hourly_total=60 (if enabled)
    """

    def __init__(self, grace_seconds=180, enable_no_signal_as_idle=True):
        self.lock = RLock()
        self.grace_seconds = grace_seconds
        self.enable_no_signal_as_idle = enable_no_signal_as_idle

        # Per-machine state
        self.on_since = {}              # m -> datetime (IST)
        self.last_count_time = {}       # m -> datetime (IST)
        self.last_json_time = {}        # m -> datetime (IST)
        self.current_hour_start = {}    # m -> datetime (hour boundary)
        
        # Idle tracking per hour
        self.completed_segments_minutes = {}  # m -> int (saved completed idle in hour)
        
        # Data source tracking
        self.data_source = {}           # m -> "COUNT" | "JSON" | "NONE"
        
        # No-signal detection
        self.hour_had_activity = {}     # m -> bool (any JSON/COUNT in current hour)

    @staticmethod
    def _ist(dt: datetime) -> datetime:
        """Ensure datetime is in IST."""
        if dt is None:
            return None
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)

    @staticmethod
    def _hour_start(dt: datetime) -> datetime:
        """Get hour boundary (e.g., 11:23 -> 11:00)."""
        dt = StrictIdlePolicy._ist(dt)
        return dt.replace(minute=0, second=0, microsecond=0)

    def _ensure_current_hour(self, m: int, now: datetime):
        """Check if hour changed; if yes, reset hourly accumulators."""
        hour = self._hour_start(now)
        prev = self.current_hour_start.get(m)
        
        if prev is None or prev != hour:
            # New hour started
            self.current_hour_start[m] = hour
            self.completed_segments_minutes[m] = 0
            self.hour_had_activity[m] = False
            print(f"🔄 M{m}: New hour started at {hour.strftime('%H:%M')}")

    def mark_json(self, m: int, t: datetime):
        """Record JSON heartbeat (machine ON signal)."""
        with self.lock:
            now = self._ist(t)
            self.last_json_time[m] = now
            self.data_source[m] = DataSource.JSON
            
            # Set ON since if not present
            if m not in self.on_since:
                self.on_since[m] = now
                print(f"🟢 M{m}: Machine ON at {now.strftime('%H:%M:%S')} (JSON)")
            
            self._ensure_current_hour(m, now)
            self.hour_had_activity[m] = True
            
            # JSON does NOT end idle segment (only COUNT ends segment)

    def mark_count(self, m: int, t: datetime):
        """Record COUNT event (production activity)."""
        with self.lock:
            now = self._ist(t)
            prev_count = self.last_count_time.get(m)
            
            # Calculate idle before resetting
            if prev_count is not None:
                live, acc, total = self._compute_live_and_accumulated(m, now)
                
                # Save completed segment (if visible)
                if live > 0:
                    self.completed_segments_minutes[m] = self.completed_segments_minutes.get(m, 0) + live
                    print(f"💤 M{m}: Idle segment SAVED = {live}m | Total accumulated: {self.completed_segments_minutes[m]}m")
            
            # Update timestamps
            self.last_count_time[m] = now
            self.data_source[m] = DataSource.COUNT
            
            if m not in self.on_since:
                self.on_since[m] = now
                print(f"🟢 M{m}: Machine ON at {now.strftime('%H:%M:%S')} (COUNT)")
            
            self._ensure_current_hour(m, now)
            self.hour_had_activity[m] = True

    def mark_off(self, m: int):
        """Mark machine as OFF/unknown."""
        with self.lock:
            if m in self.on_since:
                del self.on_since[m]
            self.data_source[m] = DataSource.NONE

    def _compute_base_time(self, m: int, now: datetime) -> datetime:
        """
        Compute base_time = max(on_since, last_count_time, hour_start)
        This is the starting point for idle calculation.
        """
        hour_start = self.current_hour_start.get(m, self._hour_start(now))
        
        candidates = [hour_start]
        
        if m in self.on_since:
            candidates.append(self.on_since[m])
        
        if m in self.last_count_time:
            candidates.append(self.last_count_time[m])
        
        return max(candidates)

    def _compute_live_and_accumulated(self, m: int, now: datetime):
        """
        Compute live_idle and accumulated_idle for current segment.
        
        Rules:
        - gap = now - base_time
        - If gap < 180s: live=0, accumulated=0
        - If gap >= 180s: visible_minutes = floor(gap/60)
        - live_idle = accumulated_idle = visible_minutes
        
        Returns: (live_idle, accumulated_idle, hourly_total)
        """
        # Check if machine is ON
        if m not in self.on_since:
            # Machine OFF
            return (0, 0, 0)
        
        base_time = self._compute_base_time(m, now)
        gap_seconds = (now - base_time).total_seconds()
        
        # Grace period check
        if gap_seconds < self.grace_seconds:
            # Within grace period
            live_idle = 0
            accumulated_idle = 0
        else:
            # Beyond grace period
            visible_minutes = int(gap_seconds / 60)
            live_idle = visible_minutes
            accumulated_idle = visible_minutes  # Mirror for current segment
        
        # Hourly total = completed segments + current live
        completed = self.completed_segments_minutes.get(m, 0)
        hourly_total = completed + live_idle
        
        return (live_idle, accumulated_idle, hourly_total)

    def get_idle_status(self, m: int, now: datetime = None):
        """Get complete idle status for a machine."""
        with self.lock:
            if now is None:
                now = datetime.now(IST)
            now = self._ist(now)
        
            self._ensure_current_hour(m, now)  # ✅ पहले चेक करो
        
        current_hour_start = self.current_hour_start.get(m, self._hour_start(now))
        
        # ✅ BETTER NO-SIGNAL CHECK (CHANGE 1)
        if self.enable_no_signal_as_idle:
            is_never_active = m not in self.on_since and \
                            m not in self.last_count_time and \
                            m not in self.last_json_time
            
            # ✅ If NO activity in current hour = 60min idle (OFFLINE)
            if is_never_active:
                # Machine offline/never turned on = 60min
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
            
            # Normal computation
            live, acc, total = self._compute_live_and_accumulated(m, now)
            
            # Determine status and idle_type
            has_count = m in self.last_count_time
            has_json = m in self.last_json_time
            
            count_seconds_ago = None
            json_seconds_ago = None
            
            if has_count:
                count_seconds_ago = int((now - self.last_count_time[m]).total_seconds())
            
            if has_json:
                json_seconds_ago = int((now - self.last_json_time[m]).total_seconds())
            
            # Determine machine state
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
                # ON but not producing
                if live > 0:
                    status = "ON (No Count)"
                else:
                    status = "ON (Grace Period)"
                idle_type = IdleType.ON_BUT_NOT_PRODUCING if live > 0 else IdleType.NONE
            
            return {
                'live_idle_time': f'{live}m' if live > 0 else '0m',
                'accumulated_idle_time': f'{acc}m',
                'hourly_idle_total': min(60, total),  # Cap at 60
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
        """Reset hourly data at hour boundary."""
        with self.lock:
            if m is None:
                # Reset all machines
                self.completed_segments_minutes.clear()
                self.current_hour_start.clear()
                self.hour_had_activity.clear()
                print("🔄 All machines: Hourly idle reset")
            else:
                # Reset specific machine
                self.completed_segments_minutes[m] = 0
                self.hour_had_activity[m] = False
                if m in self.current_hour_start:
                    del self.current_hour_start[m]
                print(f"🔄 M{m}: Hourly idle reset")



