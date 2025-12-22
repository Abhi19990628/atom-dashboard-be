# apps/mqtt/idle_tracker.py

from threading import RLock
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

class HourlyIdleTracker:
    """
    ✅ MACHINE-WISE STORAGE WITH FULL DEBUG
    """
    
    def __init__(self, strict_idle_policy, topic_machine_mapping):
        self.lock = RLock()
        self.strict_policy = strict_idle_policy
        self.topic_machine_mapping = topic_machine_mapping
        
        self.machine_idle = {}
    
    def collect_all_idle(self, now: datetime):
        """✅ Collect idle for ALL machines with FULL DEBUG"""
        with self.lock:
            now = self._ist(now)
            
            all_mapped_machines = set()
            for machines_list in self.topic_machine_mapping.values():
                all_mapped_machines.update(machines_list)
            
            print("\n" + "=" * 80)
            print(f"📊 COLLECTING IDLE FOR ALL {len(all_mapped_machines)} MACHINES")
            print(f"   Time: {now.strftime('%H:%M:%S')}")
            print("=" * 80)
            
            print(f"\n{'M#':<3} {'Idle':<6} {'Status':<20} {'Type':<20}")
            print("-" * 80)
            
            for machine_no in sorted(all_mapped_machines):
                try:
                    idle_status = self.strict_policy.get_idle_status(machine_no, now)
                    idle_minutes = idle_status['hourly_idle_total']
                    
                    self.machine_idle[machine_no] = idle_minutes
                    
                    status = idle_status['status']
                    idle_type = idle_status['idle_type']
                    
                    if idle_minutes == 60:
                        marker = "🔴"
                    elif idle_minutes > 30:
                        marker = "🟡"
                    elif idle_minutes > 0:
                        marker = "🟠"
                    else:
                        marker = "🟢"     
                    
                    print(f"{marker} M{machine_no:<2d} {idle_minutes:<6d}min {status:<20s} {idle_type:<20s}")
                
                except Exception as e:
                    self.machine_idle[machine_no] = 0
                    print(f"❌ M{machine_no:<2d} ERROR: {str(e):<50s}")
            
            print("-" * 80)
            
            producing = sum(1 for v in self.machine_idle.values() if v == 0)
            idle_some = sum(1 for v in self.machine_idle.values() if 0 < v < 60)
            offline = sum(1 for v in self.machine_idle.values() if v == 60)
            
            print(f"\n📊 SUMMARY:")
            print(f"   🟢 Producing (0 idle):  {producing} machines")
            print(f"   🟠 Some idle (1-59):    {idle_some} machines")
            print(f"   🔴 Offline (60 idle):   {offline} machines")
            print(f"   ✅ Total stored:        {len(self.machine_idle)} machines")
            
            print(f"\n📋 IDLE DICTIONARY (machine_no: idle_minutes):")
            
            for m_no in sorted(self.machine_idle.keys()):
                idle_val = self.machine_idle[m_no]
                print(f"   {m_no:2d}: {idle_val:2d}min", end="  ")
                if (m_no % 5) == 0:
                    print()
            print("\n" + "=" * 80 + "\n")
            
            return self.machine_idle
    
    def get_idle(self, machine_no: int) -> int:
        """Get idle for specific machine"""
        with self.lock:
            return self.machine_idle.get(machine_no, 0)
    
    def get_all_idle(self) -> dict:
        """Get all idle data"""
        with self.lock:
            return self.machine_idle.copy()
    
    def reset(self):
        """Clear for new hour"""
        with self.lock:
            print("\n🔄 RESETTING MACHINE IDLE STORAGE")
            print(f"   Cleared {len(self.machine_idle)} entries")
            self.machine_idle.clear()
            print("   ✅ Ready for new hour!\n")
    
    @staticmethod
    def _ist(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)


# ✅ GLOBAL INSTANCE
HOURLY_IDLE_TRACKER = None

def init_tracker(strict_idle_policy, topic_machine_mapping):
    """✅ Initialize tracker with dependencies"""
    global HOURLY_IDLE_TRACKER
    HOURLY_IDLE_TRACKER = HourlyIdleTracker(strict_idle_policy, topic_machine_mapping)
    print("✅ HourlyIdleTracker initialized!")
    return HOURLY_IDLE_TRACKER
