# backend/machine_state.py - Count increment per machine
from threading import RLock
from datetime import datetime

class MachineState:
    def __init__(self):
        self._lock = RLock()
        self._data = {}  # Key: (plant, machine)

    def upsert(self, plant_no, machine_no, tool_id, count_inc, shut_height):
        with self._lock:
            key = (int(plant_no), int(machine_no))
            prev = self._data.get(key)
            
            # Count increment - same machine का count add करते जाएंगे
            new_count = (prev["count"] + count_inc) if prev else count_inc
            
            self._data[key] = {
                "plant": int(plant_no),
                "machine_no": int(machine_no),
                "tool_id": tool_id or "Unknown",  # Latest tool ID
                "count": new_count,  # Incremental count
                "shut_height": shut_height,  # Latest shut height
                "last_seen": datetime.utcnow(),
            }

    def summarize(self, plant_filter=None, stale_after_seconds=120):
        now = datetime.utcnow()
        out = []
        
        with self._lock:
            for (pl, m), rec in self._data.items():
                if plant_filter and pl != plant_filter:
                    continue
                    
                stale = (now - rec["last_seen"]).total_seconds() > stale_after_seconds
                out.append({
                    "plant": pl,
                    "machine_no": m,
                    "tool_id": rec["tool_id"],
                    "count": rec["count"],  # Total incremented count
                    "shut_height": rec["shut_height"],
                    "last_seen": rec["last_seen"].isoformat(timespec='milliseconds') + "Z",
                    "status": "No Data" if stale else "Running",
                })
        
        out.sort(key=lambda r: r["machine_no"])
        return out

MACHINE_STATE = MachineState()
