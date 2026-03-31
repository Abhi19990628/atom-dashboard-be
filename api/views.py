from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.db import transaction
from django.db import connection
from .models import OperatorAssignment, IdleReport
from datetime import datetime, timedelta
from apps.mqtt.mqtt_client import PLANT1_TOPICS, PLANT2_TOPICS
from django.views.decorators.cache import cache_control, never_cache
from apps.machines.machine_map import COUNT52_GROUP
from django.views.decorators.cache import never_cache
from apps.machines.machine_state import MACHINE_STATE
from .models import Plant2HourlyIdletime 
from apps.mqtt.simple_plant2 import EXACT_REQUIREMENT_STATE
from apps.data_storage.hourly_idle_tracker import HOURLY_IDLE_TRACKER
from rest_framework.views import APIView
from rest_framework import status
from .serializers import InspectionReportSerializer
from .models import InspectionReport
from django.views.decorators.csrf import csrf_exempt  # ← ADD THIS LINE
from django.views.decorators.http import require_http_methods
from .models import MachineChecksheetReport, MachineChecksheetObservation
import json
import pytz

@api_view(['GET'])
def get_dashboard_data(request):
    """Get dashboard data with filters: date, shift, plant selection"""
    try:
        # Get query parameters
        selected_date = request.GET.get('date', None)
        selected_shift = request.GET.get('shift', None)
        selected_plant = request.GET.get('plant', 'plant1_data')  # Default plant1_data
        
        # Default to today if no date provided
        if not selected_date:
            selected_date = datetime.now().strftime('%Y-%m-%d')
        
        with connection.cursor() as cursor:
            # Build dynamic query based on plant selection
            plant_table = selected_plant  # plant1_data, plant2_data, or plc_data
            
            base_query = f"""
                SELECT DISTINCT machine_no
                FROM {plant_table}
                WHERE DATE(timestamp) = %s
            """
            
            params = [selected_date]
            
            # Add shift filter if provided
            if selected_shift:
                base_query += " AND shift = %s"
                params.append(selected_shift)
                
            base_query += " ORDER BY machine_no"
            
            cursor.execute(base_query, params)
            active_machines = [row[0] for row in cursor.fetchall()]
            
            # Get total distinct machine count
            count_query = f"""
                SELECT COUNT(DISTINCT machine_no) as total_machines
                FROM {plant_table}
                WHERE DATE(timestamp) = %s
            """
            count_params = [selected_date]
            
            if selected_shift:
                count_query += " AND shift = %s"
                count_params.append(selected_shift)
                
            cursor.execute(count_query, count_params)
            total_machines = cursor.fetchone()[0] or 0
            running_machines = len(active_machines)
            
            # Get machine details for each active machine
            machine_details = []
            total_production = 0
            efficiency_sum = 0
            
            for machine_no in active_machines:
                detail_query = f"""
                    SELECT 
                        machine_no,
                        MAX(cumulative_count) as production,
                        AVG(CASE WHEN idle_time = 0 THEN 100 ELSE 0 END) as efficiency,
                        MAX(timestamp) as last_update,
                        shift
                    FROM {plant_table}
                    WHERE machine_no = %s 
                    AND DATE(timestamp) = %s
                """
                detail_params = [machine_no, selected_date]
                
                if selected_shift:
                    detail_query += " AND shift = %s"
                    detail_params.append(selected_shift)
                    
                detail_query += " GROUP BY machine_no, shift ORDER BY last_update DESC LIMIT 1"
                
                cursor.execute(detail_query, detail_params)
                result = cursor.fetchone()
                
                if result:
                    machine_no, production, efficiency, last_update, shift = result
                    
                    # Determine status based on idle_time
                    status_query = f"""
                        SELECT 
                            CASE 
                                WHEN AVG(idle_time) = 0 THEN 'Running'
                                WHEN AVG(idle_time) > 0 THEN 'Idle'
                                ELSE 'Maintenance'
                            END as status
                        FROM {plant_table}
                        WHERE machine_no = %s 
                        AND DATE(timestamp) = %s
                    """
                    status_params = [machine_no, selected_date]
                    
                    if selected_shift:
                        status_query += " AND shift = %s"
                        status_params.append(selected_shift)
                        
                    cursor.execute(status_query, status_params)
                    status_result = cursor.fetchone()
                    status = status_result[0] if status_result else 'Unknown'
                    
                    machine_details.append({
                        'id': machine_no,
                        'name': f"Machine {machine_no}",
                        'status': status,
                        'efficiency': round(efficiency or 0),
                        'production': production or 0,
                        'last_update': str(last_update)[:19] if last_update else 'N/A',
                        'shift': shift or 'A'
                    })
                    
                    total_production += production or 0
                    efficiency_sum += efficiency or 0
            
            # Calculate average efficiency
            avg_efficiency = round(efficiency_sum / len(active_machines)) if active_machines else 0
            
            return Response({
                'success': True,
                'dashboard_data': {
                    'total_machines': total_machines,
                    'running_machines': running_machines,
                    'avg_efficiency': avg_efficiency,
                    'total_production': total_production,
                    'active_machines': active_machines,
                    'machine_details': machine_details,
                    'selected_date': selected_date,
                    'selected_shift': selected_shift or 'All',
                    'selected_plant': selected_plant,
                    'last_updated': f'Data for {selected_date}'
                }
            })
            
    except Exception as e:
        return Response({
            'success': False,
            'message': f'Error fetching dashboard data: {str(e)}'
        }, status=400)

@api_view(['GET'])
def get_available_dates(request):
    """Get available dates from selected plant table"""
    try:
        selected_plant = request.GET.get('plant', 'plant1_data')
        
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT DISTINCT DATE(timestamp) as available_date
                FROM {selected_plant}
                WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY available_date DESC
                LIMIT 30
            """)
            
            dates = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
            
            return Response({
                'success': True,
                'available_dates': dates
            })
            
    except Exception as e:
        return Response({
            'success': False,
            'message': f'Error fetching dates: {str(e)}'
        }, status=400)

@api_view(['POST'])
def create_assignment(request):
    """AssignMachine.js se data receive karne ke liye"""
    try:
        data = request.data
        
        assignment = OperatorAssignment.objects.create(
            machine_no=data['machine_no'],
            operator_name=data['operator_name'], 
            shift=data['shift'],
            start_time=data['start_time']
        )
        
        return Response({
            'success': True,
            'message': 'Assignment saved successfully!',
            'assignment_id': assignment.id
        })
    except Exception as e:
        return Response({
            'success': False,
            'message': f'Error: {str(e)}'
        }, status=400)

@api_view(['GET'])
def get_auto_fill_data(request, machine_no):
    """IdleCase.js ke liye auto-fill data"""
    try:
        # Operator name from operator_assignments
        try:
            latest_assignment = OperatorAssignment.objects.filter(
                machine_no=machine_no
            ).latest('created_at')
            operator_name = latest_assignment.operator_name
        except OperatorAssignment.DoesNotExist:
            operator_name = "Auto Operator"
        
        # Tool ID from plant1_data (you can make this dynamic too)
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT tool_id 
                FROM plant1_data 
                WHERE machine_no = %s 
                ORDER BY timestamp DESC 
                LIMIT 1
            """, [machine_no])
            
            result = cursor.fetchone()
            tool_id = result[0] if result else "Unknown Tool"
        
        return Response({
            'success': True,
            'machine_no': machine_no,
            'operator_name': operator_name,
            'tool_id': tool_id,
        })
    except Exception as e:
        return Response({
            'success': False,
            'message': f'Error: {str(e)}'
        }, status=400)

@api_view(['POST']) 
def create_idle_report(request):
    """IdleCase.js se data receive karne ke liye"""
    try:
        data = request.data
        
        report = IdleReport.objects.create(
            machine_no=data['machine_no'],
            operator_name=data['operator_name'],
            tool_id=data['tool_name'],
            reason=data['reason']
        )
        
        return Response({
            'success': True,
            'message': 'Idle report saved successfully!',
            'report_id': report.id
        })
    except Exception as e:
        return Response({
            'success': False,
            'message': f'Error: {str(e)}'
        }, status=400)

@api_view(['GET'])
def get_assignment_idle_data(request):
    """Get both assignment and idle report data for dashboard display"""
    try:
        # Get recent operator assignments
        assignments = OperatorAssignment.objects.all().order_by('-created_at')[:10]
        assignment_data = []
        for assignment in assignments:
            assignment_data.append({
                'id': assignment.id,
                'machine_no': assignment.machine_no,
                'operator_name': assignment.operator_name,
                'shift': assignment.shift,
                'start_time': assignment.start_time.strftime('%Y-%m-%d %H:%M') if assignment.start_time else 'N/A',
                'created_at': assignment.created_at.strftime('%Y-%m-%d %H:%M')
            })
        
        # Get recent idle reports
        idle_reports = IdleReport.objects.all().order_by('-created_at')[:10]
        idle_data = []
        for report in idle_reports:
            idle_data.append({
                'id': report.id,
                'machine_no': report.machine_no,
                'operator_name': report.operator_name,
                'tool_id': report.tool_id[:20] + '...' if len(report.tool_id) > 20 else report.tool_id,
                'reason': report.get_reason_display() if hasattr(report, 'get_reason_display') else report.reason,
                'created_at': report.created_at.strftime('%Y-%m-%d %H:%M')
            })
        
        return Response({
            'success': True,
            'assignments': assignment_data,
            'idle_reports': idle_data
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'message': f'Error fetching table data: {str(e)}'
        }, status=400)

try:
    from backend.apps.mqtt.simple_plant2 import EXACT_REQUIREMENT_STATE
except ImportError:
    EXACT_REQUIREMENT_STATE = None

@never_cache
@api_view(['GET'])
def exact_plant2_data(request):
    """Get exact Plant 2 data as per user requirement"""
    try:
        # Get live machine data
        live_machines = MACHINE_STATE.summarize(plant_filter=2, stale_after_seconds=300)
        
        if EXACT_REQUIREMENT_STATE is None:
            # Fallback: return basic machine data
            return Response({
                'success': True,
                'total_machines': len(live_machines),
                'machines': [{
                    **machine,
                    'current_hour_count': 0,
                    'last_hour_count': 0,
                    'cumulative_count': 0,
                    'shift': 'A'
                } for machine in live_machines]
            })
        
        # Enhance with exact requirement data
        enhanced_machines = []
        for machine in live_machines:
            machine_no = machine['machine_no']
            
            # Get exact data
            exact_data = EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)
            
            # Merge live data with exact data
            combined = {
                **machine,  # Live data
                **exact_data  # Exact requirement data
            }
            enhanced_machines.append(combined)
        
        return Response({
            'success': True,
            'total_machines': len(enhanced_machines),
            'machines': enhanced_machines
        })
    except Exception as e:
        print(f"Error in exact_plant2_data: {e}")
        return Response({
            'success': False,
            'error': str(e),
            'total_machines': 0,
            'machines': []
        })

@never_cache
@cache_control(no_store=True, no_cache=True, must_revalidate=True, max_age=0)
@api_view(['GET'])
def live_machines(request):
    """
    GET /api/live-machines?plant=2&stale_after=120
    Returns plant-wise live machines with status.
    """
    plant_str = request.GET.get("plant", "2")
    try:
        plant_no = int(plant_str)
    except Exception:
        plant_no = 2

    try:
        stale = int(request.GET.get("stale_after", "120"))
    except Exception:
        stale = 120

    # Pull live records for this plant
    data = MACHINE_STATE.summarize(plant_filter=plant_no, stale_after_seconds=stale)
    # Sort by machine number for stable UI
    data.sort(key=lambda r: r["machine_no"])

    resp = Response({"success": True, "plant": plant_no, "machines": data})
    resp['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    return resp

@never_cache 
@cache_control(no_store=True, no_cache=True, must_revalidate=True, max_age=0)
@api_view(['GET'])
def count52_live(request):
    plant_no = COUNT52_GROUP["plant"]
    data = MACHINE_STATE.summarize(plant_filter=plant_no, stale_after_seconds=999999)
    allowed = set(COUNT52_GROUP["machines"]) 
    out = [r for r in data if r["machine_no"] in allowed]
    out.sort(key=lambda r: r["machine_no"])
    resp = Response({"success": True, "topic": "COUNT52", "plant": plant_no, "machines": out})
    resp["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp

@never_cache
@api_view(['GET'])
def plant2_raw(request):
    """Return raw Plant 2 messages"""
    try:
        from backend.apps.mqtt.simple_plant2 import get_messages
        messages = get_messages()
        
        return Response({
            'success': True,
            'total_messages': len(messages),
            'raw_messages': messages
        })
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e),
            'total_messages': 0,
            'raw_messages': []
        })

# @api_view(['GET'])
# @cache_control(no_cache=True, must_revalidate=True, no_store=True)
# def plant1_live(request):
#     """Plant 1 Live Data - ALL 57 MACHINES (SEPARATE STATE)"""
#     try:
#         from apps.machines.machine_state import MACHINE_STATE
#         from apps.data_storage.hourly_idle_tracker import HOURLY_IDLE_TRACKER
#         # ✅ Import Plant 1's own state system
#         from apps.mqtt.simple_plant1 import PLANT1_EXACT_REQUIREMENT_STATE
        
#         print("🔍 DEBUG: Starting Plant 1 API for ALL 57 machines (SEPARATE STATE)...")
        
#         # Get live machine data for Plant 1 only
#         live_machines = MACHINE_STATE.summarize(plant_filter=1, stale_after_seconds=300)
#         print(f"🔍 Plant 1: Found {len(live_machines)} live machines from MACHINE_STATE")
        
#         # Get idle status for all machines
#         idle_status = HOURLY_IDLE_TRACKER.get_all_machine_status()
        
#         # Create complete machine list (1-57)
#         enhanced_machines = []
        
#         # Process all 57 machines
#         for machine_no in range(1, 58):
#             machine_data = None
            
#             # Find existing machine data
#             for m in live_machines:
#                 if m['machine_no'] == machine_no and m.get('plant') == 1:  # ✅ Ensure Plant 1
#                     machine_data = m
#                     break
            
#             if machine_data:
#                 # Machine has live data
#                 try:
#                     # ✅ Use Plant 1's own state system
#                     exact_data = PLANT1_EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)
#                     machine_data.update(exact_data)
#                     print(f"🟢 Plant 1 M{machine_no:02d}: Hourly={exact_data.get('current_hour_count', 0)}")
#                 except Exception as e:
#                     print(f"⚠️ Plant 1 M{machine_no:02d} hourly data error: {e}")
#                     machine_data.update({
#                         'current_hour_count': 0,
#                         'last_hour_count': 0,
#                         'cumulative_count': 0,
#                         'shift': 'A'
#                     })
                
#                 # Add idle data
#                 if machine_no in idle_status:
#                     machine_data.update(idle_status[machine_no])
#                 else:
#                     machine_data.update({
#                         'is_idle': False,
#                         'live_idle_time': '0:00',
#                         'hourly_idle_total': 0,
#                         'last_activity': 'Unknown'
#                     })
                
#                 machine_data['plant'] = 1
#                 enhanced_machines.append(machine_data)
                
#             else:
#                 # Machine has NO live data - create idle/offline structure
#                 machine_info = {
#                     'plant': 1,
#                     'machine_no': machine_no,
#                     'tool_id': f'PLANT1_M{machine_no:02d}',
#                     'count': 0,
#                     'shut_height': 'Waiting for data',
#                     'last_seen': 'Not active',
#                     'status': 'Ready',
#                     'current_hour_count': 0,
#                     'last_hour_count': 0,
#                     'cumulative_count': 0,
#                     'shift': 'A',
#                     'is_idle': True,
#                     'live_idle_time': '0:00',
#                     'hourly_idle_total': 0,
#                     'last_activity': 'Never'
#                 }
#                 enhanced_machines.append(machine_info)
        
#         # Sort by machine number
#         enhanced_machines.sort(key=lambda x: x['machine_no'])
        
#         # Count active machines
#         active_machines = [m for m in enhanced_machines if m.get('current_hour_count', 0) > 0]
        
#         print(f"✅ Plant 1: {len(active_machines)} active / {len(enhanced_machines)} total machines (SEPARATE STATE)")
        
#         return Response({
#             'success': True,
#             'machines': enhanced_machines,
#             'count': len(enhanced_machines), 
#             'active_count': len(active_machines),
#             'total_machines': len(enhanced_machines),
#             'plant': 1,
#             'message': f'Plant 1 live data - ALL {len(enhanced_machines)} machines (SEPARATE STATE)'
#         })
        
#     except Exception as e:
#         import traceback
#         error_trace = traceback.format_exc()
#         print(f"❌ Plant 1 API Error: {e}")
#         print(f"❌ Full trace: {error_trace}")
        
#         return Response({
#             'success': False,
#             'error': str(e),
#             'machines': [],
#             'plant': 1
#         }, status=500)


from django.views.decorators.cache import never_cache, cache_control
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Operator, OperatorAssignment
from django.utils import timezone
from datetime import datetime
import pytz


def get_tool_info_from_tid_map(tool_id):
    """Get tool information from TID mapping - Plant 1 version"""
    try:
        from apps.tool_mapping.tid_map import TID_MAP
        
        if tool_id and tool_id in TID_MAP:
            return TID_MAP[tool_id]
        
        return {
            'customer': 'N/A',
            'model': 'N/A',
            'part_name': 'N/A',
            'tool_name': 'N/A',
            'part_number': 'N/A',
            'tpm': 0,
            'epc': 'N/A'
        }
    except Exception as e:
        print(f"⚠️ TID_MAP lookup error: {e}")
        return {
            'customer': 'N/A',
            'model': 'N/A',
            'part_name': 'N/A',
            'tool_name': 'N/A',
            'part_number': 'N/A',
            'tpm': 0,
            'epc': 'N/A'
        }

# @never_cache
# @api_view(['GET'])
# def plant1_live(request):
#     """Plant 1 - LIVE DASHBOARD (Fixed to match Plant 2)"""
#     try:
#         from apps.machines.machine_state import MACHINE_STATE
#         from apps.mqtt.simple_plant1 import (
#             PLANT1_EXACT_REQUIREMENT_STATE,
#             J_TOPIC_MACHINE_MAPPING,
#             COUNT_TOPIC_MACHINE_MAPPING
#         )
        
#         # All machines 1-57
#         all_mapped_machines = list(range(1, 58))
#         live_machines = MACHINE_STATE.summarize(plant_filter=1, stale_after_seconds=300)
        
#         enhanced_machines = []
#         problem_machines = []
        
#         ist_tz = pytz.timezone('Asia/Kolkata')
#         now_ist = datetime.now(ist_tz)
        
#         for machine_no in all_mapped_machines:
#             machine_data = None
            
#             for m in live_machines:
#                 if m['machine_no'] == machine_no and m.get('plant') == 1:
#                     machine_data = m
#                     break
            
#             try:
#                 # ✅ FIX: Use idle_tracker like Plant 2
#                 idle_status = PLANT1_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
#                 exact_data = PLANT1_EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)
                
#                 # ✅ FIX: Check both count and JSON signals
#                 is_on = idle_status['on_since'] is not None
#                 is_producing = idle_status['count_seconds_ago'] is not None and idle_status['count_seconds_ago'] <= 180
                
#                 # Create data for JSON-only machines
#                 if is_on and not machine_data:
#                     tool_id = exact_data.get('current_tool_id', 'N/A')
#                     shut_height = exact_data.get('current_shut_height', 0.0)
                    
#                     machine_data = {
#                         'plant': 1,
#                         'machine_no': machine_no,
#                         'tool_id': tool_id,
#                         'count': 0,
#                         'shut_height': shut_height,
#                         'last_seen': 'JSON only',
#                         'status': idle_status['status'],
#                         'current_hour_count': 0,
#                         'last_hour_count': 0,
#                         'cumulative_count': 0,
#                         'shift': exact_data.get('shift', 'A'),
#                         'idle_time': idle_status['hourly_idle_total']
#                     }
                
#                 if machine_data:
#                     machine_data.update(exact_data)
                    
#                     problem_detected = is_on and not is_producing and idle_status['is_idle']
#                     machine_data['problem_detected'] = problem_detected
                    
#                     if problem_detected:
#                         problem_machines.append(machine_no)
                    
#                     current_shift = PLANT1_EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)
                    
#                     # Last activity
#                     if idle_status['last_count_time']:
#                         machine_data['last_activity'] = idle_status['last_count_time'].strftime('%H:%M:%S')
#                     else:
#                         machine_data['last_activity'] = 'Never'
                    
#                     # ✅ FIX: Shift key without date
#                     last_hour_count = PLANT1_EXACT_REQUIREMENT_STATE.last_hour_counts.get(machine_no, 0)
#                     machine_data['last_hour_count'] = last_hour_count
                    
#                     shift_key = (machine_no, current_shift)  # No date key
#                     shift_cumulative = PLANT1_EXACT_REQUIREMENT_STATE.shift_cumulative.get(shift_key, 0)
#                     current_hour_count = exact_data.get('current_hour_count', 0)
#                     total_shift_count = shift_cumulative + current_hour_count
#                     machine_data['cumulative_count'] = total_shift_count
                    
#                     # Add all fields
#                     machine_data['shut_height'] = exact_data.get('current_shut_height', 0.0)
#                     machine_data['first_count_at'] = exact_data.get('first_count_at')
#                     machine_data['time_to_first_count'] = exact_data.get('time_to_first_count')
                    
#                     # ✅ FIX: Use idle_tracker data
#                     machine_data.update({
#                         'live_idle_time': idle_status['live_idle_time'],
#                         'accumulated_idle_time': idle_status['accumulated_idle_time'],
#                         'hourly_idle_total': idle_status['hourly_idle_total'],
#                         'idle_time': idle_status['hourly_idle_total'],
#                         'is_idle': idle_status['is_idle'],
#                         'idle_type': idle_status['idle_type'],
#                         'status': idle_status['status'],
#                         'data_source': idle_status['data_source'],
#                         'on_since': idle_status['on_since'].strftime('%H:%M:%S') if idle_status['on_since'] else None,
#                         'count_seconds_ago': idle_status['count_seconds_ago'],
#                         'json_seconds_ago': idle_status['json_seconds_ago'],
#                         'machine_on': is_on,
#                         'is_producing': is_producing
#                     })
                    
#             except Exception as e:
#                 print(f"⚠️ Plant 1 M{machine_no} error: {e}")
#                 import traceback
#                 traceback.print_exc()
            
#             if machine_data:
#                 tool_id = machine_data.get('tool_id', '')
#                 tool_info = get_tool_info_from_tid_map(tool_id)
                
#                 machine_data.update({
#                     'tool_customer': tool_info.get('customer', 'N/A'),
#                     'tool_model': tool_info.get('model', 'N/A'),
#                     'tool_part_name': tool_info.get('part_name', 'N/A'),
#                     'tool_name': tool_info.get('tool_name', 'N/A'),
#                     'tool_part_number': tool_info.get('part_number', 'N/A'),
#                     'tool_tpm': tool_info.get('tpm', 0),
#                     'tool_epc': tool_info.get('epc', 'N/A')
#                 })
                
#                 machine_data['plant'] = 1
#                 enhanced_machines.append(machine_data)
#             else:
#                 # Offline machine
#                 idle_status = PLANT1_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
                
#                 enhanced_machines.append({
#                     "plant": 1,
#                     "machine_no": machine_no,
#                     "tool_id": f"PLANT1_M{machine_no:02d}",
#                     "count": 0,
#                     "shut_height": 0.0,
#                     "first_count_at": None,
#                     "time_to_first_count": None,
#                     "last_seen": "Not active",
#                     "status": idle_status['status'],
#                     "current_hour_count": 0,
#                     "last_hour_count": 0,
#                     "cumulative_count": 0,
#                     "shift": "A",
#                     "idle_time": idle_status['hourly_idle_total'],
#                     'is_idle': idle_status['is_idle'],
#                     'idle_type': idle_status['idle_type'],
#                     'live_idle_time': idle_status['live_idle_time'],
#                     'accumulated_idle_time': idle_status['accumulated_idle_time'],
#                     'hourly_idle_total': idle_status['hourly_idle_total'],
#                     "last_activity": "Never",
#                     'tool_customer': 'N/A',
#                     'tool_model': 'N/A',
#                     'tool_part_name': 'N/A',
#                     'tool_name': 'N/A',
#                     'tool_part_number': 'N/A',
#                     'tool_tpm': 0,
#                     'tool_epc': 'N/A',
#                     'machine_on': False,
#                     'is_producing': False,
#                     'problem_detected': False,
#                     'on_since': None,
#                     'data_source': idle_status['data_source']
#                 })
        
#         enhanced_machines.sort(key=lambda x: x['machine_no'])
        
#         on_machines = [m for m in enhanced_machines if m.get('machine_on')]
#         producing_machines = [m for m in enhanced_machines if m.get('is_producing')]
        
#         response = Response({
#             "success": True,
#             "total_machines": len(enhanced_machines),
#             "on_count": len(on_machines),
#             "producing_count": len(producing_machines),
#             "problem_count": len(problem_machines),
#             "problem_machines": problem_machines,
#             "machines": enhanced_machines,
#             "plant": 1,
#             "message": f"Plant 1 - ON:{len(on_machines)} | Producing:{len(producing_machines)} | Problems:{len(problem_machines)}"
#         })
        
#         response['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
#         response['Pragma'] = 'no-cache'
#         response['Expires'] = '0'
        
#         return response
        
#     except Exception as e:
#         print(f"❌ Plant 1 API ERROR: {e}")
#         import traceback
#         traceback.print_exc()
        
#         return Response({
#             "success": False,
#             "error": str(e),
#             "machines": [],
#             "plant": 1
#         }, status=500)

@never_cache
@api_view(['GET'])
def plant1_live(request):
    """Plant 1 - LIVE DASHBOARD (Fixed to match Plant 2)"""
    try:
        from apps.machines.machine_state import MACHINE_STATE
        from apps.mqtt.simple_plant1 import (
            PLANT1_EXACT_REQUIREMENT_STATE,
            J_TOPIC_MACHINE_MAPPING,
            COUNT_TOPIC_MACHINE_MAPPING
        )
        
        all_mapped_machines = list(range(1, 58))
        live_machines = MACHINE_STATE.summarize(plant_filter=1, stale_after_seconds=300)
        
        enhanced_machines = []
        problem_machines = []
        
        ist_tz = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist_tz)
        
        for machine_no in all_mapped_machines:
            machine_data = None
            
            for m in live_machines:
                if m['machine_no'] == machine_no and m.get('plant') == 1:
                    machine_data = m
                    break
            
            try:
                idle_status = PLANT1_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
                exact_data = PLANT1_EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)
                
                is_on = idle_status['on_since'] is not None
                is_producing = idle_status['count_seconds_ago'] is not None and idle_status['count_seconds_ago'] <= 180
                
                if is_on and not machine_data:
                    tool_id = exact_data.get('current_tool_id', 'N/A')
                    shut_height = exact_data.get('current_shut_height', 0.0)
                    
                    machine_data = {
                        'plant': 1,
                        'machine_no': machine_no,
                        'tool_id': tool_id,
                        'count': 0,
                        'shut_height': shut_height,
                        'last_seen': 'JSON only',
                        'status': idle_status['status'],
                        'current_hour_count': 0,
                        'last_hour_count': 0,
                        'cumulative_count': 0,
                        'shift': exact_data.get('shift', 'A'),
                        'idle_time': idle_status['hourly_idle_total']
                    }
                
                if machine_data:
                    machine_data.update(exact_data)
                    
                    problem_detected = is_on and not is_producing and idle_status['is_idle']
                    machine_data['problem_detected'] = problem_detected
                    
                    if problem_detected:
                        problem_machines.append(machine_no)
                    
                    current_shift = PLANT1_EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)
                    
                    if idle_status['last_count_time']:
                        machine_data['last_activity'] = idle_status['last_count_time'].strftime('%H:%M:%S')
                    else:
                        machine_data['last_activity'] = 'Never'
                    
                    # ✅ FIX: Use exact_data which has DB-fetched last_hour_count
                    machine_data['last_hour_count'] = exact_data.get('last_hour_count', 0)
                    machine_data['current_hour_count'] = exact_data.get('current_hour_count', 0)
                    machine_data['cumulative_count'] = exact_data.get('cumulative_count', 0)
                    machine_data['total_shift_idle_time'] = exact_data.get('total_shift_idle_time', 0)
                    
                    machine_data['shut_height'] = exact_data.get('current_shut_height', 0.0)
                    machine_data['first_count_at'] = exact_data.get('first_count_at')
                    machine_data['time_to_first_count'] = exact_data.get('time_to_first_count')
                    
                    machine_data.update({
                        'live_idle_time': idle_status['live_idle_time'],
                        'accumulated_idle_time': idle_status['accumulated_idle_time'],
                        'hourly_idle_total': idle_status['hourly_idle_total'],
                        'idle_time': idle_status['hourly_idle_total'],
                        'is_idle': idle_status['is_idle'],
                        'idle_type': idle_status['idle_type'],
                        'status': idle_status['status'],
                        'data_source': idle_status['data_source'],
                        'on_since': idle_status['on_since'].strftime('%H:%M:%S') if idle_status['on_since'] else None,
                        'count_seconds_ago': idle_status['count_seconds_ago'],
                        'json_seconds_ago': idle_status['json_seconds_ago'],
                        'machine_on': is_on,
                        'is_producing': is_producing
                    })
                    
            except Exception as e:
                print(f"⚠️ Plant 1 M{machine_no} error: {e}")
                import traceback
                traceback.print_exc()
            
            if machine_data:
                tool_id = machine_data.get('tool_id', '')
                tool_info = get_tool_info_from_tid_map(tool_id)
                
                machine_data.update({
                    'tool_customer': tool_info.get('customer', 'N/A'),
                    'tool_model': tool_info.get('model', 'N/A'),
                    'tool_part_name': tool_info.get('part_name', 'N/A'),
                    'tool_name': tool_info.get('tool_name', 'N/A'),
                    'tool_part_number': tool_info.get('part_number', 'N/A'),
                    'tool_tpm': tool_info.get('tpm', 0),
                    'tool_epc': tool_info.get('epc', 'N/A')
                })
                
                machine_data['plant'] = 1
                enhanced_machines.append(machine_data)
            else:
                idle_status = PLANT1_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
                
                enhanced_machines.append({
                    "plant": 1,
                    "machine_no": machine_no,
                    "tool_id": f"PLANT1_M{machine_no:02d}",
                    "count": 0,
                    "shut_height": 0.0,
                    "first_count_at": None,
                    "time_to_first_count": None,
                    "last_seen": "Not active",
                    "status": idle_status['status'],
                    "current_hour_count": 0,
                    "last_hour_count": 0,
                    "cumulative_count": 0,
                    "shift": "A",
                    "idle_time": idle_status['hourly_idle_total'],
                    'is_idle': idle_status['is_idle'],
                    'idle_type': idle_status['idle_type'],
                    'live_idle_time': idle_status['live_idle_time'],
                    'accumulated_idle_time': idle_status['accumulated_idle_time'],
                    'hourly_idle_total': idle_status['hourly_idle_total'],
                    "last_activity": "Never",
                    'tool_customer': 'N/A',
                    'tool_model': 'N/A',
                    'tool_part_name': 'N/A',
                    'tool_name': 'N/A',
                    'tool_part_number': 'N/A',
                    'tool_tpm': 0,
                    'tool_epc': 'N/A',
                    'machine_on': False,
                    'is_producing': False,
                    'problem_detected': False,
                    'on_since': None,
                    'data_source': idle_status['data_source']
                })
        
        enhanced_machines.sort(key=lambda x: x['machine_no'])
        
        on_machines = [m for m in enhanced_machines if m.get('machine_on')]
        producing_machines = [m for m in enhanced_machines if m.get('is_producing')]
        
        response = Response({
            "success": True,
            "total_machines": len(enhanced_machines),
            "on_count": len(on_machines),
            "producing_count": len(producing_machines),
            "problem_count": len(problem_machines),
            "problem_machines": problem_machines,
            "machines": enhanced_machines,
            "plant": 1,
            "message": f"Plant 1 - ON:{len(on_machines)} | Producing:{len(producing_machines)} | Problems:{len(problem_machines)}"
        })
        
        response['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
        response['Pragma'] = 'no-cache'
        response['Expires'] = '0'
        
        return response
        
    except Exception as e:
        print(f"❌ Plant 1 API ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        return Response({
            "success": False,
            "error": str(e),
            "machines": [],
            "plant": 1
        }, status=500)

    
# backend/apps/api/views.py (Plant 2 section update)
# backend/api/views.py

# 🔥 HELPER FUNCTION - Define this FIRST (before plant2_live)
def get_tool_info_from_tid_map(tool_id):
    """
    Query tid_map table and return tool information if EPC matches tool_id.
    Returns dict with tool details or empty values if not found.
    """
    try:
        if not tool_id or tool_id == 'NULL' or tool_id.startswith('PLANT2_M'):
            print(f"⚠️ Skipping invalid/placeholder tool_id: {tool_id}")
            return {}
        
        # Clean tool_id (first 24 characters matching EPC format)
        clean_tool_id = tool_id[:24] if len(tool_id) >= 24 else tool_id
        
        print(f"🔍 Searching tid_map for EPC: {clean_tool_id}")
        
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    customer,
                    model,
                    part_name,
                    tool_name,
                    epc,
                    part_number,
                    tpm
                FROM public.tid_map
                WHERE epc = %s
                LIMIT 1
            """, [clean_tool_id])
            
            result = cursor.fetchone()
            
            if result:
                print(f"✅ FOUND in tid_map: {result[0]} - {result[2]} (Tool: {result[3]})")
                return {
                    'customer': result[0] or 'N/A',
                    'model': result[1] or 'N/A',
                    'part_name': result[2] or 'N/A',
                    'tool_name': result[3] or 'N/A',
                    'epc': result[4] or 'N/A',
                    'part_number': result[5] or 'N/A',
                    'tpm': int(result[6]) if result[6] else 0
                }
            else:
                print(f"❌ NOT FOUND in tid_map: {clean_tool_id}")
                
                # Optional: Check if similar EPCs exist
                cursor.execute("""
                    SELECT epc, customer, part_name 
                    FROM public.tid_map 
                    WHERE epc LIKE %s 
                    LIMIT 3
                """, [f"{clean_tool_id[:10]}%"])
                
                similar = cursor.fetchall()
                if similar:
                    print(f"💡 Similar EPCs found: {[f'{s[0][:20]}... ({s[1]})' for s in similar]}")
                
                return {}
                
    except Exception as e:
        print(f"❌ Database error fetching tool info for {tool_id}: {e}")
        import traceback
        traceback.print_exc()
        return {}


# backend/api/views.py


def get_tool_info_from_tid_map(tool_id):
    """Query tid_map table - handles uppercase column names"""
    
    if not tool_id or tool_id == 'NULL' or tool_id.startswith('PLANT2_M'):
        return {}
    
    clean_tool_id = tool_id[:24] if len(tool_id) >= 24 else tool_id
    
    try:
        with connection.cursor() as cursor:
            # Get actual column names from table
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'tid_map'
                ORDER BY ordinal_position
            """)
            
            columns = [row[0] for row in cursor.fetchall()]
            
            # Find EPC column (case-insensitive)
            epc_column = None
            for col in columns:
                if col.upper() == 'EPC':
                    epc_column = col
                    break
            
            if not epc_column:
                print(f"❌ EPC column not found! Available: {columns}")
                return {}
            
            # Query with proper column name using double quotes
            query = f'SELECT * FROM public.tid_map WHERE "{epc_column}" = %s LIMIT 1'
            
            cursor.execute(query, [clean_tool_id])
            result = cursor.fetchone()
            
            if result:
                # Create dictionary with actual column names
                row_dict = dict(zip(columns, result))
                
                # Helper function for case-insensitive lookup
                def get_value(search_key):
                    for col_name, col_value in row_dict.items():
                        if col_name.upper() == search_key.upper():
                            return col_value if col_value else 'N/A'
                    return 'N/A'
                
                # Extract data
                tool_data = {
                    'customer': get_value('CUSTOMER'),
                    'model': get_value('MODEL'),
                    'part_name': get_value('PART_NAME'),
                    'tool_name': get_value('TOOL_NAME'),
                    'epc': get_value('EPC'),
                    'part_number': get_value('PART_NUMBER'),
                    'tpm': 0
                }
                
                # Handle TPM
                tpm_val = get_value('TPM')
                if tpm_val != 'N/A':
                    try:
                        tool_data['tpm'] = int(tpm_val)
                    except:
                        tool_data['tpm'] = 0
                
                print(f"✅ FOUND: {tool_data['customer']} - {tool_data['part_name']}")
                return tool_data
                
            else:
                return {}
                
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return {}


# @never_cache
# @api_view(['GET'])
# def plant2_live(request):
#     """
#     Plant 2 - LIVE DASHBOARD DATA
    
#     ✅ STRICT 3-MINUTE GRACE POLICY
#     ✅ Display starts at exactly 3m
#     ✅ live_idle == accumulated_idle (current segment mirror)
#     ✅ hourly_total = completed + live (resets per hour)
#     ✅ Cross-hour segments re-base at hour_start
#     ✅ No-signal hour: hourly_total=60
#     """
#     try:
#         from apps.machines.machine_state import MACHINE_STATE
#         from apps.mqtt.simple_plant2 import (
#             PLANT2_EXACT_REQUIREMENT_STATE,
#             STRICT_IDLE_POLICY,
#             TOPIC_MACHINE_MAPPING,
#             get_machine_group
#         )
        
#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)
#         all_mapped_machines = sorted(list(all_mapped_machines))
        
#         live_machines = MACHINE_STATE.summarize(plant_filter=2, stale_after_seconds=300)
        
#         enhanced_machines = []
#         problem_machines = []
        
#         ist_tz = pytz.timezone('Asia/Kolkata')
#         now_ist = datetime.now(ist_tz)
        
#         for machine_no in all_mapped_machines:
#             machine_data = None
            
#             # Find existing machine data
#             for m in live_machines:
#                 if m['machine_no'] == machine_no and m.get('plant') == 2:
#                     machine_data = m
#                     break
            
#             try:
#                 # Get idle status from strict policy
#                 idle_status = STRICT_IDLE_POLICY.get_idle_status(machine_no, now_ist)
                
#                 # Get basic machine data
#                 exact_data = PLANT2_EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)
                
#                 is_on = idle_status['on_since'] is not None
#                 is_producing = idle_status['count_seconds_ago'] is not None and idle_status['count_seconds_ago'] <= 180
                
#                 if is_on and not machine_data:
#                     tool_id = exact_data.get('current_tool_id', 'N/A')
#                     shut_height = exact_data.get('current_shut_height', 0.0)
                    
#                     machine_data = {
#                         'plant': 2,
#                         'machine_no': machine_no,
#                         'tool_id': tool_id,
#                         'count': 0,
#                         'shut_height': shut_height if shut_height != 0.0 else 'No data',
#                         'last_seen': 'JSON only',
#                         'status': idle_status['status'],
#                         'current_hour_count': 0,
#                         'last_hour_count': 0,
#                         'cumulative_count': 0,
#                         'shift': exact_data.get('shift', 'A'),
#                         'idle_time': idle_status['hourly_idle_total']
#                     }
                
#                 if machine_data:
#                     machine_data.update(exact_data)
                    
#                     problem_detected = is_on and not is_producing and idle_status['is_idle']
#                     machine_data['problem_detected'] = problem_detected
                    
#                     if problem_detected:
#                         problem_machines.append(machine_no)
                    
#                     # ===== STRICT IDLE DATA =====
#                     current_shift = PLANT2_EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)
                    
#                     # Last activity time
#                     if idle_status['last_count_time']:
#                         machine_data['last_activity'] = idle_status['last_count_time'].strftime('%H:%M:%S')
#                     else:
#                         machine_data['last_activity'] = 'Never'
                    
#                     # Counts
#                     last_hour_count = PLANT2_EXACT_REQUIREMENT_STATE.last_hour_counts.get(machine_no, 0)
#                     machine_data['last_hour_count'] = last_hour_count
                    
#                     shift_key = (machine_no, current_shift)
#                     shift_cumulative = PLANT2_EXACT_REQUIREMENT_STATE.shift_cumulative.get(shift_key, 0)
#                     current_hour_count = exact_data.get('current_hour_count', 0)
#                     total_shift_count = shift_cumulative + current_hour_count
#                     machine_data['cumulative_count'] = total_shift_count
                    
#                     # Apply strict idle policy fields
#                     machine_data.update({
#                         # Strict policy fields
#                         'live_idle_time': idle_status['live_idle_time'],
#                         'accumulated_idle_time': idle_status['accumulated_idle_time'],
#                         'hourly_idle_total': idle_status['hourly_idle_total'],
#                         'idle_time': idle_status['hourly_idle_total'],
#                         'is_idle': idle_status['is_idle'],
#                         'idle_type': idle_status['idle_type'],
#                         'status': idle_status['status'],
#                         'data_source': idle_status['data_source'],
                        
#                         # Diagnostic fields
#                         'on_since': idle_status['on_since'].strftime('%H:%M:%S') if idle_status['on_since'] else None,
#                         'count_seconds_ago': idle_status['count_seconds_ago'],
#                         'json_seconds_ago': idle_status['json_seconds_ago'],
                        
#                         # Machine state
#                         'machine_on': is_on,
#                         'is_producing': is_producing
#                     })
                    
#             except Exception as e:
#                 print(f"⚠️ M{machine_no} error: {e}")
#                 import traceback
#                 traceback.print_exc()
#                 if not machine_data:
#                     exact_data = {}
            
#             if machine_data:
#                 tool_id = machine_data.get('tool_id', '')
#                 tool_info = get_tool_info_from_tid_map(tool_id)
                
#                 machine_data.update({
#                     'machine_group': get_machine_group(machine_no),
#                     'tool_customer': tool_info.get('customer', 'N/A'),
#                     'tool_model': tool_info.get('model', 'N/A'),
#                     'tool_part_name': tool_info.get('part_name', 'N/A'),
#                     'tool_name': tool_info.get('tool_name', 'N/A'),
#                     'tool_part_number': tool_info.get('part_number', 'N/A'),
#                     'tool_tpm': tool_info.get('tpm', 0),
#                     'tool_epc': tool_info.get('epc', 'N/A')
#                 })
                
#                 machine_data['plant'] = 2
#                 enhanced_machines.append(machine_data)
                
#             else:
#                 # Default machine data (offline)
#                 idle_status = STRICT_IDLE_POLICY.get_idle_status(machine_no, now_ist)
                
#                 enhanced_machines.append({
#                     "plant": 2,
#                     "machine_no": machine_no,
#                     "machine_group": get_machine_group(machine_no),
#                     "tool_id": f"PLANT2_M{machine_no:02d}",
#                     "count": 0,
#                     "shut_height": "Waiting for data",
#                     "last_seen": "Not active",
#                     "status": idle_status['status'],
#                     "current_hour_count": 0,
#                     "last_hour_count": 0,
#                     "cumulative_count": 0,
#                     "shift": "A",
#                     "is_idle": idle_status['is_idle'],
#                     "idle_type": idle_status['idle_type'],
#                     "live_idle_time": idle_status['live_idle_time'],
#                     "accumulated_idle_time": idle_status['accumulated_idle_time'],
#                     "hourly_idle_total": idle_status['hourly_idle_total'],
#                     "idle_time": idle_status['hourly_idle_total'],
#                     "last_activity": "Never",
#                     'tool_customer': 'N/A',
#                     'tool_model': 'N/A',
#                     'tool_part_name': 'N/A',
#                     'tool_name': 'N/A',
#                     'tool_part_number': 'N/A',
#                     'tool_tpm': 0,
#                     'tool_epc': 'N/A',
#                     'machine_on': False,
#                     'is_producing': False,
#                     'problem_detected': False,
#                     'on_since': None,
#                     'on_duration_minutes': None,
#                     'first_count_at': None,
#                     'time_to_first_count': None,
#                     'data_source': idle_status['data_source']
#                 })
        
#         enhanced_machines.sort(key=lambda x: x['machine_no'])
        
#         on_machines = [m for m in enhanced_machines if m.get('machine_on')]
#         producing_machines = [m for m in enhanced_machines if m.get('is_producing')]
        
#         groups_summary = {}
#         for group in ['J1', 'J2', 'J3', 'J4', 'J5']:
#             group_machines = [m for m in enhanced_machines if m.get('machine_group') == group]
#             group_on = [m for m in group_machines if m.get('machine_on')]
#             group_producing = [m for m in group_machines if m.get('is_producing')]
#             group_problems = [m for m in group_machines if m.get('problem_detected')]
            
#             groups_summary[group] = {
#                 'total': len(group_machines),
#                 'on': len(group_on),
#                 'producing': len(group_producing),
#                 'problems': len(group_problems)
#             }
        
#         response = Response({
#             "success": True,
#             "total_machines": len(enhanced_machines),
#             "on_count": len(on_machines),
#             "producing_count": len(producing_machines),
#             "problem_count": len(problem_machines),
#             "problem_machines": problem_machines,
#             "groups_summary": groups_summary,
#             "machines": enhanced_machines,
#             "plant": 2,
#             "message": f"Plant 2 - ON:{len(on_machines)} | Producing:{len(producing_machines)} | Problems:{len(problem_machines)}"
#         })
        
#         response['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
#         response['Pragma'] = 'no-cache'
#         response['Expires'] = '0'
        
#         return response
        
#     except Exception as e:
#         print(f"❌ API ERROR: {e}")
#         import traceback
#         traceback.print_exc()
        
#         error_response = Response({
#             "success": False,
#             "error": str(e),
#             "machines": [],
#             "plant": 2
#         }, status=500)
        
#         error_response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
#         error_response['Pragma'] = 'no-cache'
#         error_response['Expires'] = '0'
        
#         return error_response



# @never_cache
# @api_view(['GET'])
# def plant2_live(request):
#     """
#     Plant 2 - LIVE DASHBOARD DATA
    
#     ✅ UPDATED: shut_height and first_count_time added
#     """
#     try:
#         from apps.machines.machine_state import MACHINE_STATE
#         from apps.mqtt.simple_plant2 import (
#             PLANT2_EXACT_REQUIREMENT_STATE,
#             STRICT_IDLE_POLICY,
#             TOPIC_MACHINE_MAPPING,
#             get_machine_group
#         )
        
#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)
#         all_mapped_machines = sorted(list(all_mapped_machines))
        
#         live_machines = MACHINE_STATE.summarize(plant_filter=2, stale_after_seconds=300)
        
#         enhanced_machines = []
#         problem_machines = []
        
#         ist_tz = pytz.timezone('Asia/Kolkata')
#         now_ist = datetime.now(ist_tz)
        
#         for machine_no in all_mapped_machines:
#             machine_data = None
            
#             # Find existing machine data
#             for m in live_machines:
#                 if m['machine_no'] == machine_no and m.get('plant') == 2:
#                     machine_data = m
#                     break
            
#             try:
#                 # Get idle status from strict policy
#                 idle_status = STRICT_IDLE_POLICY.get_idle_status(machine_no, now_ist)#
#                 # idle_status = PLANT2_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no)

                
                
#                 # ✅ UPDATED: Get complete machine data (includes shut_height, first_count_time)
#                 exact_data = PLANT2_EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)
                
#                 is_on = idle_status['on_since'] is not None
#                 is_producing = idle_status['count_seconds_ago'] is not None and idle_status['count_seconds_ago'] <= 180
                
#                 if is_on and not machine_data:
#                     tool_id = exact_data.get('tool_id', 'N/A')  # ✅ Changed from current_tool_id
#                     shut_height = exact_data.get('shut_height', 0.0)  # ✅ Direct from exact_data
                    
#                     machine_data = {
#                         'plant': 2,
#                         'machine_no': machine_no,
#                         'tool_id': tool_id,
#                         'count': 0,
#                         'shut_height': shut_height,  # ✅ Direct value
#                         'last_seen': 'JSON only',
#                         'status': idle_status['status'],
#                         'current_hour_count': 0,
#                         'last_hour_count': 0,
#                         'cumulative_count': 0,
#                         'shift': exact_data.get('shift', 'A'),
#                         'idle_time': idle_status['hourly_idle_total']
#                     }
                
#                 if machine_data:
#                     machine_data.update(exact_data)
                    
#                     problem_detected = is_on and not is_producing and idle_status['is_idle']
#                     machine_data['problem_detected'] = problem_detected
                    
#                     if problem_detected:
#                         problem_machines.append(machine_no)
                    
#                     # ===== STRICT IDLE DATA =====
#                     current_shift = PLANT2_EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)
                    
#                     # Last activity time
#                     if idle_status['last_count_time']:
#                         machine_data['last_activity'] = idle_status['last_count_time'].strftime('%H:%M:%S')
#                     else:
#                         machine_data['last_activity'] = 'Never'
                    
#                     # Counts
#                     last_hour_count = PLANT2_EXACT_REQUIREMENT_STATE.last_hour_counts.get(machine_no, 0)
#                     machine_data['last_hour_count'] = last_hour_count
                    
#                     shift_key = (machine_no, current_shift)
#                     shift_cumulative = PLANT2_EXACT_REQUIREMENT_STATE.shift_cumulative.get(shift_key, 0)
#                     current_hour_count = exact_data.get('current_hour_count', 0)
#                     total_shift_count = shift_cumulative + current_hour_count
#                     machine_data['cumulative_count'] = total_shift_count
                    
#                     # ✅ UPDATED: Add shut_height and first_count_time explicitly
#                     machine_data['shut_height'] = exact_data.get('shut_height', 0.0)
#                     machine_data['first_count_time'] = exact_data.get('first_count_time')  # ✅ Added
#                     machine_data['last_count_time'] = exact_data.get('last_count_time')   # ✅ Added
                    
#                     # Apply strict idle policy fields
#                     machine_data.update({
#                         # Strict policy fields
#                         'live_idle_time': idle_status['live_idle_time'],
#                         'accumulated_idle_time': idle_status['accumulated_idle_time'],
#                         'hourly_idle_total': idle_status['hourly_idle_total'],
#                         'idle_time': idle_status['hourly_idle_total'],
#                         'is_idle': idle_status['is_idle'],
#                         'idle_type': idle_status['idle_type'],
#                         'status': idle_status['status'],
#                         'data_source': idle_status['data_source'],
                        
#                         # Diagnostic fields
#                         'on_since': idle_status['on_since'].strftime('%H:%M:%S') if idle_status['on_since'] else None,
#                         'count_seconds_ago': idle_status['count_seconds_ago'],
#                         'json_seconds_ago': idle_status['json_seconds_ago'],
                        
#                         # Machine state
#                         'machine_on': is_on,
#                         'is_producing': is_producing
#                     })
                    
#             except Exception as e:
#                 print(f"⚠️ M{machine_no} error: {e}")
#                 import traceback
#                 traceback.print_exc()
#                 if not machine_data:
#                     exact_data = {}
            
#             if machine_data:
#                 tool_id = machine_data.get('tool_id', '')
#                 tool_info = get_tool_info_from_tid_map(tool_id)
                
#                 machine_data.update({
#                     'machine_group': get_machine_group(machine_no),
#                     'tool_customer': tool_info.get('customer', 'N/A'),
#                     'tool_model': tool_info.get('model', 'N/A'),
#                     'tool_part_name': tool_info.get('part_name', 'N/A'),
#                     'tool_name': tool_info.get('tool_name', 'N/A'),
#                     'tool_part_number': tool_info.get('part_number', 'N/A'),
#                     'tool_tpm': tool_info.get('tpm', 0),
#                     'tool_epc': tool_info.get('epc', 'N/A')
#                 })
                
#                 machine_data['plant'] = 2
#                 enhanced_machines.append(machine_data)
                
#             else:
#                 # Default machine data (offline)
#                 idle_status = STRICT_IDLE_POLICY.get_idle_status(machine_no, now_ist)
                
#                 enhanced_machines.append({
#                     "plant": 2,
#                     "machine_no": machine_no,
#                     "machine_group": get_machine_group(machine_no),
#                     "tool_id": f"PLANT2_M{machine_no:02d}",
#                     "count": 0,
#                     "shut_height": 0.0,  # ✅ Changed from "Waiting for data"
#                     "first_count_time": None,  # ✅ Added
#                     "last_count_time": None,   # ✅ Added
#                     "last_seen": "Not active",
#                     "status": idle_status['status'],
#                     "current_hour_count": 0,
#                     "last_hour_count": 0,
#                     "cumulative_count": 0,
#                     "shift": "A",
#                     "is_idle": idle_status['is_idle'],
#                     "idle_type": idle_status['idle_type'],
#                     "live_idle_time": idle_status['live_idle_time'],
#                     "accumulated_idle_time": idle_status['accumulated_idle_time'],
#                     "hourly_idle_total": idle_status['hourly_idle_total'],
#                     "idle_time": idle_status['hourly_idle_total'],
#                     "last_activity": "Never",
#                     'tool_customer': 'N/A',
#                     'tool_model': 'N/A',
#                     'tool_part_name': 'N/A',
#                     'tool_name': 'N/A',
#                     'tool_part_number': 'N/A',
#                     'tool_tpm': 0,
#                     'tool_epc': 'N/A',
#                     'machine_on': False,
#                     'is_producing': False,
#                     'problem_detected': False,
#                     'on_since': None,
#                     'on_duration_minutes': None,
#                     'first_count_at': None,
#                     'time_to_first_count': None,
#                     'data_source': idle_status['data_source']
#                 })
        
#         enhanced_machines.sort(key=lambda x: x['machine_no'])
        
#         on_machines = [m for m in enhanced_machines if m.get('machine_on')]
#         producing_machines = [m for m in enhanced_machines if m.get('is_producing')]
        
#         groups_summary = {}
#         for group in ['J1', 'J2', 'J3', 'J4', 'J5']:
#             group_machines = [m for m in enhanced_machines if m.get('machine_group') == group]
#             group_on = [m for m in group_machines if m.get('machine_on')]
#             group_producing = [m for m in group_machines if m.get('is_producing')]
#             group_problems = [m for m in group_machines if m.get('problem_detected')]
            
#             groups_summary[group] = {
#                 'total': len(group_machines),
#                 'on': len(group_on),
#                 'producing': len(group_producing),
#                 'problems': len(group_problems)
#             }
        
#         response = Response({
#             "success": True,
#             "total_machines": len(enhanced_machines),
#             "on_count": len(on_machines),
#             "producing_count": len(producing_machines),
#             "problem_count": len(problem_machines),
#             "problem_machines": problem_machines,
#             "groups_summary": groups_summary,
#             "machines": enhanced_machines,
#             "plant": 2,
#             "message": f"Plant 2 - ON:{len(on_machines)} | Producing:{len(producing_machines)} | Problems:{len(problem_machines)}"
#         })
        
#         response['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
#         response['Pragma'] = 'no-cache'
#         response['Expires'] = '0'
        
#         return response
        
#     except Exception as e:
#         print(f"❌ API ERROR: {e}")
#         import traceback
#         traceback.print_exc()
        
#         error_response = Response({
#             "success": False,
#             "error": str(e),
#             "machines": [],
#             "plant": 2
#         }, status=500)
        
#         error_response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
#         error_response['Pragma'] = 'no-cache'
#         error_response['Expires'] = '0'
        
#         return error_response


# @never_cache
# @api_view(['GET'])
# def plant2_live(request):
#     """
#     Plant 2 - LIVE DASHBOARD DATA
    
#     ✅ UPDATED: shut_height and first_count_time added
#     ✅ FIXED: STRICT_IDLE_POLICY replaced with idle_tracker
#     """
#     try:
#         from apps.machines.machine_state import MACHINE_STATE
#         from apps.mqtt.simple_plant2 import (
#             PLANT2_EXACT_REQUIREMENT_STATE,
#             TOPIC_MACHINE_MAPPING,
#             get_machine_group
#         )
        
#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)
#         all_mapped_machines = sorted(list(all_mapped_machines))
        
#         live_machines = MACHINE_STATE.summarize(plant_filter=2, stale_after_seconds=300)
        
#         enhanced_machines = []
#         problem_machines = []
        
#         ist_tz = pytz.timezone('Asia/Kolkata')
#         now_ist = datetime.now(ist_tz)
        
#         for machine_no in all_mapped_machines:
#             machine_data = None
            
#             # Find existing machine data
#             for m in live_machines:
#                 if m['machine_no'] == machine_no and m.get('plant') == 2:
#                     machine_data = m
#                     break
            
#             try:
#                 # ✅ FIXED: Get idle status from idle_tracker
#                 idle_status = PLANT2_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
                
#                 # ✅ UPDATED: Get complete machine data (includes shut_height, first_count_time)
#                 exact_data = PLANT2_EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)
                
#                 is_on = idle_status['on_since'] is not None
#                 is_producing = idle_status['count_seconds_ago'] is not None and idle_status['count_seconds_ago'] <= 180
                
#                 if is_on and not machine_data:
#                     tool_id = exact_data.get('current_tool_id', 'N/A')
#                     shut_height = exact_data.get('current_shut_height', 0.0)
                    
#                     machine_data = {
#                         'plant': 2,
#                         'machine_no': machine_no,
#                         'tool_id': tool_id,
#                         'count': 0,
#                         'shut_height': shut_height,
#                         'last_seen': 'JSON only',
#                         'status': idle_status['status'],
#                         'current_hour_count': 0,
#                         'last_hour_count': 0,
#                         'cumulative_count': 0,
#                         'shift': exact_data.get('shift', 'A'),
#                         'idle_time': idle_status['hourly_idle_total']
#                     }
                
#                 if machine_data:
#                     machine_data.update(exact_data)
                    
#                     problem_detected = is_on and not is_producing and idle_status['is_idle']
#                     machine_data['problem_detected'] = problem_detected
                    
#                     if problem_detected:
#                         problem_machines.append(machine_no)
                    
#                     # ===== STRICT IDLE DATA =====
#                     current_shift = PLANT2_EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)
                    
#                     # Last activity time
#                     if idle_status['last_count_time']:
#                         machine_data['last_activity'] = idle_status['last_count_time'].strftime('%H:%M:%S')
#                     else:
#                         machine_data['last_activity'] = 'Never'
                    
#                     # Counts
#                     last_hour_count = PLANT2_EXACT_REQUIREMENT_STATE.last_hour_counts.get(machine_no, 0)
#                     machine_data['last_hour_count'] = last_hour_count
                    
#                     shift_key = (machine_no, current_shift)
#                     shift_cumulative = PLANT2_EXACT_REQUIREMENT_STATE.shift_cumulative.get(shift_key, 0)
#                     current_hour_count = exact_data.get('current_hour_count', 0)
#                     total_shift_count = shift_cumulative + current_hour_count
#                     machine_data['cumulative_count'] = total_shift_count
                    
#                     # ✅ UPDATED: Add shut_height and first_count_time explicitly
#                     machine_data['shut_height'] = exact_data.get('current_shut_height', 0.0)
#                     machine_data['first_count_at'] = exact_data.get('first_count_at')
#                     machine_data['time_to_first_count'] = exact_data.get('time_to_first_count')
                    
#                     # Apply strict idle policy fields
#                     machine_data.update({
#                         # Strict policy fields
#                         'live_idle_time': idle_status['live_idle_time'],
#                         'accumulated_idle_time': idle_status['accumulated_idle_time'],
#                         'hourly_idle_total': idle_status['hourly_idle_total'],
#                         'idle_time': idle_status['hourly_idle_total'],
#                         'is_idle': idle_status['is_idle'],
#                         'idle_type': idle_status['idle_type'],
#                         'status': idle_status['status'],
#                         'data_source': idle_status['data_source'],
                        
#                         # Diagnostic fields
#                         'on_since': idle_status['on_since'].strftime('%H:%M:%S') if idle_status['on_since'] else None,
#                         'count_seconds_ago': idle_status['count_seconds_ago'],
#                         'json_seconds_ago': idle_status['json_seconds_ago'],
                        
#                         # Machine state
#                         'machine_on': is_on,
#                         'is_producing': is_producing
#                     })
                    
#             except Exception as e:
#                 print(f"⚠️ M{machine_no} error: {e}")
#                 import traceback
#                 traceback.print_exc()
#                 if not machine_data:
#                     exact_data = {}
            
#             if machine_data:
#                 tool_id = machine_data.get('tool_id', '')
#                 tool_info = get_tool_info_from_tid_map(tool_id)
                
#                 machine_data.update({
#                     'machine_group': get_machine_group(machine_no),
#                     'tool_customer': tool_info.get('customer', 'N/A'),
#                     'tool_model': tool_info.get('model', 'N/A'),
#                     'tool_part_name': tool_info.get('part_name', 'N/A'),
#                     'tool_name': tool_info.get('tool_name', 'N/A'),
#                     'tool_part_number': tool_info.get('part_number', 'N/A'),
#                     'tool_tpm': tool_info.get('tpm', 0),
#                     'tool_epc': tool_info.get('epc', 'N/A')
#                 })
                
#                 machine_data['plant'] = 2
#                 enhanced_machines.append(machine_data)
                
#             else:
#                 # ✅ FIXED: Default machine data (offline) - use idle_tracker
#                 idle_status = PLANT2_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
                
#                 enhanced_machines.append({
#                     "plant": 2,
#                     "machine_no": machine_no,
#                     "machine_group": get_machine_group(machine_no),
#                     "tool_id": f"PLANT2_M{machine_no:02d}",
#                     "count": 0,
#                     "shut_height": 0.0,
#                     "first_count_at": None,
#                     "time_to_first_count": None,
#                     "last_seen": "Not active",
#                     "status": idle_status['status'],
#                     "current_hour_count": 0,
#                     "last_hour_count": 0,
#                     "cumulative_count": 0,
#                     "shift": "A",
#                     "is_idle": idle_status['is_idle'],
#                     "idle_type": idle_status['idle_type'],
#                     "live_idle_time": idle_status['live_idle_time'],
#                     "accumulated_idle_time": idle_status['accumulated_idle_time'],
#                     "hourly_idle_total": idle_status['hourly_idle_total'],
#                     "idle_time": idle_status['hourly_idle_total'],
#                     "last_activity": "Never",
#                     'tool_customer': 'N/A',
#                     'tool_model': 'N/A',
#                     'tool_part_name': 'N/A',
#                     'tool_name': 'N/A',
#                     'tool_part_number': 'N/A',
#                     'tool_tpm': 0,
#                     'tool_epc': 'N/A',
#                     'machine_on': False,
#                     'is_producing': False,
#                     'problem_detected': False,
#                     'on_since': None,
#                     'on_duration_minutes': None,
#                     'data_source': idle_status['data_source']
#                 })
        
#         enhanced_machines.sort(key=lambda x: x['machine_no'])
        
#         on_machines = [m for m in enhanced_machines if m.get('machine_on')]
#         producing_machines = [m for m in enhanced_machines if m.get('is_producing')]
        
#         groups_summary = {}
#         for group in ['J1', 'J2', 'J3', 'J4', 'J5']:
#             group_machines = [m for m in enhanced_machines if m.get('machine_group') == group]
#             group_on = [m for m in group_machines if m.get('machine_on')]
#             group_producing = [m for m in group_machines if m.get('is_producing')]
#             group_problems = [m for m in group_machines if m.get('problem_detected')]
            
#             groups_summary[group] = {
#                 'total': len(group_machines),
#                 'on': len(group_on),
#                 'producing': len(group_producing),
#                 'problems': len(group_problems)
#             }
        
#         response = Response({
#             "success": True,
#             "total_machines": len(enhanced_machines),
#             "on_count": len(on_machines),
#             "producing_count": len(producing_machines),
#             "problem_count": len(problem_machines),
#             "problem_machines": problem_machines,
#             "groups_summary": groups_summary,
#             "machines": enhanced_machines,
#             "plant": 2,
#             "message": f"Plant 2 - ON:{len(on_machines)} | Producing:{len(producing_machines)} | Problems:{len(problem_machines)}"
#         })
        
#         response['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
#         response['Pragma'] = 'no-cache'
#         response['Expires'] = '0'
        
#         return response
        
#     except Exception as e:
#         print(f"❌ API ERROR: {e}")
#         import traceback
#         traceback.print_exc()
        
#         error_response = Response({
#             "success": False,
#             "error": str(e),
#             "machines": [],
#             "plant": 2
#         }, status=500)
        
#         error_response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
#         error_response['Pragma'] = 'no-cache'
#         error_response['Expires'] = '0'
        
#         return error_response


@never_cache
@api_view(['GET'])
def plant2_live(request):
    """
    Plant 2 - LIVE DASHBOARD DATA
    
    ✅ UPDATED: DB-based last_hour_count, cumulative_count, total_shift_idle_time
    ✅ FIXED: STRICT_IDLE_POLICY replaced with idle_tracker
    """
    try:
        from apps.machines.machine_state import MACHINE_STATE
        from apps.mqtt.simple_plant2 import (
            PLANT2_EXACT_REQUIREMENT_STATE,
            TOPIC_MACHINE_MAPPING,
            get_machine_group
        )
        
        all_mapped_machines = set()
        for machines_list in TOPIC_MACHINE_MAPPING.values():
            all_mapped_machines.update(machines_list)
        all_mapped_machines = sorted(list(all_mapped_machines))
        
        live_machines = MACHINE_STATE.summarize(plant_filter=2, stale_after_seconds=300)
        
        enhanced_machines = []
        problem_machines = []
        
        ist_tz = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist_tz)
        
        for machine_no in all_mapped_machines:
            machine_data = None
            
            # Find existing machine data
            for m in live_machines:
                if m['machine_no'] == machine_no and m.get('plant') == 2:
                    machine_data = m
                    break
            
            try:
                # ✅ FIXED: Get idle status from idle_tracker
                idle_status = PLANT2_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
                
                # ✅ UPDATED: Get complete machine data (DB-fetched values included)
                exact_data = PLANT2_EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)
                
                is_on = idle_status['on_since'] is not None
                is_producing = idle_status['count_seconds_ago'] is not None and idle_status['count_seconds_ago'] <= 180
                
                if is_on and not machine_data:
                    tool_id = exact_data.get('current_tool_id', 'N/A')
                    shut_height = exact_data.get('current_shut_height', 0.0)
                    
                    machine_data = {
                        'plant': 2,
                        'machine_no': machine_no,
                        'tool_id': tool_id,
                        'count': 0,
                        'shut_height': shut_height,
                        'last_seen': 'JSON only',
                        'status': idle_status['status'],
                        'current_hour_count': 0,
                        'last_hour_count': 0,
                        'cumulative_count': 0,
                        'shift': exact_data.get('shift', 'A'),
                        'idle_time': idle_status['hourly_idle_total']
                    }
                
                if machine_data:
                    machine_data.update(exact_data)
                    
                    problem_detected = is_on and not is_producing and idle_status['is_idle']
                    machine_data['problem_detected'] = problem_detected
                    
                    if problem_detected:
                        problem_machines.append(machine_no)
                    
                    # ===== STRICT IDLE DATA =====
                    current_shift = PLANT2_EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)
                    
                    # Last activity time
                    if idle_status['last_count_time']:
                        machine_data['last_activity'] = idle_status['last_count_time'].strftime('%H:%M:%S')
                    else:
                        machine_data['last_activity'] = 'Never'
                    
                    # ✅ UPDATED: Counts from DATABASE (not memory)
                    machine_data['last_hour_count'] = exact_data.get('last_hour_count', 0)  # FROM DB
                    machine_data['cumulative_count'] = exact_data.get('cumulative_count', 0)  # FROM DB
                    machine_data['total_shift_idle_time'] = exact_data.get('total_shift_idle_time', 0)  # FROM DB
                    
                    # ✅ UPDATED: Add shut_height and first_count_time explicitly
                    machine_data['shut_height'] = exact_data.get('current_shut_height', 0.0)
                    machine_data['first_count_at'] = exact_data.get('first_count_at')
                    machine_data['time_to_first_count'] = exact_data.get('time_to_first_count')
                    
                    # Apply strict idle policy fields
                    machine_data.update({
                        # Strict policy fields
                        'live_idle_time': idle_status['live_idle_time'],
                        'accumulated_idle_time': idle_status['accumulated_idle_time'],
                        'hourly_idle_total': idle_status['hourly_idle_total'],
                        'idle_time': idle_status['hourly_idle_total'],
                        'is_idle': idle_status['is_idle'],
                        'idle_type': idle_status['idle_type'],
                        'status': idle_status['status'],
                        'data_source': idle_status['data_source'],
                        
                        # Diagnostic fields
                        'on_since': idle_status['on_since'].strftime('%H:%M:%S') if idle_status['on_since'] else None,
                        'count_seconds_ago': idle_status['count_seconds_ago'],
                        'json_seconds_ago': idle_status['json_seconds_ago'],
                        
                        # Machine state
                        'machine_on': is_on,
                        'is_producing': is_producing
                    })
                    
            except Exception as e:
                print(f"⚠️ M{machine_no} error: {e}")
                import traceback
                traceback.print_exc()
                if not machine_data:
                    exact_data = {}
            
            if machine_data:
                tool_id = machine_data.get('tool_id', '')
                tool_info = get_tool_info_from_tid_map(tool_id)
                
                machine_data.update({
                    'machine_group': get_machine_group(machine_no),
                    'tool_customer': tool_info.get('customer', 'N/A'),
                    'tool_model': tool_info.get('model', 'N/A'),
                    'tool_part_name': tool_info.get('part_name', 'N/A'),
                    'tool_name': tool_info.get('tool_name', 'N/A'),
                    'tool_part_number': tool_info.get('part_number', 'N/A'),
                    'tool_tpm': tool_info.get('tpm', 0),
                    'tool_epc': tool_info.get('epc', 'N/A')
                })
                
                machine_data['plant'] = 2
                enhanced_machines.append(machine_data)
                
            else:
                # ✅ FIXED: Default machine data (offline) - use idle_tracker
                idle_status = PLANT2_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(machine_no, now_ist)
                
                enhanced_machines.append({
                    "plant": 2,
                    "machine_no": machine_no,
                    "machine_group": get_machine_group(machine_no),
                    "tool_id": f"PLANT2_M{machine_no:02d}",
                    "count": 0,
                    "shut_height": 0.0,
                    "first_count_at": None,
                    "time_to_first_count": None,
                    "last_seen": "Not active",
                    "status": idle_status['status'],
                    "current_hour_count": 0,
                    "last_hour_count": 0,
                    "cumulative_count": 0,
                    "total_shift_idle_time": 0,  # ✅ NEW FIELD
                    "shift": "A",
                    "is_idle": idle_status['is_idle'],
                    "idle_type": idle_status['idle_type'],
                    "live_idle_time": idle_status['live_idle_time'],
                    "accumulated_idle_time": idle_status['accumulated_idle_time'],
                    "hourly_idle_total": idle_status['hourly_idle_total'],
                    "idle_time": idle_status['hourly_idle_total'],
                    "last_activity": "Never",
                    'tool_customer': 'N/A',
                    'tool_model': 'N/A',
                    'tool_part_name': 'N/A',
                    'tool_name': 'N/A',
                    'tool_part_number': 'N/A',
                    'tool_tpm': 0,
                    'tool_epc': 'N/A',
                    'machine_on': False,
                    'is_producing': False,
                    'problem_detected': False,
                    'on_since': None,
                    'on_duration_minutes': None,
                    'data_source': idle_status['data_source']
                })
        
        enhanced_machines.sort(key=lambda x: x['machine_no'])
        
        on_machines = [m for m in enhanced_machines if m.get('machine_on')]
        producing_machines = [m for m in enhanced_machines if m.get('is_producing')]
        
        groups_summary = {}
        for group in ['J1', 'J2', 'J3', 'J4', 'J5']:
            group_machines = [m for m in enhanced_machines if m.get('machine_group') == group]
            group_on = [m for m in group_machines if m.get('machine_on')]
            group_producing = [m for m in group_machines if m.get('is_producing')]
            group_problems = [m for m in group_machines if m.get('problem_detected')]
            
            groups_summary[group] = {
                'total': len(group_machines),
                'on': len(group_on),
                'producing': len(group_producing),
                'problems': len(group_problems)
            }
        
        response = Response({
            "success": True,
            "total_machines": len(enhanced_machines),
            "on_count": len(on_machines),
            "producing_count": len(producing_machines),
            "problem_count": len(problem_machines),
            "problem_machines": problem_machines,
            "groups_summary": groups_summary,
            "machines": enhanced_machines,
            "plant": 2,
            "message": f"Plant 2 - ON:{len(on_machines)} | Producing:{len(producing_machines)} | Problems:{len(problem_machines)}"
        })
        
        response['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
        response['Pragma'] = 'no-cache'
        response['Expires'] = '0'
        
        return response
        
    except Exception as e:
        print(f"❌ API ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        error_response = Response({
            "success": False,
            "error": str(e),
            "machines": [],
            "plant": 2
        }, status=500)
        
        error_response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        error_response['Pragma'] = 'no-cache'
        error_response['Expires'] = '0'
        
        return error_response

    
@never_cache
@api_view(['POST'])
def save_hourly_snapshot(request):
    """
    ✅ FIXED - Use HOURLY_DATA_SAVER for idle_time (NOT StrictIdlePolicy)
    """
    try:
        from apps.mqtt.simple_plant2 import (
            EXACT_REQUIREMENT_STATE as PLANT2_STATE,
            TOPIC_MACHINE_MAPPING
        )
        from apps.data_storage.hourly_data_saver import HOURLY_DATA_SAVER
        from django.db import connection
        import pytz
        from datetime import datetime, timedelta
        
        ist_tz = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist_tz)
        current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
        
        # Get all machines
        all_machines = set()
        for machines_list in TOPIC_MACHINE_MAPPING.values():
            all_machines.update(machines_list)
        
        saved_count = 0
        
        print(f"\n🔥 FRONTEND SAVE REQUEST at {now_ist.strftime('%H:%M:%S')}")
        
        for machine_no in sorted(all_machines):
            try:
                with PLANT2_STATE.lock:
                    # ✅ GET IDLE FROM HOURLY_DATA_SAVER (NOT StrictIdlePolicy!)
                    hourly_snapshot = HOURLY_DATA_SAVER.get_machine_snapshot(machine_no)
                    idle_time = hourly_snapshot.get('idle_total_minutes', 0)
                    
                    # Get other data
                    hour_count = PLANT2_STATE.current_hour_counts.get(machine_no, 0)
                    tool_id = PLANT2_STATE.previous_tool_id.get(machine_no, 'UNKNOWN')
                    shut_height = PLANT2_STATE.previous_shut_height.get(machine_no, 0.0)
                    
                    # Get timestamp
                    first_count_time = PLANT2_STATE.hour_first_count_time.get(machine_no)
                    if first_count_time:
                        save_timestamp = first_count_time
                    elif machine_no in PLANT2_STATE.machine_on_since:
                        save_timestamp = PLANT2_STATE.machine_on_since[machine_no]
                    else:
                        save_timestamp = current_hour
                    
                    # ✅ Direct database insert
                    naive_timestamp = save_timestamp.replace(tzinfo=None, microsecond=0)
                    db_shut_height = 0.0 if isinstance(shut_height, str) else float(shut_height) if shut_height else 0.0
                    
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO Plant2_data 
                            (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, [
                            naive_timestamp,
                            str(tool_id),
                            str(machine_no),
                            hour_count,
                            PLANT2_STATE.shift_cumulative.get((machine_no, 'A'), 0),
                            0,
                            idle_time,  # ✅ FROM HOURLY_DATA_SAVER
                            db_shut_height,
                            'A'
                        ])
                    
                    saved_count += 1
                    if idle_time > 0:
                        print(f"  ✅ M{machine_no}: count={hour_count}, idle={idle_time}min ✓")
                    
            except Exception as db_err:
                print(f"  ❌ M{machine_no}: {db_err}")
        
        # Reset
        with PLANT2_STATE.lock:
            for machine_no in all_machines:
                current_count = PLANT2_STATE.current_hour_counts.get(machine_no, 0)
                PLANT2_STATE.last_hour_counts[machine_no] = current_count
                PLANT2_STATE.current_hour_counts[machine_no] = 0
                PLANT2_STATE.current_hours[machine_no] = current_hour.hour
                
                if machine_no in PLANT2_STATE.hour_first_count_time:
                    del PLANT2_STATE.hour_first_count_time[machine_no]
        
        print(f"✅ Saved {saved_count} machines\n")
        
        return Response({
            "success": True,
            "saved_count": saved_count
        })
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return Response({
            "success": False,
            "error": str(e)
        }, status=500)


@never_cache
@api_view(['GET'])
def get_machine_changes_from_db(request):
    """Get tool/height changes from DATABASE - TODAY + CURRENT SHIFT"""
    try:
        from django.db import connection
        from datetime import datetime, date, time as dt_time
        import pytz
        
        # Get filters
        machine_no = request.GET.get('machine_no', None)
        
        # Current date and shift
        ist_tz = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist_tz)
        today = now_ist.date()
        
        # Shift times
        shift_A_start = dt_time(8, 30)
        shift_A_end = dt_time(20, 0)
        current_time = now_ist.time()
        current_shift = 'A' if shift_A_start <= current_time < shift_A_end else 'B'
        
        # Build query
        query = """
            WITH change_detection AS (
                SELECT 
                    machine_no,
                    timestamp,
                    tool_id,
                    shut_height,
                    shift,
                    LAG(tool_id) OVER (PARTITION BY machine_no ORDER BY timestamp) as prev_tool_id,
                    LAG(shut_height) OVER (PARTITION BY machine_no ORDER BY timestamp) as prev_shut_height
                FROM Plant2_data
                WHERE DATE(timestamp) = %s
                  AND shift = %s
        """
        
        params = [today, current_shift]
        
        if machine_no:
            query += " AND machine_no = %s"
            params.append(machine_no)
        
        query += """
            )
            SELECT 
                machine_no,
                timestamp,
                tool_id,
                shut_height,
                prev_tool_id,
                prev_shut_height
            FROM change_detection
            WHERE (tool_id != prev_tool_id OR ABS(shut_height - prev_shut_height) > 10)
              AND prev_tool_id IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 50
        """
        
        # Execute query
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        
        # Format results
        changes = []
        for idx, row in enumerate(rows, 1):
            m_no, ts, tool, height, prev_tool, prev_height = row
            
            messages = []
            tool_changed = (tool != prev_tool)
            height_changed = abs(height - prev_height) > 10
            
            if tool_changed:
                messages.append("Tool ID changed")
            
            if height_changed:
                messages.append(f"Shut Height: {prev_height:.2f} → {height:.2f}")
            
            changes.append({
                'id': idx,
                'machine_no': int(m_no),
                'time': ts.strftime('%H:%M:%S'),
                'timestamp': ts.isoformat(),
                'message': ' & '.join(messages),
                'tool_changed': tool_changed,
                'height_changed': height_changed,
                'old_tool': str(prev_tool)[:12] + '...' if len(str(prev_tool)) > 12 else str(prev_tool),
                'new_tool': str(tool)[:12] + '...' if len(str(tool)) > 12 else str(tool),
                'old_height': float(prev_height),
                'new_height': float(height)
            })
        
        return Response({
            'success': True,
            'changes': changes,
            'total': len(changes),
            'date': str(today),
            'shift': current_shift,
            'message': f"{len(changes)} changes in Shift {current_shift} today (from database)"
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return Response({
            'success': False,
            'error': str(e),
            'changes': []
        }, status=500)

# backend/api/views.py

@never_cache
@api_view(['GET'])
def test_direct_query(request):
    """Direct test of tid_map query"""
    
    test_tool_id = "e2004714e7b0682188780110"  # Machine 20
    
    print(f"\n{'='*60}")
    print(f"🧪 DIRECT TEST: Querying tid_map")
    print(f"{'='*60}")
    
    try:
        from django.db import connection
        
        with connection.cursor() as cursor:
            # Test 1: Direct query
            print(f"\n1️⃣ Testing exact match for: {test_tool_id}")
            cursor.execute("""
                SELECT 
                    customer,
                    model,
                    part_name,
                    tool_name,
                    epc,
                    part_number,
                    tpm
                FROM public.tid_map
                WHERE epc = %s
                LIMIT 1
            """, [test_tool_id])
            
            result = cursor.fetchone()
            
            if result:
                print(f"✅ SUCCESS! Found:")
                print(f"   Customer: {result[0]}")
                print(f"   Model: {result[1]}")
                print(f"   Part Name: {result[2]}")
                print(f"   Tool Name: {result[3]}")
                print(f"   EPC: {result[4]}")
                print(f"   Part Number: {result[5]}")
                print(f"   TPM: {result[6]}")
                
                response_data = {
                    'success': True,
                    'query_worked': True,
                    'result': {
                        'customer': result[0],
                        'model': result[1],
                        'part_name': result[2],
                        'tool_name': result[3],
                        'epc': result[4],
                        'part_number': result[5],
                        'tpm': result[6]
                    }
                }
            else:
                print(f"❌ NOT FOUND!")
                
                # Test 2: Check if table has data
                cursor.execute("SELECT COUNT(*) FROM public.tid_map")
                count = cursor.fetchone()[0]
                print(f"\n2️⃣ Total rows in tid_map: {count}")
                
                # Test 3: Show sample EPCs
                cursor.execute("SELECT epc FROM public.tid_map LIMIT 5")
                samples = cursor.fetchall()
                print(f"\n3️⃣ Sample EPCs in table:")
                for s in samples:
                    print(f"   - {s[0]}")
                
                response_data = {
                    'success': False,
                    'query_worked': True,
                    'result': None,
                    'total_rows': count,
                    'sample_epcs': [s[0] for s in samples]
                }
        
        print(f"{'='*60}\n")
        return Response(response_data)
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        return Response({
            'success': False,
            'query_worked': False,
            'error': str(e)
        }, status=500)


@never_cache  
@api_view(['GET'])
def machine_production_data(request):
    """ENHANCED Machine Production API with Smart Filtering"""
    try:
        # Get filters
        selected_date = request.GET.get('date', datetime.now().strftime('%Y-%m-%d'))
        selected_plant = request.GET.get('plant', 'plant1_data')
        selected_shift = request.GET.get('shift', '')
        selected_machine = request.GET.get('machine', '')
        start_hour = request.GET.get('start_hour', '')
        end_hour = request.GET.get('end_hour', '')
        
        print(f"🔧 Enhanced API Parameters:")
        print(f"   Date: {selected_date}")
        print(f"   Plant: {selected_plant}")
        print(f"   Machine: '{selected_machine}'")
        print(f"   Time: {start_hour}-{end_hour}")
        
        with connection.cursor() as cursor:
            # 🔥 SMART PRODUCTION CALCULATION
            if start_hour and end_hour:
                # Hour-specific production (count only)
                production_field = "SUM(count) as production_count"
                calculation_note = "Hour-specific count only"
            else:
                # Full day production (better logic)
                production_field = """
                    CASE 
                        WHEN MAX(cumulative_count) IS NOT NULL AND MAX(cumulative_count) > 0 
                        THEN MAX(cumulative_count)
                        ELSE SUM(CASE WHEN count IS NOT NULL THEN count ELSE 0 END)
                    END as production_count
                """
                calculation_note = "MAX(cumulative_count) OR SUM(count)"
            
            # Build query
            base_query = f"""
                SELECT 
                    machine_no,
                    COUNT(*) as total_entries,
                    {production_field},
                    MIN(timestamp) as first_entry,
                    MAX(timestamp) as last_entry,
                    STRING_AGG(DISTINCT shift, ', ') as shifts_worked
                FROM {selected_plant}
                WHERE DATE(timestamp) = %s
            """
            
            params = [selected_date]
            
            # Machine filter
            if selected_machine and selected_machine.strip():
                print(f"🔥 SPECIFIC MACHINE: {selected_machine}")
                base_query += " AND machine_no = %s"
                params.append(selected_machine)
            else:
                print(f"🔥 ALL MACHINES for {selected_plant}")
            
            # Shift filter
            if selected_shift and selected_shift.strip():
                base_query += " AND shift = %s"
                params.append(selected_shift)
            
            # Time filter
            if start_hour and start_hour.strip():
                base_query += " AND EXTRACT(HOUR FROM timestamp) >= %s"
                params.append(int(start_hour))
            
            if end_hour and end_hour.strip():
                base_query += " AND EXTRACT(HOUR FROM timestamp) <= %s"
                params.append(int(end_hour))
            
            base_query += " GROUP BY machine_no ORDER BY production_count DESC"
            
            # Limit only for all machines view
            if not (selected_machine and selected_machine.strip()):
                base_query += " LIMIT 50"
            
            print(f"🔧 FINAL QUERY: {base_query}")
            
            cursor.execute(base_query, params)
            results = cursor.fetchall()
            
            print(f"✅ Query results: {len(results)} machines")
            
            if not results:
                return Response({
                    'success': False,
                    'message': f'No data found for the selected filters. Please check if data exists for {selected_date}.',
                    'suggestion': 'Try different date or plant selection.',
                    'machine_data': []
                })
            
            # Build enhanced response
            machine_data = []
            for machine_no, entries, production, first_entry, last_entry, shifts in results:
                # Format times
                first_time = first_entry.strftime('%H:%M:%S') if first_entry else 'N/A'
                last_time = last_entry.strftime('%H:%M:%S') if last_entry else 'N/A'
                
                machine_data.append({
                    'machine_no': str(machine_no),
                    'machine_name': f'Machine {str(machine_no).zfill(2)}',
                    'production_count': int(production) if production else 0,
                    'total_entries': entries,
                    'working_hours': f"{first_time} - {last_time}",
                    'shifts_worked': shifts or 'N/A',
                    'status': 'Active' if production and production > 0 else 'Idle'
                })
            
            total_production = sum(m['production_count'] for m in machine_data)
            active_machines = len([m for m in machine_data if m['status'] == 'Active'])
            
            # Smart filter description
            filter_description = f"{selected_plant.upper()} on {selected_date}"
            if selected_machine:
                filter_description += f" | Machine {selected_machine}"
            if start_hour and end_hour:
                filter_description += f" | {start_hour}:00-{end_hour}:00"
            if selected_shift:
                filter_description += f" | Shift {selected_shift}"
            
            return Response({
                'success': True,
                'machine_data': machine_data,
                'summary': {
                    'total_production': total_production,
                    'total_machines': len(machine_data),
                    'active_machines': active_machines,
                    'idle_machines': len(machine_data) - active_machines,
                    'calculation_method': calculation_note,
                    'filter_description': filter_description
                },
                'filters_applied': {
                    'date': selected_date,
                    'plant': selected_plant,
                    'machine': selected_machine or 'All',
                    'shift': selected_shift or 'All',
                    'time_range': f"{start_hour or '00'}-{end_hour or '23'}"
                }
            })
            
    except Exception as e:
        print(f"❌ Enhanced Machine Production API error: {e}")
        return Response({
            'success': False,
            'error': 'Sorry, technical problem occurred. We can solve this as soon as possible.',
            'technical_details': str(e) if request.GET.get('debug') == 'true' else None
        }, status=500)

@never_cache
@api_view(['GET'])
def production_line_status_data(request):
    """Enhanced Production Line Status with Smart Filtering"""
    try:
        # Get filters
        selected_date = request.GET.get('date', datetime.now().strftime('%Y-%m-%d'))
        selected_plant = request.GET.get('plant', 'plant1_data')
        selected_shift = request.GET.get('shift', '')
        
        print(f"📋 Enhanced Production Line Status:")
        print(f"   Machine:")
        print(f"   Date: {selected_date}")
        print(f"   Plant: {selected_plant}")
        print(f"   Shift: {selected_shift}")
        
        # Machine count based on plant
        if selected_plant == 'plant1_data':
            total_machines = 57
            plant_name = "Manufacturing Plant 1"
        elif selected_plant == 'plant2_data':
            total_machines = 26
            plant_name = "Manufacturing Plant 2"
        else:
            total_machines = 50
            plant_name = "Default Plant"
        
        production_lines = []
        
        with connection.cursor() as cursor:
            # Get machines with data
            machine_query = f"""
                SELECT 
                    machine_no,
                    COUNT(*) as total_entries,
                    MAX(CASE WHEN cumulative_count IS NOT NULL THEN cumulative_count ELSE 0 END) as max_cumulative,
                    SUM(CASE WHEN count IS NOT NULL THEN count ELSE 0 END) as sum_count,
                    MAX(timestamp) as last_update,
                    AVG(CASE 
                        WHEN idle_time = 0 OR idle_time IS NULL THEN 85 
                        ELSE 50 
                    END) as efficiency,
                    STRING_AGG(DISTINCT shift, ', ') as shifts
                FROM {selected_plant}
                WHERE DATE(timestamp) = %s
            """
            
            params = [selected_date]
            
            if selected_shift:
                machine_query += " AND shift = %s"
                params.append(selected_shift)
            
            machine_query += " GROUP BY machine_no ORDER BY machine_no"
            
            cursor.execute(machine_query, params)
            results = cursor.fetchall()
            
            # Create machine data dictionary
            machine_dict = {}
            for machine_no, entries, max_cumulative, sum_count, last_update, efficiency, shifts in results:
                # Smart production calculation
                production = max(max_cumulative or 0, sum_count or 0)
                
                machine_dict[str(machine_no)] = {
                    'entries': entries,
                    'production': production,
                    'last_update': last_update,
                    'efficiency': efficiency,
                    'shifts': shifts
                }
            
            # Build response for all machines in plant
            for machine_no in range(1, total_machines + 1):
                machine_key = str(machine_no)
                
                if machine_key in machine_dict:
                    data = machine_dict[machine_key]
                    production = data['production']
                    efficiency = data['efficiency']
                    last_update = data['last_update']
                    shifts = data['shifts']
                    entries = data['entries']
                    
                    # Determine status
                    if production > 0:
                        if efficiency > 80:
                            status = 'Running'
                            status_color = 'success'
                        elif efficiency > 50:
                            status = 'Slow Operation'
                            status_color = 'warning'
                        else:
                            status = 'Low Performance'
                            status_color = 'warning'
                    else:
                        status = 'Idle'
                        status_color = 'danger'
                        efficiency = 0
                    
                    # Time difference
                    if last_update:
                        time_diff = datetime.now() - last_update.replace(tzinfo=None)
                        minutes_ago = int(time_diff.total_seconds() / 60)
                        
                        if minutes_ago < 5:
                            last_update_str = "Live"
                        elif minutes_ago < 60:
                            last_update_str = f"{minutes_ago} mins ago"
                        else:
                            hours_ago = int(minutes_ago / 60)
                            last_update_str = f"{hours_ago}h {minutes_ago % 60}m ago"
                    else:
                        last_update_str = "No data"
                else:
                    # No data for this machine
                    production = 0
                    efficiency = 0
                    status = 'Offline'
                    status_color = 'secondary'
                    last_update_str = "No data"
                    shifts = 'N/A'
                    entries = 0
                
                production_lines.append({
                    'machine_no': machine_no,
                    'machine_name': f'Production Unit Machine {str(machine_no).zfill(2)}',
                    'status': status,
                    'status_color': status_color,
                    'efficiency': round(efficiency, 1),
                    'production_count': int(production),
                    'total_entries': entries,
                    'last_update': last_update_str,
                    'shifts_worked': shifts,
                    'plant_section': f"{plant_name}"
                })
        
        # Calculate comprehensive summary
        total_production = sum(m['production_count'] for m in production_lines)
        running_machines = len([m for m in production_lines if m['status'] == 'Running'])
        slow_machines = len([m for m in production_lines if m['status'] == 'Slow Operation'])
        idle_machines = len([m for m in production_lines if m['status'] == 'Idle'])
        offline_machines = len([m for m in production_lines if m['status'] == 'Offline'])
        
        # Performance metrics
        overall_efficiency = sum(m['efficiency'] for m in production_lines) / total_machines if total_machines > 0 else 0
        active_machines = running_machines + slow_machines
        
        return Response({
            'success': True,
            'production_lines': production_lines,
            'plant_summary': {
                'plant_name': plant_name,
                'total_machines': total_machines,
                'total_production': total_production,
                'overall_efficiency': round(overall_efficiency, 1),
                'date': selected_date,
                'shift': selected_shift or 'All Shifts'
            },
            'machine_status_breakdown': {
                'running': running_machines,
                'slow_operation': slow_machines,
                'idle': idle_machines,
                'offline': offline_machines,
                'active_total': active_machines,
                'productivity_rate': round((active_machines / total_machines) * 100, 1) if total_machines > 0 else 0
            },
            'filters_applied': {
                'date': selected_date,
                'plant': selected_plant,
                'shift': selected_shift or 'All'
            },
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
    except Exception as e:
        print(f"❌ Enhanced Production Line Status API error: {e}")
        return Response({
            'success': False,
            'error': 'Sorry, technical problem occurred. We can solve this as soon as possible.',
            'suggestion': 'Please try again or contact technical support.',
            'technical_details': str(e) if request.GET.get('debug') == 'true' else None
        }, status=500)

# ========== NEW OPERATOR ASSIGNMENT APIs ==========

@api_view(['GET'])
def get_operators_by_plant(request):
    """Get operators for selected plant - alphabetically sorted"""
    try:
        plant = request.GET.get('plant', 'plant_2')
        
        if plant not in ['plant_1', 'plant_2']:
            return Response({
                'success': False,
                'message': 'Invalid plant. Use plant_1 or plant_2'
            }, status=400)
        
        operators = Operator.objects.filter(
            plant=plant,
            is_active=True
        ).order_by('name').values('id', 'name')
        
        return Response({
            'success': True,
            'plant': plant,
            'operators': list(operators),
            'count': len(operators)
        })
        
    except Exception as e:
        print(f"❌ Error fetching operators: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=500)


@api_view(['POST'])
def add_operator(request):
    """Add new operator from frontend"""
    try:
        name = request.data.get('name')
        plant = request.data.get('plant', 'plant_2')
        
        if not name or not name.strip():
            return Response({
                'success': False,
                'message': 'Operator name is required'
            }, status=400)
        
        if plant not in ['plant_1', 'plant_2']:
            return Response({
                'success': False,
                'message': 'Invalid plant. Use plant_1 or plant_2'
            }, status=400)
        
        # Check if operator already exists
        existing = Operator.objects.filter(
            name__iexact=name.strip(),
            plant=plant
        ).first()
        
        if existing:
            return Response({
                'success': False,
                'message': f'{name} already exists in {plant}'
            }, status=400)
        
        # Create new operator
        operator = Operator.objects.create(
            name=name.strip(),
            plant=plant
        )
        
        print(f"✅ New operator added: {operator.name} to {plant}")
        
        return Response({
            'success': True,
            'message': f'{name} added successfully to {plant}',
            'operator': {
                'id': operator.id,
                'name': operator.name,
                'plant': operator.plant
            }
        })
        
    except Exception as e:
        print(f"❌ Error adding operator: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=500)


@api_view(['GET'])
def get_machines_by_plant(request):
    """Get machine numbers based on plant"""
    try:
        plant = request.GET.get('plant', 'plant_2')
        
        if plant == 'plant_1':
            machines = list(range(1, 57))  # 1 to 56
            plant_name = 'Plant 1'
        elif plant == 'plant_2':
            machines = list(range(1, 21)) + list(range(41, 47))  # 1-20, 41-46
            plant_name = 'Plant 2'
        else:
            return Response({
                'success': False,
                'message': 'Invalid plant. Use plant_1 or plant_2'
            }, status=400)
        
        return Response({
            'success': True,
            'plant': plant,
            'plant_name': plant_name,
            'machines': machines,
            'count': len(machines)
        })
        
    except Exception as e:
        print(f"❌ Error fetching machines: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=500)


@api_view(['POST'])
def save_operator_assignment(request):
    """Save operator assignment to machine"""
    try:
        plant = request.data.get('plant')
        operator_name = request.data.get('operator_name')
        machine_no = request.data.get('machine_no')
        shift = request.data.get('shift')
        
        # Validation
        if not all([plant, operator_name, machine_no, shift]):
            return Response({
                'success': False,
                'message': 'All fields are required: plant, operator_name, machine_no, shift'
            }, status=400)
        
        if plant not in ['plant_1', 'plant_2']:
            return Response({
                'success': False,
                'message': 'Invalid plant'
            }, status=400)
        
        if shift not in ['A', 'B']:
            return Response({
                'success': False,
                'message': 'Invalid shift. Use A or B'
            }, status=400)
        
        # Check for duplicate assignment
        today = timezone.now().date()
        existing = OperatorAssignment.objects.filter(
            plant=plant,
            machine_no=str(machine_no),
            shift=shift,
            start_time__date=today
        ).first()
        
        if existing:
            return Response({
                'success': False,
                'message': f'Machine {machine_no} already assigned to {existing.operator_name} for Shift {shift} today'
            }, status=400)
        
        # Create assignment
        assignment = OperatorAssignment.objects.create(
            plant=plant,
            operator_name=operator_name,
            machine_no=str(machine_no),
            shift=shift
        )
        
        # Convert to IST for display
        local_time = timezone.localtime(assignment.start_time)
        
        print(f"✅ Assignment: {operator_name} → M{machine_no} ({shift}) in {plant}")
        
        return Response({
            'success': True,
            'message': f'{operator_name} assigned to Machine {machine_no}',
            'assignment': {
                'id': assignment.id,
                'plant': assignment.plant,
                'operator_name': assignment.operator_name,
                'machine_no': assignment.machine_no,
                'shift': assignment.shift,
                'start_time': local_time.strftime('%Y-%m-%d %I:%M:%S %p IST')
            }
        })
        
    except Exception as e:
        print(f"❌ Error saving assignment: {e}")
        import traceback
        traceback.print_exc()
        return Response({
            'success': False,
            'error': str(e)
        }, status=500)


@api_view(['GET'])
def get_operator_assignments(request):
    """Get operator assignments with filtering"""
    try:
        plant = request.GET.get('plant')
        operator_name = request.GET.get('operator_name')
        shift = request.GET.get('shift')
        limit = int(request.GET.get('limit', 50))
        
        # Build query
        queryset = OperatorAssignment.objects.all()
        
        if plant:
            queryset = queryset.filter(plant=plant)
        
        if operator_name:
            queryset = queryset.filter(operator_name__icontains=operator_name)
        
        if shift:
            queryset = queryset.filter(shift=shift)
        
        # Get results
        assignments = queryset.order_by('-created_at')[:limit]
        
        # Format data
        data = []
        for a in assignments:
            local_start = timezone.localtime(a.start_time)
            
            data.append({
                'id': a.id,
                'plant': a.plant,
                'operator_name': a.operator_name,
                'machine_no': a.machine_no,
                'shift': a.shift,
                'start_time': local_start.strftime('%Y-%m-%d %I:%M:%S %p'),
                'date': local_start.strftime('%Y-%m-%d')
            })
        
        return Response({
            'success': True,
            'assignments': data,
            'count': len(data)
        })
        
    except Exception as e:
        print(f"❌ Error fetching assignments: {e}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=500)




@api_view(['GET'])
def plant2_hourly_idle(request):
    """
    Get Plant 2 hourly idle time data
    Query params: date, shift, machine_no, start_hour, end_hour
    """
    try:
        # Get filters
        selected_date = request.GET.get('date', datetime.now().strftime('%Y-%m-%d'))
        selected_shift = request.GET.get('shift', None)
        selected_machine = request.GET.get('machine', None)
        start_hour = request.GET.get('start_hour', None)
        end_hour = request.GET.get('end_hour', None)

        # Build query
        queryset = Plant2HourlyIdletime.objects.filter(
            timestamp__date=selected_date
        )

        # Apply filters
        if selected_shift:
            queryset = queryset.filter(shift=selected_shift)
        
        if selected_machine:
            queryset = queryset.filter(machine_no=selected_machine)
        
        if start_hour:
            queryset = queryset.filter(timestamp__hour__gte=int(start_hour))
        
        if end_hour:
            queryset = queryset.filter(timestamp__hour__lte=int(end_hour))

        # Get data
        data = queryset.values(
            'timestamp',
            'machine_no',
            'tool_id',
            'idle_time',
            'shut_height',
            'shift'
        ).order_by('-timestamp')

        # Format response
        idle_data = []
        for record in data:
            idle_data.append({
                'timestamp': record['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'machine_no': record['machine_no'],
                'tool_id': record['tool_id'],
                'idle_time': record['idle_time'],
                'shut_height': str(record['shut_height']),
                'shift': record['shift'],
                'hour': record['timestamp'].hour
            })

        return Response({
            'success': True,
            'count': len(idle_data),
            'data': idle_data,
            'filters': {
                'date': selected_date,
                'shift': selected_shift or 'All',
                'machine': selected_machine or 'All',
                'hours': f"{start_hour or '00'}-{end_hour or '23'}"
            }
        })

    except Exception as e:
        print(f"❌ API Error: {e}")
        import traceback
        traceback.print_exc()
        return Response({
            'success': False,
            'error': str(e),
            'data': []
        }, status=500)


@api_view(['GET'])
def plant2_hourly_idle_summary(request):
    """
    Get hourly idle summary for all machines
    Query params: date, shift
    """
    try:
        selected_date = request.GET.get('date', datetime.now().strftime('%Y-%m-%d'))
        selected_shift = request.GET.get('shift', None)

        # Build query
        query = """
            SELECT 
                machine_no,
                SUM(idle_time) as total_idle,
                COUNT(*) as hours_recorded,
                MAX(timestamp) as last_update,
                MAX(shift) as shift
            FROM "Plant2_hourly_idle"
            WHERE DATE(timestamp) = %s
        """
        params = [selected_date]

        if selected_shift:
            query += " AND shift = %s"
            params.append(selected_shift)

        query += """
            GROUP BY machine_no
            ORDER BY machine_no
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()

        summary_data = []
        for row in results:
            machine_no, total_idle, hours_recorded, last_update, shift = row
            summary_data.append({
                'machine_no': machine_no,
                'total_idle_minutes': total_idle,
                'hours_recorded': hours_recorded,
                'last_update': last_update.strftime('%Y-%m-%d %H:%M:%S') if last_update else 'N/A',
                'shift': shift or 'A'
            })

        return Response({
            'success': True,
            'count': len(summary_data),
            'data': summary_data,
            'date': selected_date,
            'shift': selected_shift or 'All'
        })

    except Exception as e:
        print(f"❌ Summary API Error: {e}")
        import traceback
        traceback.print_exc()
        return Response({
            'success': False,
            'error': str(e),
            'data': []
        }, status=500)
        






from django.http import JsonResponse
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

# Import All Relevant Models and Serializer
from .models import InspectionReport, L1_PartInfoMaster, L2_ProcessReportMaster, L3_ParameterDetailMaster
from .serializers import InspectionReportSerializer


# ==================================================
# 🟢 1. DROPDOWN API (Connects to L1 and L2 Models)
# ==================================================
class MasterDropdownView(APIView):
    def get(self, request):
        filter_type = request.query_params.get('filter') 
        
        if filter_type == 'customer':
            data = L1_PartInfoMaster.objects.values_list('customer_name', flat=True).distinct()
            return Response(list(data))
            
        elif filter_type == 'part':
            cust = request.query_params.get('cust')
            data = L1_PartInfoMaster.objects.filter(customer_name=cust).values_list('part_name', flat=True).distinct()
            return Response(list(data))

        elif filter_type == 'operation':
            cust = request.query_params.get('cust')
            part = request.query_params.get('part')
            ops = L2_ProcessReportMaster.objects.filter(
                part_info__customer_name=cust, 
                part_info__part_name=part
            ).values_list('report_name', flat=True).distinct()
            return Response(list(ops))
        
        return Response([])


# ==================================================
# 🟢 2. AUTO-FILL PARAMETERS API (Connects to L3 Model)
# ==================================================
class MasterParametersView(APIView):
    def get(self, request):
        cust = request.query_params.get('customer')
        part = request.query_params.get('part')
        op_name = request.query_params.get('operation')
        
        if not all([cust, part, op_name]):
            return Response({"error": "Missing filters"}, status=400)

        process = L2_ProcessReportMaster.objects.filter(
            part_info__customer_name=cust,
            part_info__part_name=part,
            report_name=op_name
        ).first()

        if not process: 
            return Response({"error": "Process Not Found in Master Data"}, status=404)

        params = L3_ParameterDetailMaster.objects.filter(process_report=process).order_by('id')
        
        product_list = []
        process_list = []
        prod_sr = 1
        proc_sr = 11

        for p in params:
            raw_spec = p.specification or ""
            final_spec = raw_spec
            final_tol = "-"

            if "±" in raw_spec:
                parts = raw_spec.split("±", 1)
                final_spec = parts[0].strip()          
                final_tol = "± " + parts[1].strip()    
                
            elif "+" in raw_spec:
                parts = raw_spec.split("+", 1)
                final_spec = parts[0].strip()
                final_tol = "+" + parts[1].strip()

            item_data = {
                "item": p.parameter_name,
                "spec": final_spec,
                "tol": final_tol,
                "instr": p.instrument,
                "category": p.category
            }

            if p.category == 'PRODUCT':
                item_data['sr_no'] = prod_sr
                product_list.append(item_data)
                prod_sr += 1
            else:
                item_data['sr_no'] = proc_sr
                process_list.append(item_data)
                proc_sr += 1

        return Response({
            "productItems": product_list,
            "processItems": process_list,
            "part_number": process.part_info.part_no, 
            "model_name": process.part_info.model_name
        })


# ==================================================
# 🟢 3. SAVE INSPECTION REPORT API (Fixed for your Model)
# ==================================================
class SaveInspectionReportView(APIView):
    def post(self, request):
        try:
            data = request.data
            master = data.get('master_data', {})
            logs = data.get('logs', [])
            
            date_val = master.get('date') or timezone.now().date()
            cust = master.get('customer', 'Unknown')
            part = master.get('part_name', 'Unknown')
            op = master.get('operation', 'Unknown')
            part_no = master.get('part_number', 'N/A')
            plant = master.get('plant_location', 'PLANT 1')

            # Get the latest operator and machine from the currently active log
            current_operator = logs[-1].get('operator', 'Unknown') if logs else 'Unknown'
            current_machine = logs[-1].get('machine', 'N/A') if logs else 'N/A'

            # 🔥 SMART LOGIC: Check if report exists for today
            report, created = InspectionReport.objects.get_or_create(
                customer_account=cust,
                part_name=part,
                operation=op,
                inspection_date=date_val,
                defaults={
                    'part_number': part_no,
                    'plant_location': plant,
                    'operator_name': current_operator, # Required by your model
                    'machine_number': current_machine, # Required by your model
                    'inspection_data': {}
                }
            )

            # Update top level info if it changed
            report.operator_name = current_operator
            report.machine_number = current_machine

            # 🔥 THE MAGIC: Save ALL stages (Setup, 4Hr, Last) cleanly into JSONField
            report.inspection_data = {
                "parameters": data.get('parameters', []),
                "logs": logs
            }
            
            # Save to Database
            report.save()

            if created:
                msg = "✅ New Report Created Successfully!"
            else:
                msg = "✅ Report Updated Successfully! (New Stage Added)"

            return Response({"message": msg, "report_id": report.id}, status=status.HTTP_200_OK)

        except Exception as e:
            print("Django Error: ", str(e)) 
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# ==================================================
# 🟢 4. FETCH PREVIOUS REPORT API
# ==================================================
class GetInspectionReportView(APIView):
    def get(self, request):
        customer = request.query_params.get('customer', None)
        part_name = request.query_params.get('part_name', None)
        operation = request.query_params.get('operation', None)
        date = request.query_params.get('date', None)

        filters = {}
        if customer: filters['customer_account__icontains'] = customer
        if part_name: filters['part_name__icontains'] = part_name
        if operation: filters['operation__icontains'] = operation
        if date: filters['inspection_date'] = date

        reports = InspectionReport.objects.filter(**filters).order_by('-id')

        if reports.exists():
             latest_report = reports.first() 
             serializer = InspectionReportSerializer(latest_report)
             return Response(serializer.data, status=status.HTTP_200_OK)
        else:
             return Response({"message": "No report found for given filters"}, status=status.HTTP_404_NOT_FOUND)
         
         
# ==================================================
# 🟢 5. SAVE DAILY MACHINE CHECK SHEET (POKA-YOKE)
# ==================================================
class SaveMachineChecksheetView(APIView):
    @transaction.atomic  # 
    def post(self, request):
        try:
            data = request.data
            
            # 1. Main Report Create Karo (Parent Table)
            report = MachineChecksheetReport.objects.create(
                date=data.get('date', timezone.now().date()),
                plant_name=data.get('plant_name', 'Plant 1'),
                machine_no=data.get('machine_no', ''),
                checked_by_maintenance=data.get('checked_by_maintenance', ''),
                verified_by_production=data.get('verified_by_production', '')
            )

            # 2. Check Parameters (Poka-Yoke details) Extract Karo (Child Table)
            check_points_data = data.get('check_points', [])
            observations = []
            
            # Loop chala kar saare points ko list mein daalo
            for index, item in enumerate(check_points_data):
                observations.append(
                    MachineChecksheetObservation(
                        report=report,
                        s_no=item.get('s_no', index + 1), # Agar frontend se s_no nahi aaya toh loop ka number le lega
                        poka_yoke_detail=item.get('poka_yoke_detail', ''),
                        checking_method=item.get('checking_method', ''),
                        reference_sop=item.get('reference_sop', ''),
                        is_ok=item.get('is_ok', True),
                        remarks=item.get('remarks', '')
                    )
                )
            
            # Bulk create: Ek hi baar mein saare parameters database mein save kar dega (Fast performance)
            if observations:
                MachineChecksheetObservation.objects.bulk_create(observations)

            return Response({
                "success": True, 
                "message": "✅ Daily Checksheet Saved Successfully!", 
                "report_id": report.id
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            print("❌ Django Error (Checksheet Save): ", str(e))
            import traceback
            traceback.print_exc()
            return Response({
                "success": False, 
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
@never_cache
@api_view(['GET'])
def get_today_pokayoke_data(request):
    """Simple version - No observations/check_points"""
    try:
        plant_name = request.GET.get('plant_name')
        date_str = request.GET.get('date')

        if not plant_name:
            return Response({'success': False, 'error': 'plant_name is required'}, status=400)

        # Safe date handling
        if date_str:
            try:
                filter_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                filter_date = datetime.now().date()
        else:
            filter_date = datetime.now().date()

        # Get only main report data (no related observations)
        queryset = MachineChecksheetReport.objects.filter(
            plant_name=plant_name,
            date=filter_date
        ).order_by('-created_at')

        data_list = []
        for report in queryset:
            data_list.append({
                'id': report.id,
                'date': str(report.date),
                'plant_name': report.plant_name,
                'machine_no': report.machine_no,
                'checked_by_maintenance': report.checked_by_maintenance or 'Not provided',
                'verified_by_production': report.verified_by_production or 'Not provided',
            })

        return Response({
            'success': True,
            'count': len(data_list),
            'data': data_list,
            'plant_name': plant_name,
            'date': str(filter_date)
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return Response({
            'success': False,
            'error': str(e),
            'data': []
        }, status=500)