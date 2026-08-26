from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.db import transaction
from django.utils import timezone
from django.db import connection
from .models import (
    MachineHistoryCard,
    OperatorAssignment,
    IdleReport,
    IdealTimeSegmentReason,
)
from datetime import datetime, timedelta, time as dt_time

# from apps.mqtt.mqtt_client import PLANT1_TOPICS, PLANT2_TOPICS
from django.views.decorators.cache import cache_control, never_cache
from apps.machines.machine_map import COUNT52_GROUP
from apps.machines.machine_state import MACHINE_STATE
from .models import Plant2HourlyIdletime
from apps.mqtt.simple_plant2 import EXACT_REQUIREMENT_STATE
from .models import Operator, OperatorAssignment

# from apps.data_storage.hourly_idle_tracker import HOURLY_IDLE_TRACKERER
from rest_framework.views import APIView
from rest_framework import status
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import pytz
import traceback
from django.shortcuts import get_object_or_404

from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework_simplejwt.views import TokenObtainPairView
from django.db import connections



@api_view(["GET"])
def get_dashboard_data(request):
    """Get dashboard data with filters: date, shift, plant selection"""
    try:
        # Get query parameters
        selected_date = request.GET.get("date", None)
        selected_shift = request.GET.get("shift", None)
        selected_plant = request.GET.get("plant", "plant1_data")  # Default plant1_data

        # Default to today if no date provided
        if not selected_date:
            selected_date = datetime.now().strftime("%Y-%m-%d")

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

                detail_query += (
                    " GROUP BY machine_no, shift ORDER BY last_update DESC LIMIT 1"
                )

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
                    status = status_result[0] if status_result else "Unknown"

                    machine_details.append(
                        {
                            "id": machine_no,
                            "name": f"Machine {machine_no}",
                            "status": status,
                            "efficiency": round(efficiency or 0),
                            "production": production or 0,
                            "last_update": (
                                str(last_update)[:19] if last_update else "N/A"
                            ),
                            "shift": shift or "A",
                        }
                    )

                    total_production += production or 0
                    efficiency_sum += efficiency or 0

            # Calculate average efficiency
            avg_efficiency = (
                round(efficiency_sum / len(active_machines)) if active_machines else 0
            )

            return Response(
                {
                    "success": True,
                    "dashboard_data": {
                        "total_machines": total_machines,
                        "running_machines": running_machines,
                        "avg_efficiency": avg_efficiency,
                        "total_production": total_production,
                        "active_machines": active_machines,
                        "machine_details": machine_details,
                        "selected_date": selected_date,
                        "selected_shift": selected_shift or "All",
                        "selected_plant": selected_plant,
                        "last_updated": f"Data for {selected_date}",
                    },
                }
            )

    except Exception as e:
        return Response(
            {"success": False, "message": f"Error fetching dashboard data: {str(e)}"},
            status=400,
        )


@api_view(["GET"])
def get_available_dates(request):
    """Get available dates from selected plant table"""
    try:
        selected_plant = request.GET.get("plant", "plant1_data")

        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT DISTINCT DATE(timestamp) as available_date
                FROM {selected_plant}
                WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY available_date DESC
                LIMIT 30
            """)

            dates = [row[0].strftime("%Y-%m-%d") for row in cursor.fetchall()]

            return Response({"success": True, "available_dates": dates})

    except Exception as e:
        return Response(
            {"success": False, "message": f"Error fetching dates: {str(e)}"}, status=400
        )


@api_view(["POST"])
def create_assignment(request):
    """AssignMachine.js se data receive karne ke liye"""
    try:
        data = request.data

        assignment = OperatorAssignment.objects.create(
            machine_no=data["machine_no"],
            operator_name=data["operator_name"],
            shift=data["shift"],
            start_time=data["start_time"],
        )

        return Response(
            {
                "success": True,
                "message": "Assignment saved successfully!",
                "assignment_id": assignment.id,
            }
        )
    except Exception as e:
        return Response({"success": False, "message": f"Error: {str(e)}"}, status=400)


@api_view(["GET"])
def get_auto_fill_data(request, machine_no):
    """IdleCase.js ke liye auto-fill data"""
    try:
        # Operator name from operator_assignments
        try:
            latest_assignment = OperatorAssignment.objects.filter(
                machine_no=machine_no
            ).latest("created_at")
            operator_name = latest_assignment.operator_name
        except OperatorAssignment.DoesNotExist:
            operator_name = "Auto Operator"

        # Tool ID from plant1_data (you can make this dynamic too)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT tool_id 
                FROM plant1_data 
                WHERE machine_no = %s 
                ORDER BY timestamp DESC 
                LIMIT 1
            """,
                [machine_no],
            )

            result = cursor.fetchone()
            tool_id = result[0] if result else "Unknown Tool"

        return Response(
            {
                "success": True,
                "machine_no": machine_no,
                "operator_name": operator_name,
                "tool_id": tool_id,
            }
        )
    except Exception as e:
        return Response({"success": False, "message": f"Error: {str(e)}"}, status=400)


@api_view(["POST"])
def create_idle_report(request):
    """IdleCase.js se data receive karne ke liye"""
    try:
        data = request.data

        report = IdleReport.objects.create(
            machine_no=data["machine_no"],
            operator_name=data["operator_name"],
            tool_id=data["tool_name"],
            reason=data["reason"],
        )

        return Response(
            {
                "success": True,
                "message": "Idle report saved successfully!",
                "report_id": report.id,
            }
        )
    except Exception as e:
        return Response({"success": False, "message": f"Error: {str(e)}"}, status=400)


@never_cache
@api_view(["GET"])
def get_pending_ideal_reports(request):
    """
    Common pending Ideal Report API for Plant 1 + Plant 2.

    Handles:
    - separate Ideal events independently
    - same-machine multiple events
    - HOUR_CHANGE pieces as one logical Ideal event
    """

    try:
        ist = pytz.timezone("Asia/Kolkata")

        def db_ist_iso(dt):
            if not dt:
                return None

            naive_dt = dt.replace(tzinfo=None)
            return ist.localize(naive_dt).isoformat()

        # --------------------------------------------------
        # 1. Plant validation
        # --------------------------------------------------

        plant = str(request.GET.get("plant", "")).strip().lower()

        plant_map = {
            "1": "Plant 1",
            "plant1": "Plant 1",
            "plant_1": "Plant 1",
            "plant 1": "Plant 1",
            "2": "Plant 2",
            "plant2": "Plant 2",
            "plant_2": "Plant 2",
            "plant 2": "Plant 2",
        }

        plant_location = plant_map.get(plant)

        if not plant_location:
            return Response(
                {
                    "success": False,
                    "message": ("Valid plant is required. " "Use plant=1 or plant=2."),
                },
                status=400,
            )

        # --------------------------------------------------
        # 2. Base DB query
        # --------------------------------------------------

        machine_no = request.GET.get("machine_no")

        pending_events = IdealTimeSegmentReason.objects.filter(
            plant_location=plant_location,
            report_status="PENDING",
            ideal_time__gte=180,
        )

        if machine_no:
            try:
                machine_no = int(machine_no)

            except (TypeError, ValueError):
                return Response(
                    {
                        "success": False,
                        "message": "machine_no must be a valid number.",
                    },
                    status=400,
                )

            pending_events = pending_events.filter(machine_no=machine_no)

        pending_events = list(
            pending_events.only(
                "id",
                "plant_location",
                "machine_no",
                "ideal_mode",
                "ideal_start_at",
                "ideal_end_at",
                "ideal_time",
                "closed_by",
                "reason",
                "specific_reason",
                "remark",
                "shift",
                "report_status",
            ).order_by(
                "machine_no",
                "ideal_mode",
                "ideal_start_at",
                "id",
            )
        )

        # --------------------------------------------------
        # 3. Build logical Ideal events
        #
        # Example:
        #
        # 13:33 -> 14:00 HOUR_CHANGE
        # 14:00 -> 14:09 COUNT_RESUME
        #
        # becomes ONE report/event.
        # --------------------------------------------------

        grouped_events = []
        current_group = None

        def finish_group(group):
            if not group:
                return

            # HOUR_CHANGE means the physical Ideal event
            # is still continuing.
            #
            # Do NOT show it to user until we get the
            # real closing piece.
            if group["closed_by"] == "HOUR_CHANGE":
                return

            grouped_events.append(group)

        for event in pending_events:

            if current_group is None:
                current_group = {
                    "event_id": event.id,
                    # All physical DB rows belonging
                    # to this one logical Ideal event.
                    "segment_ids": [event.id],
                    "plant_location": event.plant_location,
                    "machine_no": event.machine_no,
                    "ideal_mode": event.ideal_mode,
                    "ideal_start_at": event.ideal_start_at,
                    "ideal_end_at": event.ideal_end_at,
                    "duration_seconds": event.ideal_time,
                    "closed_by": event.closed_by,
                    "reason": event.reason,
                    "specific_reason": event.specific_reason,
                    "remark": event.remark or "",
                    "shift": event.shift,
                    "report_status": event.report_status,
                }

                continue

            # --------------------------------------------------
            # SAME physical Ideal incident?
            #
            # Previous segment must:
            # - belong to same machine
            # - same plant
            # - same ONLINE/OFFLINE mode
            # - end exactly where next starts
            # - have been split only because hour changed
            # --------------------------------------------------

            can_merge = (
                current_group["plant_location"] == event.plant_location
                and current_group["machine_no"] == event.machine_no
                and current_group["ideal_mode"] == event.ideal_mode
                and current_group["ideal_end_at"] == event.ideal_start_at
                and current_group["closed_by"] == "HOUR_CHANGE"
            )

            if can_merge:

                current_group["segment_ids"].append(event.id)

                current_group["ideal_end_at"] = event.ideal_end_at

                current_group["duration_seconds"] += event.ideal_time

                # Last segment tells us how actual
                # Ideal incident finally ended.
                current_group["closed_by"] = event.closed_by

                current_group["reason"] = event.reason

                current_group["specific_reason"] = event.specific_reason

                current_group["remark"] = event.remark or ""

            else:

                finish_group(current_group)

                current_group = {
                    "event_id": event.id,
                    "segment_ids": [event.id],
                    "plant_location": event.plant_location,
                    "machine_no": event.machine_no,
                    "ideal_mode": event.ideal_mode,
                    "ideal_start_at": event.ideal_start_at,
                    "ideal_end_at": event.ideal_end_at,
                    "duration_seconds": event.ideal_time,
                    "closed_by": event.closed_by,
                    "reason": event.reason,
                    "specific_reason": event.specific_reason,
                    "remark": event.remark or "",
                    "shift": event.shift,
                    "report_status": event.report_status,
                }

        finish_group(current_group)

        # --------------------------------------------------
        # 4. API response
        # --------------------------------------------------

        reports = []

        for event in grouped_events:

            reports.append(
                {
                    # Canonical logical event ID
                    "event_id": event["event_id"],
                    # Important for debugging/testing
                    "segment_ids": event["segment_ids"],
                    "segment_count": len(event["segment_ids"]),
                    "plant_location": (event["plant_location"]),
                    "machine_no": event["machine_no"],
                    "ideal_mode": event["ideal_mode"],
                    "ideal_start_at": db_ist_iso(event["ideal_start_at"]),
                    "ideal_end_at": db_ist_iso(event["ideal_end_at"]),
                    "duration_seconds": (event["duration_seconds"]),
                    "duration_minutes": round(
                        event["duration_seconds"] / 60,
                        2,
                    ),
                    "closed_by": event["closed_by"],
                    "reason": event["reason"],
                    "specific_reason": (event["specific_reason"]),
                    "remark": event["remark"],
                    "shift": event["shift"],
                    "report_status": (event["report_status"]),
                }
            )

        return Response(
            {
                "success": True,
                "plant": plant_location,
                "count": len(reports),
                "pending_reports": reports,
            },
            status=200,
        )

    except Exception as e:

        print(f"❌ Pending Ideal Reports API Error: {e}")

        traceback.print_exc()

        return Response(
            {
                "success": False,
                "message": str(e),
            },
            status=500,
        )


@api_view(["POST"])
def submit_ideal_report(request, event_id):
    """
    Submit one logical Ideal event.

    Handles:
    - Plant 1 + Plant 2
    - duplicate protection
    - concurrent submissions
    - HOUR_CHANGE split segments
    - one IdleReport per logical Ideal event
    """

    try:
        data = request.data

        plant_no = str(data.get("plant_no", "")).strip()
        machine_no = str(data.get("machine_no", "")).strip()
        operator_name = str(data.get("operator_name", "")).strip()
        tool_name = str(data.get("tool_name", "")).strip()
        reason = str(data.get("reason", "")).strip()

        # ==================================================
        # 1. Validate incoming form
        # ==================================================

        if not operator_name:
            return Response(
                {
                    "success": False,
                    "message": "operator_name is required.",
                },
                status=400,
            )

        if not tool_name:
            return Response(
                {
                    "success": False,
                    "message": "tool_name is required.",
                },
                status=400,
            )

        valid_reasons = {choice[0] for choice in IdleReport.IDLE_REASON_CHOICES}

        if reason not in valid_reasons:
            return Response(
                {
                    "success": False,
                    "message": "Invalid idle reason.",
                },
                status=400,
            )

        # ==================================================
        # 2. Find requested segment first
        # ==================================================

        try:
            seed_event = IdealTimeSegmentReason.objects.get(pk=event_id)

        except IdealTimeSegmentReason.DoesNotExist:
            return Response(
                {
                    "success": False,
                    "message": f"Ideal event {event_id} not found.",
                },
                status=404,
            )

        # ==================================================
        # 3. Find FIRST segment of same logical event
        #
        # Example:
        #
        # 1048: 13:33 -> 14:00 HOUR_CHANGE
        # 1099: 14:00 -> 14:09 COUNT_RESUME
        #
        # If API receives 1099 directly,
        # we still resolve it back to 1048.
        # ==================================================

        first_segment = seed_event

        while True:

            previous_segment = (
                IdealTimeSegmentReason.objects.filter(
                    plant_location=first_segment.plant_location,
                    machine_no=first_segment.machine_no,
                    ideal_mode=first_segment.ideal_mode,
                    ideal_end_at=first_segment.ideal_start_at,
                    closed_by="HOUR_CHANGE",
                )
                .exclude(pk=first_segment.pk)
                .order_by(
                    "-ideal_start_at",
                    "-id",
                )
                .first()
            )

            if not previous_segment:
                break

            first_segment = previous_segment

        # ==================================================
        # 4. Build entire logical event forward
        # ==================================================

        logical_segments = [first_segment]

        current_segment = first_segment

        while current_segment.closed_by == "HOUR_CHANGE":

            next_segment = (
                IdealTimeSegmentReason.objects.filter(
                    plant_location=current_segment.plant_location,
                    machine_no=current_segment.machine_no,
                    ideal_mode=current_segment.ideal_mode,
                    ideal_start_at=current_segment.ideal_end_at,
                )
                .exclude(pk=current_segment.pk)
                .order_by(
                    "ideal_start_at",
                    "id",
                )
                .first()
            )

            if not next_segment:
                break

            logical_segments.append(next_segment)

            current_segment = next_segment

        segment_ids = [segment.id for segment in logical_segments]

        canonical_event_id = logical_segments[0].id

        # ==================================================
        # 5. If last row is HOUR_CHANGE,
        # event is still not finally closed.
        # ==================================================

        last_segment = logical_segments[-1]

        if last_segment.closed_by == "HOUR_CHANGE":
            return Response(
                {
                    "success": False,
                    "message": (
                        "This Ideal event is still continuing "
                        "and cannot be submitted yet."
                    ),
                    "event_id": canonical_event_id,
                    "segment_ids": segment_ids,
                },
                status=409,
            )

        # ==================================================
        # 6. ATOMIC TRANSACTION + deterministic row locking
        # ==================================================

        with transaction.atomic():

            # Lock every physical segment belonging to
            # this logical event.
            #
            # order_by("id") gives consistent lock order
            # and reduces deadlock risk.
            locked_segments = list(
                IdealTimeSegmentReason.objects.select_for_update()
                .filter(id__in=segment_ids)
                .order_by("id")
            )

            if len(locked_segments) != len(segment_ids):
                return Response(
                    {
                        "success": False,
                        "message": (
                            "One or more Ideal event segments " "could not be found."
                        ),
                    },
                    status=409,
                )

            # ==============================================
            # 7. Protect LEGACY
            # ==============================================

            if any(segment.report_status == "LEGACY" for segment in locked_segments):
                return Response(
                    {
                        "success": False,
                        "message": ("Legacy Ideal event cannot be submitted."),
                        "event_id": canonical_event_id,
                    },
                    status=400,
                )

            # ==============================================
            # 8. Duplicate/concurrent protection
            # ==============================================

            already_submitted = next(
                (
                    segment
                    for segment in locked_segments
                    if segment.report_status == "SUBMITTED"
                ),
                None,
            )

            if already_submitted:

                return Response(
                    {
                        "success": False,
                        "message": ("This Ideal report has already " "been submitted."),
                        "event_id": canonical_event_id,
                        "segment_ids": segment_ids,
                        "report_status": "SUBMITTED",
                        "submitted_by": (already_submitted.submitted_by),
                        "submitted_at": (
                            already_submitted.submitted_at.isoformat()
                            if already_submitted.submitted_at
                            else None
                        ),
                    },
                    status=409,
                )

            # ==============================================
            # 9. Plant validation
            # ==============================================

            canonical_segment = logical_segments[0]

            expected_plant_no = (
                "1" if canonical_segment.plant_location == "Plant 1" else "2"
            )

            normalized_plant_no = (
                plant_no.lower().replace("plant", "").replace("_", "").replace(" ", "")
            )

            if normalized_plant_no not in ("1", "2"):
                return Response(
                    {
                        "success": False,
                        "message": "Valid plant_no is required.",
                    },
                    status=400,
                )

            if normalized_plant_no != expected_plant_no:
                return Response(
                    {
                        "success": False,
                        "message": (
                            f"Event {canonical_event_id} belongs "
                            f"to {canonical_segment.plant_location}."
                        ),
                    },
                    status=400,
                )

            # ==============================================
            # 10. Machine validation
            # ==============================================

            if machine_no != str(canonical_segment.machine_no):
                return Response(
                    {
                        "success": False,
                        "message": (
                            f"Event {canonical_event_id} belongs "
                            f"to Machine "
                            f"{canonical_segment.machine_no}."
                        ),
                    },
                    status=400,
                )

            # ==============================================
            # 11. Create ONE IdleReport only
            # ==============================================

            idle_report = IdleReport.objects.create(
                plant=f"plant_{expected_plant_no}",
                machine_no=str(canonical_segment.machine_no),
                operator_name=operator_name,
                tool_id=tool_name,
                reason=reason,
            )

            # ==============================================
            # 12. Submitted by
            # ==============================================

            if (
                hasattr(request, "user")
                and request.user
                and request.user.is_authenticated
            ):
                submitted_by = request.user.username

            else:
                submitted_by = operator_name

            submitted_at = timezone.now()

            # ==============================================
            # 13. Mark ALL segments submitted together
            # ==============================================

            updated_count = IdealTimeSegmentReason.objects.filter(
                id__in=segment_ids,
                report_status="PENDING",
            ).update(
                reason=reason,
                report_status="SUBMITTED",
                submitted_by=submitted_by,
                submitted_at=submitted_at,
            )

            # Extra safety
            if updated_count != len(segment_ids):
                raise RuntimeError("Not all Ideal event segments " "were updated.")

        # ==================================================
        # 14. Success
        # ==================================================
        
                # ==================================================
        # 14. Notify all open browsers AFTER DB COMMIT
        # ==================================================

        try:
            channel_layer = get_channel_layer()

            if channel_layer:

                if expected_plant_no == "1":
                    group_name = "plant1_live_updates"
                else:
                    group_name = "plant2_live_updates"

                async_to_sync(
                    channel_layer.group_send
                )(
                    group_name,
                    {
                        "type": "send_machine_update",
                        "message": {
                            "event_type": "ideal_report_updated",
                            "event_id": canonical_event_id,
                            "segment_ids": segment_ids,
                            "machine_no": canonical_segment.machine_no,
                            "plant": canonical_segment.plant_location,
                            "report_status": "SUBMITTED",
                        },
                    },
                )

                print(
                    f"📡 IDEAL REPORT WS SENT | "
                    f"{canonical_segment.plant_location} | "
                    f"M{canonical_segment.machine_no} | "
                    f"Event {canonical_event_id}"
                )

        except Exception as ws_err:
            # Report is already safely committed in DB.
            # WebSocket failure must NOT undo successful submission.
            print(
                f"⚠️ Ideal Report WebSocket Error: {ws_err}"
            )
        
        return Response(
            {
                "success": True,
                "message": ("Ideal report submitted successfully."),
                "event_id": canonical_event_id,
                "segment_ids": segment_ids,
                "segment_count": len(segment_ids),
                "report_id": idle_report.id,
                "plant": (canonical_segment.plant_location),
                "machine_no": (canonical_segment.machine_no),
                "report_status": "SUBMITTED",
                "submitted_by": submitted_by,
                "submitted_at": (submitted_at.isoformat()),
            },
            status=200,
        )

    except Exception as e:

        print(f"❌ Ideal Report Submit Error: {e}")

        traceback.print_exc()

        return Response(
            {
                "success": False,
                "message": str(e),
            },
            status=500,
        )


@api_view(["GET"])
def get_assignment_idle_data(request):
    """Get both assignment and idle report data for dashboard display"""
    try:
        # Get recent operator assignments
        assignments = OperatorAssignment.objects.all().order_by("-created_at")[:10]
        assignment_data = []
        for assignment in assignments:
            assignment_data.append(
                {
                    "id": assignment.id,
                    "machine_no": assignment.machine_no,
                    "operator_name": assignment.operator_name,
                    "shift": assignment.shift,
                    "start_time": (
                        assignment.start_time.strftime("%Y-%m-%d %H:%M")
                        if assignment.start_time
                        else "N/A"
                    ),
                    "created_at": assignment.created_at.strftime("%Y-%m-%d %H:%M"),
                }
            )

        # Get recent idle reports
        idle_reports = IdleReport.objects.all().order_by("-created_at")[:10]
        idle_data = []
        for report in idle_reports:
            idle_data.append(
                {
                    "id": report.id,
                    "machine_no": report.machine_no,
                    "operator_name": report.operator_name,
                    "tool_id": (
                        report.tool_id[:20] + "..."
                        if len(report.tool_id) > 20
                        else report.tool_id
                    ),
                    "reason": (
                        report.get_reason_display()
                        if hasattr(report, "get_reason_display")
                        else report.reason
                    ),
                    "created_at": report.created_at.strftime("%Y-%m-%d %H:%M"),
                }
            )

        return Response(
            {"success": True, "assignments": assignment_data, "idle_reports": idle_data}
        )

    except Exception as e:
        return Response(
            {"success": False, "message": f"Error fetching table data: {str(e)}"},
            status=400,
        )


try:
    from backend.apps.mqtt.simple_plant2 import EXACT_REQUIREMENT_STATE
except ImportError:
    EXACT_REQUIREMENT_STATE = None


@never_cache
@api_view(["GET"])
def exact_plant2_data(request):
    """Get exact Plant 2 data as per user requirement"""
    try:
        # Get live machine data
        live_machines = MACHINE_STATE.summarize(plant_filter=2, stale_after_seconds=300)

        if EXACT_REQUIREMENT_STATE is None:
            # Fallback: return basic machine data
            return Response(
                {
                    "success": True,
                    "total_machines": len(live_machines),
                    "machines": [
                        {
                            **machine,
                            "current_hour_count": 0,
                            "last_hour_count": 0,
                            "cumulative_count": 0,
                            "shift": "A",
                        }
                        for machine in live_machines
                    ],
                }
            )

        # Enhance with exact requirement data
        enhanced_machines = []
        for machine in live_machines:
            machine_no = machine["machine_no"]

            # Get exact data
            exact_data = EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)

            # Merge live data with exact data
            combined = {**machine, **exact_data}  # Live data  # Exact requirement data
            enhanced_machines.append(combined)

        return Response(
            {
                "success": True,
                "total_machines": len(enhanced_machines),
                "machines": enhanced_machines,
            }
        )
    except Exception as e:
        print(f"Error in exact_plant2_data: {e}")
        return Response(
            {"success": False, "error": str(e), "total_machines": 0, "machines": []}
        )


@never_cache
@cache_control(no_store=True, no_cache=True, must_revalidate=True, max_age=0)
@api_view(["GET"])
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
    resp["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


@never_cache
@cache_control(no_store=True, no_cache=True, must_revalidate=True, max_age=0)
@api_view(["GET"])
def count52_live(request):
    plant_no = COUNT52_GROUP["plant"]
    data = MACHINE_STATE.summarize(plant_filter=plant_no, stale_after_seconds=999999)
    allowed = set(COUNT52_GROUP["machines"])
    out = [r for r in data if r["machine_no"] in allowed]
    out.sort(key=lambda r: r["machine_no"])
    resp = Response(
        {"success": True, "topic": "COUNT52", "plant": plant_no, "machines": out}
    )
    resp["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


@never_cache
@api_view(["GET"])
def plant2_raw(request):
    """Return raw Plant 2 messages"""
    try:
        from backend.apps.mqtt.simple_plant2 import get_messages

        messages = get_messages()

        return Response(
            {"success": True, "total_messages": len(messages), "raw_messages": messages}
        )
    except Exception as e:
        return Response(
            {"success": False, "error": str(e), "total_messages": 0, "raw_messages": []}
        )


# ==============================================================
# PLANT 1 + PLANT 2 API UPDATE ONLY
# Paste this block in api/views.py and replace old:
#   - get_tool_info_from_tid_map
#   - plant1_live
#   - plant2_live
#   - get_plant1_machine_history
#   - get_machine_history
# Nothing here changes MQTT / Redis / DB insert logic.
# ==============================================================

from django.views.decorators.cache import never_cache
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.db import connection


def get_tool_info_from_tid_map(tool_id):
    """EPC / Tool ID se public.tid_map table se full tool/part info fetch karta hai.

    Supports uppercase/lowercase column names and EPC with/without leading e.
    If data is not found, returns {} so UI can show N/A but still show tool_id.
    """
    if not tool_id:
        return {}

    raw_tool_id = str(tool_id).strip()
    if not raw_tool_id:
        return {}

    upper = raw_tool_id.upper()
    if upper in ["NULL", "UNKNOWN", "N/A", "NO DATA", "FAILED", "NONE"]:
        return {}
    if upper.startswith("PLANT1_M") or upper.startswith("PLANT2_M"):
        return {}

    clean_24 = raw_tool_id[:24].strip().lower()
    candidates = []
    for val in [clean_24, raw_tool_id.lower(), raw_tool_id[:24].lower()]:
        val = str(val).strip().lower()
        if not val:
            continue
        if val not in candidates:
            candidates.append(val)
        if val.startswith("e") and val[1:] and val[1:] not in candidates:
            candidates.append(val[1:])
        if (not val.startswith("e")) and len(val) >= 23:
            e_val = "e" + val
            if e_val not in candidates:
                candidates.append(e_val)

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'tid_map'
                ORDER BY ordinal_position
                """)
            columns = [row[0] for row in cursor.fetchall()]
            if not columns:
                print("❌ public.tid_map table not found")
                return {}

            epc_column = next((col for col in columns if col.upper() == "EPC"), None)
            if not epc_column:
                print(
                    f"❌ EPC column not found in public.tid_map. Available columns: {columns}"
                )
                return {}

            placeholders = ",".join(["%s"] * len(candidates))
            query = (
                f"SELECT * FROM public.tid_map "
                f'WHERE LOWER(TRIM("{epc_column}"::text)) IN ({placeholders}) '
                f"LIMIT 1"
            )
            cursor.execute(query, candidates)
            result = cursor.fetchone()
            if not result:
                return {}

            row_dict = dict(zip(columns, result))

            def get_value(search_key, default="N/A"):
                for col_name, col_value in row_dict.items():
                    if str(col_name).upper() == str(search_key).upper():
                        if col_value in [None, "", "NULL", "None"]:
                            return default
                        return str(col_value).strip()
                return default

            tpm = 0
            try:
                raw_tpm = get_value("TPM")
                if raw_tpm != "N/A":
                    tpm = int(float(raw_tpm))
            except Exception:
                tpm = 0

            model_value = get_value("MODEL")
            data = {
                "customer": get_value("CUSTOMER"),
                "customer_name": get_value("CUSTOMER"),
                "model": model_value,
                "model_name": model_value,
                "part_name": get_value("PART_NAME"),
                "part_number": get_value("PART_NUMBER"),
                "tool_name": get_value("TOOL_NAME"),
                "epc": get_value("EPC", clean_24 or raw_tool_id),
                "tpm": tpm,
            }
            return data
    except Exception as e:
        print(f"❌ TID map lookup error for {tool_id}: {e}")
        return {}


def get_safe_tid_value(tool_info, key, default="N/A"):
    value = (tool_info or {}).get(key, default)
    if value in [None, "", "None", "NULL"]:
        return default
    return value


def _seconds_to_display(total_seconds):
    total_seconds = int(total_seconds or 0)
    if total_seconds < 0:
        total_seconds = 0
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours > 0:
        return f"{hours} hr {minutes} min {seconds} sec"
    if minutes > 0:
        return f"{minutes} min {seconds} sec"
    return f"{seconds} sec"


def _normalize_tool_id(tool_id):
    if tool_id in [None, "", "NULL", "UNKNOWN", "N/A", "No data", "Failed", "None"]:
        return None
    clean = str(tool_id).strip().lower()[:24]
    if len(clean) != 24:
        return None
    if any(ch not in "0123456789abcdef" for ch in clean):
        return None
    if clean.startswith("e000"):
        return None
    if not clean.startswith("e2"):
        return None
    return clean


def _parse_valid_shut_height(value):
    if value in [
        None,
        "",
        "0",
        "0.0",
        "0.00",
        0,
        0.0,
        "No data",
        "Failed",
        "None",
        "N/A",
    ]:
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if num <= 10.0:
        return None
    return num


def _is_failed_shut_height_reading(value):
    if value in ["Failed", "failed", "FAILED"]:
        return True
    if value in [None, "", "0", "0.0", "0.00", 0, 0.0, "No data", "None", "N/A"]:
        return False
    try:
        num = float(value)
    except Exception:
        return False
    return 0 < num <= 10.0


def _extract_tool_id_from_text(text):
    try:
        import re

        matches = re.findall(r"e2[0-9a-fA-F]{22}", str(text or ""))
        for match in matches:
            clean = _normalize_tool_id(match)
            if clean:
                return clean
    except Exception:
        pass
    return None


def _tid_payload(tool_id):
    clean_tool_id = _normalize_tool_id(tool_id)
    if not clean_tool_id:
        return {
            "tool_id": tool_id or "N/A",
            "epc": tool_id or "N/A",
            "customer": "N/A",
            "customer_name": "N/A",
            "model": "N/A",
            "model_name": "N/A",
            "part_name": "N/A",
            "part_number": "N/A",
            "tool_name": "N/A",
            "tool_tpm": 0,
            "tool_customer": "N/A",
            "tool_model": "N/A",
            "tool_part_name": "N/A",
            "tool_part_number": "N/A",
            "tool_epc": tool_id or "N/A",
        }

    info = get_tool_info_from_tid_map(clean_tool_id)
    customer = get_safe_tid_value(info, "customer")
    model = get_safe_tid_value(info, "model_name")
    part_name = get_safe_tid_value(info, "part_name")
    part_number = get_safe_tid_value(info, "part_number")
    tool_name = get_safe_tid_value(info, "tool_name")
    epc = get_safe_tid_value(info, "epc", clean_tool_id)
    tpm = int((info or {}).get("tpm", 0) or 0)
    return {
        "tool_id": clean_tool_id,
        "epc": epc,
        "customer": customer,
        "customer_name": customer,
        "model": model,
        "model_name": model,
        "part_name": part_name,
        "part_number": part_number,
        "tool_name": tool_name,
        "tool_tpm": tpm,
        # old FE keys
        "tool_customer": customer,
        "tool_model": model,
        "tool_part_name": part_name,
        "tool_part_number": part_number,
        "tool_epc": epc,
    }


def _to_ist_naive(dt, ist_tz):
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(ist_tz).replace(tzinfo=None, microsecond=0)
    return dt.replace(microsecond=0)


def _get_current_hour_count_from_db(data_table, machine_no, start_naive, end_naive):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COALESCE(SUM(count), 0)
                FROM {data_table}
                WHERE machine_no = %s
                  AND timestamp >= %s
                  AND timestamp < %s
                """,
                [str(machine_no), start_naive, end_naive],
            )
            row = cursor.fetchone()
            return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _plant_live_common(
    request,
    plant_no,
    plant_location,
    data_table,
    state_obj,
    topic_mapping,
    get_machine_group_func,
    group_names,
):
    try:
        from apps.machines.machine_state import MACHINE_STATE
        import pytz
        from datetime import datetime, timedelta

        ist_tz = pytz.timezone("Asia/Kolkata")
        now_ist = datetime.now(ist_tz)

        all_mapped_machines = set()
        for machines in topic_mapping.values():
            all_mapped_machines.update(machines)
        all_mapped_machines = sorted(all_mapped_machines)

        live_machines = MACHINE_STATE.summarize(
            plant_filter=plant_no, stale_after_seconds=300
        )
        live_by_machine = {
            int(m.get("machine_no")): dict(m)
            for m in live_machines
            if m.get("plant") == plant_no and m.get("machine_no") is not None
        }

        current_shift = state_obj.get_shift_from_time(now_ist)
        current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
        previous_hour_start = current_hour - timedelta(hours=1)
        shift_start = state_obj.get_shift_start_datetime(now_ist)
        today_start = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)

        now_naive = _to_ist_naive(now_ist, ist_tz)
        today_start_naive = _to_ist_naive(today_start, ist_tz)
        shift_start_naive = _to_ist_naive(shift_start, ist_tz)
        current_hour_naive = _to_ist_naive(current_hour, ist_tz)
        previous_hour_start_naive = _to_ist_naive(previous_hour_start, ist_tz)

        bulk_current_hour = {}
        bulk_last_hour = {}
        bulk_cumulative = {}
        bulk_ideal_today = {}
        bulk_ideal_shift = {}
        bulk_ideal_hour = {}
        bulk_latest_tool = {}
        bulk_latest_shut_height = {}

        def add_ideal_row(target, machine_key, online_seconds, offline_seconds):
            target[str(machine_key).strip()] = {
                "ONLINE": int(online_seconds or 0),
                "OFFLINE": int(offline_seconds or 0),
            }

        def empty_ideal_summary():
            return {"ONLINE": 0, "OFFLINE": 0}

        try:
            with connection.cursor() as cursor:
                # Current hour count - direct DB sum, so restart ke baad bhi count sahi rahega.
                cursor.execute(
                    f"""
                    SELECT TRIM(machine_no::text), COALESCE(SUM(count), 0)
                    FROM {data_table}
                    WHERE timestamp >= %s AND timestamp < %s
                    GROUP BY TRIM(machine_no::text)
                    """,
                    [current_hour_naive, now_naive],
                )
                for machine_key, total_count in cursor.fetchall():
                    bulk_current_hour[str(machine_key).strip()] = int(total_count or 0)

                # Last completed hour count.
                cursor.execute(
                    f"""
                    SELECT TRIM(machine_no::text), COALESCE(SUM(count), 0)
                    FROM {data_table}
                    WHERE timestamp >= %s AND timestamp < %s
                    GROUP BY TRIM(machine_no::text)
                    """,
                    [previous_hour_start_naive, current_hour_naive],
                )
                for machine_key, total_count in cursor.fetchall():
                    bulk_last_hour[str(machine_key).strip()] = int(total_count or 0)

                # Latest shift cumulative.
                cursor.execute(
                    f"""
                    SELECT DISTINCT ON (TRIM(machine_no::text))
                        TRIM(machine_no::text) AS machine_key,
                        COALESCE(cumulative_count, 0) AS cumulative_count
                    FROM {data_table}
                    WHERE shift = %s
                      AND timestamp >= %s
                      AND timestamp < %s
                    ORDER BY TRIM(machine_no::text), timestamp DESC
                    """,
                    [current_shift, shift_start_naive, now_naive],
                )
                for machine_key, cumulative_count in cursor.fetchall():
                    bulk_cumulative[str(machine_key).strip()] = int(
                        cumulative_count or 0
                    )

                # Ideal summaries: today, current shift, current hour.
                for target, start_bound, end_bound, with_shift in [
                    (bulk_ideal_today, today_start_naive, now_naive, False),
                    (bulk_ideal_shift, shift_start_naive, now_naive, True),
                    (bulk_ideal_hour, current_hour_naive, now_naive, False),
                ]:
                    params = [plant_location]
                    shift_sql = ""
                    if with_shift:
                        shift_sql = " AND TRIM(shift) = %s"
                        params.append(current_shift)
                    params.extend([start_bound, end_bound])
                    cursor.execute(
                        f"""
                        SELECT
                            TRIM(machine_no::text) AS machine_key,
                            COALESCE(SUM(CASE WHEN UPPER(TRIM(ideal_mode)) = 'ONLINE' THEN COALESCE(ideal_time, 0) ELSE 0 END), 0) AS online_seconds,
                            COALESCE(SUM(CASE WHEN UPPER(TRIM(ideal_mode)) = 'OFFLINE' THEN COALESCE(ideal_time, 0) ELSE 0 END), 0) AS offline_seconds
                        FROM live_data.ideal_time_segments_reason
                        WHERE TRIM(plant_location) = %s
                          {shift_sql}
                          AND ideal_start_at >= %s
                          AND ideal_start_at < %s
                        GROUP BY TRIM(machine_no::text)
                        """,
                        params,
                    )
                    for row in cursor.fetchall():
                        add_ideal_row(target, row[0], row[1], row[2])

                # Latest valid tool id in current shift.
                cursor.execute(
                    f"""
                    SELECT DISTINCT ON (TRIM(machine_no::text))
                        TRIM(machine_no::text) AS machine_key,
                        LOWER(LEFT(TRIM(tool_id::text), 24)) AS clean_tool_id
                    FROM {data_table}
                    WHERE timestamp >= %s
                      AND timestamp < %s
                      AND tool_id IS NOT NULL
                      AND LOWER(LEFT(TRIM(tool_id::text), 24)) ~ '^e2[0-9a-f]{{22}}$'
                      AND LOWER(LEFT(TRIM(tool_id::text), 24)) NOT LIKE 'e000%%'
                    ORDER BY TRIM(machine_no::text), timestamp DESC
                    """,
                    [shift_start_naive, now_naive],
                )
                for machine_key, tool_id in cursor.fetchall():
                    bulk_latest_tool[str(machine_key).strip()] = tool_id

                # Latest valid shut height in current shift.
                cursor.execute(
                    f"""
                    WITH valid_height AS (
                        SELECT
                            TRIM(machine_no::text) AS machine_key,
                            timestamp,
                            CASE
                                WHEN TRIM(shut_height::text) ~ '^[0-9]+(\\.[0-9]+)?$'
                                THEN TRIM(shut_height::text)::numeric
                                ELSE NULL
                            END AS height_value
                        FROM {data_table}
                        WHERE timestamp >= %s
                          AND timestamp < %s
                    )
                    SELECT DISTINCT ON (machine_key)
                        machine_key,
                        height_value
                    FROM valid_height
                    WHERE height_value > 10
                    ORDER BY machine_key, timestamp DESC
                    """,
                    [shift_start_naive, now_naive],
                )
                for machine_key, height_value in cursor.fetchall():
                    bulk_latest_shut_height[str(machine_key).strip()] = float(
                        height_value
                    )

        except Exception as e:
            print(f"❌ Plant {plant_no} live bulk query error: {e}")

        enhanced_machines = []
        problem_machines = []

        for machine_no in all_mapped_machines:
            m_str = str(machine_no)
            machine_data = live_by_machine.get(machine_no)
            try:
                idle_status = state_obj.idle_tracker.get_idle_status(
                    machine_no, now_ist
                )
                status_info = state_obj.get_machine_status(machine_no)

                is_on = bool(status_info.get("machine_on"))
                is_producing = bool(status_info.get("is_producing"))

                db_ideal_today = bulk_ideal_today.get(m_str, empty_ideal_summary())
                db_ideal_shift = bulk_ideal_shift.get(m_str, empty_ideal_summary())
                db_ideal_hour = bulk_ideal_hour.get(m_str, empty_ideal_summary())

                live_ideal_mode = None
                live_ideal_seconds = 0
                live_ideal_hour_seconds = 0

                last_signal_time = None
                last_count_time_map = getattr(state_obj, "last_count_time", {})
                machine_json_status = getattr(state_obj, "machine_json_status", {})
                if (
                    machine_no in last_count_time_map
                    and machine_no in machine_json_status
                ):
                    last_signal_time = max(
                        last_count_time_map[machine_no],
                        machine_json_status[machine_no]["last_json_time"],
                    )
                elif machine_no in last_count_time_map:
                    last_signal_time = last_count_time_map[machine_no]
                elif machine_no in machine_json_status:
                    last_signal_time = machine_json_status[machine_no]["last_json_time"]

                if last_signal_time and last_signal_time < shift_start:
                    last_signal_time = None

                offline_since_str = None
                offline_duration_minutes = None

                if not is_on:
                    offline_since_obj = last_signal_time or shift_start
                    offline_since_str = offline_since_obj.strftime("%H:%M:%S")
                    offline_duration_minutes = int(
                        max(0, (now_ist - offline_since_obj).total_seconds()) / 60
                    )
                    live_ideal_mode = "OFFLINE"
                    live_ideal_seconds = max(
                        0, int((now_ist - offline_since_obj).total_seconds())
                    )
                    live_ideal_hour_seconds = max(
                        0,
                        int(
                            (
                                now_ist - max(offline_since_obj, current_hour)
                            ).total_seconds()
                        ),
                    )

                on_since_str = None
                first_count_str = None
                time_to_first_count = None
                machine_on_since = getattr(state_obj, "machine_on_since", {})
                first_count_time = getattr(state_obj, "first_count_time", {})

                if is_on and machine_no in machine_on_since:
                    on_since = machine_on_since[machine_no]
                    if on_since >= shift_start:
                        on_since_str = on_since.strftime("%H:%M:%S")
                        if (
                            machine_no in first_count_time
                            and first_count_time[machine_no] >= shift_start
                        ):
                            first_count = first_count_time[machine_no]
                            first_count_str = first_count.strftime("%H:%M:%S")
                            time_to_first_count = int(
                                (first_count - on_since).total_seconds() / 60
                            )

                segment_info = getattr(state_obj, "machine_segments", {}).get(
                    machine_no, {}
                )
                segment_shut_height = (
                    segment_info.get("shut_height")
                    if isinstance(segment_info, dict)
                    else None
                )
                status_shut_height = status_info.get("shut_height")

                if _is_failed_shut_height_reading(status_shut_height):
                    final_shut_height = "Failed"
                else:
                    final_shut_height = (
                        _parse_valid_shut_height(status_shut_height)
                        or _parse_valid_shut_height(segment_shut_height)
                        or bulk_latest_shut_height.get(m_str)
                        or "No data"
                    )

                safe_current_tool_id = (
                    _normalize_tool_id(status_info.get("tool_id"))
                    or _normalize_tool_id(
                        segment_info.get("tool_id")
                        if isinstance(segment_info, dict)
                        else None
                    )
                    or bulk_latest_tool.get(m_str)
                    or "N/A"
                )

                if is_on and (not is_producing) and idle_status.get("is_idle"):
                    live_ideal_mode = "ONLINE"
                    online_start_obj = last_count_time_map.get(machine_no)
                    if not online_start_obj or online_start_obj < shift_start:
                        online_start_obj = machine_on_since.get(machine_no, shift_start)
                    live_ideal_seconds = max(
                        0, int((now_ist - online_start_obj).total_seconds())
                    )
                    live_ideal_hour_seconds = max(
                        0,
                        int(
                            (
                                now_ist - max(online_start_obj, current_hour)
                            ).total_seconds()
                        ),
                    )

                online_ideal_today_seconds = db_ideal_today["ONLINE"] + (
                    live_ideal_seconds if live_ideal_mode == "ONLINE" else 0
                )
                offline_ideal_today_seconds = db_ideal_today["OFFLINE"] + (
                    live_ideal_seconds if live_ideal_mode == "OFFLINE" else 0
                )
                online_ideal_shift_seconds = db_ideal_shift["ONLINE"] + (
                    live_ideal_seconds if live_ideal_mode == "ONLINE" else 0
                )
                offline_ideal_shift_seconds = db_ideal_shift["OFFLINE"] + (
                    live_ideal_seconds if live_ideal_mode == "OFFLINE" else 0
                )
                online_ideal_hour_seconds = db_ideal_hour["ONLINE"] + (
                    live_ideal_hour_seconds if live_ideal_mode == "ONLINE" else 0
                )
                offline_ideal_hour_seconds = db_ideal_hour["OFFLINE"] + (
                    live_ideal_hour_seconds if live_ideal_mode == "OFFLINE" else 0
                )
                
                pending_reason = getattr(
                    state_obj,
                    "pending_reasons",
                    {}
                ).get(machine_no)
                
                has_pending_reason = bool(
                    pending_reason
                )

                exact_data = {
                    "machine_no": machine_no,
                    "current_hour_count": bulk_current_hour.get(m_str, 0),
                    "last_hour_count": bulk_last_hour.get(m_str, 0),
                    "cumulative_count": bulk_cumulative.get(m_str, 0),
                    "idle_time": online_ideal_hour_seconds + offline_ideal_hour_seconds,
                    "total_shift_idle_time": online_ideal_shift_seconds
                    + offline_ideal_shift_seconds,
                    "live_ideal_mode": live_ideal_mode,
                    "live_ideal_time": live_ideal_seconds,
                    "live_ideal_display": _seconds_to_display(live_ideal_seconds),
                    "online_ideal_this_hour": online_ideal_hour_seconds,
                    "offline_ideal_this_hour": offline_ideal_hour_seconds,
                    "total_ideal_this_hour": online_ideal_hour_seconds
                    + offline_ideal_hour_seconds,
                    "online_ideal_this_hour_display": _seconds_to_display(
                        online_ideal_hour_seconds
                    ),
                    "offline_ideal_this_hour_display": _seconds_to_display(
                        offline_ideal_hour_seconds
                    ),
                    "total_ideal_this_hour_display": _seconds_to_display(
                        online_ideal_hour_seconds + offline_ideal_hour_seconds
                    ),
                    "online_ideal_shift": online_ideal_shift_seconds,
                    "offline_ideal_shift": offline_ideal_shift_seconds,
                    "total_ideal_shift": online_ideal_shift_seconds
                    + offline_ideal_shift_seconds,
                    "online_ideal_shift_display": _seconds_to_display(
                        online_ideal_shift_seconds
                    ),
                    "offline_ideal_shift_display": _seconds_to_display(
                        offline_ideal_shift_seconds
                    ),
                    "total_ideal_shift_display": _seconds_to_display(
                        online_ideal_shift_seconds + offline_ideal_shift_seconds
                    ),
                    "online_ideal_today": online_ideal_today_seconds,
                    "offline_ideal_today": offline_ideal_today_seconds,
                    "total_ideal_today": online_ideal_today_seconds
                    + offline_ideal_today_seconds,
                    "online_ideal_today_display": _seconds_to_display(
                        online_ideal_today_seconds
                    ),
                    "offline_ideal_today_display": _seconds_to_display(
                        offline_ideal_today_seconds
                    ),
                    "total_ideal_today_display": _seconds_to_display(
                        online_ideal_today_seconds + offline_ideal_today_seconds
                    ),
                    "shift": current_shift,
                    "machine_on": is_on,
                    "is_producing": is_producing,
                    "has_pending_reason":
                        has_pending_reason,
                    "has_count_data": status_info.get("has_count_data", False),
                    "has_json_data": status_info.get("has_json_data", False),
                    "count_seconds_ago": status_info.get("count_seconds_ago"),
                    "json_seconds_ago": status_info.get("json_seconds_ago"),
                    "current_tool_id": safe_current_tool_id,
                    "tool_id": safe_current_tool_id,
                    "shut_height": final_shut_height,
                    "current_shut_height": final_shut_height,
                    "data_source": status_info.get("data_source", "NONE"),
                    "on_since": on_since_str,
                    "first_count_at": first_count_str,
                    "time_to_first_count": time_to_first_count,
                    "offline_since": offline_since_str,
                    "offline_duration_minutes": offline_duration_minutes,
                    "last_activity": (
                        last_count_time_map[machine_no].strftime("%H:%M:%S")
                        if machine_no in last_count_time_map
                        and last_count_time_map[machine_no] >= shift_start
                        else "Never"
                    ),
                    "live_idle_time": idle_status.get("live_idle_time", "0m"),
                    "accumulated_idle_time": idle_status.get(
                        "accumulated_idle_time", "0m"
                    ),
                    "hourly_idle_total": idle_status.get("hourly_idle_total", 0),
                    "is_idle": idle_status.get("is_idle", False),
                    "idle_type": idle_status.get("idle_type"),
                    "status": (
                        "OFFLINE" if not is_on else idle_status.get("status", "ONLINE")
                    ),
                    "machine_group": get_machine_group_func(machine_no),
                    "plant": plant_no,
                }

                m_data = machine_data or {
                    "plant": plant_no,
                    "machine_no": machine_no,
                    "tool_id": (
                        safe_current_tool_id
                        if safe_current_tool_id != "N/A"
                        else f"PLANT{plant_no}_M{machine_no:02d}"
                    ),
                    "count": 0,
                    "shut_height": final_shut_height,
                    "last_seen": "JSON only" if is_on else "Not active",
                    "status": exact_data["status"],
                }
                m_data.update(exact_data)
                m_data.update(_tid_payload(safe_current_tool_id))
                m_data["tool_id"] = safe_current_tool_id
                m_data["current_tool_id"] = safe_current_tool_id
                m_data["machine_group"] = get_machine_group_func(machine_no)

                problem_detected = (
                    is_on and (not is_producing) and idle_status.get("is_idle")
                )
                m_data["problem_detected"] = problem_detected
                if problem_detected:
                    problem_machines.append(machine_no)

                enhanced_machines.append(m_data)
            except Exception as e:
                print(f"⚠️ Plant {plant_no} M{machine_no} live API error: {e}")
                enhanced_machines.append(
                    {
                        "plant": plant_no,
                        "machine_no": machine_no,
                        "tool_id": f"PLANT{plant_no}_M{machine_no:02d}",
                        "current_tool_id": "N/A",
                        "count": 0,
                        "shut_height": "No data",
                        "current_hour_count": 0,
                        "last_hour_count": 0,
                        "cumulative_count": 0,
                        "shift": current_shift,
                        "machine_group": get_machine_group_func(machine_no),
                        "machine_on": False,
                        "is_producing": False,
                        "problem_detected": False,
                        "status": "OFFLINE",
                        "idle_time": 0,
                        "total_shift_idle_time": 0,
                        "tool_customer": "N/A",
                        "tool_model": "N/A",
                        "tool_part_name": "N/A",
                        "tool_part_number": "N/A",
                        "tool_name": "N/A",
                        "tool_epc": "N/A",
                    }
                )

        enhanced_machines.sort(key=lambda x: int(x.get("machine_no", 0)))
        on_machines = [m for m in enhanced_machines if m.get("machine_on")]
        producing_machines = [m for m in enhanced_machines if m.get("is_producing")]

        groups_summary = {}
        for group in group_names:
            group_machines = [
                m for m in enhanced_machines if m.get("machine_group") == group
            ]
            if group_machines:
                groups_summary[group] = {
                    "total": len(group_machines),
                    "on": len([m for m in group_machines if m.get("machine_on")]),
                    "producing": len(
                        [m for m in group_machines if m.get("is_producing")]
                    ),
                    "problems": len(
                        [m for m in group_machines if m.get("problem_detected")]
                    ),
                }

        response = Response(
            {
                "success": True,
                "total_machines": len(enhanced_machines),
                "on_count": len(on_machines),
                "producing_count": len(producing_machines),
                "problem_count": len(problem_machines),
                "problem_machines": problem_machines,
                "groups_summary": groups_summary,
                "machines": enhanced_machines,
                "plant": plant_no,
            }
        )
        response["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
        response["Pragma"] = "no-cache"
        response["Expires"] = "0"
        return response
    except Exception as e:
        import traceback

        traceback.print_exc()
        return Response(
            {"success": False, "error": str(e), "machines": [], "plant": plant_no},
            status=500,
        )


@never_cache
@api_view(["GET"])
def plant1_live(request):
    from apps.mqtt.simple_plant1 import (
        PLANT1_EXACT_REQUIREMENT_STATE,
        TOPIC_MACHINE_MAPPING,
        get_machine_group,
    )

    return _plant_live_common(
        request=request,
        plant_no=1,
        plant_location="Plant 1",
        data_table="live_data.plant1_data",
        state_obj=PLANT1_EXACT_REQUIREMENT_STATE,
        topic_mapping=TOPIC_MACHINE_MAPPING,
        get_machine_group_func=get_machine_group,
        group_names=[
            "JJ5",
            "JJ6",
            "JJ7",
            "JJ8",
            "JJ9",
            "JJ10",
            "JJ11",
            "JJ12",
            "JJ13",
            "JJ14",
            "JJ15",
        ],
    )


@never_cache
@api_view(["GET"])
def plant2_live(request):
    from apps.mqtt.simple_plant2 import (
        PLANT2_EXACT_REQUIREMENT_STATE,
        TOPIC_MACHINE_MAPPING,
        get_machine_group,
    )

    return _plant_live_common(
        request=request,
        plant_no=2,
        plant_location="Plant 2",
        data_table="live_data.plant2_data",
        state_obj=PLANT2_EXACT_REQUIREMENT_STATE,
        topic_mapping=TOPIC_MACHINE_MAPPING,
        get_machine_group_func=get_machine_group,
        group_names=["J1", "J2", "J3", "J4", "J5", "J6", "J7", "J8", "J9"],
    )


def _plant_history_common(
    request, plant_no, plant_location, data_table, lunch_start_tuple, lunch_end_tuple
):
    try:
        import pytz
        from datetime import datetime, timedelta, time

        request_plant_no = int(request.GET.get("plant_no", plant_no))
        # Force correct endpoint plant no; request param should not switch tables.
        request_plant_no = plant_no

        machine_no = str(request.GET.get("machine_no", "")).strip()
        date_str = request.GET.get("date", "").strip()
        shift_param = str(request.GET.get("shift", "A")).strip().upper()

        ist_tz = pytz.timezone("Asia/Kolkata")
        now_ist = datetime.now(ist_tz)

        if not date_str:
            date_str = now_ist.strftime("%Y-%m-%d")
        if not machine_no:
            return Response(
                {"success": False, "error": "machine_no is required"}, status=400
            )

        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        def localize_ist(dt):
            if dt.tzinfo is None:
                return ist_tz.localize(dt)
            return dt.astimezone(ist_tz)

        def to_naive_str(dt):
            return (
                localize_ist(dt)
                .replace(tzinfo=None, microsecond=0)
                .strftime("%Y-%m-%d %H:%M:%S")
            )

        def title_time(dt):
            return localize_ist(dt).strftime("%I:%M %p")

        def system_time(dt):
            return localize_ist(dt).strftime("%Y-%m-%d %H:%M:%S")

        def get_shift_window(date_obj, shift_name):
            """A = 08:30-20:00, B = 20:00-next 08:30, ALL = 08:30-next 08:30."""
            shift_name = (shift_name or "A").upper()
            if shift_name == "B":
                start = ist_tz.localize(datetime.combine(date_obj, time(20, 0, 0)))
                end = ist_tz.localize(
                    datetime.combine(date_obj + timedelta(days=1), time(8, 30, 0))
                )
                return start, end, "B"
            if shift_name == "ALL":
                start = ist_tz.localize(datetime.combine(date_obj, time(8, 30, 0)))
                end = ist_tz.localize(
                    datetime.combine(date_obj + timedelta(days=1), time(8, 30, 0))
                )
                return start, end, "ALL"
            start = ist_tz.localize(datetime.combine(date_obj, time(8, 30, 0)))
            end = ist_tz.localize(datetime.combine(date_obj, time(20, 0, 0)))
            return start, end, "A"

        shift_start, shift_end, selected_shift = get_shift_window(
            target_date, shift_param
        )
        effective_end = shift_end
        if shift_start <= now_ist <= shift_end:
            effective_end = min(shift_end, now_ist)

        start_str_naive = to_naive_str(shift_start)
        end_str_naive = to_naive_str(effective_end)
        start_str_tz = localize_ist(shift_start).strftime("%Y-%m-%d %H:%M:%S+05:30")
        end_str_tz = localize_ist(effective_end).strftime("%Y-%m-%d %H:%M:%S+05:30")

        hour_buckets = []
        cursor_time = shift_start
        while cursor_time < effective_end:
            if cursor_time.minute != 0 or cursor_time.second != 0:
                next_time = cursor_time.replace(
                    minute=0, second=0, microsecond=0
                ) + timedelta(hours=1)
            else:
                next_time = cursor_time + timedelta(hours=1)
            if next_time > shift_end:
                next_time = shift_end
            bucket_end = min(next_time, effective_end)
            hour_buckets.append(
                {
                    "bucket_key": to_naive_str(cursor_time),
                    "start": cursor_time,
                    "end": bucket_end,
                    "scheduled_end": next_time,
                    "count": 0,
                    "latest_cumulative": 0,
                    "online_ideal_seconds": 0,
                    "offline_ideal_seconds": 0,
                    "total_ideal_seconds": 0,
                    "ideal_segments": [],
                    "machine_events": [],
                    "on_off_events": [],
                    "tool_changes": [],
                    "shut_height_changes": [],
                }
            )
            cursor_time = next_time

        bucket_by_key = {b["bucket_key"]: b for b in hour_buckets}

        def find_bucket_for_time(dt):
            dt = localize_ist(dt)
            for bucket in hour_buckets:
                if bucket["start"] <= dt < bucket["scheduled_end"]:
                    return bucket
            if hour_buckets and dt == hour_buckets[-1]["scheduled_end"]:
                return hour_buckets[-1]
            return None

        def add_timeline_event(
            events, dt, event_type, title, details, shift="", extra=None
        ):
            dt = localize_ist(dt)
            payload = {
                "timestamp": dt.timestamp(),
                "time": dt.strftime("%I:%M %p"),
                "time_str": dt.strftime("%I:%M %p"),
                "system_time": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "type": event_type,
                "title": title,
                "details": details or "",
                "shift": shift or ("A" if selected_shift == "ALL" else selected_shift),
            }
            if extra:
                payload.update(extra)
            events.append(payload)
            return payload

        events = []
        machine_meta = {
            "customer": "N/A",
            "customer_name": "N/A",
            "model": "N/A",
            "model_name": "N/A",
            "part_name": "N/A",
            "part_number": "N/A",
            "tool_name": "N/A",
            "tool_id": "N/A",
            "epc": "N/A",
            "shut_height": "N/A",
        }

        if shift_start <= now_ist or target_date < now_ist.date():
            add_timeline_event(
                events,
                shift_start,
                "SHIFT_START",
                (
                    f"Shift {selected_shift} Started"
                    if selected_shift != "ALL"
                    else "Production Day Started"
                ),
                f"History window started at {shift_start.strftime('%I:%M %p')}.",
                selected_shift,
            )

        lunch_start = ist_tz.localize(
            datetime.combine(
                target_date, time(lunch_start_tuple[0], lunch_start_tuple[1], 0)
            )
        )
        lunch_end = ist_tz.localize(
            datetime.combine(
                target_date, time(lunch_end_tuple[0], lunch_end_tuple[1], 0)
            )
        )
        if shift_start <= lunch_start < effective_end:
            add_timeline_event(
                events,
                lunch_start,
                "LUNCH_START",
                "Lunch Break Started",
                f"Scheduled lunch break started at {lunch_start.strftime('%I:%M %p')}.",
                "A",
            )
        if shift_start <= lunch_end < effective_end:
            add_timeline_event(
                events,
                lunch_end,
                "LUNCH_END",
                "Lunch Break Ended",
                f"Scheduled lunch break ended at {lunch_end.strftime('%I:%M %p')}.",
                "A",
            )

        if selected_shift == "A" and shift_end <= effective_end:
            add_timeline_event(
                events,
                shift_end,
                "SHIFT_END",
                "Shift A Ended",
                "Shift A ended at 08:00 PM.",
                "A",
            )
        elif selected_shift == "B" and shift_end <= effective_end:
            add_timeline_event(
                events,
                shift_end,
                "SHIFT_END",
                "Shift B Ended",
                "Shift B ended at 08:30 AM.",
                "B",
            )
        elif selected_shift == "ALL":
            shift_a_end = ist_tz.localize(datetime.combine(target_date, time(20, 0, 0)))
            if shift_start <= shift_a_end <= effective_end:
                add_timeline_event(
                    events,
                    shift_a_end,
                    "SHIFT_END",
                    "Shift A Ended",
                    "Shift A ended at 08:00 PM.",
                    "A",
                )

        count_summary = {
            "total_count": 0,
            "first_count_time": None,
            "last_count_time": None,
            "latest_cumulative": 0,
        }
        shift_ideal_summary = {
            "online_ideal_seconds": 0,
            "offline_ideal_seconds": 0,
            "total_ideal_seconds": 0,
            "online_ideal_display": "0 sec",
            "offline_ideal_display": "0 sec",
            "total_ideal_display": "0 sec",
        }

        with connection.cursor() as cursor:
            # 1) Latest tool metadata.
            cursor.execute(
                f"""
                SELECT LOWER(LEFT(TRIM(tool_id::text), 24)) AS clean_tool_id
                FROM {data_table}
                WHERE machine_no = %s
                  AND timestamp >= %s::timestamp WITHOUT TIME ZONE
                  AND timestamp <  %s::timestamp WITHOUT TIME ZONE
                  AND tool_id IS NOT NULL
                  AND LOWER(LEFT(TRIM(tool_id::text), 24)) ~ '^e2[0-9a-f]{{22}}$'
                  AND LOWER(LEFT(TRIM(tool_id::text), 24)) NOT LIKE 'e000%%'
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                [machine_no, start_str_naive, end_str_naive],
            )
            tool_res = cursor.fetchone()
            if tool_res and tool_res[0]:
                tool_id = _normalize_tool_id(tool_res[0])
                if tool_id:
                    machine_meta.update(_tid_payload(tool_id))
                    machine_meta["tool_id"] = tool_id

            # Latest shut height.
            cursor.execute(
                f"""
                SELECT shut_height
                FROM {data_table}
                WHERE machine_no = %s
                  AND timestamp >= %s::timestamp WITHOUT TIME ZONE
                  AND timestamp <  %s::timestamp WITHOUT TIME ZONE
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                [machine_no, start_str_naive, end_str_naive],
            )
            latest_raw_height_res = cursor.fetchone()
            latest_raw_height = (
                latest_raw_height_res[0] if latest_raw_height_res else None
            )
            if _is_failed_shut_height_reading(latest_raw_height):
                machine_meta["shut_height"] = "Failed"
            else:
                cursor.execute(
                    f"""
                    WITH valid_height AS (
                        SELECT
                            timestamp,
                            CASE
                                WHEN TRIM(shut_height::text) ~ '^[0-9]+(\\.[0-9]+)?$'
                                THEN TRIM(shut_height::text)::numeric
                                ELSE NULL
                            END AS height_value
                        FROM {data_table}
                        WHERE machine_no = %s
                          AND timestamp >= %s::timestamp WITHOUT TIME ZONE
                          AND timestamp <  %s::timestamp WITHOUT TIME ZONE
                    )
                    SELECT height_value
                    FROM valid_height
                    WHERE height_value > 10
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    [machine_no, start_str_naive, end_str_naive],
                )
                height_res = cursor.fetchone()
                if height_res and height_res[0] is not None:
                    machine_meta["shut_height"] = f"{float(height_res[0]):.2f}"

            # 2) Hour-wise production count.
            first_bucket_end = (
                hour_buckets[0]["scheduled_end"] if hour_buckets else effective_end
            )
            first_bucket_start_str = to_naive_str(shift_start)
            first_bucket_end_str = to_naive_str(first_bucket_end)

            cursor.execute(
                f"""
                SELECT
                    CASE
                        WHEN timestamp >= %s::timestamp WITHOUT TIME ZONE
                         AND timestamp <  %s::timestamp WITHOUT TIME ZONE
                        THEN %s::timestamp WITHOUT TIME ZONE
                        ELSE date_trunc('hour', timestamp)
                    END AS bucket_start,
                    COALESCE(SUM(count), 0) AS total_count,
                    COALESCE(MAX(cumulative_count), 0) AS latest_cumulative,
                    MIN(timestamp) AS first_count_time,
                    MAX(timestamp) AS last_count_time
                FROM {data_table}
                WHERE machine_no = %s
                  AND timestamp >= %s::timestamp WITHOUT TIME ZONE
                  AND timestamp <  %s::timestamp WITHOUT TIME ZONE
                GROUP BY bucket_start
                ORDER BY bucket_start ASC
                """,
                [
                    first_bucket_start_str,
                    first_bucket_end_str,
                    first_bucket_start_str,
                    machine_no,
                    start_str_naive,
                    end_str_naive,
                ],
            )
            for (
                bucket_start,
                total_count,
                latest_cumulative,
                first_count_time,
                last_count_time,
            ) in cursor.fetchall():
                bucket_key = bucket_start.strftime("%Y-%m-%d %H:%M:%S")
                bucket = bucket_by_key.get(bucket_key)
                if bucket:
                    bucket["count"] = int(total_count or 0)
                    bucket["latest_cumulative"] = int(latest_cumulative or 0)
                    count_summary["total_count"] += int(total_count or 0)
                    count_summary["latest_cumulative"] = max(
                        count_summary["latest_cumulative"], int(latest_cumulative or 0)
                    )
                    if first_count_time and (
                        not count_summary["first_count_time"]
                        or first_count_time < count_summary["first_count_time"]
                    ):
                        count_summary["first_count_time"] = first_count_time
                    if last_count_time and (
                        not count_summary["last_count_time"]
                        or last_count_time > count_summary["last_count_time"]
                    ):
                        count_summary["last_count_time"] = last_count_time

            # 3) Machine events.
            cursor.execute(
                """
                SELECT event_type, timestamp, shift, details
                FROM live_data."Machine_Event_Logs"
                WHERE plant_no = %s
                  AND machine_no = %s
                  AND timestamp >= %s::timestamp WITH TIME ZONE
                  AND timestamp <  %s::timestamp WITH TIME ZONE
                ORDER BY timestamp ASC
                """,
                [plant_no, machine_no, start_str_tz, end_str_tz],
            )
            machine_event_rows = cursor.fetchall()
            event_titles = {
                "ON": "Machine Powered ON",
                "OFF": "Machine Offline",
                "SHUT_HEIGHT_CHANGE": "Shut Height Adjusted",
                "TOOL_CHANGE": "Tool ID Changed",
            }
            for event_type, ts_obj, shift_val, details in machine_event_rows:
                details_text_raw = str(details or "")
                if event_type == "TOOL_CHANGE" and "e000" in details_text_raw.lower():
                    continue
                if event_type == "SHUT_HEIGHT_CHANGE" and (
                    "1.01" in details_text_raw or "0.01" in details_text_raw
                ):
                    continue
                ts_obj = localize_ist(ts_obj)
                title = event_titles.get(
                    event_type, str(event_type).replace("_", " ").title()
                )
                event_tool_id = _extract_tool_id_from_text(details_text_raw)
                event_extra = _tid_payload(event_tool_id) if event_tool_id else {}
                event_payload = add_timeline_event(
                    events,
                    ts_obj,
                    event_type,
                    title,
                    details,
                    shift_val,
                    extra=event_extra if event_extra else None,
                )
                bucket = find_bucket_for_time(ts_obj)
                if bucket:
                    bucket_event = {
                        "type": event_type,
                        "title": title,
                        "time": ts_obj.strftime("%I:%M:%S %p"),
                        "system_time": system_time(ts_obj),
                        "details": details or "",
                        **event_extra,
                    }
                    bucket["machine_events"].append(bucket_event)
                    if event_type in ["ON", "OFF"]:
                        bucket["on_off_events"].append(bucket_event)
                    elif event_type == "TOOL_CHANGE":
                        bucket["tool_changes"].append(bucket_event)
                    elif event_type == "SHUT_HEIGHT_CHANGE":
                        bucket["shut_height_changes"].append(bucket_event)

            # 4) Ideal segments.
            ideal_params = [
                plant_location,
                int(machine_no),
                end_str_naive,
                start_str_naive,
            ]
            shift_filter_sql = ""
            if selected_shift in ["A", "B"]:
                shift_filter_sql = " AND shift = %s"
                ideal_params.append(selected_shift)

            cursor.execute(
                f"""
                SELECT
                    id,
                    ideal_mode,
                    ideal_start_at,
                    ideal_end_at,
                    ideal_time,
                    closed_by,
                    reason,
                    specific_reason,
                    remark,
                    shift
                FROM live_data.ideal_time_segments_reason
                WHERE plant_location = %s
                  AND machine_no = %s
                  AND ideal_start_at <  %s::timestamp WITHOUT TIME ZONE
                  AND ideal_end_at   >  %s::timestamp WITHOUT TIME ZONE
                  AND ideal_time >= 180
                  {shift_filter_sql}
                ORDER BY ideal_start_at ASC
                """,
                ideal_params,
            )
            ideal_rows = cursor.fetchall()
            for row in ideal_rows:
                (
                    ideal_id,
                    ideal_mode,
                    ideal_start_at,
                    ideal_end_at,
                    ideal_time,
                    closed_by,
                    reason,
                    specific_reason,
                    remark,
                    shift_val,
                ) = row
                ideal_start_at = localize_ist(ideal_start_at)
                ideal_end_at = localize_ist(ideal_end_at)
                ideal_mode = str(ideal_mode or "").upper()
                ideal_time = int(ideal_time or 0)
                segment_payload = {
                    "id": ideal_id,
                    "mode": ideal_mode,
                    "start_time": ideal_start_at.strftime("%I:%M:%S %p"),
                    "end_time": ideal_end_at.strftime("%I:%M:%S %p"),
                    "start_system_time": system_time(ideal_start_at),
                    "end_system_time": system_time(ideal_end_at),
                    "duration_seconds": ideal_time,
                    "duration_display": _seconds_to_display(ideal_time),
                    "closed_by": closed_by,
                    "reason": reason or "Uncategorized",
                    "specific_reason": specific_reason or "Reason Not Provided",
                    "remark": remark or "",
                    "shift": shift_val,
                }
                title = "Online Ideal" if ideal_mode == "ONLINE" else "Offline Ideal"
                details = f"{title}: {segment_payload['duration_display']} ({segment_payload['start_time']} - {segment_payload['end_time']}). Reason: {segment_payload['reason']} / {segment_payload['specific_reason']}"
                add_timeline_event(
                    events,
                    ideal_start_at,
                    f"IDEAL_{ideal_mode}",
                    title,
                    details,
                    shift_val,
                    extra={"ideal_segment": segment_payload},
                )

                for bucket in hour_buckets:
                    overlap_start = max(ideal_start_at, bucket["start"])
                    overlap_end = min(
                        ideal_end_at, bucket["scheduled_end"], effective_end
                    )
                    if overlap_end <= overlap_start:
                        continue
                    overlap_seconds = int((overlap_end - overlap_start).total_seconds())
                    if overlap_seconds <= 0:
                        continue
                    bucket_segment = dict(segment_payload)
                    bucket_segment["bucket_overlap_seconds"] = overlap_seconds
                    bucket_segment["bucket_overlap_display"] = _seconds_to_display(
                        overlap_seconds
                    )
                    bucket["ideal_segments"].append(bucket_segment)
                    if ideal_mode == "ONLINE":
                        bucket["online_ideal_seconds"] += overlap_seconds
                        shift_ideal_summary["online_ideal_seconds"] += overlap_seconds
                    elif ideal_mode == "OFFLINE":
                        bucket["offline_ideal_seconds"] += overlap_seconds
                        shift_ideal_summary["offline_ideal_seconds"] += overlap_seconds

        hourly_summary = []
        for bucket in hour_buckets:
            bucket["total_ideal_seconds"] = int(
                bucket["online_ideal_seconds"] + bucket["offline_ideal_seconds"]
            )
            bucket["online_ideal_display"] = _seconds_to_display(
                bucket["online_ideal_seconds"]
            )
            bucket["offline_ideal_display"] = _seconds_to_display(
                bucket["offline_ideal_seconds"]
            )
            bucket["total_ideal_display"] = _seconds_to_display(
                bucket["total_ideal_seconds"]
            )
            bucket["tool_change_count"] = len(bucket["tool_changes"])
            bucket["shut_height_change_count"] = len(bucket["shut_height_changes"])
            bucket["on_off_event_count"] = len(bucket["on_off_events"])

            summary_details = f"Production: {bucket['count']} pieces."
            if bucket["online_ideal_seconds"] > 0:
                summary_details += f" | Online ideal: {bucket['online_ideal_display']}."
            if bucket["offline_ideal_seconds"] > 0:
                summary_details += (
                    f" | Offline ideal: {bucket['offline_ideal_display']}."
                )
            if bucket["shut_height_change_count"] > 0:
                summary_details += f" | Shut height changed {bucket['shut_height_change_count']} time(s)."
            if bucket["tool_change_count"] > 0:
                summary_details += (
                    f" | Tool changed {bucket['tool_change_count']} time(s)."
                )

            hour_payload = {
                "bucket_start": system_time(bucket["start"]),
                "bucket_end": system_time(bucket["scheduled_end"]),
                "bucket_start_display": title_time(bucket["start"]),
                "bucket_end_display": title_time(bucket["scheduled_end"]),
                "count": int(bucket["count"]),
                "latest_cumulative": int(bucket["latest_cumulative"]),
                "online_ideal_seconds": int(bucket["online_ideal_seconds"]),
                "offline_ideal_seconds": int(bucket["offline_ideal_seconds"]),
                "total_ideal_seconds": int(bucket["total_ideal_seconds"]),
                "online_ideal_display": bucket["online_ideal_display"],
                "offline_ideal_display": bucket["offline_ideal_display"],
                "total_ideal_display": bucket["total_ideal_display"],
                "ideal_segments": bucket["ideal_segments"],
                "machine_events": bucket["machine_events"],
                "on_off_events": bucket["on_off_events"],
                "tool_changes": bucket["tool_changes"],
                "shut_height_changes": bucket["shut_height_changes"],
                "tool_change_count": bucket["tool_change_count"],
                "shut_height_change_count": bucket["shut_height_change_count"],
                "on_off_event_count": bucket["on_off_event_count"],
                "details": summary_details,
            }
            hourly_summary.append(hour_payload)
            add_timeline_event(
                events,
                bucket["scheduled_end"] - timedelta(seconds=1),
                "HOUR_SUMMARY",
                f"Hourly Summary ({title_time(bucket['start'])} - {title_time(bucket['scheduled_end'])})",
                summary_details,
                selected_shift,
                extra=hour_payload,
            )

        shift_ideal_summary["total_ideal_seconds"] = int(
            shift_ideal_summary["online_ideal_seconds"]
            + shift_ideal_summary["offline_ideal_seconds"]
        )
        shift_ideal_summary["online_ideal_display"] = _seconds_to_display(
            shift_ideal_summary["online_ideal_seconds"]
        )
        shift_ideal_summary["offline_ideal_display"] = _seconds_to_display(
            shift_ideal_summary["offline_ideal_seconds"]
        )
        shift_ideal_summary["total_ideal_display"] = _seconds_to_display(
            shift_ideal_summary["total_ideal_seconds"]
        )

        if count_summary["first_count_time"]:
            count_summary["first_count_time"] = system_time(
                count_summary["first_count_time"]
            )
        if count_summary["last_count_time"]:
            count_summary["last_count_time"] = system_time(
                count_summary["last_count_time"]
            )

        events.sort(key=lambda x: x["timestamp"])

        response_data = {
            "success": True,
            "plant_no": request_plant_no,
            "machine_no": machine_no,
            "date": date_str,
            "shift": selected_shift,
            "shift_start": system_time(shift_start),
            "shift_end": system_time(shift_end),
            "effective_end": system_time(effective_end),
            "machine_meta": machine_meta,
            "schedule": {
                "lunch_start": f"{lunch_start.strftime('%I:%M %p')}",
                "lunch_end": f"{lunch_end.strftime('%I:%M %p')}",
                "shift_a_start": "08:30 AM",
                "shift_a_end": "08:00 PM",
                "shift_b_start": "08:00 PM",
                "shift_b_end": "08:30 AM",
            },
            "summary": {
                "production": count_summary,
                "ideal": shift_ideal_summary,
                "total_hours": len(hourly_summary),
                "total_events": len(events),
            },
            "hourly_summary": hourly_summary,
            "events": events,
            "total_events": len(events),
        }
        response = Response(response_data)
        response["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
        response["Pragma"] = "no-cache"
        response["Expires"] = "0"
        return response
    except Exception as e:
        import traceback

        traceback.print_exc()
        return Response(
            {"success": False, "error": str(e), "events": [], "hourly_summary": []},
            status=500,
        )


@never_cache
@api_view(["GET"])
def get_plant1_machine_history(request):
    return _plant_history_common(
        request=request,
        plant_no=1,
        plant_location="Plant 1",
        data_table="live_data.plant1_data",
        lunch_start_tuple=(12, 45),
        lunch_end_tuple=(13, 15),
    )


@never_cache
@api_view(["GET"])
def get_machine_history(request):
    return _plant_history_common(
        request=request,
        plant_no=2,
        plant_location="Plant 2",
        data_table="live_data.plant2_data",
        lunch_start_tuple=(12, 15),
        lunch_end_tuple=(12, 45),
    )


# from .models import Operator
# from django.utils import timezone


# @never_cache
# @api_view(["GET"])
# def plant1_live(request):
#     """Plant 1 - LIVE DASHBOARD (Fixed to match Plant 2)"""
#     try:
#         from apps.machines.machine_state import MACHINE_STATE
#         from apps.mqtt.simple_plant1 import (
#             PLANT1_EXACT_REQUIREMENT_STATE,
#             J_TOPIC_MACHINE_MAPPING,
#             COUNT_TOPIC_MACHINE_MAPPING,
#         )

#         all_mapped_machines = list(range(1, 58))
#         live_machines = MACHINE_STATE.summarize(plant_filter=1, stale_after_seconds=300)

#         enhanced_machines = []
#         problem_machines = []

#         ist_tz = pytz.timezone("Asia/Kolkata")
#         now_ist = datetime.now(ist_tz)

#         for machine_no in all_mapped_machines:
#             machine_data = None

#             for m in live_machines:
#                 if m["machine_no"] == machine_no and m.get("plant") == 1:
#                     machine_data = m
#                     break

#             try:
#                 idle_status = (
#                     PLANT1_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(
#                         machine_no, now_ist
#                     )
#                 )
#                 exact_data = PLANT1_EXACT_REQUIREMENT_STATE.get_machine_data(machine_no)

#                 is_on = idle_status["on_since"] is not None
#                 is_producing = (
#                     idle_status["count_seconds_ago"] is not None
#                     and idle_status["count_seconds_ago"] <= 180
#                 )

#                 if is_on and not machine_data:
#                     tool_id = exact_data.get("current_tool_id", "N/A")
#                     shut_height = exact_data.get("current_shut_height", 0.0)

#                     machine_data = {
#                         "plant": 1,
#                         "machine_no": machine_no,
#                         "tool_id": tool_id,
#                         "count": 0,
#                         "shut_height": shut_height,
#                         "last_seen": "JSON only",
#                         "status": idle_status["status"],
#                         "current_hour_count": 0,
#                         "last_hour_count": 0,
#                         "cumulative_count": 0,
#                         "shift": exact_data.get("shift", "A"),
#                         "idle_time": idle_status["hourly_idle_total"],
#                     }

#                 if machine_data:
#                     machine_data.update(exact_data)

#                     problem_detected = (
#                         is_on and not is_producing and idle_status["is_idle"]
#                     )
#                     machine_data["problem_detected"] = problem_detected

#                     if problem_detected:
#                         problem_machines.append(machine_no)

#                     current_shift = PLANT1_EXACT_REQUIREMENT_STATE.get_shift_from_time(
#                         now_ist
#                     )

#                     if idle_status["last_count_time"]:
#                         machine_data["last_activity"] = idle_status[
#                             "last_count_time"
#                         ].strftime("%H:%M:%S")
#                     else:
#                         machine_data["last_activity"] = "Never"

#                     # ✅ FIX: Use exact_data which has DB-fetched last_hour_count
#                     machine_data["last_hour_count"] = exact_data.get(
#                         "last_hour_count", 0
#                     )
#                     machine_data["current_hour_count"] = exact_data.get(
#                         "current_hour_count", 0
#                     )
#                     machine_data["cumulative_count"] = exact_data.get(
#                         "cumulative_count", 0
#                     )
#                     machine_data["total_shift_idle_time"] = exact_data.get(
#                         "total_shift_idle_time", 0
#                     )

#                     machine_data["shut_height"] = exact_data.get(
#                         "current_shut_height", 0.0
#                     )
#                     machine_data["first_count_at"] = exact_data.get("first_count_at")
#                     machine_data["time_to_first_count"] = exact_data.get(
#                         "time_to_first_count"
#                     )

#                     machine_data.update(
#                         {
#                             "live_idle_time": idle_status["live_idle_time"],
#                             "accumulated_idle_time": idle_status[
#                                 "accumulated_idle_time"
#                             ],
#                             "hourly_idle_total": idle_status["hourly_idle_total"],
#                             "idle_time": idle_status["hourly_idle_total"],
#                             "is_idle": idle_status["is_idle"],
#                             "idle_type": idle_status["idle_type"],
#                             "status": idle_status["status"],
#                             "data_source": idle_status["data_source"],
#                             "on_since": (
#                                 idle_status["on_since"].strftime("%H:%M:%S")
#                                 if idle_status["on_since"]
#                                 else None
#                             ),
#                             "count_seconds_ago": idle_status["count_seconds_ago"],
#                             "json_seconds_ago": idle_status["json_seconds_ago"],
#                             "machine_on": is_on,
#                             "is_producing": is_producing,
#                         }
#                     )

#             except Exception as e:
#                 print(f"⚠️ Plant 1 M{machine_no} error: {e}")
#                 import traceback

#                 traceback.print_exc()

#             if machine_data:
#                 tool_id = machine_data.get("tool_id", "")
#                 tool_info = get_tool_info_from_tid_map(tool_id)

#                 machine_data.update(
#                     {
#                         "tool_customer": tool_info.get("customer", "N/A"),
#                         "tool_model": tool_info.get("model", "N/A"),
#                         "tool_part_name": tool_info.get("part_name", "N/A"),
#                         "tool_name": tool_info.get("tool_name", "N/A"),
#                         "tool_part_number": tool_info.get("part_number", "N/A"),
#                         "tool_tpm": tool_info.get("tpm", 0),
#                         "tool_epc": tool_info.get("epc", "N/A"),
#                     }
#                 )

#                 machine_data["plant"] = 1
#                 enhanced_machines.append(machine_data)
#             else:
#                 idle_status = (
#                     PLANT1_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(
#                         machine_no, now_ist
#                     )
#                 )

#                 enhanced_machines.append(
#                     {
#                         "plant": 1,
#                         "machine_no": machine_no,
#                         "tool_id": f"PLANT1_M{machine_no:02d}",
#                         "count": 0,
#                         "shut_height": 0.0,
#                         "first_count_at": None,
#                         "time_to_first_count": None,
#                         "last_seen": "Not active",
#                         "status": idle_status["status"],
#                         "current_hour_count": 0,
#                         "last_hour_count": 0,
#                         "cumulative_count": 0,
#                         "shift": "A",
#                         "idle_time": idle_status["hourly_idle_total"],
#                         "is_idle": idle_status["is_idle"],
#                         "idle_type": idle_status["idle_type"],
#                         "live_idle_time": idle_status["live_idle_time"],
#                         "accumulated_idle_time": idle_status["accumulated_idle_time"],
#                         "hourly_idle_total": idle_status["hourly_idle_total"],
#                         "last_activity": "Never",
#                         "tool_customer": "N/A",
#                         "tool_model": "N/A",
#                         "tool_part_name": "N/A",
#                         "tool_name": "N/A",
#                         "tool_part_number": "N/A",
#                         "tool_tpm": 0,
#                         "tool_epc": "N/A",
#                         "machine_on": False,
#                         "is_producing": False,
#                         "problem_detected": False,
#                         "on_since": None,
#                         "data_source": idle_status["data_source"],
#                     }
#                 )

#         enhanced_machines.sort(key=lambda x: x["machine_no"])

#         on_machines = [m for m in enhanced_machines if m.get("machine_on")]
#         producing_machines = [m for m in enhanced_machines if m.get("is_producing")]

#         response = Response(
#             {
#                 "success": True,
#                 "total_machines": len(enhanced_machines),
#                 "on_count": len(on_machines),
#                 "producing_count": len(producing_machines),
#                 "problem_count": len(problem_machines),
#                 "problem_machines": problem_machines,
#                 "machines": enhanced_machines,
#                 "plant": 1,
#                 "message": f"Plant 1 - ON:{len(on_machines)} | Producing:{len(producing_machines)} | Problems:{len(problem_machines)}",
#             }
#         )

#         response["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
#         response["Pragma"] = "no-cache"
#         response["Expires"] = "0"

#         return response

#     except Exception as e:
#         print(f"❌ Plant 1 API ERROR: {e}")
#         import traceback

#         traceback.print_exc()

#         return Response(
#             {"success": False, "error": str(e), "machines": [], "plant": 1}, status=500
#         )


# # ==============================================================
# # ✅ TID MAP HELPER - EPC / TOOL ID se full tool-part info
# # Put this helper in api/views.py above plant2_live and get_machine_history
# # ==============================================================
# def get_tool_info_from_tid_map(tool_id):
#     from django.db import connection
#     """Query public.tid_map table and return full tool/part info.

#     Handles uppercase/lowercase column names and EPC values.
#     Also supports matching EPC with/without leading 'e' where needed.
#     """
#     if not tool_id:
#         return {}

#     raw_tool_id = str(tool_id).strip()
#     if not raw_tool_id or raw_tool_id.upper() in ["NULL", "UNKNOWN", "N/A", "NO DATA", "FAILED"]:
#         return {}
#     if raw_tool_id.upper().startswith("PLANT2_M"):
#         return {}

#     clean_tool_id = raw_tool_id[:24].lower()

#     # Candidate EPC values. Some master sheets may store e200..., some may store 200...
#     candidates = []
#     for val in [clean_tool_id, raw_tool_id.lower(), raw_tool_id[:24].lower()]:
#         val = str(val).strip().lower()
#         if val and val not in candidates:
#             candidates.append(val)
#         if val.startswith("e") and val[1:] and val[1:] not in candidates:
#             candidates.append(val[1:])
#         if (not val.startswith("e")) and len(val) >= 23:
#             e_val = "e" + val
#             if e_val not in candidates:
#                 candidates.append(e_val)

#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT column_name
#                 FROM information_schema.columns
#                 WHERE table_schema = 'public'
#                   AND table_name = 'tid_map'
#                 ORDER BY ordinal_position
#             """)
#             columns = [row[0] for row in cursor.fetchall()]

#             if not columns:
#                 print("❌ tid_map table not found in public schema")
#                 return {}

#             epc_column = None
#             for col in columns:
#                 if col.upper() == "EPC":
#                     epc_column = col
#                     break

#             if not epc_column:
#                 print(f"❌ EPC column not found in public.tid_map. Available: {columns}")
#                 return {}

#             placeholders = ",".join(["%s"] * len(candidates))
#             query = (
#                 f'SELECT * FROM public.tid_map '
#                 f'WHERE LOWER(TRIM("{epc_column}"::text)) IN ({placeholders}) '
#                 f'LIMIT 1'
#             )
#             cursor.execute(query, candidates)
#             result = cursor.fetchone()

#             if not result:
#                 return {}

#             row_dict = dict(zip(columns, result))

#             def get_value(search_key):
#                 for col_name, col_value in row_dict.items():
#                     if col_name.upper() == search_key.upper():
#                         return str(col_value).strip() if col_value not in [None, ""] else "N/A"
#                 return "N/A"

#             tpm = 0
#             try:
#                 tpm_raw = get_value("TPM")
#                 if tpm_raw != "N/A":
#                     tpm = int(float(tpm_raw))
#             except Exception:
#                 tpm = 0

#             model_value = get_value("MODEL")
#             tool_data = {
#                 "customer": get_value("CUSTOMER"),
#                 "customer_name": get_value("CUSTOMER"),
#                 "model": model_value,
#                 "model_name": model_value,
#                 "part_name": get_value("PART_NAME"),
#                 "tool_name": get_value("TOOL_NAME"),
#                 "epc": get_value("EPC"),
#                 "part_number": get_value("PART_NUMBER"),
#                 "tpm": tpm,
#             }

#             print(
#                 f"✅ TID MAP FOUND | EPC={tool_data['epc']} | "
#                 f"{tool_data['customer']} | {tool_data['model_name']} | "
#                 f"{tool_data['part_name']} | {tool_data['part_number']}"
#             )
#             return tool_data

#     except Exception as e:
#         print(f"❌ TID map lookup error for {tool_id}: {e}")
#         return {}


# def get_safe_tid_value(tool_info, key, default="N/A"):
#     """Small helper for clean API values."""
#     value = (tool_info or {}).get(key, default)
#     if value in [None, "", "None", "NULL"]:
#         return default
#     return value


# @never_cache
# @api_view(["GET"])
# def plant2_live(request):
#     """
#     Plant 2 - LIVE DASHBOARD DATA
#     🌟 FINAL FIX: JSON Heartbeat Reset Bug Fixed. Continuous Idle Timer.
#     🌟 FIX 2: Shut Height logic improved to ensure UI visibility.
#     🌟 FIX 3: Timezone Offset & Last Hour Cumulative Count Bug Fixed.
#     """
#     try:
#         from apps.machines.machine_state import MACHINE_STATE
#         from apps.mqtt.simple_plant2 import (
#             PLANT2_EXACT_REQUIREMENT_STATE,
#             TOPIC_MACHINE_MAPPING,
#             get_machine_group,
#         )
#         from django.db import connection
#         import pytz
#         from datetime import datetime, timedelta

#         all_mapped_machines = set()
#         for machines_list in TOPIC_MACHINE_MAPPING.values():
#             all_mapped_machines.update(machines_list)
#         all_mapped_machines = sorted(list(all_mapped_machines))

#         live_machines = MACHINE_STATE.summarize(plant_filter=2, stale_after_seconds=300)

#         enhanced_machines = []
#         problem_machines = []

#         ist_tz = pytz.timezone("Asia/Kolkata")
#         now_ist = datetime.now(ist_tz)

#         def seconds_to_display(total_seconds):
#             total_seconds = int(total_seconds or 0)
#             hours = total_seconds // 3600
#             minutes = (total_seconds % 3600) // 60
#             seconds = total_seconds % 60
#             if hours > 0:
#                 return f"{hours} hour {minutes} min {seconds} sec"
#             if minutes > 0:
#                 return f"{minutes} min {seconds} sec"
#             return f"{seconds} sec"

#         def empty_ideal_summary():
#             return {"ONLINE": 0, "OFFLINE": 0}

#         def add_ideal_row(target, machine_no, online_seconds, offline_seconds):
#             key = str(machine_no).strip()
#             target[key] = {
#                 "ONLINE": int(online_seconds or 0),
#                 "OFFLINE": int(offline_seconds or 0),
#             }

#         # ✅ IMPORTANT FIX:
#         # ideal_time_segments_reason table ke time columns timestamp WITHOUT time zone hain.
#         # Isliye API me bhi boundary times ko simple IST naive datetime me pass karna zaroori hai.
#         # Nahi to PostgreSQL comparison 5:30 hour shift / zero summary issue de sakta hai.
#         def to_ist_naive(dt):
#             if dt is None:
#                 return None
#             if dt.tzinfo is not None:
#                 return dt.astimezone(ist_tz).replace(tzinfo=None, microsecond=0)
#             return dt.replace(microsecond=0)


#         def normalize_tool_id(tool_id):
#             """e000... / invalid RFID ko UI/history me valid tool nahi maanenge."""
#             if tool_id in [None, '', 'NULL', 'UNKNOWN', 'N/A', 'No data', 'Failed']:
#                 return None
#             clean = str(tool_id).strip().lower()[:24]
#             if len(clean) != 24:
#                 return None
#             if any(ch not in '0123456789abcdef' for ch in clean):
#                 return None
#             if clean.startswith('e000'):
#                 return None
#             if not clean.startswith('e2'):
#                 return None
#             return clean

#         def parse_valid_shut_height(value):
#             """0.01 / 1.01 / 0 / Failed / No data ko valid shut height nahi maanenge."""
#             if value in [None, '', '0', '0.0', '0.00', 0, 0.0, 'No data', 'Failed', 'None']:
#                 return None
#             try:
#                 num = float(value)
#             except Exception:
#                 return None
#             if num <= 10.0:
#                 return None
#             return num

#         def is_failed_shut_height_reading(value):
#             """Current reading 0.01 / 1.01 / Failed ho to UI me Failed show karna hai."""
#             if value in ['Failed', 'failed', 'FAILED']:
#                 return True
#             if value in [None, '', '0', '0.0', '0.00', 0, 0.0, 'No data', 'None', 'N/A']:
#                 return False
#             try:
#                 num = float(value)
#             except Exception:
#                 return False
#             return 0 < num <= 10.0

#         # =====================================================================
#         # 🚀 STEP 1: BULK DB QUERIES
#         # =====================================================================
#         current_shift = PLANT2_EXACT_REQUIREMENT_STATE.get_shift_from_time(now_ist)
#         current_hour = now_ist.replace(minute=0, second=0, microsecond=0)
#         previous_hour_start = current_hour - timedelta(hours=1)
#         shift_start = PLANT2_EXACT_REQUIREMENT_STATE.get_shift_start_datetime(now_ist)

#         bulk_last_hour = {}
#         bulk_cumulative = {}

#         # New ideal summary dictionaries from live_data.ideal_time_segments_reason
#         bulk_ideal_today = {}
#         bulk_ideal_shift = {}
#         bulk_ideal_hour = {}

#         # Latest valid tool/shut height from DB for UI fallback.
#         # This prevents card showing 0.00 when current MQTT sent 1.01/0/cache-miss.
#         bulk_latest_tool = {}
#         bulk_latest_shut_height = {}

#         today_start = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)

#         # ✅ DB timestamp columns WITHOUT time zone hain, so all query boundaries must be naive IST.
#         now_naive = to_ist_naive(now_ist)
#         today_start_naive = to_ist_naive(today_start)
#         shift_start_naive = to_ist_naive(shift_start)
#         current_hour_naive = to_ist_naive(current_hour)
#         previous_hour_start_naive = to_ist_naive(previous_hour_start)

#         try:
#             with connection.cursor() as cursor:
#                 # 🌟 FIX APPLIED HERE: Use MAX(cumulative) - MIN(cumulative) for accurate last hour count
#                 cursor.execute(
#                     """
#                     SELECT machine_no, COALESCE((MAX(cumulative_count) - MIN(cumulative_count)), 0)
#                     FROM Plant2_data
#                     WHERE timestamp >= %s AND timestamp < %s
#                     GROUP BY machine_no
#                 """,
#                     [previous_hour_start_naive, current_hour_naive],
#                 )
#                 for row in cursor.fetchall():
#                     bulk_last_hour[str(row[0]).strip()] = int(row[1])

#                 cursor.execute(
#                     """
#                     SELECT p1.machine_no, p1.cumulative_count
#                     FROM Plant2_data p1
#                     INNER JOIN (
#                         SELECT machine_no, MAX(timestamp) as max_ts
#                         FROM Plant2_data
#                         WHERE shift = %s AND timestamp >= %s
#                         GROUP BY machine_no
#                     ) p2 ON p1.machine_no = p2.machine_no AND p1.timestamp = p2.max_ts
#                 """,
#                     [current_shift, shift_start_naive],
#                 )
#                 for row in cursor.fetchall():
#                     bulk_cumulative[str(row[0]).strip()] = int(row[1])

#                 # ==========================================================
#                 # ✅ NEW IDEAL SUMMARY QUERY
#                 # Table: live_data.ideal_time_segments_reason
#                 # ideal_time is stored in seconds.
#                 # Backend code must split rows on hour-change, so hour summary
#                 # remains accurate without extra hour_bucket column.
#                 # ==========================================================

#                 # Today summary
#                 cursor.execute(
#                     """
#                     SELECT
#                         TRIM(machine_no::text) AS machine_key,
#                         COALESCE(SUM(CASE WHEN UPPER(TRIM(ideal_mode)) = 'ONLINE' THEN COALESCE(ideal_time, 0) ELSE 0 END), 0) AS online_seconds,
#                         COALESCE(SUM(CASE WHEN UPPER(TRIM(ideal_mode)) = 'OFFLINE' THEN COALESCE(ideal_time, 0) ELSE 0 END), 0) AS offline_seconds
#                     FROM live_data.ideal_time_segments_reason
#                     WHERE TRIM(plant_location) = %s
#                       AND ideal_start_at >= %s
#                       AND ideal_start_at < %s
#                     GROUP BY TRIM(machine_no::text)
#                     """,
#                     ["Plant 2", today_start_naive, now_naive],
#                 )
#                 for row in cursor.fetchall():
#                     add_ideal_row(bulk_ideal_today, row[0], row[1], row[2])

#                 # Current shift summary - FE card ab isi shift value ko show karega.
#                 cursor.execute(
#                     """
#                     SELECT
#                         TRIM(machine_no::text) AS machine_key,
#                         COALESCE(SUM(CASE WHEN UPPER(TRIM(ideal_mode)) = 'ONLINE' THEN COALESCE(ideal_time, 0) ELSE 0 END), 0) AS online_seconds,
#                         COALESCE(SUM(CASE WHEN UPPER(TRIM(ideal_mode)) = 'OFFLINE' THEN COALESCE(ideal_time, 0) ELSE 0 END), 0) AS offline_seconds
#                     FROM live_data.ideal_time_segments_reason
#                     WHERE TRIM(plant_location) = %s
#                       AND TRIM(shift) = %s
#                       AND ideal_start_at >= %s
#                       AND ideal_start_at < %s
#                     GROUP BY TRIM(machine_no::text)
#                     """,
#                     ["Plant 2", current_shift, shift_start_naive, now_naive],
#                 )
#                 for row in cursor.fetchall():
#                     add_ideal_row(bulk_ideal_shift, row[0], row[1], row[2])

#                 # Current hour summary
#                 cursor.execute(
#                     """
#                     SELECT
#                         TRIM(machine_no::text) AS machine_key,
#                         COALESCE(SUM(CASE WHEN UPPER(TRIM(ideal_mode)) = 'ONLINE' THEN COALESCE(ideal_time, 0) ELSE 0 END), 0) AS online_seconds,
#                         COALESCE(SUM(CASE WHEN UPPER(TRIM(ideal_mode)) = 'OFFLINE' THEN COALESCE(ideal_time, 0) ELSE 0 END), 0) AS offline_seconds
#                     FROM live_data.ideal_time_segments_reason
#                     WHERE TRIM(plant_location) = %s
#                       AND ideal_start_at >= %s
#                       AND ideal_start_at < %s
#                     GROUP BY TRIM(machine_no::text)
#                     """,
#                     ["Plant 2", current_hour_naive, now_naive],
#                 )
#                 for row in cursor.fetchall():
#                     add_ideal_row(bulk_ideal_hour, row[0], row[1], row[2])

#                 # Latest valid tool id in current shift. Ignore e000... fake RFID.
#                 cursor.execute(
#                     """
#                     SELECT DISTINCT ON (TRIM(machine_no::text))
#                         TRIM(machine_no::text) AS machine_key,
#                         LOWER(LEFT(TRIM(tool_id::text), 24)) AS clean_tool_id
#                     FROM live_data.plant2_data
#                     WHERE timestamp >= %s
#                       AND timestamp < %s
#                       AND tool_id IS NOT NULL
#                       AND LOWER(LEFT(TRIM(tool_id::text), 24)) ~ '^e2[0-9a-f]{22}$'
#                       AND LOWER(LEFT(TRIM(tool_id::text), 24)) NOT LIKE 'e000%%'
#                     ORDER BY TRIM(machine_no::text), timestamp DESC
#                     """,
#                     [shift_start_naive, now_naive],
#                 )
#                 for row in cursor.fetchall():
#                     bulk_latest_tool[str(row[0]).strip()] = row[1]

#                 # Latest valid shut height in current shift. Ignore 0/1.01/cache-miss.
#                 cursor.execute(
#                     """
#                     WITH valid_height AS (
#                         SELECT
#                             TRIM(machine_no::text) AS machine_key,
#                             timestamp,
#                             CASE
#                                 WHEN TRIM(shut_height::text) ~ '^[0-9]+(\.[0-9]+)?$'
#                                 THEN TRIM(shut_height::text)::numeric
#                                 ELSE NULL
#                             END AS height_value
#                         FROM live_data.plant2_data
#                         WHERE timestamp >= %s
#                           AND timestamp < %s
#                     )
#                     SELECT DISTINCT ON (machine_key)
#                         machine_key,
#                         height_value
#                     FROM valid_height
#                     WHERE height_value > 10
#                     ORDER BY machine_key, timestamp DESC
#                     """,
#                     [shift_start_naive, now_naive],
#                 )
#                 for row in cursor.fetchall():
#                     bulk_latest_shut_height[str(row[0]).strip()] = float(row[1])

#                 print(
#                     f"✅ IDEAL API SUMMARY | Shift={current_shift} | "
#                     f"TodayRows={len(bulk_ideal_today)} | ShiftRows={len(bulk_ideal_shift)} | HourRows={len(bulk_ideal_hour)} | "
#                     f"ToolRows={len(bulk_latest_tool)} | HeightRows={len(bulk_latest_shut_height)}"
#                 )
#         except Exception as e:
#             print(f"❌ Bulk Data Query Error: {e}")

#         # =====================================================================
#         # 🚀 STEP 2: LOOP THROUGH MACHINES
#         # =====================================================================
#         collected_tools = set()
#         intermediate_machine_data = []

#         for machine_no in all_mapped_machines:
#             machine_data = None
#             for m in live_machines:
#                 if m["machine_no"] == machine_no and m.get("plant") == 2:
#                     machine_data = m
#                     break

#             try:
#                 idle_status = (
#                     PLANT2_EXACT_REQUIREMENT_STATE.idle_tracker.get_idle_status(
#                         machine_no, now_ist
#                     )
#                 )
#                 status_info = PLANT2_EXACT_REQUIREMENT_STATE.get_machine_status(
#                     machine_no
#                 )

#                 m_str = str(machine_no)
#                 db_last_hour = bulk_last_hour.get(m_str, 0)
#                 db_cumulative = bulk_cumulative.get(m_str, 0)
#                 db_ideal_today = bulk_ideal_today.get(m_str, empty_ideal_summary())
#                 db_ideal_shift = bulk_ideal_shift.get(m_str, empty_ideal_summary())
#                 db_ideal_hour = bulk_ideal_hour.get(m_str, empty_ideal_summary())

#                 is_on = status_info["machine_on"]
#                 is_producing = status_info["is_producing"]

#                 # Live ideal values are added to DB completed values for FE display.
#                 # DB insert still happens only when segment closes, not every second.
#                 live_ideal_mode = None
#                 live_ideal_seconds = 0
#                 live_ideal_hour_seconds = 0

#                 # 🌟 FIX 1: OFFLINE TIME TRACKING (Uses both JSON + COUNT to know when machine died)
#                 last_signal_time = None
#                 if (
#                     machine_no in PLANT2_EXACT_REQUIREMENT_STATE.last_count_time
#                     and machine_no in PLANT2_EXACT_REQUIREMENT_STATE.machine_json_status
#                 ):
#                     last_signal_time = max(
#                         PLANT2_EXACT_REQUIREMENT_STATE.last_count_time[machine_no],
#                         PLANT2_EXACT_REQUIREMENT_STATE.machine_json_status[machine_no][
#                             "last_json_time"
#                         ],
#                     )
#                 elif machine_no in PLANT2_EXACT_REQUIREMENT_STATE.last_count_time:
#                     last_signal_time = PLANT2_EXACT_REQUIREMENT_STATE.last_count_time[
#                         machine_no
#                     ]
#                 elif machine_no in PLANT2_EXACT_REQUIREMENT_STATE.machine_json_status:
#                     last_signal_time = (
#                         PLANT2_EXACT_REQUIREMENT_STATE.machine_json_status[machine_no][
#                             "last_json_time"
#                         ]
#                     )

#                 # Ignore yesterday's signal
#                 if last_signal_time and last_signal_time < shift_start:
#                     last_signal_time = None

#                 offline_since_str = None
#                 offline_duration_minutes = None
#                 offline_since_obj = None

#                 if not is_on:  # Completely offline
#                     if last_signal_time:
#                         offline_since_obj = last_signal_time
#                     else:
#                         offline_since_obj = shift_start

#                     offline_since_str = offline_since_obj.strftime("%H:%M:%S")
#                     offline_duration_minutes = int(
#                         (now_ist - offline_since_obj).total_seconds() / 60
#                     )
#                     live_ideal_mode = "OFFLINE"
#                     live_ideal_seconds = max(0, int((now_ist - offline_since_obj).total_seconds()))
#                     live_ideal_hour_seconds = max(
#                         0,
#                         int((now_ist - max(offline_since_obj, current_hour)).total_seconds()),
#                     )

#                 on_since_str = None
#                 first_count_str = None
#                 time_to_first_count = None

#                 if (
#                     is_on
#                     and machine_no in PLANT2_EXACT_REQUIREMENT_STATE.machine_on_since
#                 ):
#                     on_since = PLANT2_EXACT_REQUIREMENT_STATE.machine_on_since[
#                         machine_no
#                     ]
#                     if on_since >= shift_start:
#                         on_since_str = on_since.strftime("%H:%M:%S")
#                         if (
#                             machine_no
#                             in PLANT2_EXACT_REQUIREMENT_STATE.first_count_time
#                         ):
#                             first_count = (
#                                 PLANT2_EXACT_REQUIREMENT_STATE.first_count_time[
#                                     machine_no
#                                 ]
#                             )
#                             if first_count >= shift_start:
#                                 first_count_str = first_count.strftime("%H:%M:%S")
#                                 delay = (first_count - on_since).total_seconds()
#                                 time_to_first_count = int(delay / 60)

#                 # 🌟 FIX 2: IMPROVED SHUT HEIGHT + TOOL FETCHING 🌟
#                 # UI requirement:
#                 # Current MQTT/cache miss me 0.01 / 1.01 / Failed aaye to card me "Failed" show hoga.
#                 # Valid height aaye to wahi show hoga. No-data me 0.00 nahi dikhayenge.
#                 segment_info = PLANT2_EXACT_REQUIREMENT_STATE.machine_segments.get(
#                     machine_no, {}
#                 )
#                 segment_shut_height = segment_info.get("shut_height")
#                 status_shut_height = status_info.get("shut_height")

#                 if is_failed_shut_height_reading(status_shut_height):
#                     final_shut_height = "Failed"
#                 else:
#                     final_shut_height = (
#                         parse_valid_shut_height(status_shut_height)
#                         or parse_valid_shut_height(segment_shut_height)
#                         or bulk_latest_shut_height.get(m_str)
#                         or "No data"
#                     )

#                 safe_current_tool_id = (
#                     normalize_tool_id(status_info.get("tool_id"))
#                     or normalize_tool_id(segment_info.get("tool_id"))
#                     or bulk_latest_tool.get(m_str)
#                     or "N/A"
#                 )

#                 # ONLINE ideal live calculation:
#                 # Machine ON hai, production/count nahi aa raha, aur idle tracker says idle.
#                 if is_on and (not is_producing) and idle_status.get("is_idle"):
#                     live_ideal_mode = "ONLINE"
#                     online_start_obj = PLANT2_EXACT_REQUIREMENT_STATE.last_count_time.get(machine_no)
#                     if not online_start_obj or online_start_obj < shift_start:
#                         online_start_obj = PLANT2_EXACT_REQUIREMENT_STATE.machine_on_since.get(machine_no, shift_start)
#                     live_ideal_seconds = max(0, int((now_ist - online_start_obj).total_seconds()))
#                     live_ideal_hour_seconds = max(
#                         0,
#                         int((now_ist - max(online_start_obj, current_hour)).total_seconds()),
#                     )

#                 # Completed DB values + current live running ideal for FE summary
#                 online_ideal_today_seconds = db_ideal_today["ONLINE"] + (live_ideal_seconds if live_ideal_mode == "ONLINE" else 0)
#                 offline_ideal_today_seconds = db_ideal_today["OFFLINE"] + (live_ideal_seconds if live_ideal_mode == "OFFLINE" else 0)
#                 total_ideal_today_seconds = online_ideal_today_seconds + offline_ideal_today_seconds

#                 online_ideal_shift_seconds = db_ideal_shift["ONLINE"] + (live_ideal_seconds if live_ideal_mode == "ONLINE" else 0)
#                 offline_ideal_shift_seconds = db_ideal_shift["OFFLINE"] + (live_ideal_seconds if live_ideal_mode == "OFFLINE" else 0)
#                 total_ideal_shift_seconds = online_ideal_shift_seconds + offline_ideal_shift_seconds

#                 online_ideal_hour_seconds = db_ideal_hour["ONLINE"] + (live_ideal_hour_seconds if live_ideal_mode == "ONLINE" else 0)
#                 offline_ideal_hour_seconds = db_ideal_hour["OFFLINE"] + (live_ideal_hour_seconds if live_ideal_mode == "OFFLINE" else 0)
#                 total_ideal_hour_seconds = online_ideal_hour_seconds + offline_ideal_hour_seconds

#                 exact_data = {
#                     "machine_no": machine_no,
#                     "current_hour_count": PLANT2_EXACT_REQUIREMENT_STATE.current_hour_counts.get(
#                         machine_no, 0
#                     ),
#                     "last_hour_count": db_last_hour,
#                     "cumulative_count": db_cumulative,
#                     # Old keys kept for existing FE compatibility.
#                     # Now these come from new ideal table + current live ideal.
#                     "idle_time": total_ideal_hour_seconds,
#                     "total_shift_idle_time": total_ideal_shift_seconds,

#                     # New ideal summary fields for FE. All values are seconds.
#                     "live_ideal_mode": live_ideal_mode,
#                     "live_ideal_time": live_ideal_seconds,
#                     "live_ideal_display": seconds_to_display(live_ideal_seconds),

#                     "online_ideal_this_hour": online_ideal_hour_seconds,
#                     "offline_ideal_this_hour": offline_ideal_hour_seconds,
#                     "total_ideal_this_hour": total_ideal_hour_seconds,
#                     "online_ideal_this_hour_display": seconds_to_display(online_ideal_hour_seconds),
#                     "offline_ideal_this_hour_display": seconds_to_display(offline_ideal_hour_seconds),
#                     "total_ideal_this_hour_display": seconds_to_display(total_ideal_hour_seconds),

#                     "online_ideal_shift": online_ideal_shift_seconds,
#                     "offline_ideal_shift": offline_ideal_shift_seconds,
#                     "total_ideal_shift": total_ideal_shift_seconds,
#                     "online_ideal_shift_display": seconds_to_display(online_ideal_shift_seconds),
#                     "offline_ideal_shift_display": seconds_to_display(offline_ideal_shift_seconds),
#                     "total_ideal_shift_display": seconds_to_display(total_ideal_shift_seconds),

#                     "online_ideal_today": online_ideal_today_seconds,
#                     "offline_ideal_today": offline_ideal_today_seconds,
#                     "total_ideal_today": total_ideal_today_seconds,
#                     "online_ideal_today_display": seconds_to_display(online_ideal_today_seconds),
#                     "offline_ideal_today_display": seconds_to_display(offline_ideal_today_seconds),
#                     "total_ideal_today_display": seconds_to_display(total_ideal_today_seconds),
#                     "shift": current_shift,
#                     "machine_on": is_on,
#                     "is_producing": is_producing,
#                     "has_count_data": status_info["has_count_data"],
#                     "has_json_data": status_info["has_json_data"],
#                     "count_seconds_ago": status_info["count_seconds_ago"],
#                     "json_seconds_ago": status_info["json_seconds_ago"],
#                     "current_tool_id": safe_current_tool_id,
#                     "tool_id": safe_current_tool_id,
#                     # ✅ ULTIMATE FIX: Key ka naam 'shut_height' hona chahiye (Pehle 'current_shut_height' tha)
#                     "shut_height": final_shut_height,
#                     "data_source": status_info["data_source"],
#                     "on_since": on_since_str,
#                     "first_count_at": first_count_str,
#                     "time_to_first_count": time_to_first_count,
#                     "offline_since": offline_since_str,
#                     "offline_duration_minutes": offline_duration_minutes,
#                 }

#                 tool_id = exact_data.get("current_tool_id", "N/A")

#                 m_data = machine_data or {
#                     "plant": 2,
#                     "machine_no": machine_no,
#                     "tool_id": safe_current_tool_id if safe_current_tool_id != "N/A" else f"PLANT2_M{machine_no:02d}",
#                     "count": 0,
#                     "shut_height": final_shut_height,
#                     "last_seen": "JSON only" if is_on else "Not active",
#                     "status": "OFFLINE" if not is_on else idle_status["status"],
#                     "current_hour_count": 0,
#                     "last_hour_count": 0,
#                     "cumulative_count": 0,
#                     "shift": exact_data.get("shift", "A"),
#                     "idle_time": total_ideal_hour_seconds,
#                 }

#                 m_data.update(exact_data)

#                 problem_detected = is_on and not is_producing and idle_status["is_idle"]
#                 m_data["problem_detected"] = problem_detected
#                 if problem_detected:
#                     problem_machines.append(machine_no)

#                 # 🌟 MASTER FIX 3: IDLE TIMER FIX 🌟
#                 last_count_obj = PLANT2_EXACT_REQUIREMENT_STATE.last_count_time.get(
#                     machine_no
#                 )
#                 if last_count_obj and last_count_obj >= shift_start:
#                     m_data["last_activity"] = last_count_obj.strftime("%H:%M:%S")
#                 else:
#                     m_data["last_activity"] = "Never"

#                 m_data.update(
#                     {
#                         "live_idle_time": idle_status["live_idle_time"],
#                         "accumulated_idle_time": idle_status["accumulated_idle_time"],
#                         "hourly_idle_total": idle_status["hourly_idle_total"],
#                         "idle_time": total_ideal_hour_seconds,
#                         "is_idle": idle_status["is_idle"],
#                         "idle_type": idle_status["idle_type"],
#                     }
#                 )

#                 tool_id_for_bulk = normalize_tool_id(m_data.get("tool_id", ""))
#                 if tool_id_for_bulk:
#                     collected_tools.add(tool_id_for_bulk[:24])

#                 intermediate_machine_data.append(
#                     {
#                         "has_data": True,
#                         "data": m_data,
#                         "machine_no": machine_no,
#                         "idle_status": idle_status,
#                     }
#                 )

#             except Exception as e:
#                 print(f"⚠️ M{machine_no} error: {e}")
#                 intermediate_machine_data.append(
#                     {
#                         "has_data": False,
#                         "data": None,
#                         "machine_no": machine_no,
#                         "idle_status": None,
#                     }
#                 )

#         # =====================================================================
#         # 🚀 STEP 3: TID MAP TOOL FETCH
#         # =====================================================================
#         # Requirement:
#         # - EPC/tool_id ke basis par public.tid_map se full info fetch karni hai.
#         # - Customer, Model, Part Name, Part Number, Tool Name sab API me bhejna hai.
#         # - Agar master me info nahi mile, UI ko N/A milega; count/Redis/WebSocket untouched.
#         bulk_tool_info = {}
#         if collected_tools:
#             for tid in collected_tools:
#                 try:
#                     info = get_tool_info_from_tid_map(tid)
#                     if info:
#                         clean_epc = str(info.get("epc") or tid).strip().lower()[:24]
#                         bulk_tool_info[tid] = info
#                         bulk_tool_info[clean_epc] = info
#                     else:
#                         bulk_tool_info[tid] = {}
#                 except Exception as e:
#                     print(f"❌ TID map fetch error for {tid}: {e}")
#                     bulk_tool_info[tid] = {}

#         # =====================================================================
#         # 🚀 STEP 4: FINAL ASSEMBLY
#         # =====================================================================
#         for item in intermediate_machine_data:
#             machine_no = item["machine_no"]
#             if item["has_data"]:
#                 machine_data = item["data"]
#                 tool_id = machine_data.get("tool_id", "")
#                 clean_tool_id = tool_id[:24] if tool_id else ""
#                 tool_info = bulk_tool_info.get(clean_tool_id, {})

#                 # ✅ TID MAP DATA added with both old FE keys and new plain keys.
#                 # Old keys: tool_customer/tool_model/tool_part_name/tool_part_number
#                 # New keys: customer/model_name/part_name/part_number/tool_name/epc
#                 customer_val = get_safe_tid_value(tool_info, "customer")
#                 model_val = get_safe_tid_value(tool_info, "model_name")
#                 part_name_val = get_safe_tid_value(tool_info, "part_name")
#                 part_number_val = get_safe_tid_value(tool_info, "part_number")
#                 tool_name_val = get_safe_tid_value(tool_info, "tool_name")
#                 epc_val = get_safe_tid_value(tool_info, "epc", clean_tool_id or "N/A")

#                 machine_data.update(
#                     {
#                         "machine_group": get_machine_group(machine_no),

#                         # Existing FE compatibility
#                         "tool_customer": customer_val,
#                         "tool_model": model_val,
#                         "tool_part_name": part_name_val,
#                         "tool_name": tool_name_val,
#                         "tool_part_number": part_number_val,
#                         "tool_tpm": int(tool_info.get("tpm", 0) or 0) if tool_info else 0,
#                         "tool_epc": epc_val,

#                         # New clean API keys
#                         "customer": customer_val,
#                         "customer_name": customer_val,
#                         "model": model_val,
#                         "model_name": model_val,
#                         "part_name": part_name_val,
#                         "part_number": part_number_val,
#                         "epc": epc_val,
#                         "plant": 2,
#                     }
#                 )
#                 enhanced_machines.append(machine_data)

#         enhanced_machines.sort(key=lambda x: x["machine_no"])

#         on_machines = [m for m in enhanced_machines if m.get("machine_on")]
#         producing_machines = [m for m in enhanced_machines if m.get("is_producing")]

#         groups_summary = {}
#         for group in ["J1", "J2", "J3", "J4", "J5", "J6", "J7", "J8", "J9"]:
#             group_machines = [
#                 m for m in enhanced_machines if m.get("machine_group") == group
#             ]
#             if group_machines:
#                 groups_summary[group] = {
#                     "total": len(group_machines),
#                     "on": len([m for m in group_machines if m.get("machine_on")]),
#                     "producing": len(
#                         [m for m in group_machines if m.get("is_producing")]
#                     ),
#                     "problems": len(
#                         [m for m in group_machines if m.get("problem_detected")]
#                     ),
#                 }

#         response = Response(
#             {
#                 "success": True,
#                 "total_machines": len(enhanced_machines),
#                 "on_count": len(on_machines),
#                 "producing_count": len(producing_machines),
#                 "problem_count": len(problem_machines),
#                 "problem_machines": problem_machines,
#                 "groups_summary": groups_summary,
#                 "machines": enhanced_machines,
#                 "plant": 2,
#             }
#         )
#         response["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
#         return response

#     except Exception as e:
#         import traceback

#         traceback.print_exc()
#         return Response(
#             {"success": False, "error": str(e), "machines": [], "plant": 2}, status=500
#         )

# @never_cache
# @api_view(["GET"])
# def get_machine_history(request):
#     """
#     Plant 2 Machine History - FAST SHIFT WISE STORY API

#     Goal:
#     - 1 API call me shift-wise machine story ready.
#     - Hour-wise production count from live_data.plant2_data.
#     - ONLINE/OFFLINE ideal segments with reason from live_data.ideal_time_segments_reason.
#     - ON/OFF/TOOL_CHANGE/SHUT_HEIGHT_CHANGE from Machine_Event_Logs.
#     - Lunch break shown as 12:15 PM to 12:45 PM.
#     - Shift end shown at scheduled shift end (Shift A = 08:00 PM).
#     - Old Plant2_hourly_idle table is NOT used for ideal history.

#     Query params:
#         plant_no=2
#         machine_no=7
#         date=2026-07-14
#         shift=A        optional, default A. Supports A / B / ALL.
#     """
#     try:
#         from django.db import connection
#         import pytz
#         from datetime import datetime, timedelta, time

#         plant_no = int(request.GET.get("plant_no", 2))
#         machine_no = str(request.GET.get("machine_no", "")).strip()
#         date_str = request.GET.get("date", "").strip()
#         shift_param = str(request.GET.get("shift", "A")).strip().upper()

#         ist_tz = pytz.timezone("Asia/Kolkata")
#         now_ist = datetime.now(ist_tz)

#         if not date_str:
#             date_str = now_ist.strftime("%Y-%m-%d")

#         if not machine_no:
#             return Response(
#                 {"success": False, "error": "machine_no is required"},
#                 status=400,
#             )

#         target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

#         def localize_ist(dt):
#             if dt.tzinfo is None:
#                 return ist_tz.localize(dt)
#             return dt.astimezone(ist_tz)

#         def to_naive_str(dt):
#             dt = localize_ist(dt)
#             return dt.replace(tzinfo=None, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

#         def seconds_to_display(total_seconds):
#             total_seconds = int(total_seconds or 0)
#             if total_seconds < 0:
#                 total_seconds = 0
#             hours = total_seconds // 3600
#             minutes = (total_seconds % 3600) // 60
#             seconds = total_seconds % 60
#             if hours > 0:
#                 return f"{hours} hr {minutes} min {seconds} sec"
#             if minutes > 0:
#                 return f"{minutes} min {seconds} sec"
#             return f"{seconds} sec"

#         def title_time(dt):
#             return localize_ist(dt).strftime("%I:%M %p")

#         def system_time(dt):
#             return localize_ist(dt).strftime("%Y-%m-%d %H:%M:%S")

#         def json_ts(dt):
#             return localize_ist(dt).isoformat()


#         def normalize_tool_id(tool_id):
#             if tool_id in [None, '', 'NULL', 'UNKNOWN', 'N/A', 'No data', 'Failed']:
#                 return None
#             clean = str(tool_id).strip().lower()[:24]
#             if len(clean) != 24:
#                 return None
#             if any(ch not in '0123456789abcdef' for ch in clean):
#                 return None
#             if clean.startswith('e000'):
#                 return None
#             if not clean.startswith('e2'):
#                 return None
#             return clean

#         def parse_valid_shut_height(value):
#             if value in [None, '', '0', '0.0', '0.00', 0, 0.0, 'No data', 'Failed', 'None']:
#                 return None
#             try:
#                 num = float(value)
#             except Exception:
#                 return None
#             if num <= 10.0:
#                 return None
#             return num

#         def is_failed_shut_height_reading(value):
#             if value in ['Failed', 'failed', 'FAILED']:
#                 return True
#             if value in [None, '', '0', '0.0', '0.00', 0, 0.0, 'No data', 'None', 'N/A']:
#                 return False
#             try:
#                 num = float(value)
#             except Exception:
#                 return False
#             return 0 < num <= 10.0

#         def extract_tool_id_from_text(text):
#             """Event details text se pehla valid e2... EPC nikalo."""
#             try:
#                 import re
#                 matches = re.findall(r'e2[0-9a-fA-F]{22}', str(text or ''))
#                 for match in matches:
#                     clean = normalize_tool_id(match)
#                     if clean:
#                         return clean
#             except Exception:
#                 pass
#             return None

#         def get_shift_window(date_obj, shift_name):
#             """
#             Same Plant 2 live logic ke saath shift window.
#             A = 08:30 to 20:00
#             B = 20:30 to next day 08:30
#             ALL = full production day 08:30 to next day 08:30
#             """
#             shift_name = (shift_name or "A").upper()
#             if shift_name == "B":
#                 start = ist_tz.localize(datetime.combine(date_obj, time(20, 30, 0)))
#                 end = ist_tz.localize(datetime.combine(date_obj + timedelta(days=1), time(8, 30, 0)))
#                 return start, end, "B"
#             if shift_name == "ALL":
#                 start = ist_tz.localize(datetime.combine(date_obj, time(8, 30, 0)))
#                 end = ist_tz.localize(datetime.combine(date_obj + timedelta(days=1), time(8, 30, 0)))
#                 return start, end, "ALL"
#             start = ist_tz.localize(datetime.combine(date_obj, time(8, 30, 0)))
#             end = ist_tz.localize(datetime.combine(date_obj, time(20, 0, 0)))
#             return start, end, "A"

#         shift_start, shift_end, selected_shift = get_shift_window(target_date, shift_param)

#         # Current day me future part avoid karo. Old dates me full shift dikhega.
#         effective_end = shift_end
#         if shift_start.date() <= now_ist.date() <= shift_end.date():
#             effective_end = min(shift_end, now_ist)

#         start_str_naive = to_naive_str(shift_start)
#         end_str_naive = to_naive_str(effective_end)
#         start_str_tz = localize_ist(shift_start).strftime("%Y-%m-%d %H:%M:%S+05:30")
#         end_str_tz = localize_ist(effective_end).strftime("%Y-%m-%d %H:%M:%S+05:30")

#         # ------------------------------------------------------------------
#         # Hour buckets: 08:30-09:00, 09:00-10:00, ...
#         # ------------------------------------------------------------------
#         hour_buckets = []
#         cursor_time = shift_start
#         while cursor_time < effective_end:
#             if cursor_time.minute != 0 or cursor_time.second != 0:
#                 next_time = cursor_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
#             else:
#                 next_time = cursor_time + timedelta(hours=1)
#             if next_time > shift_end:
#                 next_time = shift_end
#             bucket_end = min(next_time, effective_end)
#             hour_buckets.append(
#                 {
#                     "bucket_key": to_naive_str(cursor_time),
#                     "start": cursor_time,
#                     "end": bucket_end,
#                     "scheduled_end": next_time,
#                     "count": 0,
#                     "latest_cumulative": 0,
#                     "online_ideal_seconds": 0,
#                     "offline_ideal_seconds": 0,
#                     "total_ideal_seconds": 0,
#                     "ideal_segments": [],
#                     "machine_events": [],
#                     "on_off_events": [],
#                     "tool_changes": [],
#                     "shut_height_changes": [],
#                 }
#             )
#             cursor_time = next_time

#         bucket_by_key = {b["bucket_key"]: b for b in hour_buckets}

#         def find_bucket_for_time(dt):
#             dt = localize_ist(dt)
#             for bucket in hour_buckets:
#                 if bucket["start"] <= dt < bucket["scheduled_end"]:
#                     return bucket
#             # Exact end time ko last bucket me daal do.
#             if hour_buckets and dt == hour_buckets[-1]["scheduled_end"]:
#                 return hour_buckets[-1]
#             return None

#         def add_timeline_event(events, dt, event_type, title, details, shift="", extra=None):
#             dt = localize_ist(dt)
#             payload = {
#                 "timestamp": dt.timestamp(),
#                 "time": dt.strftime("%I:%M %p"),
#                 "time_str": dt.strftime("%I:%M %p"),
#                 "system_time": dt.strftime("%Y-%m-%d %H:%M:%S"),
#                 "type": event_type,
#                 "title": title,
#                 "details": details or "",
#                 "shift": shift or ("A" if selected_shift == "ALL" else selected_shift),
#             }
#             if extra:
#                 payload.update(extra)
#             events.append(payload)
#             return payload

#         events = []
#         machine_meta = {
#             "customer": "N/A",
#             "customer_name": "N/A",
#             "model": "N/A",
#             "model_name": "N/A",
#             "part_name": "N/A",
#             "part_number": "N/A",
#             "tool_name": "N/A",
#             "tool_id": "N/A",
#             "epc": "N/A",
#             "shut_height": "N/A",
#         }

#         # Shift synthetic events
#         if shift_start <= now_ist or target_date < now_ist.date():
#             add_timeline_event(
#                 events,
#                 shift_start,
#                 "SHIFT_START",
#                 f"Shift {selected_shift} Started" if selected_shift != "ALL" else "Production Day Started",
#                 f"History window started at {shift_start.strftime('%I:%M %p')}.",
#                 selected_shift,
#             )

#         # Lunch only Shift A / ALL window me
#         lunch_start = ist_tz.localize(datetime.combine(target_date, time(12, 15, 0)))
#         lunch_end = ist_tz.localize(datetime.combine(target_date, time(12, 45, 0)))
#         if shift_start <= lunch_start < effective_end:
#             add_timeline_event(
#                 events,
#                 lunch_start,
#                 "LUNCH_START",
#                 "Lunch Break Started",
#                 "Scheduled lunch break started at 12:15 PM.",
#                 "A",
#             )
#         if shift_start <= lunch_end < effective_end:
#             add_timeline_event(
#                 events,
#                 lunch_end,
#                 "LUNCH_END",
#                 "Lunch Break Ended",
#                 "Scheduled lunch break ended at 12:45 PM.",
#                 "A",
#             )

#         # Shift end event
#         # Shift A end should show at 08:00 PM when the shift is completed.
#         if selected_shift == "A" and shift_end <= effective_end:
#             add_timeline_event(
#                 events,
#                 shift_end,
#                 "SHIFT_END",
#                 "Shift A Ended",
#                 "Shift A ended at 08:00 PM.",
#                 "A",
#             )
#         elif selected_shift == "B" and shift_end <= effective_end:
#             add_timeline_event(
#                 events,
#                 shift_end,
#                 "SHIFT_END",
#                 "Shift B Ended",
#                 "Shift B ended at 08:30 AM.",
#                 "B",
#             )
#         elif selected_shift == "ALL":
#             shift_a_end_for_all = ist_tz.localize(datetime.combine(target_date, time(20, 0, 0)))
#             if shift_start <= shift_a_end_for_all <= effective_end:
#                 add_timeline_event(
#                     events,
#                     shift_a_end_for_all,
#                     "SHIFT_END",
#                     "Shift A Ended",
#                     "Shift A ended at 08:00 PM.",
#                     "A",
#                 )

#         count_summary = {
#             "total_count": 0,
#             "first_count_time": None,
#             "last_count_time": None,
#             "latest_cumulative": 0,
#         }
#         shift_ideal_summary = {
#             "online_ideal_seconds": 0,
#             "offline_ideal_seconds": 0,
#             "total_ideal_seconds": 0,
#             "online_ideal_display": "0 sec",
#             "offline_ideal_display": "0 sec",
#             "total_ideal_display": "0 sec",
#         }

#         with connection.cursor() as cursor:
#             # --------------------------------------------------------------
#             # 1) Latest tool meta in selected window
#             # --------------------------------------------------------------
#             cursor.execute(
#                 """
#                 SELECT LOWER(LEFT(TRIM(tool_id::text), 24)) AS clean_tool_id
#                 FROM live_data.plant2_data
#                 WHERE machine_no = %s
#                   AND timestamp >= %s::timestamp WITHOUT TIME ZONE
#                   AND timestamp <  %s::timestamp WITHOUT TIME ZONE
#                   AND tool_id IS NOT NULL
#                   AND LOWER(LEFT(TRIM(tool_id::text), 24)) ~ '^e2[0-9a-f]{22}$'
#                   AND LOWER(LEFT(TRIM(tool_id::text), 24)) NOT LIKE 'e000%%'
#                 ORDER BY timestamp DESC
#                 LIMIT 1
#                 """,
#                 [machine_no, start_str_naive, end_str_naive],
#             )
#             tool_res = cursor.fetchone()
#             if tool_res and tool_res[0]:
#                 tool_id = normalize_tool_id(tool_res[0])
#                 if tool_id:
#                     tid_info = get_tool_info_from_tid_map(tool_id)
#                     machine_meta.update({
#                         "tool_id": tool_id,
#                         "epc": get_safe_tid_value(tid_info, "epc", tool_id),
#                         "customer": get_safe_tid_value(tid_info, "customer"),
#                         "customer_name": get_safe_tid_value(tid_info, "customer"),
#                         "model": get_safe_tid_value(tid_info, "model_name"),
#                         "model_name": get_safe_tid_value(tid_info, "model_name"),
#                         "part_name": get_safe_tid_value(tid_info, "part_name"),
#                         "part_number": get_safe_tid_value(tid_info, "part_number"),
#                         "tool_name": get_safe_tid_value(tid_info, "tool_name"),
#                     })

#             # Latest shut height for history header:
#             # Agar latest reading 0.01 / 1.01 / Failed hai to header me Failed show hoga.
#             # Agar latest valid hai to valid height show hoga.
#             cursor.execute(
#                 """
#                 SELECT shut_height
#                 FROM live_data.plant2_data
#                 WHERE machine_no = %s
#                   AND timestamp >= %s::timestamp WITHOUT TIME ZONE
#                   AND timestamp <  %s::timestamp WITHOUT TIME ZONE
#                 ORDER BY timestamp DESC
#                 LIMIT 1
#                 """,
#                 [machine_no, start_str_naive, end_str_naive],
#             )
#             latest_raw_height_res = cursor.fetchone()
#             latest_raw_height = latest_raw_height_res[0] if latest_raw_height_res else None

#             if is_failed_shut_height_reading(latest_raw_height):
#                 machine_meta["shut_height"] = "Failed"
#             else:
#                 cursor.execute(
#                     """
#                     WITH valid_height AS (
#                         SELECT
#                             timestamp,
#                             CASE
#                                 WHEN TRIM(shut_height::text) ~ '^[0-9]+(\.[0-9]+)?$'
#                                 THEN TRIM(shut_height::text)::numeric
#                                 ELSE NULL
#                             END AS height_value
#                         FROM live_data.plant2_data
#                         WHERE machine_no = %s
#                           AND timestamp >= %s::timestamp WITHOUT TIME ZONE
#                           AND timestamp <  %s::timestamp WITHOUT TIME ZONE
#                     )
#                     SELECT height_value
#                     FROM valid_height
#                     WHERE height_value > 10
#                     ORDER BY timestamp DESC
#                     LIMIT 1
#                     """,
#                     [machine_no, start_str_naive, end_str_naive],
#                 )
#                 height_res = cursor.fetchone()
#                 if height_res and height_res[0] is not None:
#                     machine_meta["shut_height"] = f"{float(height_res[0]):.2f}"

#             # --------------------------------------------------------------
#             # 2) Hour-wise production count in ONE grouped query
#             # First bucket starts at 08:30/20:30, not date_trunc hour.
#             # --------------------------------------------------------------
#             first_bucket_end = hour_buckets[0]["scheduled_end"] if hour_buckets else effective_end
#             first_bucket_start_str = to_naive_str(shift_start)
#             first_bucket_end_str = to_naive_str(first_bucket_end)

#             cursor.execute(
#                 """
#                 SELECT
#                     CASE
#                         WHEN timestamp >= %s::timestamp WITHOUT TIME ZONE
#                          AND timestamp <  %s::timestamp WITHOUT TIME ZONE
#                         THEN %s::timestamp WITHOUT TIME ZONE
#                         ELSE date_trunc('hour', timestamp)
#                     END AS bucket_start,
#                     COALESCE(SUM(count), 0) AS total_count,
#                     COALESCE(MAX(cumulative_count), 0) AS latest_cumulative,
#                     MIN(timestamp) AS first_count_time,
#                     MAX(timestamp) AS last_count_time
#                 FROM live_data.plant2_data
#                 WHERE machine_no = %s
#                   AND timestamp >= %s::timestamp WITHOUT TIME ZONE
#                   AND timestamp <  %s::timestamp WITHOUT TIME ZONE
#                 GROUP BY bucket_start
#                 ORDER BY bucket_start ASC
#                 """,
#                 [
#                     first_bucket_start_str,
#                     first_bucket_end_str,
#                     first_bucket_start_str,
#                     machine_no,
#                     start_str_naive,
#                     end_str_naive,
#                 ],
#             )
#             for bucket_start, total_count, latest_cumulative, first_count_time, last_count_time in cursor.fetchall():
#                 bucket_key = bucket_start.strftime("%Y-%m-%d %H:%M:%S")
#                 bucket = bucket_by_key.get(bucket_key)
#                 if bucket:
#                     bucket["count"] = int(total_count or 0)
#                     bucket["latest_cumulative"] = int(latest_cumulative or 0)
#                     count_summary["total_count"] += int(total_count or 0)
#                     count_summary["latest_cumulative"] = max(
#                         count_summary["latest_cumulative"], int(latest_cumulative or 0)
#                     )
#                     if first_count_time and (not count_summary["first_count_time"] or first_count_time < count_summary["first_count_time"]):
#                         count_summary["first_count_time"] = first_count_time
#                     if last_count_time and (not count_summary["last_count_time"] or last_count_time > count_summary["last_count_time"]):
#                         count_summary["last_count_time"] = last_count_time

#             # --------------------------------------------------------------
#             # 3) Machine events: ON/OFF/TOOL_CHANGE/SHUT_HEIGHT_CHANGE
#             # --------------------------------------------------------------
#             cursor.execute(
#                 """
#                 SELECT event_type, timestamp, shift, details
#                 FROM live_data."Machine_Event_Logs"
#                 WHERE plant_no = %s
#                   AND machine_no = %s
#                   AND timestamp >= %s::timestamp WITH TIME ZONE
#                   AND timestamp <  %s::timestamp WITH TIME ZONE
#                 ORDER BY timestamp ASC
#                 """,
#                 [plant_no, machine_no, start_str_tz, end_str_tz],
#             )
#             machine_event_rows = cursor.fetchall()

#             event_titles = {
#                 "ON": "Machine Powered ON",
#                 "OFF": "Machine Offline",
#                 "SHUT_HEIGHT_CHANGE": "Shut Height Adjusted",
#                 "TOOL_CHANGE": "Tool ID Changed",
#             }
#             for event_type, ts_obj, shift_val, details in machine_event_rows:
#                 # Old bad rows cleanup at API level: fake e000... tool change ko history me show nahi karenge.
#                 details_text_raw = str(details or "")
#                 if event_type == "TOOL_CHANGE" and "e000" in details_text_raw.lower():
#                     continue
#                 if event_type == "SHUT_HEIGHT_CHANGE" and ("1.01" in details_text_raw or "1.0" in details_text_raw):
#                     continue

#                 ts_obj = localize_ist(ts_obj)
#                 title = event_titles.get(event_type, str(event_type).replace("_", " ").title())
#                 event_tool_id = extract_tool_id_from_text(details_text_raw)
#                 event_tool_info = get_tool_info_from_tid_map(event_tool_id) if event_tool_id else {}
#                 event_extra = {}
#                 if event_tool_id:
#                     event_extra = {
#                         "tool_id": event_tool_id,
#                         "epc": get_safe_tid_value(event_tool_info, "epc", event_tool_id),
#                         "customer": get_safe_tid_value(event_tool_info, "customer"),
#                         "customer_name": get_safe_tid_value(event_tool_info, "customer"),
#                         "model": get_safe_tid_value(event_tool_info, "model_name"),
#                         "model_name": get_safe_tid_value(event_tool_info, "model_name"),
#                         "part_name": get_safe_tid_value(event_tool_info, "part_name"),
#                         "part_number": get_safe_tid_value(event_tool_info, "part_number"),
#                         "tool_name": get_safe_tid_value(event_tool_info, "tool_name"),
#                     }

#                 event_payload = add_timeline_event(
#                     events,
#                     ts_obj,
#                     event_type,
#                     title,
#                     details,
#                     shift_val,
#                     extra=event_extra if event_extra else None,
#                 )
#                 bucket = find_bucket_for_time(ts_obj)
#                 if bucket:
#                     bucket_event = {
#                         "type": event_type,
#                         "title": title,
#                         "time": ts_obj.strftime("%I:%M:%S %p"),
#                         "system_time": system_time(ts_obj),
#                         "details": details or "",
#                         **event_extra,
#                     }
#                     bucket["machine_events"].append(bucket_event)
#                     if event_type in ["ON", "OFF"]:
#                         bucket["on_off_events"].append(bucket_event)
#                     elif event_type == "TOOL_CHANGE":
#                         bucket["tool_changes"].append(bucket_event)
#                     elif event_type == "SHUT_HEIGHT_CHANGE":
#                         bucket["shut_height_changes"].append(bucket_event)

#             # --------------------------------------------------------------
#             # 4) Ideal segments from correct table, old Plant2_hourly_idle not used.
#             # Filter ideal_time >= 180 so old 11 sec / 28 sec rows do not pollute history.
#             # --------------------------------------------------------------
#             # ✅ FIX: overlap query order must be END first, START second
#             # SQL: ideal_start_at < window_end AND ideal_end_at > window_start
#             # Pehle ulta params ja rahe the, isliye ideal_rows empty aa rahe the.
#             ideal_params = ["Plant 2", int(machine_no), end_str_naive, start_str_naive]
#             shift_filter_sql = ""
#             if selected_shift in ["A", "B"]:
#                 shift_filter_sql = " AND shift = %s"
#                 ideal_params.append(selected_shift)

#             cursor.execute(
#                 f"""
#                 SELECT
#                     id,
#                     ideal_mode,
#                     ideal_start_at,
#                     ideal_end_at,
#                     ideal_time,
#                     closed_by,
#                     reason,
#                     specific_reason,
#                     remark,
#                     shift
#                 FROM live_data.ideal_time_segments_reason
#                 WHERE plant_location = %s
#                   AND machine_no = %s
#                   AND ideal_start_at <  %s::timestamp WITHOUT TIME ZONE
#                   AND ideal_end_at   >  %s::timestamp WITHOUT TIME ZONE
#                   AND ideal_time >= 180
#                   {shift_filter_sql}
#                 ORDER BY ideal_start_at ASC
#                 """,
#                 ideal_params,
#             )
#             ideal_rows = cursor.fetchall()

#             for row in ideal_rows:
#                 (
#                     ideal_id,
#                     ideal_mode,
#                     ideal_start_at,
#                     ideal_end_at,
#                     ideal_time,
#                     closed_by,
#                     reason,
#                     specific_reason,
#                     remark,
#                     shift_val,
#                 ) = row

#                 ideal_start_at = localize_ist(ideal_start_at)
#                 ideal_end_at = localize_ist(ideal_end_at)
#                 ideal_mode = str(ideal_mode or "").upper()
#                 ideal_time = int(ideal_time or 0)

#                 segment_payload = {
#                     "id": ideal_id,
#                     "mode": ideal_mode,
#                     "start_time": ideal_start_at.strftime("%I:%M:%S %p"),
#                     "end_time": ideal_end_at.strftime("%I:%M:%S %p"),
#                     "start_system_time": system_time(ideal_start_at),
#                     "end_system_time": system_time(ideal_end_at),
#                     "duration_seconds": ideal_time,
#                     "duration_display": seconds_to_display(ideal_time),
#                     "closed_by": closed_by,
#                     "reason": reason or "Uncategorized",
#                     "specific_reason": specific_reason or "Reason Not Provided",
#                     "remark": remark or "",
#                     "shift": shift_val,
#                 }

#                 # Timeline event for exact ideal segment
#                 title = "Online Ideal" if ideal_mode == "ONLINE" else "Offline Ideal"
#                 details = (
#                     f"{title}: {segment_payload['duration_display']} "
#                     f"({segment_payload['start_time']} - {segment_payload['end_time']}). "
#                     f"Reason: {segment_payload['reason']} / {segment_payload['specific_reason']}"
#                 )
#                 add_timeline_event(
#                     events,
#                     ideal_start_at,
#                     f"IDEAL_{ideal_mode}",
#                     title,
#                     details,
#                     shift_val,
#                     extra={"ideal_segment": segment_payload},
#                 )

#                 # Attach ideal segment to overlapping buckets and calculate clipped duration per bucket.
#                 for bucket in hour_buckets:
#                     overlap_start = max(ideal_start_at, bucket["start"])
#                     overlap_end = min(ideal_end_at, bucket["scheduled_end"], effective_end)
#                     if overlap_end <= overlap_start:
#                         continue
#                     overlap_seconds = int((overlap_end - overlap_start).total_seconds())
#                     if overlap_seconds <= 0:
#                         continue

#                     bucket_segment = dict(segment_payload)
#                     bucket_segment["bucket_overlap_seconds"] = overlap_seconds
#                     bucket_segment["bucket_overlap_display"] = seconds_to_display(overlap_seconds)
#                     bucket["ideal_segments"].append(bucket_segment)

#                     if ideal_mode == "ONLINE":
#                         bucket["online_ideal_seconds"] += overlap_seconds
#                         shift_ideal_summary["online_ideal_seconds"] += overlap_seconds
#                     elif ideal_mode == "OFFLINE":
#                         bucket["offline_ideal_seconds"] += overlap_seconds
#                         shift_ideal_summary["offline_ideal_seconds"] += overlap_seconds

#         # ------------------------------------------------------------------
#         # Build hourly summary events after count/ideal/events attached
#         # ------------------------------------------------------------------
#         hourly_summary = []
#         for bucket in hour_buckets:
#             bucket["total_ideal_seconds"] = int(bucket["online_ideal_seconds"] + bucket["offline_ideal_seconds"])
#             bucket["online_ideal_display"] = seconds_to_display(bucket["online_ideal_seconds"])
#             bucket["offline_ideal_display"] = seconds_to_display(bucket["offline_ideal_seconds"])
#             bucket["total_ideal_display"] = seconds_to_display(bucket["total_ideal_seconds"])
#             bucket["tool_change_count"] = len(bucket["tool_changes"])
#             bucket["shut_height_change_count"] = len(bucket["shut_height_changes"])
#             bucket["on_off_event_count"] = len(bucket["on_off_events"])

#             summary_details = f"Production: {bucket['count']} pieces."
#             if bucket["online_ideal_seconds"] > 0:
#                 summary_details += f" | Online ideal: {bucket['online_ideal_display']}."
#             if bucket["offline_ideal_seconds"] > 0:
#                 summary_details += f" | Offline ideal: {bucket['offline_ideal_display']}."
#             if bucket["shut_height_change_count"] > 0:
#                 summary_details += f" | Shut height changed {bucket['shut_height_change_count']} time(s)."
#             if bucket["tool_change_count"] > 0:
#                 summary_details += f" | Tool changed {bucket['tool_change_count']} time(s)."

#             hour_payload = {
#                 "bucket_start": system_time(bucket["start"]),
#                 "bucket_end": system_time(bucket["scheduled_end"]),
#                 "bucket_start_display": title_time(bucket["start"]),
#                 "bucket_end_display": title_time(bucket["scheduled_end"]),
#                 "count": int(bucket["count"]),
#                 "latest_cumulative": int(bucket["latest_cumulative"]),
#                 "online_ideal_seconds": int(bucket["online_ideal_seconds"]),
#                 "offline_ideal_seconds": int(bucket["offline_ideal_seconds"]),
#                 "total_ideal_seconds": int(bucket["total_ideal_seconds"]),
#                 "online_ideal_display": bucket["online_ideal_display"],
#                 "offline_ideal_display": bucket["offline_ideal_display"],
#                 "total_ideal_display": bucket["total_ideal_display"],
#                 "ideal_segments": bucket["ideal_segments"],
#                 "machine_events": bucket["machine_events"],
#                 "on_off_events": bucket["on_off_events"],
#                 "tool_changes": bucket["tool_changes"],
#                 "shut_height_changes": bucket["shut_height_changes"],
#                 "tool_change_count": bucket["tool_change_count"],
#                 "shut_height_change_count": bucket["shut_height_change_count"],
#                 "on_off_event_count": bucket["on_off_event_count"],
#                 "details": summary_details,
#             }
#             hourly_summary.append(hour_payload)

#             add_timeline_event(
#                 events,
#                 bucket["scheduled_end"] - timedelta(seconds=1),
#                 "HOUR_SUMMARY",
#                 f"Hourly Summary ({title_time(bucket['start'])} - {title_time(bucket['scheduled_end'])})",
#                 summary_details,
#                 selected_shift,
#                 extra=hour_payload,
#             )

#         shift_ideal_summary["total_ideal_seconds"] = int(
#             shift_ideal_summary["online_ideal_seconds"] + shift_ideal_summary["offline_ideal_seconds"]
#         )
#         shift_ideal_summary["online_ideal_display"] = seconds_to_display(shift_ideal_summary["online_ideal_seconds"])
#         shift_ideal_summary["offline_ideal_display"] = seconds_to_display(shift_ideal_summary["offline_ideal_seconds"])
#         shift_ideal_summary["total_ideal_display"] = seconds_to_display(shift_ideal_summary["total_ideal_seconds"])

#         if count_summary["first_count_time"]:
#             count_summary["first_count_time"] = system_time(count_summary["first_count_time"])
#         if count_summary["last_count_time"]:
#             count_summary["last_count_time"] = system_time(count_summary["last_count_time"])

#         events.sort(key=lambda x: x["timestamp"])

#         response_data = {
#             "success": True,
#             "plant_no": plant_no,
#             "machine_no": machine_no,
#             "date": date_str,
#             "shift": selected_shift,
#             "shift_start": system_time(shift_start),
#             "shift_end": system_time(shift_end),
#             "effective_end": system_time(effective_end),
#             "machine_meta": machine_meta,
#             "schedule": {
#                 "lunch_start": "12:15 PM",
#                 "lunch_end": "12:45 PM",
#                 "shift_a_start": "08:30 AM",
#                 "shift_a_end": "08:00 PM",
#             },
#             "summary": {
#                 "production": count_summary,
#                 "ideal": shift_ideal_summary,
#                 "total_hours": len(hourly_summary),
#                 "total_events": len(events),
#             },
#             "hourly_summary": hourly_summary,
#             # Existing frontend compatibility: timeline still available as events.
#             "events": events,
#             "total_events": len(events),
#         }

#         response = Response(response_data)
#         response["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
#         return response

#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return Response({"success": False, "error": str(e), "events": [], "hourly_summary": []}, status=500)


@never_cache
@api_view(["POST"])
def save_hourly_snapshot(request):
    """
    ✅ FIXED - Use HOURLY_DATA_SAVER for idle_time (NOT StrictIdlePolicy)
    """
    try:
        from apps.mqtt.simple_plant2 import (
            EXACT_REQUIREMENT_STATE as PLANT2_STATE,
            TOPIC_MACHINE_MAPPING,
        )
        from apps.data_storage.hourly_data_saver import HOURLY_DATA_SAVER
        from django.db import connection
        import pytz
        from datetime import datetime, timedelta

        ist_tz = pytz.timezone("Asia/Kolkata")
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
                    idle_time = hourly_snapshot.get("idle_total_minutes", 0)

                    # Get other data
                    hour_count = PLANT2_STATE.current_hour_counts.get(machine_no, 0)
                    tool_id = PLANT2_STATE.previous_tool_id.get(machine_no, "UNKNOWN")
                    shut_height = PLANT2_STATE.previous_shut_height.get(machine_no, 0.0)

                    # Get timestamp
                    first_count_time = PLANT2_STATE.hour_first_count_time.get(
                        machine_no
                    )
                    if first_count_time:
                        save_timestamp = first_count_time
                    elif machine_no in PLANT2_STATE.machine_on_since:
                        save_timestamp = PLANT2_STATE.machine_on_since[machine_no]
                    else:
                        save_timestamp = current_hour

                    # ✅ Direct database insert
                    naive_timestamp = save_timestamp.replace(tzinfo=None, microsecond=0)
                    db_shut_height = (
                        0.0
                        if isinstance(shut_height, str)
                        else float(shut_height) if shut_height else 0.0
                    )

                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            INSERT INTO Plant2_data 
                            (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                            [
                                naive_timestamp,
                                str(tool_id),
                                str(machine_no),
                                hour_count,
                                PLANT2_STATE.shift_cumulative.get((machine_no, "A"), 0),
                                0,
                                idle_time,  # ✅ FROM HOURLY_DATA_SAVER
                                db_shut_height,
                                "A",
                            ],
                        )

                    saved_count += 1
                    if idle_time > 0:
                        print(
                            f"  ✅ M{machine_no}: count={hour_count}, idle={idle_time}min ✓"
                        )

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

        return Response({"success": True, "saved_count": saved_count})

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return Response({"success": False, "error": str(e)}, status=500)


@never_cache
@api_view(["GET"])
def get_machine_changes_from_db(request):
    """Get tool/height changes from DATABASE - TODAY + CURRENT SHIFT"""
    try:
        from django.db import connection
        from datetime import datetime, date, time as dt_time
        import pytz

        # Get filters
        machine_no = request.GET.get("machine_no", None)

        # Current date and shift
        ist_tz = pytz.timezone("Asia/Kolkata")
        now_ist = datetime.now(ist_tz)
        today = now_ist.date()

        # Shift times
        shift_A_start = dt_time(8, 30)
        shift_A_end = dt_time(20, 0)
        current_time = now_ist.time()
        current_shift = "A" if shift_A_start <= current_time < shift_A_end else "B"

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
            tool_changed = tool != prev_tool
            height_changed = abs(height - prev_height) > 10

            if tool_changed:
                messages.append("Tool ID changed")

            if height_changed:
                messages.append(f"Shut Height: {prev_height:.2f} → {height:.2f}")

            changes.append(
                {
                    "id": idx,
                    "machine_no": int(m_no),
                    "time": ts.strftime("%H:%M:%S"),
                    "timestamp": ts.isoformat(),
                    "message": " & ".join(messages),
                    "tool_changed": tool_changed,
                    "height_changed": height_changed,
                    "old_tool": (
                        str(prev_tool)[:12] + "..."
                        if len(str(prev_tool)) > 12
                        else str(prev_tool)
                    ),
                    "new_tool": (
                        str(tool)[:12] + "..." if len(str(tool)) > 12 else str(tool)
                    ),
                    "old_height": float(prev_height),
                    "new_height": float(height),
                }
            )

        return Response(
            {
                "success": True,
                "changes": changes,
                "total": len(changes),
                "date": str(today),
                "shift": current_shift,
                "message": f"{len(changes)} changes in Shift {current_shift} today (from database)",
            }
        )

    except Exception as e:
        import traceback

        traceback.print_exc()
        return Response({"success": False, "error": str(e), "changes": []}, status=500)


# backend/api/views.py


@never_cache
@api_view(["GET"])
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
            cursor.execute(
                """
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
            """,
                [test_tool_id],
            )

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
                    "success": True,
                    "query_worked": True,
                    "result": {
                        "customer": result[0],
                        "model": result[1],
                        "part_name": result[2],
                        "tool_name": result[3],
                        "epc": result[4],
                        "part_number": result[5],
                        "tpm": result[6],
                    },
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
                    "success": False,
                    "query_worked": True,
                    "result": None,
                    "total_rows": count,
                    "sample_epcs": [s[0] for s in samples],
                }

        print(f"{'='*60}\n")
        return Response(response_data)

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback

        traceback.print_exc()

        return Response(
            {"success": False, "query_worked": False, "error": str(e)}, status=500
        )


@never_cache
@api_view(["GET"])
def machine_production_data(request):
    """ENHANCED Machine Production API with Smart Filtering"""
    try:
        # Get filters
        selected_date = request.GET.get("date", datetime.now().strftime("%Y-%m-%d"))
        selected_plant = request.GET.get("plant", "plant1_data")
        selected_shift = request.GET.get("shift", "")
        selected_machine = request.GET.get("machine", "")
        start_hour = request.GET.get("start_hour", "")
        end_hour = request.GET.get("end_hour", "")

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
                return Response(
                    {
                        "success": False,
                        "message": f"No data found for the selected filters. Please check if data exists for {selected_date}.",
                        "suggestion": "Try different date or plant selection.",
                        "machine_data": [],
                    }
                )

            # Build enhanced response
            machine_data = []
            for (
                machine_no,
                entries,
                production,
                first_entry,
                last_entry,
                shifts,
            ) in results:
                # Format times
                first_time = first_entry.strftime("%H:%M:%S") if first_entry else "N/A"
                last_time = last_entry.strftime("%H:%M:%S") if last_entry else "N/A"

                machine_data.append(
                    {
                        "machine_no": str(machine_no),
                        "machine_name": f"Machine {str(machine_no).zfill(2)}",
                        "production_count": int(production) if production else 0,
                        "total_entries": entries,
                        "working_hours": f"{first_time} - {last_time}",
                        "shifts_worked": shifts or "N/A",
                        "status": "Active" if production and production > 0 else "Idle",
                    }
                )

            total_production = sum(m["production_count"] for m in machine_data)
            active_machines = len([m for m in machine_data if m["status"] == "Active"])

            # Smart filter description
            filter_description = f"{selected_plant.upper()} on {selected_date}"
            if selected_machine:
                filter_description += f" | Machine {selected_machine}"
            if start_hour and end_hour:
                filter_description += f" | {start_hour}:00-{end_hour}:00"
            if selected_shift:
                filter_description += f" | Shift {selected_shift}"

            return Response(
                {
                    "success": True,
                    "machine_data": machine_data,
                    "summary": {
                        "total_production": total_production,
                        "total_machines": len(machine_data),
                        "active_machines": active_machines,
                        "idle_machines": len(machine_data) - active_machines,
                        "calculation_method": calculation_note,
                        "filter_description": filter_description,
                    },
                    "filters_applied": {
                        "date": selected_date,
                        "plant": selected_plant,
                        "machine": selected_machine or "All",
                        "shift": selected_shift or "All",
                        "time_range": f"{start_hour or '00'}-{end_hour or '23'}",
                    },
                }
            )

    except Exception as e:
        print(f"❌ Enhanced Machine Production API error: {e}")
        return Response(
            {
                "success": False,
                "error": "Sorry, technical problem occurred. We can solve this as soon as possible.",
                "technical_details": (
                    str(e) if request.GET.get("debug") == "true" else None
                ),
            },
            status=500,
        )


@never_cache
@api_view(["GET"])
def production_line_status_data(request):
    """Enhanced Production Line Status with Smart Filtering"""
    try:
        # Get filters
        selected_date = request.GET.get("date", datetime.now().strftime("%Y-%m-%d"))
        selected_plant = request.GET.get("plant", "plant1_data")
        selected_shift = request.GET.get("shift", "")

        print(f"📋 Enhanced Production Line Status:")
        print(f"   Date: {selected_date}")
        print(f"   Plant: {selected_plant}")
        print(f"   Shift: {selected_shift}")

        # Machine count based on plant — FIXED
        if selected_plant == "plant1_data":
            total_machines = 57
            plant_name = "Manufacturing Plant 1"
        elif selected_plant == "plant2_data":
            total_machines = 49  # FIXED: 26 → 49
            plant_name = "Manufacturing Plant 2"
        else:
            total_machines = 57
            plant_name = "Default Plant"

        production_lines = []

        with connection.cursor() as cursor:
            # FIXED: cumulative_count hataya, sirf SUM(count) use kiya
            # FIXED: real idle_time SUM add kiya efficiency ke liye
            machine_query = f"""
                SELECT 
                    machine_no,
                    COUNT(*) as total_entries,
                    SUM(CASE WHEN count IS NOT NULL THEN count ELSE 0 END) as total_production,
                    SUM(CASE WHEN idle_time IS NOT NULL THEN idle_time ELSE 0 END) as total_idle_minutes,
                    MAX(timestamp) as last_update,
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
            for (
                machine_no,
                entries,
                total_production,
                total_idle_minutes,
                last_update,
                shifts,
            ) in results:

                # FIXED: real efficiency from actual idle_time
                idle_mins = total_idle_minutes or 0
                efficiency = round(((480 - idle_mins) / 480) * 100, 1)
                efficiency = max(0, min(100, efficiency))  # 0-100 ke beech rakho

                machine_dict[str(machine_no)] = {
                    "entries": entries,
                    "production": total_production or 0,
                    "total_idle_minutes": idle_mins,
                    "efficiency": efficiency,
                    "last_update": last_update,
                    "shifts": shifts,
                }

            # Build response for all machines in plant
            for machine_no in range(1, total_machines + 1):
                machine_key = str(machine_no)

                if machine_key in machine_dict:
                    data = machine_dict[machine_key]
                    production = data["production"]
                    efficiency = data["efficiency"]
                    last_update = data["last_update"]
                    shifts = data["shifts"]
                    entries = data["entries"]
                    idle_mins = data["total_idle_minutes"]

                    # Status determine karo real efficiency se
                    if production > 0:
                        if efficiency > 80:
                            status = "Running"
                            status_color = "success"
                        elif efficiency > 50:
                            status = "Slow Operation"
                            status_color = "warning"
                        else:
                            status = "Low Performance"
                            status_color = "warning"
                    else:
                        status = "Idle"
                        status_color = "danger"
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
                    idle_mins = 0
                    status = "Offline"
                    status_color = "secondary"
                    last_update_str = "No data"
                    shifts = "N/A"
                    entries = 0

                production_lines.append(
                    {
                        "machine_no": machine_no,
                        "machine_name": f"Production Unit Machine {str(machine_no).zfill(2)}",
                        "status": status,
                        "status_color": status_color,
                        "efficiency": round(efficiency, 1),
                        "production_count": int(production),
                        "idle_minutes": int(idle_mins),
                        "total_entries": entries,
                        "last_update": last_update_str,
                        "shifts_worked": shifts,
                        "plant_section": f"{plant_name}",
                    }
                )

        # Summary calculate karo
        total_production = sum(m["production_count"] for m in production_lines)
        running_machines = len(
            [m for m in production_lines if m["status"] == "Running"]
        )
        slow_machines = len(
            [m for m in production_lines if m["status"] == "Slow Operation"]
        )
        idle_machines = len([m for m in production_lines if m["status"] == "Idle"])
        offline_machines = len(
            [m for m in production_lines if m["status"] == "Offline"]
        )
        low_machines = len(
            [m for m in production_lines if m["status"] == "Low Performance"]
        )

        overall_efficiency = (
            sum(m["efficiency"] for m in production_lines) / total_machines
            if total_machines > 0
            else 0
        )
        active_machines = running_machines + slow_machines

        return Response(
            {
                "success": True,
                "production_lines": production_lines,
                "plant_summary": {
                    "plant_name": plant_name,
                    "total_machines": total_machines,
                    "total_production": total_production,
                    "overall_efficiency": round(overall_efficiency, 1),
                    "date": selected_date,
                    "shift": selected_shift or "All Shifts",
                },
                "machine_status_breakdown": {
                    "running": running_machines,
                    "slow_operation": slow_machines,
                    "low_performance": low_machines,
                    "idle": idle_machines,
                    "offline": offline_machines,
                    "active_total": active_machines,
                    "productivity_rate": (
                        round((active_machines / total_machines) * 100, 1)
                        if total_machines > 0
                        else 0
                    ),
                },
                "filters_applied": {
                    "date": selected_date,
                    "plant": selected_plant,
                    "shift": selected_shift or "All",
                },
                "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    except Exception as e:
        print(f"❌ Production Line Status API error: {e}")
        return Response(
            {
                "success": False,
                "error": "Sorry, technical problem occurred.",
                "suggestion": "Please try again or contact technical support.",
                "technical_details": (
                    str(e) if request.GET.get("debug") == "true" else None
                ),
            },
            status=500,
        )


# ========== NEW OPERATOR ASSIGNMENT APIs ==========


@api_view(["GET"])
def get_operators_by_plant(request):
    """Get operators for selected plant - alphabetically sorted"""
    try:
        plant = request.GET.get("plant", "plant_2")

        if plant not in ["plant_1", "plant_2"]:
            return Response(
                {"success": False, "message": "Invalid plant. Use plant_1 or plant_2"},
                status=400,
            )

        operators = (
            Operator.objects.filter(plant=plant, is_active=True)
            .order_by("name")
            .values("id", "name")
        )

        return Response(
            {
                "success": True,
                "plant": plant,
                "operators": list(operators),
                "count": len(operators),
            }
        )

    except Exception as e:
        print(f"❌ Error fetching operators: {e}")
        return Response({"success": False, "error": str(e)}, status=500)


@api_view(["POST"])
def add_operator(request):
    """Add new operator from frontend"""
    try:
        name = request.data.get("name")
        plant = request.data.get("plant", "plant_2")
        emp_code = request.data.get("employee_code", "")  # 👇 Naya data aayega

        if not name or not name.strip():
            return Response(
                {"success": False, "message": "Operator name is required"}, status=400
            )

        if plant not in ["plant_1", "plant_2"]:
            return Response({"success": False, "message": "Invalid plant."}, status=400)

        existing = Operator.objects.filter(
            name__iexact=name.strip(), plant=plant
        ).first()
        if existing:
            return Response(
                {"success": False, "message": f"{name} already exists"}, status=400
            )

        # 👇 Employee code save kar rahe hain
        operator = Operator.objects.create(
            name=name.strip(), plant=plant, employee_code=emp_code
        )

        return Response(
            {
                "success": True,
                "message": f"{name} added successfully",
                "operator": {
                    "id": operator.id,
                    "name": operator.name,
                    "plant": operator.plant,
                },
            }
        )

    except Exception as e:
        print(f"❌ Error adding operator: {e}")
        return Response({"success": False, "error": str(e)}, status=500)


@api_view(["GET"])
def get_machines_by_plant(request):
    """Get machine numbers based on plant"""
    try:
        plant = request.GET.get("plant", "plant_2")

        if plant == "plant_1":
            machines = list(range(1, 58))  # 1 to 56
            plant_name = "Plant 1"
        elif plant == "plant_2":
            machines = list(range(1, 21)) + list(range(41, 47))  # 1-20, 41-46
            plant_name = "Plant 2"
        else:
            return Response(
                {"success": False, "message": "Invalid plant. Use plant_1 or plant_2"},
                status=400,
            )

        return Response(
            {
                "success": True,
                "plant": plant,
                "plant_name": plant_name,
                "machines": machines,
                "count": len(machines),
            }
        )

    except Exception as e:
        print(f"❌ Error fetching machines: {e}")
        return Response({"success": False, "error": str(e)}, status=500)


from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.utils import timezone
from .models import Operator, OperatorAssignment  # Ensure ye models imported hain


@api_view(["POST"])
def save_operator_assignment(request):
    """Save operator assignment to machine"""
    try:
        plant = request.data.get("plant")
        operator_name = request.data.get("operator_name")
        machine_no = request.data.get("machine_no")
        shift = request.data.get("shift")
        assigned_by = request.data.get("assigned_by", "Admin")

        # Frontend se override ka order
        override = request.data.get("override", False)

        if not all([plant, operator_name, machine_no, shift]):
            return Response(
                {"success": False, "message": "All fields are required"}, status=400
            )

        # 1. Operator ID nikalo
        if operator_name == "No Operator Available":
            operator_id = 0
        else:
            operator = Operator.objects.filter(
                name=operator_name, plant=plant, is_active=True
            ).first()
            if not operator:
                return Response(
                    {"success": False, "message": "Operator not found"}, status=404
                )
            operator_id = operator.id

        # 2. Check karo kya Machine pehle se busy hai?
        existing_machine = OperatorAssignment.objects.filter(
            plant=plant, machine_no=str(machine_no), is_current=True
        ).first()

        # 3. Check karo kya Operator kisi aur machine par busy hai?
        existing_operator = OperatorAssignment.objects.filter(
            plant=plant, operator_id=operator_id, is_current=True
        ).first()

        # Agar Override FALSE hai aur koi ek bhi busy hai, toh error do
        if not override:
            if existing_machine:
                return Response(
                    {"success": False, "message": f"Machine {machine_no} is busy"},
                    status=400,
                )
            if existing_operator and operator_id != 0:
                return Response(
                    {
                        "success": False,
                        "message": f"{operator_name} is busy on Machine {existing_operator.machine_no}",
                    },
                    status=400,
                )

        # 🔥 SMART LOGIC: Agar Override TRUE hai, toh dono ko purani duty se free karo!
        if override:
            # A. Agar machine par pehle se koi (Abhishek) tha, usko hatao
            if existing_machine:
                existing_machine.status = "Transferred"
                existing_machine.end_time = timezone.now()
                existing_machine.is_current = False
                existing_machine.save()

            # B. Agar naya operator (Bablu) pehle kisi aur machine par tha, uski wo duty khatam karo
            if existing_operator and operator_id != 0:
                existing_operator.status = "Transferred"
                existing_operator.end_time = timezone.now()
                existing_operator.is_current = False
                existing_operator.save()

        # 4. Naya Assignment Banao (Bablu -> Machine 2)
        assignment = OperatorAssignment.objects.create(
            plant=plant,
            operator_id=operator_id,
            operator_name=operator_name,
            machine_no=str(machine_no),
            shift=shift,
            start_time=timezone.now(),
            status="Assigned",
            reason="Operator Reallocated" if override else "Initial Assignment",
            remarks="",
            assigned_by=assigned_by,
            is_current=True,
        )

        return Response(
            {
                "success": True,
                "message": f"{operator_name} assigned successfully",
                "assignment": {"id": assignment.id},
            }
        )

    except Exception as e:
        print(f"❌ Error saving assignment: {e}")
        return Response({"success": False, "error": str(e)}, status=500)


@api_view(["GET"])
def get_operator_assignments(request):
    """Current machine assignments"""

    try:
        plant = request.GET.get("plant")

        queryset = OperatorAssignment.objects.filter(is_current=True)

        if plant:
            queryset = queryset.filter(plant=plant)

        queryset = queryset.order_by("machine_no")

        data = []

        for a in queryset:

            data.append(
                {
                    "id": a.id,
                    "plant": a.plant,
                    "machine_no": a.machine_no,
                    "operator_name": a.operator_name,
                    "shift": a.shift,
                    "status": a.status,
                    "start_time": timezone.localtime(a.start_time).strftime(
                        "%Y-%m-%d %I:%M %p"
                    ),
                }
            )

        return Response(
            {
                "success": True,
                "assignments": data,
            }
        )

    except Exception as e:
        return Response(
            {
                "success": False,
                "error": str(e),
            },
            status=500,
        )


@api_view(["POST"])
def transfer_operator(request):
    """Transfer operator from one machine to another"""

    try:
        operator_name = request.data.get("operator_name")
        plant = request.data.get("plant")
        from_machine = str(request.data.get("from_machine"))
        to_machine = str(request.data.get("to_machine"))
        shift = request.data.get("shift")
        reason = request.data.get("reason", "")
        remarks = request.data.get("remarks", "")
        assigned_by = request.data.get("assigned_by", "Admin")

        if not all([operator_name, plant, from_machine, to_machine, shift]):
            return Response(
                {
                    "success": False,
                    "message": "All required fields are missing",
                },
                status=400,
            )

        # Check current assignment
        current = OperatorAssignment.objects.filter(
            plant=plant,
            machine_no=from_machine,
            operator_name=operator_name,
            is_current=True,
        ).first()

        if not current:
            return Response(
                {
                    "success": False,
                    "message": "Current assignment not found",
                },
                status=404,
            )

        # Check destination machine
        machine_busy = OperatorAssignment.objects.filter(
            plant=plant,
            machine_no=to_machine,
            is_current=True,
        ).first()

        if machine_busy:
            return Response(
                {
                    "success": False,
                    "message": f"Machine {to_machine} already assigned to {machine_busy.operator_name}",
                },
                status=400,
            )

        # Close old assignment
        current.status = "Transferred"
        current.end_time = timezone.now()
        current.reason = reason
        current.remarks = remarks
        current.assigned_by = assigned_by
        current.is_current = False
        current.save()

        # Create new assignment
        new_assignment = OperatorAssignment.objects.create(
            plant=plant,
            machine_no=to_machine,
            operator_name=operator_name,
            operator_id=current.operator_id,
            shift=shift,
            start_time=timezone.now(),
            status="Assigned",
            reason=reason,
            remarks=remarks,
            assigned_by=assigned_by,
            is_current=True,
        )

        return Response(
            {
                "success": True,
                "message": f"{operator_name} transferred successfully",
                "assignment": {
                    "id": new_assignment.id,
                    "machine_no": new_assignment.machine_no,
                    "operator_name": new_assignment.operator_name,
                    "status": new_assignment.status,
                },
            }
        )

    except Exception as e:
        print(e)
        return Response(
            {
                "success": False,
                "error": str(e),
            },
            status=500,
        )


from django.utils import timezone
from django.db.models import Q
from datetime import datetime


@api_view(["GET"])
def get_assignment_history(request):
    """Complete operator transfer history with Duration Calculation"""
    try:
        plant = request.GET.get("plant")
        date_filter = request.GET.get("date")  # Format: YYYY-MM-DD

        query = OperatorAssignment.objects.all()

        if plant:
            query = query.filter(plant=plant)

        if date_filter:
            # 🔥 FIX: Jo assignment is date ko START hui ya is date ko END hui, dono dikhegi
            query = query.filter(
                Q(start_time__date=date_filter) | Q(end_time__date=date_filter)
            )

        history = query.order_by("-start_time")

        data = []
        for h in history:
            # Time ko India timezone (IST) mein convert kar rahe hain
            start_local = timezone.localtime(h.start_time) if h.start_time else None
            end_local = timezone.localtime(h.end_time) if h.end_time else None

            # 🔥 NAYA: Kitne ghante kaam kiya (Working Hours Calculation)
            working_hours = "0h 0m"
            if start_local:
                # Agar operator abhi bhi kaam kar raha hai, toh current time se minus karenge
                end_calc = (
                    end_local if end_local else timezone.localtime(timezone.now())
                )
                diff = end_calc - start_local
                total_seconds = int(diff.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                working_hours = f"{hours}h {minutes}m"

            data.append(
                {
                    "id": h.id,
                    "plant": h.plant,
                    "operator_name": h.operator_name,
                    "machine_no": h.machine_no,
                    "shift": h.shift,
                    "status": h.status,
                    "reason": h.reason,
                    "remarks": h.remarks,
                    "assigned_by": h.assigned_by,
                    "start_time": (
                        start_local.strftime("%Y-%m-%d %I:%M %p")
                        if start_local
                        else None
                    ),
                    "end_time": (
                        end_local.strftime("%Y-%m-%d %I:%M %p") if end_local else None
                    ),
                    "duration": working_hours,  # Bhejo UI ko ki kitni der kaam kiya
                    "is_current": h.is_current,
                }
            )

        return Response(
            {
                "success": True,
                "history": data,
            }
        )

    except Exception as e:
        print(f"❌ History Error: {e}")
        return Response(
            {
                "success": False,
                "error": str(e),
            },
            status=500,
        )


@api_view(["GET"])
def plant2_hourly_idle(request):
    """
    Get Plant 2 hourly idle time data
    Query params: date, shift, machine_no, start_hour, end_hour
    """
    try:
        # Get filters
        selected_date = request.GET.get("date", datetime.now().strftime("%Y-%m-%d"))
        selected_shift = request.GET.get("shift", None)
        selected_machine = request.GET.get("machine", None)
        start_hour = request.GET.get("start_hour", None)
        end_hour = request.GET.get("end_hour", None)

        # Build query
        queryset = Plant2HourlyIdletime.objects.filter(timestamp__date=selected_date)

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
            "timestamp", "machine_no", "tool_id", "idle_time", "shut_height", "shift"
        ).order_by("-timestamp")

        # Format response
        idle_data = []
        for record in data:
            idle_data.append(
                {
                    "timestamp": record["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                    "machine_no": record["machine_no"],
                    "tool_id": record["tool_id"],
                    "idle_time": record["idle_time"],
                    "shut_height": str(record["shut_height"]),
                    "shift": record["shift"],
                    "hour": record["timestamp"].hour,
                }
            )

        return Response(
            {
                "success": True,
                "count": len(idle_data),
                "data": idle_data,
                "filters": {
                    "date": selected_date,
                    "shift": selected_shift or "All",
                    "machine": selected_machine or "All",
                    "hours": f"{start_hour or '00'}-{end_hour or '23'}",
                },
            }
        )

    except Exception as e:
        print(f"❌ API Error: {e}")
        import traceback

        traceback.print_exc()
        return Response({"success": False, "error": str(e), "data": []}, status=500)


@api_view(["GET"])
def plant2_hourly_idle_summary(request):
    """
    Get hourly idle summary for all machines
    Query params: date, shift
    """
    try:
        selected_date = request.GET.get("date", datetime.now().strftime("%Y-%m-%d"))
        selected_shift = request.GET.get("shift", None)

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
            summary_data.append(
                {
                    "machine_no": machine_no,
                    "total_idle_minutes": total_idle,
                    "hours_recorded": hours_recorded,
                    "last_update": (
                        last_update.strftime("%Y-%m-%d %H:%M:%S")
                        if last_update
                        else "N/A"
                    ),
                    "shift": shift or "A",
                }
            )

        return Response(
            {
                "success": True,
                "count": len(summary_data),
                "data": summary_data,
                "date": selected_date,
                "shift": selected_shift or "All",
            }
        )

    except Exception as e:
        print(f"❌ Summary API Error: {e}")
        import traceback

        traceback.print_exc()
        return Response({"success": False, "error": str(e), "data": []}, status=500)


# class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
#     def validate(self, attrs):
#         # Pehle default validation run karo (ID/Password check)
#         data = super().validate(attrs)

#         # Ab check karo ki user kisi group mein hai ya nahi
#         if self.user.groups.exists():
#             data["role"] = self.user.groups.first().name
#         else:
#             data["role"] = "Default_User"


#         data["username"] = self.user.username
#         return data
class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    def validate(self, attrs):
        data = super().validate(attrs)

        groups = list(self.user.groups.values_list("name", flat=True))

        data["role"] = groups[0] if groups else "Default_User"
        data["groups"] = groups
        data["username"] = self.user.username
        data["is_superuser"] = self.user.is_superuser

        return data


class CustomLoginView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer


# Helper function table name validate karne ke liye
def get_plant_table(plant_param):
    if plant_param == "plant2":
        return "plant2_data", 49
    return "plant1_data", 57


@never_cache
@api_view(["GET"])
def plant_wise_total(request):
    try:
        # Dono plants ka ek basic total bhejte hain
        return Response(
            {
                "success": True,
                "plant1": {"status": "Active", "total_machines": 57},
                "plant2": {"status": "Active", "total_machines": 49},
            }
        )
    except Exception as e:
        return Response({"success": False, "error": str(e)}, status=500)


@never_cache
@api_view(["GET"])
def date_range(request):
    plant = request.GET.get("plant", "plant1")
    table_name, _ = get_plant_table(plant)

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT MIN(DATE(timestamp)), MAX(DATE(timestamp)) FROM {table_name}"
            )
            row = cursor.fetchone()

        return Response(
            {
                "success": True,
                "first_date": row[0] if row[0] else "2024-01-01",
                "last_date": row[1] if row[1] else datetime.now().strftime("%Y-%m-%d"),
            }
        )
    except Exception as e:
        return Response({"success": False, "error": str(e)}, status=500)


@never_cache
@api_view(["GET"])
def realtime_dashboard(request):
    # Ye wahi data dega jo aapka original function aaj ka nikalta hai, par summary format me
    plant = request.GET.get("plant", "plant1")
    table_name, total_machines = get_plant_table(plant)
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(DISTINCT machine_no), SUM(count)
                FROM {table_name} 
                WHERE DATE(timestamp) = %s
            """,
                [today],
            )
            row = cursor.fetchone()

        return Response(
            {
                "success": True,
                "summary": {
                    "active_machines": row[0] or 0,
                    "total_machines": total_machines,
                    "total_production": row[1] or 0,
                    "date": today,
                },
            }
        )
    except Exception as e:
        return Response({"success": False, "error": str(e)}, status=500)


@never_cache
@api_view(["GET"])
def monthly_summary(request):
    plant = request.GET.get("plant", "plant1")
    month = int(request.GET.get("month", datetime.now().month))
    year = int(request.GET.get("year", datetime.now().year))

    table_name, _ = get_plant_table(plant)
    days_in_month = calendar.monthrange(year, month)[1]

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT 
                    EXTRACT(DAY FROM timestamp) as day,
                    SUM(count) as total_prod,
                    SUM(idle_time) as total_idle
                FROM {table_name}
                WHERE EXTRACT(MONTH FROM timestamp) = %s AND EXTRACT(YEAR FROM timestamp) = %s
                GROUP BY EXTRACT(DAY FROM timestamp)
                ORDER BY day
            """,
                [month, year],
            )
            results = cursor.fetchall()

        # Data map banate hain taaki daily chart me gap na aaye
        db_data = {
            int(row[0]): {"prod": row[1] or 0, "idle": row[2] or 0} for row in results
        }

        daily_breakdown = []
        total_prod = 0
        total_idle_mins = 0
        days_with_data = 0

        for day in range(1, days_in_month + 1):
            if day in db_data:
                prod = db_data[day]["prod"]
                idle = db_data[day]["idle"]
                has_data = True
                days_with_data += 1
                total_prod += prod
                total_idle_mins += idle
            else:
                prod = 0
                idle = 0
                has_data = False

            daily_breakdown.append(
                {
                    "day": day,
                    "production": prod,
                    "idle_minutes": idle,
                    "has_data": has_data,
                }
            )

        return Response(
            {
                "month_name": calendar.month_name[month],
                "summary": {
                    "total_production": total_prod,
                    "total_idle_hours": round(total_idle_mins / 60, 1),
                    "days_with_data": days_with_data,
                    "days_in_month": days_in_month,
                    "coverage": (
                        round((days_with_data / days_in_month) * 100, 1)
                        if days_in_month > 0
                        else 0
                    ),
                },
                "daily_breakdown": daily_breakdown,
            }
        )
    except Exception as e:
        return Response({"success": False, "error": str(e)}, status=500)


@never_cache
@api_view(["GET"])
def machine_wise(request):
    plant = request.GET.get("plant", "plant1")
    month = int(request.GET.get("month", datetime.now().month))
    year = int(request.GET.get("year", datetime.now().year))
    table_name, total_machines = get_plant_table(plant)

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT machine_no, SUM(count), SUM(idle_time)
                FROM {table_name}
                WHERE EXTRACT(MONTH FROM timestamp) = %s AND EXTRACT(YEAR FROM timestamp) = %s
                GROUP BY machine_no
            """,
                [month, year],
            )
            results = cursor.fetchall()

        machine_data = []
        for row in results:
            machine_data.append(
                {
                    "machine_no": row[0],
                    "production": row[1] or 0,
                    "idle_minutes": row[2] or 0,
                }
            )

        return Response({"success": True, "data": machine_data})
    except Exception as e:
        return Response({"success": False, "error": str(e)}, status=500)


import calendar


@never_cache
@api_view(["GET"])
def machine_analysis(request):
    # Jab user frontend pe kisi specific machine (e.g., "01") par click karega toh ye chalega
    plant = request.GET.get("plant", "plant1")
    machine_no = request.GET.get("machine_no")
    month = int(request.GET.get("month", datetime.now().month))
    year = int(request.GET.get("year", datetime.now().year))

    table_name, _ = get_plant_table(plant)
    days_in_month = calendar.monthrange(year, month)[1]

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT 
                    EXTRACT(DAY FROM timestamp) as day,
                    SUM(count) as total_prod,
                    SUM(idle_time) as total_idle
                FROM {table_name}
                WHERE machine_no = %s AND EXTRACT(MONTH FROM timestamp) = %s AND EXTRACT(YEAR FROM timestamp) = %s
                GROUP BY EXTRACT(DAY FROM timestamp)
                ORDER BY day
            """,
                [machine_no, month, year],
            )
            results = cursor.fetchall()

        db_data = {
            int(row[0]): {"prod": row[1] or 0, "idle": row[2] or 0} for row in results
        }

        daily_breakdown = []
        total_prod = 0
        total_idle_mins = 0
        active_days = 0

        for day in range(1, days_in_month + 1):
            if day in db_data:
                prod = db_data[day]["prod"]
                idle = db_data[day]["idle"]
                has_data = True
                if prod > 0 or idle > 0:
                    active_days += 1
                total_prod += prod
                total_idle_mins += idle
            else:
                prod = 0
                idle = 0
                has_data = False

            daily_breakdown.append(
                {
                    "day": day,
                    "production": prod,
                    "idle_minutes": idle,
                    "idle_hours": round(idle / 60, 2),
                    "has_data": has_data,
                    "status": "Active" if has_data else "Offline",
                }
            )

        return Response(
            {
                "machine_info": {
                    "machine_no": machine_no,
                    "machine_id": f"M-{str(machine_no).zfill(2)}",
                    "month_name": calendar.month_name[month],
                    "days_in_month": days_in_month,
                },
                "production_summary": {
                    "total_production": total_prod,
                    "average_daily": (
                        round(total_prod / active_days, 1) if active_days > 0 else 0
                    ),
                },
                "idle_summary": {
                    "total_idle_hours": round(total_idle_mins / 60, 1),
                    "total_idle_minutes": total_idle_mins,
                },
                "machine_status": {
                    "active_days": active_days,
                    "inactive_days": days_in_month - active_days,
                    "days_without_data": days_in_month - len(db_data),
                    "active_percentage": round((active_days / days_in_month) * 100, 1),
                    "status": "Operational" if active_days > 0 else "Offline",
                },
                "daily_breakdown": daily_breakdown,
            }
        )
    except Exception as e:
        return Response({"success": False, "error": str(e)}, status=500)


@api_view(["POST"])
def log_idle_reason(request):
    """
    Save live idle/offline reason in backend state
    and immediately notify all browsers for that plant.
    """
    print("🟡 IDLE API 1: REQUEST ENTERED", flush=True)
    
    try:
        data = request.data

        machine_no = data.get("machine_no")
        plant_no = data.get("plant_no")
        category = str(data.get("category", "")).strip()
        reason = str(data.get("reason", "")).strip()
        remarks = str(data.get("remarks", "")).strip()
        machine_status = str(
            data.get("machine_status", "ONLINE")
        ).strip().upper()

        # -----------------------------
        # 1. Validation
        # -----------------------------
        if not machine_no:
            return Response(
                {
                    "success": False,
                    "error": "machine_no is required",
                },
                status=400,
            )

        if not plant_no:
            return Response(
                {
                    "success": False,
                    "error": "plant_no is required",
                },
                status=400,
            )

        if not category or not reason:
            return Response(
                {
                    "success": False,
                    "error": "category and reason are required",
                },
                status=400,
            )

        try:
            machine_no = int(machine_no)
            plant_no = int(plant_no)
        except (TypeError, ValueError):
            return Response(
                {
                    "success": False,
                    "error": "Invalid machine_no or plant_no",
                },
                status=400,
            )

        # -----------------------------
        # 2. Select correct Plant state
        # -----------------------------
        if plant_no == 1:
            from apps.mqtt.simple_plant1 import (
                PLANT1_EXACT_REQUIREMENT_STATE,
            )

            state_obj = PLANT1_EXACT_REQUIREMENT_STATE
            group_name = "plant1_live_updates"
            plant_location = "Plant 1"

        elif plant_no == 2:
            from apps.mqtt.simple_plant2 import (
                PLANT2_EXACT_REQUIREMENT_STATE,
            )

            state_obj = PLANT2_EXACT_REQUIREMENT_STATE
            group_name = "plant2_live_updates"
            plant_location = "Plant 2"

        else:
            return Response(
                {
                    "success": False,
                    "error": "plant_no must be 1 or 2",
                },
                status=400,
            )

        if state_obj is None:
            return Response(
                {
                    "success": False,
                    "error": "Plant MQTT state is not available",
                },
                status=500,
            )

        # -----------------------------
        # 3. Save reason in backend
        # -----------------------------
        state_obj.set_pending_reason(
            machine_no=machine_no,
            category=category,
            reason=reason,
            remarks=remarks,
        )

        reason_state = (
            "OFFLINE"
            if machine_status == "OFFLINE"
            else "IDLE"
        )

        # -----------------------------
        # 4. Notify ALL browsers
        # -----------------------------
        try:
            channel_layer = get_channel_layer()

            if channel_layer:
                async_to_sync(
                    channel_layer.group_send
                )(
                    group_name,
                    {
                        "type": "send_machine_update",
                        "message": {
                            "event_type": "idle_reason_updated",
                            "plant_no": plant_no,
                            "plant": plant_location,
                            "machine_no": machine_no,
                            "machine_status": machine_status,
                            "reason_state": reason_state,
                            "has_pending_reason": True,
                            "category": category,
                            "reason": reason,
                        },
                    },
                )

                print(
                    f"📡 IDLE REASON WS SENT | "
                    f"{plant_location} | "
                    f"M{machine_no} | "
                    f"{reason_state}"
                )

        except Exception as ws_err:
            print(
                f"⚠️ Idle Reason WebSocket Error: {ws_err}"
            )

        return Response(
            {
                "success": True,
                "message": (
                    f"Reason saved successfully "
                    f"for Machine {machine_no}"
                ),
                "plant_no": plant_no,
                "machine_no": machine_no,
                "reason_state": reason_state,
                "has_pending_reason": True,
            },
            status=200,
        )

    except Exception as e:
        print(f"❌ API Error in log_idle_reason: {e}")
        traceback.print_exc()

        return Response(
            {
                "success": False,
                "error": str(e),
            },
            status=500,
        )


import json
from django.http import JsonResponse
from .models import PartMaster


@csrf_exempt  # Testing ke liye CSRF disable kiya hai taaki Postman se request directly chali jaye
def bulk_insert_parts(request):
    if request.method == "POST":
        try:
            # Body se JSON data nikalna
            data = json.loads(request.body)
            parts_to_create = []

            for item in data:
                parts_to_create.append(
                    PartMaster(
                        customer_name=item.get("customer_name"),
                        part_name=item.get("part_name"),
                        part_no=item.get("part_no"),
                        part_model=item.get("model"),
                        inspection_data=item.get("inspection_data", []),
                    )
                )

            # Bulk create: fast insertion ke liye
            PartMaster.objects.bulk_create(parts_to_create)

            return JsonResponse(
                {"message": "Master data successfully insert ho gaya bhai!"}, status=201
            )

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=400)

    return JsonResponse({"error": "Only POST method is allowed"}, status=405)


# 1. Sirf unique Customers ki list laane ke liye
def get_unique_customers(request):
    # 'customer_name' ke basis pe unique list nikal rahe hain
    customers = list(
        PartMaster.objects.values_list("customer_name", flat=True).distinct()
    )
    return JsonResponse({"customers": customers})


# 2. Customer select hone par uske parts laane ke liye (UPDATED)
def get_parts_by_customer(request, customer_name):
    # 'part_model' add kiya hai taaki UI mein bracket mein model show ho sake
    parts = list(
        PartMaster.objects.filter(customer_name=customer_name).values(
            "id", "part_name", "part_no", "part_model", "inspection_data"
        )
    )
    return JsonResponse({"parts": parts})


## --- AUTHENTICATION & SECURITY APIs ---
import random
from rest_framework.permissions import IsAuthenticated
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.core.mail import send_mail
from django.conf import settings

# 🔥 Import your actual model connected to your database table here
from .models import UserProfile

User = get_user_model()


# ==========================================
# 1. FOR LOGGED-IN USERS (Profile Settings)
# ==========================================
class ChangePasswordView(APIView):
    # This permission ensures the API cannot be accessed without a valid token
    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = request.user

        # 🚨 ADMIN SECURITY LOCK: Admins cannot change their password via this API even when logged in
        if user.is_superuser:
            return Response(
                {
                    "error": "Admin passwords cannot be changed via API. Please contact the System Administrator."
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        old_password = request.data.get("old_password")
        new_password = request.data.get("new_password")

        if not old_password or not new_password:
            return Response(
                {"error": "Both old and new passwords are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not user.check_password(old_password):
            return Response(
                {"error": "Incorrect old password."}, status=status.HTTP_400_BAD_REQUEST
            )

        user.set_password(new_password)
        user.save()

        return Response(
            {"message": "Password updated successfully!"}, status=status.HTTP_200_OK
        )


# ==========================================
# 2. FORGOT PASSWORD - OTP REQUEST (Send to Head)
# ==========================================
# Format: ('department', 'location'): 'Head_Email'
HEAD_MAPPING = {
    ("Production", "Plant 1"): "abhishek.kumar@atomone.in",
    ("Production", "Plant 2"): "ashok.reddy@atomone.in",
    ("QA", "Plant 1"): "Rajesh.dhiman@atomone.in",
    ("QA", "Plant 2"): "head.plant2.qa@atomone.in",
}


class RequestPasswordResetOTPView(APIView):
    permission_classes = []

    def post(self, request):
        username = request.data.get("username")

        if not username:
            return Response(
                {"error": "Username is required."}, status=status.HTTP_400_BAD_REQUEST
            )

        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            return Response(
                {"error": "This username does not exist."},
                status=status.HTTP_404_NOT_FOUND,
            )

        # 🚨 ADMIN SECURITY LOCK
        if user.is_superuser:
            return Response(
                {"error": "Admin passwords cannot be changed via API. Access Denied."},
                status=status.HTTP_403_FORBIDDEN,
            )

        # Fetch User Profile from the database
        try:
            # Use user_id or whatever the relational field is named in your model
            user_profile = UserProfile.objects.get(user_id=user.id)
        except UserProfile.DoesNotExist:
            return Response(
                {"error": "User profile not found in the database."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        user_dept = user_profile.department
        user_plant = user_profile.location

        if not user_dept or not user_plant:
            return Response(
                {"error": f"Department or location is not set for user '{username}'."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        head_email = HEAD_MAPPING.get((user_dept, user_plant))

        if not head_email:
            return Response(
                {
                    "error": f"Head email for {user_dept} and {user_plant} is not configured in the system."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        otp = str(random.randint(100000, 999999))
        cache_key = f"pwd_reset_otp_{username}"
        cache.set(cache_key, otp, timeout=600)  # 10 mins

        subject = f"🚨 SECURITY: Password Reset Request for {username}"
        message = (
            f"Hello Head,\n\n"
            f"User '{username}' from your department ({user_dept} - {user_plant}) has requested a password reset.\n\n"
            f"🔑 OTP: {otp}\n"
            f"(This OTP is valid for 10 minutes.)\n\n"
            f"If you approve this request, please provide this OTP to the user."
        )

        send_mail(
            subject,
            message,
            settings.EMAIL_HOST_USER,
            [head_email],
            fail_silently=False,
        )

        return Response(
            {
                "message": f"OTP has been successfully sent to your department head ({head_email})."
            },
            status=status.HTTP_200_OK,
        )


# ==========================================
# 3. FORGOT PASSWORD - VERIFY OTP & RESET
# ==========================================
class VerifyOTPAndResetPasswordView(APIView):
    permission_classes = []

    def post(self, request):
        username = request.data.get("username")
        otp_entered = request.data.get("otp")
        new_password = request.data.get("new_password")

        if not all([username, otp_entered, new_password]):
            return Response(
                {"error": "Username, OTP, and New Password are all required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            return Response(
                {"error": "User not found."}, status=status.HTTP_404_NOT_FOUND
            )

        # 🚨 ADMIN SECURITY LOCK
        if user.is_superuser:
            return Response(
                {"error": "Action Denied."}, status=status.HTTP_403_FORBIDDEN
            )

        cache_key = f"pwd_reset_otp_{username}"
        saved_otp = cache.get(cache_key)

        if not saved_otp:
            return Response(
                {"error": "OTP has expired or is invalid. Please request a new OTP."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if str(saved_otp) != str(otp_entered):
            return Response(
                {"error": "Invalid OTP!"}, status=status.HTTP_400_BAD_REQUEST
            )

        # Set new password if OTP matches
        user.set_password(new_password)
        user.save()

        # Security check: remove OTP from cache after one-time use
        cache.delete(cache_key)

        return Response(
            {"message": "Password updated successfully! You can now log in."},
            status=status.HTTP_200_OK,
        )


from django.contrib.auth.models import User
from django.utils.timezone import localtime
from .models import ReportActivityLog, QANotification


from django.utils import timezone


class ApproveReportView(APIView):
    permission_classes = []

    def post(self, request):
        log_id = request.data.get("log_id")
        approver_username = (
            request.data.get("approver_username")
            or request.data.get("approved_by")
            or "Approver"
        )
        remarks = request.data.get("remarks") or request.data.get("remark") or ""

        if not log_id:
            return Response(
                {"error": "log_id is required."}, status=status.HTTP_400_BAD_REQUEST
            )

        try:
            report = ReportActivityLog.objects.get(id=log_id)

            reviewed_at = timezone.localtime(timezone.now()).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            report.status = f"Approved by {approver_username}"
            report.approved_or_rejected_at = reviewed_at
            report.remarks = remarks
            report.save(update_fields=["status", "approved_or_rejected_at", "remarks"])
            if report.record_id and normalize_report_name(report.report_name) in [
                "machine history card",
                "machine history form",
            ]:
                MachineHistoryCard.objects.filter(id=report.record_id).update(
                    approved_by=approver_username
                )

            QANotification.objects.filter(report_log=report).update(is_read=True)

            return Response(
                {
                    "success": True,
                    "message": f"Report successfully approved by {approver_username}!",
                    "status": report.status,
                    "approved_or_rejected_at": str(reviewed_at),
                    "remarks": remarks,
                },
                status=status.HTTP_200_OK,
            )

        except ReportActivityLog.DoesNotExist:
            return Response(
                {"error": "Report log not found."}, status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class RejectReportView(APIView):
    permission_classes = []

    def post(self, request):
        log_id = request.data.get("log_id")
        approver_username = (
            request.data.get("approver_username")
            or request.data.get("rejected_by")
            or "Approver"
        )
        remarks = (
            request.data.get("remarks")
            or request.data.get("remark")
            or request.data.get("rejection_remark")
            or ""
        )

        if not log_id:
            return Response(
                {"error": "log_id is required."}, status=status.HTTP_400_BAD_REQUEST
            )

        if not remarks.strip():
            return Response(
                {"error": "Rejection remark is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            report = ReportActivityLog.objects.get(id=log_id)

            reviewed_at = timezone.localtime(timezone.now()).replace(
                microsecond=0, tzinfo=None
            )

            report.status = f"Rejected by {approver_username}"
            report.approved_or_rejected_at = reviewed_at
            report.remarks = remarks
            report.save(update_fields=["status", "approved_or_rejected_at", "remarks"])

            QANotification.objects.filter(report_log=report).update(is_read=True)

            return Response(
                {
                    "success": True,
                    "message": f"Report successfully rejected by {approver_username}!",
                    "status": report.status,
                    "approved_or_rejected_at": str(reviewed_at),
                    "remarks": remarks,
                },
                status=status.HTTP_200_OK,
            )

        except ReportActivityLog.DoesNotExist:
            return Response(
                {"error": "Report log not found."}, status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


from api.services.report_registry import get_route_config, normalize_report_name


def resolve_hub_from_department(department_name):
    text = str(department_name or "").lower()

    if "production" in text:
        return "production-hub"

    if "qa" in text or "quality" in text:
        return "qa-hub"

    if "maintenance" in text:
        return "maintenance-hub"

    return ""


class GetQANotificationsView(APIView):
    def get(self, request, username):
        try:
            user = User.objects.get(username=username)

            notifications = (
                QANotification.objects.filter(user=user, is_read=False)
                .select_related("report_log")
                .order_by("-created_at")
            )

            notifications_data = []

            for n in notifications:
                log = n.report_log

                route_config = get_route_config(log.report_name) if log else {}

                department_name = log.department_name if log else ""
                report_name = log.report_name if log else ""

                notifications_data.append(
                    {
                        "id": n.id,
                        "message": n.message,
                        "time": timezone.localtime(n.created_at).strftime(
                            "%d-%b-%Y %I:%M %p"
                        ),
                        "report_log_id": log.id if log else None,
                        "report_name": report_name,
                        "department_name": department_name,
                        # These are route values derived from report_name/department_name.
                        # They are NOT database columns now.
                        "formRoute": route_config.get("form_key", ""),
                        "hub": resolve_hub_from_department(department_name)
                        or route_config.get("hub", ""),
                        "submitted_by": log.username if log else "",
                    }
                )

            return Response(
                {"success": True, "notifications": notifications_data}, status=200
            )

        except User.DoesNotExist:
            return Response(
                {"success": False, "error": "User not found", "notifications": []},
                status=404,
            )

        except Exception as e:
            return Response(
                {"success": False, "error": str(e), "notifications": []}, status=500
            )


# ==============================================================================
# 🏭 ENTERPRISE DYNAMIC ROUTING (LOCATION + DEPARTMENT AWARE)
# ==============================================================================


from django.contrib.auth.models import User
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from api.services.report_logging import auto_log_report
from api.services.report_registry import get_route_config


def get_user_profile_obj(user_obj):
    try:
        return user_obj.userprofile
    except Exception:
        pass

    try:
        return user_obj.profile
    except Exception:
        pass

    return None


def get_target_group_from_department(department):
    dept = str(department or "").strip().lower()

    if dept in ["qa", "quality"]:
        return "Quality_Approvers"

    if dept == "production":
        return "Production_Approvers"

    if dept == "maintenance":
        return "Maintenance_Approvers"

    return None


class SaveReportLogView(APIView):
    permission_classes = []

    def post(self, request):
        username = request.data.get("username")
        report_name = request.data.get("report_name")
        record_id = request.data.get("record_id")

        if not username or not report_name:
            return Response(
                {"error": "username and report_name are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        user_obj = User.objects.filter(username=username).first()

        if not user_obj:
            return Response(
                {"error": f"User '{username}' not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        profile = get_user_profile_obj(user_obj)

        if not profile:
            return Response(
                {"error": f"User profile not found for '{username}'."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        location = str(getattr(profile, "location", "") or "").strip()
        department = str(getattr(profile, "department", "") or "").strip()

        if not location or not department:
            return Response(
                {
                    "error": (
                        f"Location/Department missing for user '{username}'. "
                        "Set it in Django Admin."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # ✅ IMPORTANT FIX
        # Frontend value ignore karo. Backend user profile se plant banayega.
        department_name = f"{location} ({department})"

        route_config = get_route_config(report_name)

        target_group = get_target_group_from_department(department) or route_config.get(
            "target_group"
        )

        log = auto_log_report(
            username=username,
            report_name=report_name,
            record_id=record_id,
            department_name=department_name,
            target_group=target_group,
        )

        if not log:
            return Response(
                {"error": "Activity log failed. Check backend console."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            {
                "success": True,
                "message": "Activity log and notification created successfully.",
                "log_id": log.id,
                "department_name": department_name,
                "target_group": target_group,
                "form_key": route_config.get("form_key", ""),
                "hub": route_config.get("hub", ""),
            },
            status=status.HTTP_201_CREATED,
        )


from rest_framework.parsers import MultiPartParser, FormParser, JSONParser

from .serializers import UserDepartmentProfileSerializer


class CurrentUserProfileView(APIView):
    permission_classes = [IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def get(self, request):
        profile = UserProfile.objects.get(user=request.user)
        serializer = UserDepartmentProfileSerializer(
            profile, context={"request": request}
        )
        return Response(serializer.data)

    def patch(self, request):
        profile = UserProfile.objects.get(user=request.user)

        serializer = UserDepartmentProfileSerializer(
            profile, data=request.data, partial=True, context={"request": request}
        )

        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)

        return Response(serializer.errors, status=400)

    def get_object(self):

        profile, created = UserProfile.objects.get_or_create(user=self.request.user)
        return profile


@api_view(["GET"])
def get_department_stats(request):
    # ── 1. GET THE USERNAME OF THE VIEWER FROM FRONTEND ──
    viewer_username = request.GET.get("username")

    if not viewer_username:
        return Response(
            {"error": "Username is required to fetch stats."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        user_obj = User.objects.get(username=viewer_username)
    except User.DoesNotExist:
        return Response(
            {"error": f"User '{viewer_username}' not found in database."},
            status=status.HTTP_404_NOT_FOUND,
        )

    # ── 2. EXTRACT PLANT FROM THE VIEWER'S PROFILE ──
    user_location = None
    user_department = None

    try:
        profile = getattr(user_obj, "userprofile", getattr(user_obj, "profile", None))
        if profile:
            user_location = str(getattr(profile, "location", "")).strip()
            user_department = str(getattr(profile, "department", "")).strip()
    except Exception as e:
        print(f" Profile check exception for {viewer_username}: {e}")

    if not user_location or not user_department:
        return Response(
            {
                "error": f"Validation Error: User '{viewer_username}' lacks valid Location or Department in Admin."
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    # ── 3. ROLE-BASED ACCESS CONTROL (Admin vs Head vs Engineer) ──
    print(f" DEBUG: Checking roles for user -> {viewer_username}")

    # Case A: Admin (Superuser ya All access)
    if (
        user_obj.is_superuser
        or user_department.lower() == "all"
        or user_location.lower() == "all"
    ):
        print(f" STATUS: {viewer_username} is an ADMIN (Showing Everything)")
        logs = ReportActivityLog.objects.all()

    # Case B: Head/Approver (If the user belongs to any Approver group)
    elif user_obj.groups.filter(name__icontains="Approvers").exists():
        target_department = f"{user_location} ({user_department})"
        print(f" STATUS: {viewer_username} is a HEAD for {target_department}")
        logs = ReportActivityLog.objects.filter(
            department_name__icontains=target_department
        )

    # Case C: Engineer
    else:
        print(f" STATUS: {viewer_username} is an ENGINEER (Showing only own data)")
        #  MAIN FIX: Sirf us operator ka khud ka data nikalenge
        logs = ReportActivityLog.objects.filter(username=viewer_username)

    # ── 4. DATA FORMATTING & DYNAMIC HEAD DETECTION ──
    user_data_dict = {}
    dynamic_head_cache = {}

    for log in logs:
        raw_username = log.username
        db_department = log.department_name

        # Dynamic Head Detection
        if db_department not in dynamic_head_cache:
            head_name = "Admin (Head)"
            try:
                if "(" in db_department and ")" in db_department:
                    loc_part = db_department.split("(")[0].strip()
                    dept_part = db_department.split("(")[1].replace(")", "").strip()

                    target_group = f"{dept_part}_Approvers"
                    target_loc_code = loc_part.replace(" ", "").lower()

                    approvers = User.objects.filter(groups__name=target_group)

                    for approver in approvers:
                        profile = getattr(
                            approver, "userprofile", getattr(approver, "profile", None)
                        )
                        if profile and getattr(profile, "location", None):
                            approver_loc_code = (
                                str(profile.location).strip().replace(" ", "").lower()
                            )

                            if approver_loc_code == target_loc_code:
                                head_name = f"{approver.username.split('@')[0]} (Head)"
                                break
            except Exception as e:
                pass

            dynamic_head_cache[db_department] = head_name

        actual_head_name = dynamic_head_cache[db_department]

        # Formatting user dict (Removing '@')
        if raw_username not in user_data_dict:
            user_data_dict[raw_username] = {
                "user_id": raw_username,
                "username": raw_username.split("@")[0],
                "filled": 0,
                "approved": 0,
                "pending": 0,
                "rejected": 0,
                "head": actual_head_name,
                "reportsList": [],
            }

        db_status = log.status.strip().lower() if log.status else "in progress"

        if "approved" in db_status:
            display_status = "Approved"
            user_data_dict[raw_username]["approved"] += 1

        elif "rejected" in db_status:
            display_status = "Rejected"
            user_data_dict[raw_username]["rejected"] += 1
        else:
            display_status = "Pending"
            user_data_dict[raw_username]["pending"] += 1

        user_data_dict[raw_username]["filled"] += 1

        report_display_id = str(log.record_id) if log.record_id else "N/A"

        if log.timestamp:
            ts = (
                datetime.fromisoformat(str(log.timestamp))
                if isinstance(log.timestamp, str)
                else log.timestamp
            )
            formatted_date = ts.strftime("%d-%b-%Y")
        else:
            formatted_date = ""

        user_data_dict[raw_username]["reportsList"].append(
            {
                "id": report_display_id,
                "record_id": report_display_id,  # 🔥 Safety ke liye purani key
                "activity_log_id": log.id,  # 🔥 NAYI KEY: Backend ab properly 308 (Log ID) bhejega
                "name": log.report_name,
                "date": formatted_date,
                "status": display_status,
            }
        )

    response_data = list(user_data_dict.values())
    return Response(response_data)




# ==========================================================
# ATTENDANCE SECTION V2 - paste in views.py
# Required imports at top of views.py:
# from rest_framework.decorators import api_view
# from rest_framework.response import Response
# from django.db import connections
# from datetime import datetime, timedelta, time
# ==========================================================

DEPARTMENT_MAP = {
    "001": "HR & Admin",
    "002": "Accounts",
    "003": "Dispatch",
    "004": "Maintenance",
    "005": "Tool Room",
    "006": "Quality",
    "007": "Production",
    "008": "NPD",
    "009": "CNC",
    "010": "IOT Dev",
    "011": "R&D",
    "013": "Design",
    "014": "QMS",
    "015": "Weld Shop",
}

DAY_SHIFT_START = dt_time(8, 30)
DAY_SHIFT_END = dt_time(17, 30)
LATE_GRACE_MINUTES = 10
GATE_PASS_LIMIT_MINUTES = 120
HALF_DAY_MIN_WORK_MINUTES = 270


def clean_sql_value(value):
    if value is None:
        return ""
    return str(value).strip()


def row_get(row, key, default=None):
    if key in row:
        return row.get(key, default)

    key_lower = key.lower()
    for k, v in row.items():
        if str(k).lower() == key_lower:
            return v
    return default


def serialize_sql_value(value):
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return clean_sql_value(value)


def get_raw_master(row):
    raw = {}
    for key, value in row.items():
        key_text = str(key)
        if key_text.startswith("att") or key_text.startswith("life") or key_text.startswith("month"):
            continue
        raw[key] = serialize_sql_value(value)
    return raw


def plant_to_company_code(plant):
    plant = clean_sql_value(plant)
    if plant in ["Plant 1", "plant_1", "1", "001"]:
        return "001"
    if plant in ["Plant 2", "plant_2", "2", "002"]:
        return "002"
    return ""


def company_code_to_plant(company_code):
    code = clean_sql_value(company_code).zfill(3)
    if code == "001":
        return "Plant 1"
    if code == "002":
        return "Plant 2"
    return "Unknown"


def get_department_name(code):
    code = clean_sql_value(code).zfill(3)
    return DEPARTMENT_MAP.get(code, f"Dept {code}" if code else "--")


def get_numeric_paycode(paycode):
    value = clean_sql_value(paycode).upper()
    if value.startswith("E"):
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits.lstrip("0") or "0")
    except Exception:
        return None


def classify_employee_type_and_vendor(paycode, company_code):
    """
    E / EE series = Office Employee.
    Numeric worker series = Worker.
    Vendor name only for vendor workers.
    """
    paycode_clean = clean_sql_value(paycode).upper()
    plant = company_code_to_plant(company_code)
    number_code = get_numeric_paycode(paycode_clean)

    if paycode_clean.startswith("E"):
        return "Employee", "Office Employee", ""

    if number_code is None:
        return "Worker", "Company Worker", ""

    if plant == "Plant 1":
        if 24000 <= number_code <= 24999:
            return "Worker", "Vendor Worker", "Shiv"
        if 22000 <= number_code <= 22999:
            return "Worker", "Vendor Worker", "Unati"
        if 21000 <= number_code <= 21999:
            return "Worker", "Vendor Worker", "Abhishek"
        if 20000 <= number_code <= 20999:
            return "Worker", "Vendor Worker", "VVMS"
        if 2000 <= number_code <= 2999:
            return "Worker", "Company Worker", ""
        return "Worker", "Company Worker", ""

    if plant == "Plant 2":
        if 18000 <= number_code <= 18999:
            return "Worker", "Vendor Worker", "Abhishek"
        if 17000 <= number_code <= 17999:
            return "Worker", "Vendor Worker", "VVMS"
        if 16000 <= number_code <= 16999:
            return "Worker", "Vendor Worker", "Shiv"
        if 14000 <= number_code <= 14999:
            return "Worker", "Vendor Worker", "Unati"
        if 4000 <= number_code <= 4999:
            return "Worker", "Vendor Worker", "VVMS"
        if 7000 <= number_code <= 7999:
            return "Worker", "Vendor Worker", "VVMS"
        if 1000 <= number_code <= 1999:
            return "Worker", "Company Worker", ""
        return "Worker", "Company Worker", ""

    return "Worker", "Company Worker", ""


def get_display_designation(row, employee_type, department):
    designation = clean_sql_value(row_get(row, "DESIGNATION"))
    if designation:
        return designation
    if employee_type == "Employee":
        return department or "Office Employee"
    return "Worker"


def format_att_time(value):
    if not value:
        return "--"
    try:
        return value.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return str(value)


def format_att_date(value):
    if not value:
        return "--"
    try:
        return value.strftime("%d %b %Y")
    except Exception:
        return str(value)


def format_api_date(value):
    if not value:
        return None
    try:
        return value.strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def number_value(value):
    try:
        return float(value or 0)
    except Exception:
        return 0


def minutes_to_working_hours(value):
    minutes = int(number_value(value))
    if minutes <= 0:
        return "--"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins:02d}m"


def minutes_to_total_hours(value):
    minutes = int(number_value(value))
    hours = minutes // 60
    mins = minutes % 60
    return {"minutes": minutes, "label": f"{hours}h {mins:02d}m"}


def safe_datetime(value):
    if not value:
        return None
    if hasattr(value, "date") and hasattr(value, "time"):
        return value
    return None


def minutes_between(start_dt, end_dt):
    try:
        if not start_dt or not end_dt:
            return 0
        if end_dt < start_dt:
            end_dt = end_dt + timedelta(days=1)
        return int((end_dt - start_dt).total_seconds() // 60)
    except Exception:
        return 0


def calculate_late_minutes(punch_in, shift_start):
    db_late = number_value(shift_start.get("late") if isinstance(shift_start, dict) else None)
    if db_late > 0:
        return int(db_late)
    return 0


def get_late_minutes(row, prefix="att"):
    db_late = int(number_value(row_get(row, f"{prefix}LateArrival")))
    if db_late > 0:
        return db_late

    punch_in = safe_datetime(row_get(row, f"{prefix}In1"))
    shift_start = safe_datetime(row_get(row, f"{prefix}ShiftStartTime"))

    if not punch_in:
        return 0

    # If shift start is missing, use standard office shift 08:30.
    if not shift_start:
        shift_start = datetime.combine(punch_in.date(), DAY_SHIFT_START)

    if punch_in <= shift_start:
        return 0

    return minutes_between(shift_start, punch_in)


def get_worked_minutes(row, prefix="att"):
    hours_worked = int(number_value(row_get(row, f"{prefix}HoursWorked")))
    if hours_worked > 0:
        return hours_worked

    punch_in = safe_datetime(row_get(row, f"{prefix}In1"))
    out_time = safe_datetime(row_get(row, f"{prefix}Out2")) or safe_datetime(row_get(row, f"{prefix}Out1"))
    return minutes_between(punch_in, out_time)


def has_any_punch(row, prefix="att"):
    return bool(
        row_get(row, f"{prefix}In1") or
        row_get(row, f"{prefix}In2") or
        row_get(row, f"{prefix}Out1") or
        row_get(row, f"{prefix}Out2")
    )


def get_fe_status(row, prefix="att"):
    """
    Final rule:
    - ABSENT only when no punch in/out exists.
    - If punch exists, do not mark absent just because raw STATUS says A.
    - Late/Gate Pass/Half Day are based on shift 08:30 and attendance gap.
    """
    raw_status = clean_sql_value(row_get(row, f"{prefix}Status")).upper()
    leave_value = number_value(row_get(row, f"{prefix}LeaveValue"))
    holiday_value = number_value(row_get(row, f"{prefix}HolidayValue"))
    wo_value = number_value(row_get(row, f"{prefix}WoValue"))

    punch_available = has_any_punch(row, prefix)

    if not punch_available:
        if leave_value > 0 or raw_status.startswith("L"):
            return "ON LEAVE"
        if holiday_value > 0 or raw_status in ["HLD", "H", "HOLIDAY"]:
            return "HOLIDAY"
        if wo_value > 0 or raw_status in ["WO", "WEEK OFF", "WEEKOFF"]:
            return "WEEK OFF"
        return "ABSENT"

    late_minutes = get_late_minutes(row, prefix)
    worked_minutes = get_worked_minutes(row, prefix)

    if worked_minutes and worked_minutes < HALF_DAY_MIN_WORK_MINUTES:
        return "HALF DAY"

    if late_minutes > GATE_PASS_LIMIT_MINUTES:
        return "HALF DAY"

    if late_minutes > LATE_GRACE_MINUTES:
        return "GATE PASS"

    if late_minutes > 0:
        return "LATE"

    return "PRESENT"


def get_status_type(status):
    value = clean_sql_value(status).upper()
    if value == "PRESENT":
        return "success"
    if value in ["LATE", "GATE PASS", "HALF DAY"]:
        return "warning"
    if value == "ABSENT":
        return "danger"
    return "info"


def get_status_remark(row, prefix="att"):
    status = get_fe_status(row, prefix)
    late_minutes = get_late_minutes(row, prefix)

    if status == "LATE" and late_minutes:
        return f"Late {late_minutes}m"
    if status == "GATE PASS" and late_minutes:
        return f"Gate pass {late_minutes}m"
    if status == "HALF DAY" and late_minutes:
        return f"Half day gap {late_minutes}m"
    return None


def get_fe_shift(shift_start, employee_type="Worker"):
    if employee_type == "Employee":
        return "Day"
    if not shift_start:
        return "Day"
    try:
        hour = shift_start.hour
        if 5 <= hour < 18:
            return "Day"
        return "Night"
    except Exception:
        return "Day"


def get_today_dt():
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def get_month_range(selected_date):
    selected_dt = datetime.strptime(selected_date, "%Y-%m-%d")
    today_dt = get_today_dt()
    if selected_dt > today_dt:
        selected_dt = today_dt

    month_start = selected_dt.replace(day=1)
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)

    return selected_dt, month_start, next_month, today_dt


def build_full_employee_row(row):
    paycode = clean_sql_value(row_get(row, "PAYCODE"))
    company_code = clean_sql_value(row_get(row, "COMPANYCODE")).zfill(3)

    employee_type, worker_category, vendor_name = classify_employee_type_and_vendor(paycode, company_code)
    department_name = get_department_name(row_get(row, "DEPARTMENTCODE"))
    display_designation = get_display_designation(row, employee_type, department_name)

    active = clean_sql_value(row_get(row, "ACTIVE")).upper()
    is_active = active == "Y"

    shift_start = row_get(row, "attShiftStartTime")
    shift_end = row_get(row, "attShiftEndTime")
    out_time = row_get(row, "attOut2") or row_get(row, "attOut1")

    if employee_type == "Employee":
        shift_start_label = "8:30 AM"
        shift_end_label = "5:30 PM"
    else:
        shift_start_label = format_att_time(shift_start)
        shift_end_label = format_att_time(shift_end)

    month_total_days = int(number_value(row_get(row, "monthTotalDays")))
    month_present_days = int(number_value(row_get(row, "monthPresentDays")))
    month_absent_days = int(number_value(row_get(row, "monthAbsentDays")))
    month_leave_days = int(number_value(row_get(row, "monthLeaveDays")))
    month_holiday_days = int(number_value(row_get(row, "monthHolidayDays")))
    month_weekoff_days = int(number_value(row_get(row, "monthWeekOffDays")))
    month_late_days = int(number_value(row_get(row, "monthLateDays")))
    month_gate_pass_days = int(number_value(row_get(row, "monthGatePassDays")))
    month_half_days = int(number_value(row_get(row, "monthHalfDays")))
    month_worked_minutes = int(number_value(row_get(row, "monthTotalWorkedMinutes")))
    month_percentage = round((month_present_days / month_total_days) * 100, 1) if month_total_days else 0

    life_total_days = int(number_value(row_get(row, "lifeTotalDays")))
    life_present_days = int(number_value(row_get(row, "lifePresentDays")))
    life_absent_days = int(number_value(row_get(row, "lifeAbsentDays")))
    life_percentage = round((life_present_days / life_total_days) * 100, 1) if life_total_days else 0

    status = get_fe_status(row, "att")

    return {
        "id": paycode,
        "paycode": paycode,
        "name": clean_sql_value(row_get(row, "EMPNAME")),
        "designation": display_designation,
        "department": department_name,
        "departmentCode": clean_sql_value(row_get(row, "DEPARTMENTCODE")).zfill(3),
        "employeeType": employee_type,
        "typeDisplay": "Office Employee" if employee_type == "Employee" else worker_category,
        "workerCategory": worker_category,
        "vendorName": vendor_name,
        "plant": company_code_to_plant(company_code),
        "companyCode": company_code,
        "shift": get_fe_shift(shift_start, employee_type),
        "inTime": format_att_time(row_get(row, "attIn1")),
        "outTime": format_att_time(out_time),
        "workingHours": minutes_to_working_hours(row_get(row, "attHoursWorked")),
        "status": status,
        "attendancePercentage": month_percentage,
        "avatar": "/default-avatar.png",
        "active": active,
        "isActive": is_active,
        "activeText": "Active" if is_active else "Inactive",
        "cardNo": clean_sql_value(row_get(row, "PRESENTCARDNO")),
        "dateOfJoining": format_att_date(row_get(row, "DateOFJOIN")),
        "joiningDate": format_att_date(row_get(row, "DateOFJOIN")),
        "dateOfBirth": format_att_date(row_get(row, "DateOFBIRTH")),
        "guardianName": clean_sql_value(row_get(row, "GUARDIANNAME")),
        "gender": clean_sql_value(row_get(row, "SEX")),
        "qualification": clean_sql_value(row_get(row, "QUALIFICATION")),
        "address": clean_sql_value(row_get(row, "ADDRESS1")),
        "address2": clean_sql_value(row_get(row, "ADDRESS2")),
        "telephone": clean_sql_value(row_get(row, "TELEPHONE1")),
        "mobile": clean_sql_value(row_get(row, "MobileNo")),
        "email": clean_sql_value(row_get(row, "Email")) or clean_sql_value(row_get(row, "E_MAIL1")),
        "leavingDate": format_att_date(row_get(row, "Leavingdate")),
        "todayAttendance": {
            "date": format_att_date(row_get(row, "attDateOffice")),
            "shiftStart": shift_start_label,
            "shiftEnd": shift_end_label,
            "punchIn": format_att_time(row_get(row, "attIn1")),
            "punchOut": format_att_time(out_time),
            "hoursWorked": minutes_to_working_hours(row_get(row, "attHoursWorked")),
            "rawStatus": clean_sql_value(row_get(row, "attStatus")),
            "status": status,
            "lateMinutes": get_late_minutes(row, "att"),
            "remark": get_status_remark(row, "att"),
        },
        "monthlyAttendance": {
            "totalDays": month_total_days,
            "presentDays": month_present_days,
            "absentDays": month_absent_days,
            "leaveDays": month_leave_days,
            "holidayDays": month_holiday_days,
            "weekOffDays": month_weekoff_days,
            "lateDays": month_late_days,
            "gatePassDays": month_gate_pass_days,
            "halfDays": month_half_days,
            "totalWorking": minutes_to_total_hours(month_worked_minutes),
            "attendancePercentage": month_percentage,
        },
        "attendanceFromJoining": {
            "totalDays": life_total_days,
            "presentDays": life_present_days,
            "absentDays": life_absent_days,
            "attendancePercentage": life_percentage,
        },
        "rawMaster": get_raw_master(row),
    }


def summarize_history_records(records):
    total = len(records)
    present = late = gate_pass = half_day = absent = leave = holiday = weekoff = 0
    total_worked = 0

    for row in records:
        status = get_fe_status(row, "att")
        total_worked += get_worked_minutes(row, "att")

        if status == "PRESENT":
            present += 1
        elif status == "LATE":
            late += 1
            present += 1
        elif status == "GATE PASS":
            gate_pass += 1
            present += 1
        elif status == "HALF DAY":
            half_day += 1
            present += 0.5
        elif status == "ABSENT":
            absent += 1
        elif status == "ON LEAVE":
            leave += 1
        elif status == "HOLIDAY":
            holiday += 1
        elif status == "WEEK OFF":
            weekoff += 1

    percentage = round((present / total) * 100, 1) if total else 0

    return {
        "totalDays": total,
        "presentDays": int(present) if float(present).is_integer() else present,
        "absentDays": absent,
        "leaveDays": leave,
        "holidayDays": holiday,
        "weekOffDays": weekoff,
        "lateDays": late,
        "gatePassDays": gate_pass,
        "halfDays": half_day,
        "totalWorking": minutes_to_total_hours(total_worked),
        "attendancePercentage": percentage,
    }


def records_to_history(records):
    history = []
    for row in records:
        out_time = row_get(row, "attOut2") or row_get(row, "attOut1")
        status = get_fe_status(row, "att")
        history.append({
            "date": format_att_date(row_get(row, "attDateOffice")),
            "displayDate": format_att_date(row_get(row, "attDateOffice")),
            "inTime": None if not row_get(row, "attIn1") else format_att_time(row_get(row, "attIn1")),
            "outTime": None if not out_time else format_att_time(out_time),
            "hours": None if not get_worked_minutes(row, "att") else minutes_to_working_hours(get_worked_minutes(row, "att")),
            "late": get_status_remark(row, "att"),
            "lateMinutes": get_late_minutes(row, "att"),
            "status": status,
            "type": get_status_type(status),
        })
    return history


def get_employee_month_records(paycode, month_start, next_month, today_dt):
    month_end = min(next_month, today_dt + timedelta(days=1))

    with connections["sqlserver_db"].cursor() as cursor:
        cursor.execute("""
            SELECT
                DateOFFICE AS attDateOffice,
                SHIFTSTARTTIME AS attShiftStartTime,
                SHIFTENDTIME AS attShiftEndTime,
                HOURSWORKED AS attHoursWorked,
                STATUS AS attStatus,
                SHIFT AS attShift,
                SHIFTATTENDED AS attShiftAttended,
                IN1 AS attIn1,
                IN2 AS attIn2,
                OUT1 AS attOut1,
                OUT2 AS attOut2,
                PRESENTVALUE AS attPresentValue,
                ABSENTVALUE AS attAbsentValue,
                LEAVEVALUE AS attLeaveValue,
                HOLIDAY_VALUE AS attHolidayValue,
                WO_VALUE AS attWoValue,
                LATEARRIVAL AS attLateArrival
            FROM dbo.tblTimeRegister
            WHERE LTRIM(RTRIM(PAYCODE)) = %s
              AND DateOFFICE >= %s
              AND DateOFFICE < %s
            ORDER BY DateOFFICE
        """, [paycode, month_start.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ==========================================================
# GET /api/attendance/
# ==========================================================
@api_view(["GET"])
def attendance_dashboard(request):
    try:
        plant = request.GET.get("plant", "Plant 1")
        employee_type = request.GET.get("employee_type", "Worker")
        shift_filter = request.GET.get("shift", "Day")
        selected_date = request.GET.get("date") or datetime.now().strftime("%Y-%m-%d")
        active_filter = request.GET.get("active", "Y")

        company_code = plant_to_company_code(plant)
        if not company_code:
            return Response({"success": False, "message": "Invalid plant", "employees": [], "summary": {}}, status=400)

        selected_dt, month_start, next_month, today_dt = get_month_range(selected_date)
        next_day = selected_dt + timedelta(days=1)
        month_end = min(next_month, today_dt + timedelta(days=1))
        selected_date = selected_dt.strftime("%Y-%m-%d")

        where_parts = ["e.COMPANYCODE = %s"]
        where_params = [company_code]

        if clean_sql_value(active_filter).upper() in ["Y", "N"]:
            where_parts.append("e.ACTIVE = %s")
            where_params.append(clean_sql_value(active_filter).upper())

        where_sql = " AND ".join(where_parts)

        query = f"""
            ;WITH life_summary AS (
                SELECT
                    LTRIM(RTRIM(PAYCODE)) AS lifePaycode,
                    COUNT(*) AS lifeTotalDays,
                    SUM(CASE WHEN ISNULL(PRESENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS lifePresentDays,
                    SUM(CASE WHEN ISNULL(ABSENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS lifeAbsentDays
                FROM dbo.tblTimeRegister
                WHERE DateOFFICE <= %s
                GROUP BY LTRIM(RTRIM(PAYCODE))
            ),
            month_summary AS (
                SELECT
                    LTRIM(RTRIM(PAYCODE)) AS monthPaycode,
                    COUNT(*) AS monthTotalDays,
                    SUM(CASE WHEN ISNULL(PRESENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthPresentDays,
                    SUM(CASE WHEN ISNULL(ABSENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthAbsentDays,
                    SUM(CASE WHEN ISNULL(LEAVEVALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthLeaveDays,
                    SUM(CASE WHEN ISNULL(HOLIDAY_VALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthHolidayDays,
                    SUM(CASE WHEN ISNULL(WO_VALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthWeekOffDays,
                    SUM(CASE WHEN ISNULL(LATEARRIVAL, 0) > 0 AND ISNULL(LATEARRIVAL, 0) <= 10 THEN 1 ELSE 0 END) AS monthLateDays,
                    SUM(CASE WHEN ISNULL(LATEARRIVAL, 0) > 10 AND ISNULL(LATEARRIVAL, 0) <= 120 THEN 1 ELSE 0 END) AS monthGatePassDays,
                    SUM(CASE WHEN ISNULL(LATEARRIVAL, 0) > 120 THEN 1 ELSE 0 END) AS monthHalfDays,
                    SUM(ISNULL(HOURSWORKED, 0)) AS monthTotalWorkedMinutes
                FROM dbo.tblTimeRegister
                WHERE DateOFFICE >= %s
                  AND DateOFFICE < %s
                GROUP BY LTRIM(RTRIM(PAYCODE))
            )
            SELECT
                e.*,
                tr.DateOFFICE AS attDateOffice,
                tr.SHIFTSTARTTIME AS attShiftStartTime,
                tr.SHIFTENDTIME AS attShiftEndTime,
                tr.HOURSWORKED AS attHoursWorked,
                tr.STATUS AS attStatus,
                tr.SHIFT AS attShift,
                tr.SHIFTATTENDED AS attShiftAttended,
                tr.IN1 AS attIn1,
                tr.IN2 AS attIn2,
                tr.OUT1 AS attOut1,
                tr.OUT2 AS attOut2,
                tr.PRESENTVALUE AS attPresentValue,
                tr.ABSENTVALUE AS attAbsentValue,
                tr.LEAVEVALUE AS attLeaveValue,
                tr.HOLIDAY_VALUE AS attHolidayValue,
                tr.WO_VALUE AS attWoValue,
                tr.LATEARRIVAL AS attLateArrival,
                ISNULL(ls.lifeTotalDays, 0) AS lifeTotalDays,
                ISNULL(ls.lifePresentDays, 0) AS lifePresentDays,
                ISNULL(ls.lifeAbsentDays, 0) AS lifeAbsentDays,
                ISNULL(ms.monthTotalDays, 0) AS monthTotalDays,
                ISNULL(ms.monthPresentDays, 0) AS monthPresentDays,
                ISNULL(ms.monthAbsentDays, 0) AS monthAbsentDays,
                ISNULL(ms.monthLeaveDays, 0) AS monthLeaveDays,
                ISNULL(ms.monthHolidayDays, 0) AS monthHolidayDays,
                ISNULL(ms.monthWeekOffDays, 0) AS monthWeekOffDays,
                ISNULL(ms.monthLateDays, 0) AS monthLateDays,
                ISNULL(ms.monthGatePassDays, 0) AS monthGatePassDays,
                ISNULL(ms.monthHalfDays, 0) AS monthHalfDays,
                ISNULL(ms.monthTotalWorkedMinutes, 0) AS monthTotalWorkedMinutes
            FROM dbo.TblEmployee e
            LEFT JOIN dbo.tblTimeRegister tr
                ON LTRIM(RTRIM(e.PAYCODE)) = LTRIM(RTRIM(tr.PAYCODE))
               AND tr.DateOFFICE >= %s
               AND tr.DateOFFICE < %s
            LEFT JOIN life_summary ls
                ON LTRIM(RTRIM(e.PAYCODE)) = ls.lifePaycode
            LEFT JOIN month_summary ms
                ON LTRIM(RTRIM(e.PAYCODE)) = ms.monthPaycode
            WHERE {where_sql}
            ORDER BY e.EMPNAME
        """

        params = [today_dt.strftime("%Y-%m-%d"), month_start.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d"), selected_dt.strftime("%Y-%m-%d"), next_day.strftime("%Y-%m-%d"), *where_params]

        with connections["sqlserver_db"].cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        employees = []
        for row in rows:
            emp = build_full_employee_row(row)
            if emp["employeeType"] != employee_type:
                continue
            if emp["shift"] != shift_filter:
                continue
            employees.append(emp)

        summary = {
            "total": len(employees),
            "present": len([x for x in employees if x["status"] == "PRESENT"]),
            "absent": len([x for x in employees if x["status"] == "ABSENT"]),
            "leave": len([x for x in employees if x["status"] == "ON LEAVE"]),
            "late": len([x for x in employees if x["status"] in ["LATE", "GATE PASS", "HALF DAY"]]),
            "active": len([x for x in employees if x["isActive"]]),
            "inactive": len([x for x in employees if not x["isActive"]]),
        }

        return Response({"success": True, "date": selected_date, "month": month_start.strftime("%Y-%m"), "plant": plant, "employee_type": employee_type, "shift": shift_filter, "active_filter": active_filter, "summary": summary, "employees": employees})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return Response({"success": False, "message": str(e), "employees": [], "summary": {"total": 0, "present": 0, "absent": 0, "leave": 0, "late": 0, "active": 0, "inactive": 0}}, status=500)


# ==========================================================
# GET /api/attendance/employees-master/
# ==========================================================
@api_view(["GET"])
def attendance_employee_master(request):
    # This endpoint uses the same dashboard logic but without shift/date attendance row requirement.
    # Keep previous employees-master API if already working. Dashboard/profile/calendar are the important updated APIs.
    try:
        plant = request.GET.get("plant", "")
        employee_type = request.GET.get("employee_type", "")
        active_filter = request.GET.get("active", "all")
        selected_date = request.GET.get("date") or datetime.now().strftime("%Y-%m-%d")
        selected_dt, month_start, next_month, today_dt = get_month_range(selected_date)
        month_end = min(next_month, today_dt + timedelta(days=1))

        where_parts = ["1 = 1"]
        where_params = []
        company_code = plant_to_company_code(plant)
        if company_code:
            where_parts.append("e.COMPANYCODE = %s")
            where_params.append(company_code)
        if clean_sql_value(active_filter).upper() in ["Y", "N"]:
            where_parts.append("e.ACTIVE = %s")
            where_params.append(clean_sql_value(active_filter).upper())
        where_sql = " AND ".join(where_parts)

        query = f"""
            ;WITH life_summary AS (
                SELECT LTRIM(RTRIM(PAYCODE)) AS lifePaycode, COUNT(*) AS lifeTotalDays,
                       SUM(CASE WHEN ISNULL(PRESENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS lifePresentDays,
                       SUM(CASE WHEN ISNULL(ABSENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS lifeAbsentDays
                FROM dbo.tblTimeRegister
                WHERE DateOFFICE <= %s
                GROUP BY LTRIM(RTRIM(PAYCODE))
            ),
            month_summary AS (
                SELECT LTRIM(RTRIM(PAYCODE)) AS monthPaycode, COUNT(*) AS monthTotalDays,
                       SUM(CASE WHEN ISNULL(PRESENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthPresentDays,
                       SUM(CASE WHEN ISNULL(ABSENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthAbsentDays,
                       SUM(CASE WHEN ISNULL(LEAVEVALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthLeaveDays,
                       SUM(CASE WHEN ISNULL(HOLIDAY_VALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthHolidayDays,
                       SUM(CASE WHEN ISNULL(WO_VALUE, 0) > 0 THEN 1 ELSE 0 END) AS monthWeekOffDays,
                       SUM(CASE WHEN ISNULL(LATEARRIVAL, 0) > 0 AND ISNULL(LATEARRIVAL, 0) <= 10 THEN 1 ELSE 0 END) AS monthLateDays,
                       SUM(CASE WHEN ISNULL(LATEARRIVAL, 0) > 10 AND ISNULL(LATEARRIVAL, 0) <= 120 THEN 1 ELSE 0 END) AS monthGatePassDays,
                       SUM(CASE WHEN ISNULL(LATEARRIVAL, 0) > 120 THEN 1 ELSE 0 END) AS monthHalfDays,
                       SUM(ISNULL(HOURSWORKED, 0)) AS monthTotalWorkedMinutes
                FROM dbo.tblTimeRegister
                WHERE DateOFFICE >= %s AND DateOFFICE < %s
                GROUP BY LTRIM(RTRIM(PAYCODE))
            )
            SELECT e.*, NULL AS attDateOffice, NULL AS attShiftStartTime, NULL AS attShiftEndTime, NULL AS attHoursWorked,
                   NULL AS attStatus, NULL AS attShift, NULL AS attShiftAttended, NULL AS attIn1, NULL AS attIn2,
                   NULL AS attOut1, NULL AS attOut2, 0 AS attPresentValue, 0 AS attAbsentValue, 0 AS attLeaveValue,
                   0 AS attHolidayValue, 0 AS attWoValue, 0 AS attLateArrival,
                   ISNULL(ls.lifeTotalDays, 0) AS lifeTotalDays, ISNULL(ls.lifePresentDays, 0) AS lifePresentDays,
                   ISNULL(ls.lifeAbsentDays, 0) AS lifeAbsentDays,
                   ISNULL(ms.monthTotalDays, 0) AS monthTotalDays, ISNULL(ms.monthPresentDays, 0) AS monthPresentDays,
                   ISNULL(ms.monthAbsentDays, 0) AS monthAbsentDays, ISNULL(ms.monthLeaveDays, 0) AS monthLeaveDays,
                   ISNULL(ms.monthHolidayDays, 0) AS monthHolidayDays, ISNULL(ms.monthWeekOffDays, 0) AS monthWeekOffDays,
                   ISNULL(ms.monthLateDays, 0) AS monthLateDays, ISNULL(ms.monthGatePassDays, 0) AS monthGatePassDays,
                   ISNULL(ms.monthHalfDays, 0) AS monthHalfDays, ISNULL(ms.monthTotalWorkedMinutes, 0) AS monthTotalWorkedMinutes
            FROM dbo.TblEmployee e
            LEFT JOIN life_summary ls ON LTRIM(RTRIM(e.PAYCODE)) = ls.lifePaycode
            LEFT JOIN month_summary ms ON LTRIM(RTRIM(e.PAYCODE)) = ms.monthPaycode
            WHERE {where_sql}
            ORDER BY e.COMPANYCODE, e.EMPNAME
        """
        params = [today_dt.strftime("%Y-%m-%d"), month_start.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d"), *where_params]
        with connections["sqlserver_db"].cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        employees = []
        for row in rows:
            emp = build_full_employee_row(row)
            if employee_type and emp["employeeType"] != employee_type:
                continue
            employees.append(emp)

        return Response({"success": True, "employees": employees, "summary": {"total": len(employees), "active": len([x for x in employees if x["isActive"]]), "inactive": len([x for x in employees if not x["isActive"]])}})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return Response({"success": False, "message": str(e), "employees": []}, status=500)


# ==========================================================
# GET /api/attendance/employees/<paycode>/
# ==========================================================
@api_view(["GET"])
def attendance_employee_profile(request, paycode):
    try:
        selected_date = request.GET.get("date") or datetime.now().strftime("%Y-%m-%d")
        selected_dt, month_start, next_month, today_dt = get_month_range(selected_date)
        next_day = selected_dt + timedelta(days=1)
        month_end = min(next_month, today_dt + timedelta(days=1))

        query = """
            ;WITH life_summary AS (
                SELECT LTRIM(RTRIM(PAYCODE)) AS lifePaycode, COUNT(*) AS lifeTotalDays,
                       SUM(CASE WHEN ISNULL(PRESENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS lifePresentDays,
                       SUM(CASE WHEN ISNULL(ABSENTVALUE, 0) > 0 THEN 1 ELSE 0 END) AS lifeAbsentDays
                FROM dbo.tblTimeRegister
                WHERE DateOFFICE <= %s
                GROUP BY LTRIM(RTRIM(PAYCODE))
            )
            SELECT e.*,
                   tr.DateOFFICE AS attDateOffice, tr.SHIFTSTARTTIME AS attShiftStartTime, tr.SHIFTENDTIME AS attShiftEndTime,
                   tr.HOURSWORKED AS attHoursWorked, tr.STATUS AS attStatus, tr.SHIFT AS attShift, tr.SHIFTATTENDED AS attShiftAttended,
                   tr.IN1 AS attIn1, tr.IN2 AS attIn2, tr.OUT1 AS attOut1, tr.OUT2 AS attOut2,
                   tr.PRESENTVALUE AS attPresentValue, tr.ABSENTVALUE AS attAbsentValue, tr.LEAVEVALUE AS attLeaveValue,
                   tr.HOLIDAY_VALUE AS attHolidayValue, tr.WO_VALUE AS attWoValue, tr.LATEARRIVAL AS attLateArrival,
                   ISNULL(ls.lifeTotalDays, 0) AS lifeTotalDays, ISNULL(ls.lifePresentDays, 0) AS lifePresentDays,
                   ISNULL(ls.lifeAbsentDays, 0) AS lifeAbsentDays,
                   0 AS monthTotalDays, 0 AS monthPresentDays, 0 AS monthAbsentDays, 0 AS monthLeaveDays,
                   0 AS monthHolidayDays, 0 AS monthWeekOffDays, 0 AS monthLateDays, 0 AS monthGatePassDays,
                   0 AS monthHalfDays, 0 AS monthTotalWorkedMinutes
            FROM dbo.TblEmployee e
            LEFT JOIN dbo.tblTimeRegister tr
                ON LTRIM(RTRIM(e.PAYCODE)) = LTRIM(RTRIM(tr.PAYCODE))
               AND tr.DateOFFICE >= %s
               AND tr.DateOFFICE < %s
            LEFT JOIN life_summary ls ON LTRIM(RTRIM(e.PAYCODE)) = ls.lifePaycode
            WHERE LTRIM(RTRIM(e.PAYCODE)) = %s
        """

        params = [today_dt.strftime("%Y-%m-%d"), selected_dt.strftime("%Y-%m-%d"), next_day.strftime("%Y-%m-%d"), paycode]
        with connections["sqlserver_db"].cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            row = cursor.fetchone()

        if not row:
            return Response({"success": False, "message": "Employee not found"}, status=404)

        emp = build_full_employee_row(dict(zip(columns, row)))
        month_records = get_employee_month_records(paycode, month_start, next_month, today_dt)
        month_summary = summarize_history_records(month_records)
        month_history = records_to_history(month_records)
        today_status = emp["todayAttendance"]["status"]

        emp["monthlyAttendance"] = month_summary
        emp["attendancePercentage"] = month_summary["attendancePercentage"]

        profile = {
            **emp,
            "manager": "--",
            "shiftTiming": "8:30 AM - 5:30 PM" if emp["employeeType"] == "Employee" else f"{emp['todayAttendance']['shiftStart']} - {emp['todayAttendance']['shiftEnd']}",
            "today": {
                "punchIn": None if emp["inTime"] == "--" else emp["inTime"],
                "punchOut": None if emp["outTime"] == "--" else emp["outTime"],
                "workingHours": None if emp["workingHours"] == "--" else emp["workingHours"],
                "status": today_status,
                "shiftStart": "8:30 AM" if emp["employeeType"] == "Employee" else emp["todayAttendance"]["shiftStart"],
                "shiftEnd": "5:30 PM" if emp["employeeType"] == "Employee" else emp["todayAttendance"]["shiftEnd"],
            },
            "machineWorking": None if emp["employeeType"] == "Employee" else None,
            "attendanceHealth": {
                "score": month_summary["attendancePercentage"],
                "totalDays": month_summary["totalDays"],
                "presentDays": month_summary["presentDays"],
                "lateArrivals": month_summary["lateDays"],
                "gatePassDays": month_summary["gatePassDays"],
                "halfDays": month_summary["halfDays"],
                "absentDays": month_summary["absentDays"],
                "previousMonthDifference": 0,
            },
            "history": month_history,
            "shiftInformation": {
                "shift": emp["shift"],
                "timing": "8:30 AM - 5:30 PM" if emp["employeeType"] == "Employee" else f"{emp['todayAttendance']['shiftStart']} - {emp['todayAttendance']['shiftEnd']}",
                "breakTime": "Lunch included",
                "gracePeriod": f"{LATE_GRACE_MINUTES} min",
                "weeklyOff": "Sunday",
                "fullDayMinimum": "As per HR rule",
                "halfDayMinimum": "More than 2 hours gap / low working hours",
                "nextShift": {"time": "8:30 AM" if emp["employeeType"] == "Employee" else "--", "date": "Next working day"},
            },
            "recentActivity": [
                {"date": "Selected Date", "text": f"Attendance status: {today_status}", "type": get_status_type(today_status)},
                {"date": "Selected Date", "text": emp["todayAttendance"].get("remark") or "Punch data checked", "type": get_status_type(today_status)},
            ],
        }

        return Response(profile)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return Response({"success": False, "message": str(e)}, status=500)


# ==========================================================
# GET /api/attendance/employees/<paycode>/calendar/
# ==========================================================
@api_view(["GET"])
def attendance_employee_calendar(request, paycode):
    try:
        year = int(request.GET.get("year"))
        month = int(request.GET.get("month"))
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        today_dt = get_today_dt()
        rows = get_employee_month_records(paycode, start_date, end_date, today_dt)
        records = []

        for row in rows:
            out_time = row_get(row, "attOut2") or row_get(row, "attOut1")
            worked_minutes = get_worked_minutes(row, "att")
            records.append({
                "date": format_api_date(row_get(row, "attDateOffice")),
                "displayDate": format_att_date(row_get(row, "attDateOffice")),
                "status": get_fe_status(row, "att"),
                "punchIn": None if not row_get(row, "attIn1") else format_att_time(row_get(row, "attIn1")),
                "punchOut": None if not out_time else format_att_time(out_time),
                "workingHours": None if not worked_minutes else minutes_to_working_hours(worked_minutes),
                "lateMinutes": get_late_minutes(row, "att"),
                "remark": get_status_remark(row, "att"),
            })

        return Response(records)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return Response({"success": False, "message": str(e), "records": []}, status=500)