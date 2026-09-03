from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.contrib.auth.models import Group
from api.models import Notification, PushSubscription
from api.serializers import NotificationSerializer
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

import traceback


# 1. Aapka existing view (Modified to save DB)
@api_view(["POST"])
# @permission_classes([IsAuthenticated])
def send_push_alert(request):
    try:
        data = request.data
        machine_no = data.get("machine_no")
        target_role = data.get("target_role")
        message = data.get("message")

        # DEBUGGING: Terminal mein check karein kya aa raha hai
        print(f"DEBUG: Role received: {target_role}")

        target_group = Group.objects.filter(name=target_role).first()
        if not target_group:
            print("DEBUG: Group not found!")  # Agar ye dikhe, toh spelling check karein
            return Response(
                {"success": False, "error": f"Role {target_role} not found"}, status=404
            )

        users = target_group.user_set.all()
        print(
            f"DEBUG: Users found: {users.count()}"
        )  # Agar ye 0 hai, toh group khali hai

        # Database mein har user ke liye notification save karein
        notifications_to_create = []
        for user in users:
            notifications_to_create.append(
                Notification(user=user, machine_no=machine_no, message=message)
            )

        # Bulk create for better performance
        Notification.objects.bulk_create(notifications_to_create)

        return Response(
            {
                "success": True,
                "message": f"Alert saved for {users.count()} {target_role}(s)",
            }
        )

    except Exception as e:
        return Response({"success": False, "error": str(e)}, status=500)


# 2. Logged-in user ki notifications fetch karne ke liye API
@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_my_notifications(request):
    # Sirf unread notifications chahiye toh `.filter(is_read=False)` lagayein
    notifications = Notification.objects.filter(user=request.user)
    serializer = NotificationSerializer(notifications, many=True)
    return Response({"success": True, "data": serializer.data})


# 3. Notification ko read mark karne ke liye API
@api_view(["POST"])
@permission_classes([IsAuthenticated])
def mark_notification_read(request, notif_id):
    try:
        notification = Notification.objects.get(id=notif_id, user=request.user)
        notification.is_read = True
        notification.save()
        return Response({"success": True, "message": "Marked as read"})
    except Notification.DoesNotExist:
        return Response(
            {"success": False, "error": "Notification not found"}, status=404
        )


import re  # Make sure ye file ke top par imported ho


@api_view(["POST"])
def log_idle_reason(request):

    print("🟡 IDLE API 1: REQUEST ENTERED", flush=True)

    try:
        # ============================================
        # 1. Read frontend data
        # ============================================

        data = request.data

        print("🟡 IDLE API 2: REQUEST DATA READ", flush=True)

        machine_no = data.get("machine_no")
        plant_no = data.get("plant_no")

        category = str(data.get("category", "")).strip()

        reason = str(data.get("reason", "")).strip()

        remarks = str(data.get("remarks", "")).strip()

        machine_status = str(data.get("machine_status", "ONLINE")).strip().upper()

        # ============================================
        # 2. Validation
        # ============================================

        if not machine_no:
            return Response(
                {"success": False, "error": "machine_no is required"}, status=400
            )

        if not plant_no:
            return Response(
                {"success": False, "error": "plant_no is required"}, status=400
            )

        if not category or not reason:
            return Response(
                {"success": False, "error": "category and reason are required"},
                status=400,
            )

        try:
            machine_no = int(machine_no)
            plant_no = int(plant_no)

        except (TypeError, ValueError):
            return Response(
                {"success": False, "error": "Invalid machine_no or plant_no"},
                status=400,
            )

        print(f"🟡 IDLE API 3: " f"Plant={plant_no} M{machine_no}", flush=True)

        # ============================================
        # 3. Select correct plant MQTT state
        # ============================================

        if plant_no == 1:

            from apps.mqtt.simple_plant1 import PLANT1_EXACT_REQUIREMENT_STATE

            state_obj = PLANT1_EXACT_REQUIREMENT_STATE

            group_name = "plant1_live_updates"

            plant_location = "Plant 1"

        elif plant_no == 2:

            from apps.mqtt.simple_plant2 import PLANT2_EXACT_REQUIREMENT_STATE

            state_obj = PLANT2_EXACT_REQUIREMENT_STATE

            group_name = "plant2_live_updates"

            plant_location = "Plant 2"

        else:

            return Response(
                {"success": False, "error": "plant_no must be 1 or 2"}, status=400
            )

        if state_obj is None:

            return Response(
                {"success": False, "error": ("Plant MQTT state " "is not available")},
                status=500,
            )

        # ============================================
        # 4. Save pending reason
        # ============================================

        print(f"🟡 IDLE API 4: " f"BEFORE SAVE M{machine_no}", flush=True)

        state_obj.set_pending_reason(
            machine_no=machine_no, category=category, reason=reason, remarks=remarks
        )

        print(f"🟢 IDLE API 5: " f"AFTER SAVE M{machine_no}", flush=True)

        reason_state = "OFFLINE" if machine_status == "OFFLINE" else "IDLE"

        # ============================================
        # 5. Send update to ALL browsers
        # ============================================

        print("🟡 IDLE API 6: " "BEFORE WEBSOCKET", flush=True)

        try:

            channel_layer = get_channel_layer()

            if channel_layer:

                async_to_sync(channel_layer.group_send)(
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

            print("🟢 IDLE API 7: " "AFTER WEBSOCKET", flush=True)

        except Exception as ws_err:

            print("⚠️ IDLE WS ERROR:", ws_err, flush=True)

        # ============================================
        # 6. Return quickly to frontend
        # ============================================

        print("✅ IDLE API 8: RETURNING 200", flush=True)

        return Response(
            {
                "success": True,
                "message": f"Reason saved successfully " f"for Machine {machine_no}",
                "plant_no": plant_no,
                "machine_no": machine_no,
                "reason_state": reason_state,
                "has_pending_reason": True,
            },
            status=200,
        )

    except Exception as e:

        print(f"❌ API Error in " f"log_idle_reason: {e}", flush=True)

        traceback.print_exc()

        return Response({"success": False, "error": str(e)}, status=500)


@csrf_exempt
def save_subscription(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            endpoint = data.get("endpoint")
            keys = data.get("keys", {})
            auth = keys.get("auth")
            p256dh = keys.get("p256dh")

            if endpoint and auth and p256dh:
                PushSubscription.objects.get_or_create(
                    endpoint=endpoint, defaults={"auth": auth, "p256dh": p256dh}
                )
                return JsonResponse(
                    {"status": "success", "message": "Subscribed!"}, status=200
                )
        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)}, status=400)

    return JsonResponse({"status": "error", "message": "Invalid request"}, status=400)
