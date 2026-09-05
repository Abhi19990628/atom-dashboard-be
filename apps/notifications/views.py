from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.contrib.auth.models import Group
from api.models import (
    Notification,
    PushSubscription,
    IdealTimeSegmentReason,
)
from api.serializers import NotificationSerializer
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from django.utils import timezone
import traceback

from apps import notifications


@api_view(["POST"])
def send_push_alert(request):
    """
    Legacy endpoint retired.

    Idle notifications are now automatically created
    from the exact IdealTimeSegmentReason event.
    """

    return Response(
        {
            "success": False,
            "error": "Legacy send-alert endpoint is retired.",
        },
        status=410,
    )

# 2. Logged-in user ki notifications fetch karne ke liye API
@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_my_notifications(request):

    user = request.user

    # ==========================================================
    # ONE EVENT = ONE GLOBAL NOTIFICATION
    #
    # Notification kisi recipient user ki property nahi hai.
    # Visibility Ideal event + logged-in user's plant/group
    # se decide hogi.
    # ==========================================================

    notifications = (
        Notification.objects
        .filter(
            ideal_event__isnull=False,
            ideal_event__report_status="PENDING",
        )
        .select_related("ideal_event")
    )

    # ----------------------------------------------------------
    # Superuser can see all plant notifications
    # ----------------------------------------------------------
    # ----------------------------------------------------------
    # Only Idle_Reason_Responder users can see
    # machine Idle / Offline notifications.
    #
    # Superuser/Admin does NOT bypass this rule.
    # ----------------------------------------------------------
    
    if not user.groups.filter(
        name="Idle_Reason_Responder"
    ).exists():
    
        return Response({
            "success": True,
            "data": [],
        })
    
    
    # ----------------------------------------------------------
    # Normal responder user:
    # show only his/her plant notifications.
    #
    # If someday a superuser is intentionally added to
    # Idle_Reason_Responder, superuser can see all plants.
    # ----------------------------------------------------------
    
    if not user.is_superuser:
    
        profile = getattr(user, "profile", None)
    
        if profile is None or not profile.location:
        
            return Response({
                "success": True,
                "data": [],
            })
    
        notifications = notifications.filter(
            ideal_event__plant_location=profile.location
        )

    notifications = notifications.order_by(
    "-ideal_event__ideal_start_at",
    "-ideal_event_id",
    )

    serializer = NotificationSerializer(
        notifications,
        many=True,
    )

    return Response({
        "success": True,
        "data": serializer.data,
    })


# 3. Notification ko read mark karne ke liye API
@api_view(["POST"])
@permission_classes([IsAuthenticated])
def mark_notification_read(request, notif_id):

    try:

        notification = (
            Notification.objects
            .select_related("ideal_event")
            .get(
                pk=notif_id,
                ideal_event__report_status="PENDING",
            )
        )

        user = request.user

        # ======================================================
        # AUTHORIZATION
        # ======================================================

        if not user.is_superuser:

            if not user.groups.filter(
                name="Idle_Reason_Responder"
            ).exists():

                return Response(
                    {
                        "success": False,
                        "error": (
                            "You are not authorized "
                            "to open this notification."
                        ),
                    },
                    status=403,
                )

            profile = getattr(
                user,
                "profile",
                None,
            )

            if (
                profile is None
                or profile.location
                != notification.ideal_event.plant_location
            ):

                return Response(
                    {
                        "success": False,
                        "error": (
                            "This notification belongs "
                            "to another plant."
                        ),
                    },
                    status=403,
                )

        # ======================================================
        # IMPORTANT:
        #
        # Opening/viewing notification DOES NOT mark it read.
        #
        # is_read becomes TRUE only after reason form submit.
        # ======================================================

        return Response(
            {
                "success": True,
                "message": (
                    "Notification opened. "
                    "It will be marked read after "
                    "downtime reason submission."
                ),
                "is_read": notification.is_read,
            }
        )

    except Notification.DoesNotExist:

        return Response(
            {
                "success": False,
                "error": "Notification not found",
            },
            status=404,
        )


import re  # Make sure ye file ke top par imported ho


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def log_idle_reason(request):
    """
    Legacy idle-reason endpoint retired.

    Use:
    POST /api/ideal-reports/<event_id>/submit/
    """

    return Response(
        {
            "success": False,
            "error": (
                "Legacy idle reason endpoint is retired. "
                "Use the Ideal event submit endpoint."
            ),
        },
        status=410,
    )


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
