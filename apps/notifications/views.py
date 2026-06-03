from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.contrib.auth.models import Group
from api.models import Notification, PushSubscription
from api.serializers import NotificationSerializer
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json

# 1. Aapka existing view (Modified to save DB)
@api_view(['POST'])
# @permission_classes([IsAuthenticated])
def send_push_alert(request):
    try:
        data = request.data
        machine_no = data.get('machine_no')
        target_role = data.get('target_role')
        message = data.get('message')
        
        # DEBUGGING: Terminal mein check karein kya aa raha hai
        print(f"DEBUG: Role received: {target_role}") 
        
        target_group = Group.objects.filter(name=target_role).first()
        if not target_group:
            print("DEBUG: Group not found!") # Agar ye dikhe, toh spelling check karein
            return Response({'success': False, 'error': f'Role {target_role} not found'}, status=404)
            
        users = target_group.user_set.all()
        print(f"DEBUG: Users found: {users.count()}") # Agar ye 0 hai, toh group khali hai
        
        # Database mein har user ke liye notification save karein
        notifications_to_create = []
        for user in users:
            notifications_to_create.append(
                Notification(
                    user=user,
                    machine_no=machine_no,
                    message=message
                )
            )
        
        # Bulk create for better performance
        Notification.objects.bulk_create(notifications_to_create)

        return Response({'success': True, 'message': f'Alert saved for {users.count()} {target_role}(s)'})
        
    except Exception as e:
        return Response({'success': False, 'error': str(e)}, status=500)


# 2. Logged-in user ki notifications fetch karne ke liye API
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_my_notifications(request):
    # Sirf unread notifications chahiye toh `.filter(is_read=False)` lagayein
    notifications = Notification.objects.filter(user=request.user)
    serializer = NotificationSerializer(notifications, many=True)
    return Response({'success': True, 'data': serializer.data})


# 3. Notification ko read mark karne ke liye API
@api_view(['POST'])
@permission_classes([IsAuthenticated])
def mark_notification_read(request, notif_id):
    try:
        notification = Notification.objects.get(id=notif_id, user=request.user)
        notification.is_read = True
        notification.save()
        return Response({'success': True, 'message': 'Marked as read'})
    except Notification.DoesNotExist:
        return Response({'success': False, 'error': 'Notification not found'}, status=404)



import re # Make sure ye file ke top par imported ho

@api_view(['POST'])
def log_idle_reason(request):
    try:
        data = request.data
        machine_no = data.get('machine_no')
        category = data.get('category')
        reason = data.get('reason')
        remarks = data.get('remarks', '')
        
        # 1. RAM Buffer mein reason save karo
        try:
            from apps.mqtt.simple_plant2 import EXACT_REQUIREMENT_STATE
            EXACT_REQUIREMENT_STATE.set_pending_reason(machine_no, category, reason, remarks)
        except Exception as state_err:
            print(f"⚠️ State Update Warning: {state_err}")
        
        # ==============================================================
        # ✅ ULTIMATE NOTIFICATION CLEAR LOGIC (Yahi theek karega sab kuch)
        # ==============================================================
        try:
            # Frontend se jo number aaya (jaise 3), usko clean int banalo
            mach_num_to_clear = str(int(re.sub(r'\D', '', str(machine_no))))
            
            # Saari unread notification nikalo
            unread_notifications = Notification.objects.filter(is_read=False)
            cleared_count = 0
            
            for n in unread_notifications:
                # Database ke "Machine-03" ko "3" banakar compare karo
                n_num = str(int(re.sub(r'\D', '', str(n.machine_no))))
                if n_num == mach_num_to_clear:
                    n.is_read = True
                    n.save()
                    cleared_count += 1
                    
            print(f"✅ Auto-Cleared {cleared_count} notifications for M-{machine_no} from log_idle_reason")
        except Exception as notif_err:
            print(f"❌ Notification Clear Error in log_idle_reason: {notif_err}")
        # ==============================================================
        
        return Response({
            "success": True, 
            "message": "Reason logged and notification cleared successfully"
        })
    except Exception as e:
        print(f"❌ API Error: {e}")
        return Response({"success": False, "error": str(e)}, status=500)
    

@csrf_exempt
def save_subscription(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            endpoint = data.get('endpoint')
            keys = data.get('keys', {})
            auth = keys.get('auth')
            p256dh = keys.get('p256dh')

            if endpoint and auth and p256dh:
                PushSubscription.objects.get_or_create(
                    endpoint=endpoint,
                    defaults={'auth': auth, 'p256dh': p256dh}
                )
                return JsonResponse({'status': 'success', 'message': 'Subscribed!'}, status=200)
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=400)
            
    return JsonResponse({'status': 'error', 'message': 'Invalid request'}, status=400)