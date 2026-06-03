from django.urls import path
from . import views

urlpatterns = [
    path('send-alert/', views.send_push_alert, name='send_push_alert'),
    path('my-notifications/', views.get_my_notifications, name='get_my_notifications'),
    path('read-notification/<int:notif_id>/', views.mark_notification_read, name='mark_notification_read'),
    path('log-idle-reason/', views.log_idle_reason, name='log_idle_reason'),
    path('save-subscription/', views.save_subscription, name='save_subscription'),
]