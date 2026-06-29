# backend/api/urls.py
from django.urls import path, include  # 🔥 NAYA: yahan 'include' add kiya hai
from . import views
# from .views import MasterDropdownView, MasterParametersView
from .views import ApproveReportView, GetQANotificationsView , RejectReportView, log_idle_reason
from .views import ChangePasswordView, RequestPasswordResetOTPView, VerifyOTPAndResetPasswordView
from .views import (

     plant_wise_total,
    date_range,
    realtime_dashboard,
    monthly_summary,
    machine_wise,
    machine_analysis,

)

from .views import get_machine_history
from django.contrib import admin
from .views import CustomLoginView, ChangePasswordView 
from .views import CurrentUserProfileView,get_department_stats

urlpatterns = [
    # Dashboard APIs
    path('admin/', admin.site.urls),
    
    path('login/', CustomLoginView.as_view(), name='api_login'),
    path('change-password/', ChangePasswordView.as_view(), name='change-password'),
    
    # Forgot password flow (Bina login ke)
    path('request-reset-otp/', RequestPasswordResetOTPView.as_view(), name='request-reset-otp'),
    path('verify-reset-otp/', VerifyOTPAndResetPasswordView.as_view(), name='verify-reset-otp'),
    
  
    path('approve-report/', ApproveReportView.as_view(), name='api_approve_report'),
    path('reject-report/', RejectReportView.as_view(), name='api_reject_report'),
    
    # 👇 NAYA NOTIFICATION MODULE YAHAN ADD KIYA HAI 👇
    path('', include('apps.notifications.urls')),
    path('', include('apps.production_reports.urls')),
    path('', include('apps.qa_reports.urls')),
    path('', include ('apps.maintenance_reports.urls')),

   
    path('dashboard/', views.get_dashboard_data, name='get_dashboard_data'),
    path('available-dates/', views.get_available_dates, name='get_available_dates'),
    path('dashboard-tables/', views.get_assignment_idle_data, name='get_assignment_idle_data'),
    
    # ✅ NEW OPERATOR ASSIGNMENT APIs
    path('operators/', views.get_operators_by_plant, name='get_operators'),
    path('operators/add/', views.add_operator, name='add_operator'),
    path('machines/list/', views.get_machines_by_plant, name='get_machines'),
    path('assignment/save/', views.save_operator_assignment, name='save+_operator_assignment'),
    path('assignments/list/', views.get_operator_assignments, name='get_operator_assignments'),
    
    # Old Assignment APIs (Keep existing)
    path('assignments/', views.create_assignment, name='create_assignment'),
    path('machines/<str:machine_no>/auto-fill/', views.get_auto_fill_data, name='get_auto_fill_data'),
    path('idle-reports/', views.create_idle_report, name='create_idle_report'),
    
    # Production APIs
    path('machine-production/', views.machine_production_data, name='machine_production_data'), 
    path('production-line-status/', views.production_line_status_data, name='production_line_status_data'),
    path('test-direct-query/', views.test_direct_query, name='test_direct_query'),
    
    # New React Dashboard Analytics APIs
    path('plant-wise-total/', plant_wise_total, name='plant_wise_total'),
    path('monthly-summary/', monthly_summary, name='monthly_summary'),
    path('machine-wise/', machine_wise, name='machine_wise'),
    path('date-range/', date_range, name='date_range'),
    path('realtime-dashboard/', realtime_dashboard, name='realtime_dashboard'),
    path('machine-analysis/', machine_analysis, name='machine_analysis'),
    
    # Live Machine APIs
    path('live-machines/', views.live_machines, name='live_machines'),
    path('count52-live/', views.count52_live, name='count52_live'),
    path('plant2-raw/', views.plant2_raw, name='plant2_raw'),
    path('plant2-live/', views.plant2_live, name='plant2_live'),
    path('plant1-live/', views.plant1_live, name='plant1_live'),
    path('machine-history/', get_machine_history, name='machine_history'),
    

    path('department-stats/', get_department_stats, name='department_stats'),
    # Data Saving
    path('save-hourly-snapshot/', views.save_hourly_snapshot, name='save_hourly_snapshot'),
    path('machine-changes/', views.get_machine_changes_from_db, name='machine_changes_db'),
    path('exact-plant2/', views.exact_plant2_data, name='exact_plant2_data'),
    
    path('plant2/hourly-idle/', views.plant2_hourly_idle, name='plant2-hourly-idle'),
    path('plant2/hourly-idle/summary/', views.plant2_hourly_idle_summary, name='plant2-hourly-idle-summary'),
    path('bulk-insert-master-data/', views.bulk_insert_parts, name='bulk_insert_parts'),
    
    path('customers/', views.get_unique_customers, name='get_customers'),
    path('parts/<str:customer_name>/', views.get_parts_by_customer, name='get_parts'),
    path('qa-notifications/<str:username>/', GetQANotificationsView.as_view(), name='api_get_qa_notifications'),
    path('log-report/', views.SaveReportLogView.as_view(), name='api_log_report'),
    path('profile/me/', CurrentUserProfileView.as_view(), name='current-user-profile'),
]

