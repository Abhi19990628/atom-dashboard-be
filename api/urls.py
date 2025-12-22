# from django.urls import path
# from . import views


# urlpatterns = [
#     path('dashboard/', views.get_dashboard_data, name='get_dashboard_data'),
#     path('available-dates/', views.get_available_dates, name='get_available_dates'),
#     path('dashboard-tables/', views.get_assignment_idle_data, name='get_assignment_idle_data'),
#     path('assignments/', views.create_assignment, name='create_assignment'),
#     path('machines/<str:machine_no>/auto-fill/', views.get_auto_fill_data, name='get_auto_fill_data'),
#     path('idle-reports/', views.create_idle_report, name='create_idle_report'),
    
#     # Your existing URLs...
#     # path('hourly-production/', views.hourly_production_data, name='hourly_production_data'),
#     path('machine-production/', views.machine_production_data, name='machine_production_data'), 
#     path('production-line-status/', views.production_line_status_data, name='production_line_status_data'),
#     path('test-direct-query/', views.test_direct_query, name='test_direct_query'),


#     # ... rest of your URLs


    
#     # Your existing URLs
#     path('live-machines/', views.live_machines, name='live_machines'),
#     path('count52-live/', views.count52_live, name='count52_live'),
#     path('plant2-raw/', views.plant2_raw, name='plant2_raw'),
#     path('plant2-live/', views.plant2_live, name='plant2_live'),
#     path('save-hourly-snapshot/', views.save_hourly_snapshot, name='save_hourly_snapshot'),

   
   
#     path('machine-changes/', views.get_machine_changes_from_db, name='machine_changes_db'),




#     # path('enhanced-plant2/', views.enhanced_plant2_data, name='enhanced_plant2_data'),
#     # path('correct-plant2/', views.correct_plant2_data, name='correct_plant2_data'),
#     path('exact-plant2/', views.exact_plant2_data, name='exact_plant2_data'),
#     path('plant1-live/', views.plant1_live, name='plant1_live'),
#     # path('debug-machine-state/', views.debug_machine_state_all, name='debug_machine_state'),
# ]


# backend/api/urls.py
from django.urls import path
from . import views

urlpatterns = [
    # Dashboard APIs
    path('dashboard/', views.get_dashboard_data, name='get_dashboard_data'),
    path('available-dates/', views.get_available_dates, name='get_available_dates'),
    path('dashboard-tables/', views.get_assignment_idle_data, name='get_assignment_idle_data'),
    
    # ✅ NEW OPERATOR ASSIGNMENT APIs
    path('operators/', views.get_operators_by_plant, name='get_operators'),
    path('operators/add/', views.add_operator, name='add_operator'),
    path('machines/list/', views.get_machines_by_plant, name='get_machines'),
    path('assignment/save/', views.save_operator_assignment, name='save_operator_assignment'),
    path('assignments/list/', views.get_operator_assignments, name='get_operator_assignments'),
    
    # Old Assignment APIs (Keep existing)
    path('assignments/', views.create_assignment, name='create_assignment'),
    path('machines/<str:machine_no>/auto-fill/', views.get_auto_fill_data, name='get_auto_fill_data'),
    path('idle-reports/', views.create_idle_report, name='create_idle_report'),
    
    # Production APIs
    path('machine-production/', views.machine_production_data, name='machine_production_data'), 
    path('production-line-status/', views.production_line_status_data, name='production_line_status_data'),
    path('test-direct-query/', views.test_direct_query, name='test_direct_query'),
    
    # Live Machine APIs
    path('live-machines/', views.live_machines, name='live_machines'),
    path('count52-live/', views.count52_live, name='count52_live'),
    path('plant2-raw/', views.plant2_raw, name='plant2_raw'),
    path('plant2-live/', views.plant2_live, name='plant2_live'),
    path('plant1-live/', views.plant1_live, name='plant1_live'),
    
    # Data Saving
    path('save-hourly-snapshot/', views.save_hourly_snapshot, name='save_hourly_snapshot'),
    path('machine-changes/', views.get_machine_changes_from_db, name='machine_changes_db'),
    path('exact-plant2/', views.exact_plant2_data, name='exact_plant2_data'),
    
    path('plant2/hourly-idle/', views.plant2_hourly_idle, name='plant2-hourly-idle'),
    path('plant2/hourly-idle/summary/', views.plant2_hourly_idle_summary, name='plant2-hourly-idle-summary'),
]
