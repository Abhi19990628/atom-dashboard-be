from django.urls import path
from . import views

urlpatterns = [
    # Machine Maintenance
    path('machine-critical-spare/save/', views.SaveMachineCriticalSpareView.as_view(), name='save_machine_critical_spare'),
    path('machine-breakdown-summary/save/', views.SaveMachineBreakdownSummaryView.as_view(), name='save_machine_breakdown_summary'), # Fixed Duplicate URL
    path('checksheets/daily-power-press/', views.SaveDailyPowerPressView.as_view(), name='save_daily_power_press'),
    path('machine-history/save/', views.SaveMachineHistoryCardView.as_view(), name='save_machine_history_card'),
    path('machine-breakdown-slip/save/', views.SaveMachineBreakdownView.as_view(), name='save_machine_breakdown_slip'), # Fixed Duplicate URL
    
    # Tool Maintenance
    path('tool-critical-spare/save/', views.SaveToolCriticalSpareView.as_view(), name='save_tool_critical_spare'),
    path('tool-breakdown/save/', views.SaveToolBreakdownSummaryView.as_view(), name='save_tool_breakdown'),
    path('tool-history/save/', views.SaveToolHistoryView.as_view(), name='save_tool_history'),
    path('tool-pm/save/', views.SaveToolPreventiveMaintenanceView.as_view(), name='save_tool_pm'),
    
    # Maintenance Data Fetch API
    path('maintenance-data/<str:form_key>/', views.maintenance_data_view, name='maintenance_data_view'),
]