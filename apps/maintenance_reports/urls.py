from django.urls import path
from . import views

urlpatterns = [
    # Machine Maintenance
    path('machine-critical-spare/save/', views.SaveMachineCriticalSpareView.as_view(), name='save_machine_critical_spare'),
    path('machine-breakdown-summary/save/', views.SaveMachineBreakdownSummaryView.as_view(), name='save_machine_breakdown_summary'), # Fixed Duplicate URL
    path('checksheets/daily-power-press/', views.SaveDailyPowerPressView.as_view(), name='save_daily_power_press'),
    path('machine-history/save/', views.SaveMachineHistoryCardView.as_view(), name='save_machine_history_card'),
    path('machine-breakdown-slip/save/', views.SaveMachineBreakdownView.as_view(), name='save_machine_breakdown_slip'), # Fixed Duplicate URL
    
    #machine maintenance weekly 
    path('spot-welding-maintenance/save/', views.SaveSpotWeldingMaintenanceView.as_view(), name='save_spot_welding_maintenance'),
    path('compressor-maintenance/save/', views.SaveCompressorMaintenanceView.as_view(), name='save_compressor_maintenance'),
    path('lathe-machine-maintenance/save/', views.SaveLatheMachineMaintenanceView.as_view(), name='save_lathe_machine_maintenance'),
    path('vertical-drill-maintenance/save/', views.SaveVerticalDrillMachineMaintenanceView.as_view(), name='save_vertical_drill_maintenance'),
    path('surface-grinder-maintenance/save/', views.SaveSurfaceGrinderMaintenanceView.as_view(), name='save_surface_grinder_maintenance'),
    path('tig-welding-maintenance/save/', views.SaveTigWeldingMaintenanceView.as_view(), name='save_tig_welding_maintenance'),
    path('base-grinder-maintenance/save/', views.SaveBaseGrinderMaintenanceView.as_view(), name='save_base_grinder_maintenance'),
    path('belt-grinder-maintenance/save/', views.SaveBeltGrinderMaintenanceView.as_view(), name='save_belt_grinder_maintenance'),
    path('pipe-cutting-maintenance/save/', views.SavePipeCuttingMaintenanceView.as_view(), name='save_pipe_cutting_maintenance'),
    path('vibra-maintenance/save/', views.SaveVibraMaintenanceView.as_view(), name='save_vibra_maintenance'),
    path('dip-molding-maintenance/save/', views.SaveDipMoldingMaintenanceView.as_view(), name='save_dip_molding_maintenance'),
    path('servo-press-maintenance/save/', views.SaveServoPressMaintenanceView.as_view(), name='save_servo_press_maintenance'),
    path('machine-preventive-maintenance/save/', views.SaveMachinePreventiveMaintenanceView.as_view(), name='save_machine_preventive_maintenance'),
    path('cnc-maintenance/save/', views.SaveCNCMaintenanceReportView.as_view(), name='save_cnc_maintenance_report'),
    path('vertical-milling-checksheet/save/', views.SaveVerticalMillingMachineCheckSheetView.as_view(), name='save_vertical_milling_checksheet'),
    path('projection-welding-pm/save/', views.SaveProjectionWeldingPMCheckSheetView.as_view(), name='save_projection_welding_pm_checksheet'),
    path('power-press-pm/save/', views.SavePowerPressPMCheckSheetView.as_view(), name='save_power_press_pm_checksheet'),
    path('hydraulic-pm/save/', views.SaveHydraulicPMCheckSheetView.as_view(), name='save_hydraulic_pm_checksheet'),
    # Tool Maintenance
    path('tool-critical-spare/save/', views.SaveToolCriticalSpareView.as_view(), name='save_tool_critical_spare'),
    path('tool-breakdown/save/', views.SaveToolBreakdownSummaryView.as_view(), name='save_tool_breakdown'),
    path('tool-history/save/', views.SaveToolHistoryView.as_view(), name='save_tool_history'),
    path('tool-pm/save/', views.SaveToolPreventiveMaintenanceView.as_view(), name='save_tool_pm'),
    path('tool-breakdown-slip/save/', views.SaveToolBreakdownSlipView.as_view(), name='save_tool_breakdown_slip'),
   
    path('fixture-maintenance/save/', views.SaveFixtureMaintenanceView.as_view(), name='save_fixture_maintenance'),

    # Maintenance Data Fetch API
    path('maintenance-data/<str:form_key>/', views.maintenance_data_view, name='maintenance_data_view'),
]