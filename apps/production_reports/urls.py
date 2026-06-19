# apps/production_reports/urls.py
from django.urls import path
from . import views

urlpatterns = [
    # Production Daily URLs
    path('save-checksheet/', views.SaveMachineChecksheetView.as_view(), name='save_checksheet'),
    path('save-tip-data/', views.SaveTipChangeView.as_view(), name='save-tip-data'),
    path('rework/save/', views.SaveReworkReportView.as_view(), name='save-rework'),
    path('check-5s-status/', views.CheckFiveSStatusView.as_view(), name='check-5s-status'),
    path('5s-checksheet/save/', views.SaveFiveSReportView.as_view(), name='save-5s-checksheet'),
    path('bin-trolley/save/', views.SaveBinTrolleyReportView.as_view(), name='save-bin-trolley'),
    path('save-daily-production/', views.SaveDailyProductionPlanView.as_view(), name='save-daily-production'),
    path('get-today-production-data/', views.get_today_production_data, name='get_today_production_data'),
    path('save-4m-change/', views.SaveFourMChangeInspectionView.as_view(), name='save-4m-change'),
    path('save-4m-record/', views.SaveFourMChangeRecordView.as_view(), name='save-4m-record'),
    path('save-4m-display/', views.SaveFourMDisplayView.as_view(), name='save_4m_display'),
    path('save-4m-summary/', views.SaveFourMSummaryView.as_view(), name='save-4m-summary'),

    # Update API
    path('update-daily-production/<int:pk>/', views.UpdateDailyProductionPlanView.as_view(), name='update_daily_production'),

    # Production Monthly URLs
    path('monthly-prod-plan/save/', views.SaveMonthlyProdPlanView.as_view(), name='save_monthly_prod_plan'),
    path('operator-observance-checklist/save/', views.SaveOperatorObservanceChecklistView.as_view(), name='save_operator_observance_checklist'),
    path('operator-observance-plan/save/', views.SaveOperatorObservancePlanView.as_view(), name='save_operator_observance_plan'),
    path('pm-checklist-mhe/save/', views.SavePMChecklistMHEView.as_view(), name='save_pm_checklist_mhe'),
    path('save-projection-welder/', views.SaveProjectionWelderView.as_view(), name='save_projection_welder'),
    path('save-spot-welder/', views.SaveSpotWelderView.as_view(), name='save_spot_welder'),
    path('save-tig-mig-welder/', views.SaveTigMigWelderView.as_view(), name='save_tig_mig_welder'),
    path('save-process-validation/', views.SaveProcessValidationView.as_view(), name='save_process_validation'),

    # Production Data Fetch View
    path('get-single-production-report/<str:form_key>/<int:report_id>/', views.get_single_production_report_view, name='get-single-production-report'),

    #  Main Data Fetch View -> YE SABSE ZAROORI HAI TABLE DIKHANE KE LIYE
    path('production-data/<str:form_key>/', views.production_data_view, name='production_data_view'),

]