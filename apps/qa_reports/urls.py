from django.urls import path
from . import views

urlpatterns = [
    #  Master Data (Dropdowns & Auto-fill) -> YE MISSING THE
    path('master-dropdown/', views.MasterDropdownView.as_view(), name='master_dropdown'),
    path('master-parameters/', views.MasterParametersView.as_view(), name='master_parameters'),

    #  Daily Reports / Forms
    path('incoming-material-inspection/save/', views.SaveIncomingMaterialInspectionView.as_view(), name='save-incoming-material-inspection'),
    path('redbin-analysis/save/', views.SaveRedBinAnalysisView.as_view(), name='save-redbin-analysis'),
    path('redbin-attendance/save/', views.SaveRedBinAttendanceView.as_view(), name='save-redbin-attendance'),
    path('scrap-note/save/', views.SaveScrapNoteView.as_view(), name='save-scrap-note'),
    path('save-deviation/', views.SaveDeviationApprovalView.as_view(), name='save_deviation'),
    path('good-receipt/create/', views.SaveGoodReceiptView.as_view(), name='create_good_receipt'),
    path('save-inspection-report/', views.SaveInspectionReportView.as_view(), name='save_inspection_report'),
    path('get-inspection-report/', views.GetInspectionReportView.as_view(), name='get_inspection_report'),

    #  QA Monthly
    path('save-process-audit/', views.SaveProcessAuditView.as_view(), name='save_process_audit'),
    path('coherence/', views.SaveCoherenceChecklistView.as_view(), name='save_coherence'),
    path('layout-inspection/', views.SaveLayoutInspectionView.as_view(), name='save_layout_inspection'),
    path('product-audit-plan/', views.SaveProductAuditPlanView.as_view(), name='save_product_audit'),
    path('customer-complaint/', views.SaveCustomerComplaintView.as_view(), name='save_customer_complaint'),
    path('customer-satisfaction/', views.SaveCustomerSatisfactionView.as_view(), name='save_customer_satisfaction'),
    path('warranty-claim/', views.SaveWarrantyClaimView.as_view(), name='save_warranty_claim'),
    path('mom/', views.SaveMinutesOfMeetingView.as_view(), name='save_mom'),

    #  Main Data Fetch View -> YE SABSE ZAROORI HAI TABLE DIKHANE KE LIYE
    path('qa-data/<str:form_key>/', views.qa_data_view, name='qa_data_view'),
]