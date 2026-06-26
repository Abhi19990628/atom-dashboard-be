import json
import traceback
from datetime import datetime
import pytz

from django.db import connection, transaction
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.utils import timezone
from django.utils.timezone import localtime
from django.contrib.auth.models import User

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view

# ==============================================================================
# IMPORTS FROM MAIN API APP
# ==============================================================================
from api.models import (
    ReportTrackHistory, RedBinAnalysisReport, RedBinAttendance, ScrapNoteEntry, 
    DeviationApproval, GoodReceiptEntry, InspectionReport, ProcessAuditChecksheet, 
    CoherenceChecklist, LayoutInspection, ProductAuditPlan, CustomerComplaint, 
    CustomerSatisfaction, WarrantyClaim, MinutesOfMeeting, ReworkEntry,
    L1_PartInfoMaster, L2_ProcessReportMaster, L3_ParameterDetailMaster,IncomingMaterialInspection,
    ReportActivityLog, QANotification
)

from api.serializers import (
    InspectionReportSerializer, ProcessAuditChecksheetSerializer, CoherenceChecklistSerializer,
    LayoutInspectionSerializer, ProductAuditPlanSerializer, CustomerComplaintSerializer,
    CustomerSatisfactionSerializer, WarrantyClaimSerializer, MinutesOfMeetingSerializer,IncomingMaterialInspectionSerializer
)

# ==============================================================================
# HELPER FUNCTIONS & BASE CLASSES
# ==============================================================================
def clean_val(val, default=None):
    return val if val != '' and val is not None else default

def parse_date(date_str):
    if not date_str:
        return None
    if '.' in date_str:
        try:
            return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return date_str

def clean_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val, '%d-%m-%Y').strftime('%Y-%m-%d')
    except ValueError:
        return val      



# ==============================================================================
# ✅ QA HUB ROUTE MAPPING + AUTO ACTIVITY LOG & NOTIFICATION ROUTER
# =================================================================================================

def normalize_report_name(value):
    return str(value or "").strip().lower()


QA_REPORT_ROUTE_MAP = {
    normalize_report_name("Deviation Approval Form"): {
        "form_key": "deviation",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Redbin Approval Form"): {
        "form_key": "redbin",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Red Bin Attendance"): {
        "form_key": "redbin-attendance",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Incoming Inspection"): {
        "form_key": "incoming",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Incoming Material Inspection"): {
        "form_key": "incoming",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Inspection Report"): {
        "form_key": "inspection",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Inspection"): {
        "form_key": "inspection",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Scrap Note"): {
        "form_key": "scrap",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Poka Yoke"): {
        "form_key": "poka-yoke",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("PDI"): {
        "form_key": "pdi",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("QA Rework Report"): {
        "form_key": "rework",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Sample Inspection"): {
        "form_key": "sample-inspection",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Good Receipt"): {
        "form_key": "good-receipt",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Material Requisition Slip"): {
        "form_key": "good-receipt",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("RM Quality"): {
        "form_key": "rm-quality-plan",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Raw Material"): {
        "form_key": "rm-quality-plan",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Process Audit"): {
        "form_key": "process-audit",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Coherence"): {
        "form_key": "coherence",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Coherence Checklist"): {
        "form_key": "coherence",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Layout Inspection"): {
        "form_key": "layout-inspection",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Product Audit"): {
        "form_key": "product-audit-plan",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Product Audit Plan"): {
        "form_key": "product-audit-plan",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Customer Complaint"): {
        "form_key": "customer-complaint",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Customer Satisfaction"): {
        "form_key": "customer-satisfaction",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Warranty Claim"): {
        "form_key": "warranty-claim",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("MOM"): {
        "form_key": "mom",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Meeting"): {
        "form_key": "mom",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
}


DEFAULT_QA_ROUTE_CONFIG = {
    "form_key": "inspection",
    "hub": "qa-hub",
    "target_group": "Quality_Approvers",
}


def get_qa_route_config(report_name):
    return QA_REPORT_ROUTE_MAP.get(
        normalize_report_name(report_name),
        DEFAULT_QA_ROUTE_CONFIG,
    )


def auto_log_report(username, report_name, record_id, form_key=None, hub=None, target_group=None):
    if not username:
        username = "Unknown User"

    try:
        route_config = get_qa_route_config(report_name)

        final_form_key = form_key or route_config["form_key"]
        final_hub = hub or route_config["hub"]
        final_target_group = target_group or route_config["target_group"]

        user_obj = User.objects.filter(username=username).first()

        dept_name = final_hub
        submitter_location_code = ""

        if user_obj:
            profile = getattr(user_obj, "userprofile", getattr(user_obj, "profile", None))

            if profile:
                loc = str(getattr(profile, "location", "") or "").strip()
                dept = str(getattr(profile, "department", "") or "").strip()

                if loc or dept:
                    dept_name = f"{loc} ({dept})".strip()

                submitter_location_code = loc.replace(" ", "").lower()

        log = ReportActivityLog.objects.create(
            username=username,
            department_name=dept_name,
            report_name=report_name,
            record_id=record_id,
            form_key=final_form_key,
            hub=final_hub,
        )

        approvers = User.objects.filter(groups__name=final_target_group)

        date_str = timezone.localtime().strftime("%d-%b-%Y")
        time_str = timezone.localtime().strftime("%I:%M %p")
        msg = f"{username} submitted {report_name} on {date_str} at {time_str}."

        for approver in approvers:
            approver_profile = getattr(
                approver,
                "userprofile",
                getattr(approver, "profile", None),
            )

            # If both submitter and approver have location, send only same plant/location.
            # If submitter has no location, notification will go to all Quality_Approvers.
            if submitter_location_code and approver_profile and getattr(approver_profile, "location", None):
                approver_location_code = str(
                    approver_profile.location
                ).strip().replace(" ", "").lower()

                if submitter_location_code != approver_location_code:
                    continue

            QANotification.objects.create(
                user=approver,
                message=msg,
                report_log=log,
            )

        return log

    except Exception as e:
        print(f"🔥 QA Auto Log Failed for {report_name}: {str(e)}")
        traceback.print_exc()
        return None

class TrackedAPIView(APIView):
    report_name = "General Report"
    def finalize_response(self, request, response, *args, **kwargs):
        if response.status_code in [200, 201] and request.method in ['POST', 'PATCH', 'PUT']:
            try:
                username = 'Unknown'
                department = 'Unknown'
                if request.user and request.user.is_authenticated:
                    username = request.user.username
                    if request.user.groups.exists():
                        department = request.user.groups.first().name
                    else:
                        department = 'Default_User'
                else:
                    if hasattr(request, 'data') and isinstance(request.data, dict):
                        username = request.data.get('username', 'Unknown')
                        department = request.data.get('department', 'Unknown')
                ReportTrackHistory.objects.create(username=username, department=department, report_name=self.report_name)
            except Exception as e:
                print("⚠️ Tracking Error:", e)
        return super().finalize_response(request, response, *args, **kwargs)
    
# ==================================================
# 🟢 1. DROPDOWN & MASTER APIs
# ==================================================
class MasterDropdownView(APIView):
    def get(self, request):
        filter_type = request.query_params.get('filter') 
        if filter_type == 'customer':
            data = L1_PartInfoMaster.objects.values_list('customer_name', flat=True).distinct()
            return Response(list(data))
        elif filter_type == 'all_parts':
            data = L1_PartInfoMaster.objects.values_list('part_name','part_no').distinct()
            return Response(list(data))
        elif filter_type == 'operations_by_part':
            part = request.query_params.get('part')
            ops = L2_ProcessReportMaster.objects.filter(part_info__part_name=part).values_list('report_name', flat=True).distinct()
            return Response(list(ops))
        elif filter_type == 'part':
            cust = request.query_params.get('cust')
            data = L1_PartInfoMaster.objects.filter(customer_name=cust).values_list('part_name', flat=True).distinct()
            return Response(list(data))
        elif filter_type == 'operation':
            cust = request.query_params.get('cust')
            part = request.query_params.get('part')
            ops = L2_ProcessReportMaster.objects.filter(part_info__customer_name=cust, part_info__part_name=part).values_list('report_name', flat=True).distinct()
            return Response(list(ops))
        elif filter_type == 'part_no':
            part = request.query_params.get('part')
            data = L1_PartInfoMaster.objects.filter(part_name=part).values_list('part_no', flat=True).distinct()
            return Response(list(data))
        elif filter_type == 'model_by_part':
            part = request.query_params.get('part')
            data = L1_PartInfoMaster.objects.filter(part_name=part).values_list('model_name', flat=True).distinct()
            return Response(list(data))
        elif filter_type == 'method':
            data = L3_ParameterDetailMaster.objects.values_list('instrument', flat=True).distinct()
            clean_data = sorted(list(set([str(x).strip() for x in data if x and str(x).strip()])))
            return Response(clean_data)
        elif filter_type == 'parameter':
            data = L3_ParameterDetailMaster.objects.values_list('parameter_name', flat=True).distinct()
            clean_data = sorted(list(set([str(x).strip() for x in data if x and str(x).strip()])))
            return Response(clean_data)
        elif filter_type == 'spec':
            try:
                data = L3_ParameterDetailMaster.objects.values_list('specification', flat=True).distinct()
                clean_data = sorted(list(set([str(x).strip() for x in data if x and str(x).strip()])))
                return Response(clean_data)
            except Exception as e:
                return Response([])

class MasterParametersView(APIView):
    def get(self, request):
        cust = request.query_params.get('customer')
        part = request.query_params.get('part')
        op_name = request.query_params.get('operation')
        if not all([cust, part, op_name]): return Response({"error": "Missing filters"}, status=400)
        process = L2_ProcessReportMaster.objects.filter(part_info__customer_name=cust, part_info__part_name=part, report_name=op_name).first()
        if not process: return Response({"error": "Process Not Found in Master Data"}, status=404)

        params = L3_ParameterDetailMaster.objects.filter(process_report=process).order_by('id')
        product_list, process_list, prod_sr, proc_sr = [], [], 1, 11

        for p in params:
            raw_spec = p.specification or ""
            final_spec, final_tol = raw_spec, "-"
            if "±" in raw_spec:
                parts = raw_spec.split("±", 1)
                final_spec = parts[0].strip()          
                final_tol = "± " + parts[1].strip()    
            elif "+" in raw_spec:
                parts = raw_spec.split("+", 1)
                final_spec = parts[0].strip()
                final_tol = "+" + parts[1].strip()

            item_data = {"item": p.parameter_name, "spec": final_spec, "tol": final_tol, "instr": p.instrument, "category": p.category}
            if p.category == 'PRODUCT':
                item_data['sr_no'] = prod_sr
                product_list.append(item_data)
                prod_sr += 1
            else:
                item_data['sr_no'] = proc_sr
                process_list.append(item_data)
                proc_sr += 1

        return Response({
            "productItems": product_list, "processItems": process_list,
            "part_number": process.part_info.part_no, "model_name": process.part_info.model_name
        })

# =========================================================
# 📝 QA HUB SAVE APIs (All return record_id now)
# =========================================================

class SaveRedBinAnalysisView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            items = data.get('items', []) if 'items' in data else [data]
            def format_date(d):
                if not d: return None
                try: return datetime.strptime(d, "%d-%m-%Y").strftime("%Y-%m-%d")
                except ValueError: return d 
            
            current_time = datetime.now()
            formatted_time = current_time.strftime("%Y-%m-%d %I:%M %p").lower()
            
            entries_to_create = []
            for row in items:
                entries_to_create.append(
                    RedBinAnalysisReport(
                        entry_date=format_date(row.get('entry_date')) or current_time.date(),
                        part_name_model=row.get('part_name_model', '') or row.get('part_name', ''),
                        operation=row.get('operation', ''),
                        total_rej_qty=int(row.get('total_rej_qty') or 0),
                        defect_detail=row.get('defect_detail', ''),
                        root_cause_reason=row.get('root_cause_reason', ''),
                        action_taken=row.get('action_taken', ''),
                        responsible_person=row.get('responsible_person', ''),
                        target_date=format_date(row.get('target_date')),          
                        completion_date=format_date(row.get('completion_date')),  
                        created_time=formatted_time 
                    )
                )
            if entries_to_create:
                RedBinAnalysisReport.objects.bulk_create(entries_to_create)
            
            last_record = RedBinAnalysisReport.objects.last()
            return Response({"success": True, "message": "Saved EXACTLY as AM/PM!", "record_id": last_record.id if last_record else None}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveRedBinAttendanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            entries_to_create = []
            for item in data:
                entries_to_create.append(
                    RedBinAttendance(
                        date=item.get('date'), month=item.get('month'), year=int(item.get('year')),
                        employee_name=item.get('employee_name', ''), designation=item.get('designation', ''), status=item.get('status', '') 
                    )
                )
            if entries_to_create: RedBinAttendance.objects.bulk_create(entries_to_create)
            last_record = RedBinAttendance.objects.last()
            return Response({"success": True, "message": "Attendance Saved!", "record_id": last_record.id if last_record else None}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

class SaveScrapNoteView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            items = data.get('items', []) if 'items' in data else [data]
            entries_to_create = []
            for row in items:
                entries_to_create.append(
                    ScrapNoteEntry(
                        entry_date=row.get('entry_date'), part_name=row.get('part_name', ''), part_no=row.get('part_no', ''),
                        defect_detail=row.get('defect_detail', ''), quantity=int(row.get('quantity') or 0), remarks=row.get('remarks', '')
                    )
                )
            if entries_to_create: ScrapNoteEntry.objects.bulk_create(entries_to_create)
            last_record = ScrapNoteEntry.objects.last()
            return Response({"success": True, "message": "Scrap Note Saved!", "record_id": last_record.id if last_record else None}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveDeviationApprovalView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            report = DeviationApproval.objects.create(
                tool_name_no=data.get('tool_name_no', ''), location=data.get('location', ''), problem=data.get('problem', ''),
                reason_for_deviation=data.get('reason_for_deviation', ''), date=data.get('date'), duration=data.get('duration', ''),
                prod_incharge=data.get('prod_incharge', ''), qa_incharge=data.get('qa_incharge', ''), remarks=data.get('remarks', '')
            )
            return Response({"success": True, "message": "Deviation Approval Data Saved!", "record_id": report.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveGoodReceiptView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            report = GoodReceiptEntry.objects.create(
                requested_by=data.get('requestedBy', ''), item_name=data.get('itemName', ''), specification=data.get('specification', ''),
                department=data.get('department', ''), qty=data.get('qty', ''), remark=data.get('remark', ''),
                received_by=data.get('receivedBy', ''), received_date=data.get('receivedDate')
            )
            return Response({"success": True, "message": "Material Requisition Slip Saved!", "record_id": report.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveInspectionReportView(APIView):
    def post(self, request):
        try:
            data = request.data
            master = data.get('master_data', {})
            logs = data.get('logs', [])
            
            date_val = master.get('date') or timezone.now().date()
            report, created = InspectionReport.objects.get_or_create(
                customer_account=master.get('customer', 'Unknown'), part_name=master.get('part_name', 'Unknown'), operation=master.get('operation', 'Unknown'), inspection_date=date_val,
                defaults={
                    'part_number': master.get('part_number', 'N/A'), 'plant_location': master.get('plant_location', 'PLANT 1'),
                    'operator_name': logs[-1].get('operator', 'Unknown') if logs else 'Unknown', 'machine_number': logs[-1].get('machine', 'N/A') if logs else 'N/A',
                    'inspection_data': {}
                }
            )
            report.operator_name = logs[-1].get('operator', 'Unknown') if logs else 'Unknown'
            report.machine_number = logs[-1].get('machine', 'N/A') if logs else 'N/A'
            report.inspection_data = {"parameters": data.get('parameters', []), "logs": logs}
            report.save()

            msg = "New Report Created!" if created else "Report Updated!"
            return Response({"message": msg, "report_id": report.id, "record_id": report.id}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveProcessAuditView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            mapped_data = {
                'part_name_no': f"{raw_data.get('part_name', '')} - {raw_data.get('part_no', '')}", 'machine_model': raw_data.get('model_name', ''),
                'date': clean_val(raw_data.get('audit_date')), 'auditor': raw_data.get('auditor_name', ''),
                'auditee': raw_data.get('auditee_name', ''), 'audit_details': raw_data.get('audit_details', [])
            }
            serializer = ProcessAuditChecksheetSerializer(data=mapped_data)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Process Audit Saved!", "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveCoherenceChecklistView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            mapped_data = {
                'part_name': raw_data.get('partName', ''), 'part_no': raw_data.get('partNo', ''), 'date': clean_val(raw_data.get('date')),
                'model_name': raw_data.get('model', ''), 'prepared_by': raw_data.get('preparedBy', ''), 'verified_by': raw_data.get('verifiedBy', ''),
                'operations': raw_data.get('operations', [])
            }
            serializer = CoherenceChecklistSerializer(data=mapped_data)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Coherence Checklist Saved!", "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveLayoutInspectionView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            mapped_data = {
                'part_name': raw_data.get('partName', ''), 'part_no': raw_data.get('partNo', ''), 'model_name': raw_data.get('model', ''),
                'customer_name': raw_data.get('customer', ''), 'date': clean_val(raw_data.get('date')), 'sample_size': str(raw_data.get('sampleSize', '')),
                'prepared_by': raw_data.get('preparedBy', ''), 'verified_by': raw_data.get('verifiedBy', ''), 'inspections': raw_data.get('inspections', [])
            }
            serializer = LayoutInspectionSerializer(data=mapped_data)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Layout Inspection Saved!", "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveProductAuditPlanView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            mapped_data = {
                'doc_no': raw_data.get('doc_no', ''), 'rev_no': raw_data.get('rev_no', ''), 'date': clean_val(raw_data.get('date')),
                'plan_year': raw_data.get('plan_year', ''), 'prepared_by': raw_data.get('prepared_by', ''),  'approved_by': raw_data.get('approved_by', ''),
                'audit_rows': raw_data.get('rows', [])
            }
            serializer = ProductAuditPlanSerializer(data=mapped_data)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Product Audit Plan Saved!", "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveCustomerComplaintView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            mapped_data = {
                'date': clean_val(raw_data.get('date')), 'part_details': raw_data.get('part_details', ''), 'model_name': raw_data.get('model_name', ''),              
                'customer_name': raw_data.get('customer_name', ''), 'problem_description': raw_data.get('problem_description', ''), 
                'counter_measure': raw_data.get('counter_measure', ''), 'target_date': clean_val(raw_data.get('target_date')),      
                'horizontal_action': raw_data.get('horizontal_action', ''), 'status': raw_data.get('status', 'OPEN')
            }
            serializer = CustomerComplaintSerializer(data=mapped_data)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Customer Complaint Saved!", "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveCustomerSatisfactionView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            mapped_data = {
                'customer_name': raw_data.get('customerName', ''), 'month_year': raw_data.get('monthYear', ''),
                'performance_indicators': {
                    'line_complaints': raw_data.get('lineComplaints', ''), 'warranty_complaints': raw_data.get('warrantyComplaints', ''),
                    'premium_freight_incidents': raw_data.get('premiumFreightIncidents', ''), 'line_stoppage_quality': raw_data.get('lineStoppageQuality', ''),
                    'line_stoppage_supply': raw_data.get('lineStoppageSupply', ''), 'premium_fight_incident': raw_data.get('premiumFightIncident', ''),
                    'schedule_vs_dispatch': raw_data.get('scheduleVsDispatch', ''), 'customer_audit_score': raw_data.get('customerAuditScore', '')
                }
            }
            serializer = CustomerSatisfactionSerializer(data=mapped_data)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Customer Satisfaction Saved!", "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveWarrantyClaimView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            mapped_data = {
                'date': raw_data.get('date') or None, 'customer_name': raw_data.get('customerName', ''), 'part_details': raw_data.get('partDetails', ''),
                'claim_qty': raw_data.get('claimQty', ''), 'warranty_defect': raw_data.get('warrantyDefect', ''), 'decision': raw_data.get('decision', 'PENDING'),
                'rejection_root_cause': raw_data.get('rejectionRootCause', ''), 'disposal_action': raw_data.get('disposalAction', ''), 'capa_analysis': raw_data.get('capaAnalysis', '')
            }
            serializer = WarrantyClaimSerializer(data=mapped_data)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Warranty Claim Saved!", "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveMinutesOfMeetingView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            mapped_data = {
                'date': raw_data.get('date') or None, 'time': raw_data.get('time') or None, 'subject': raw_data.get('subject', ''),
                'aot_members': raw_data.get('aotMembers', ''), 'supplier_members': raw_data.get('supplierMembers', ''),
                'discussions': [{
                        'sr_no': 1, 'part_name_no': raw_data.get('partDetails', ''), 'defects_problem_details': raw_data.get('problemDetails', ''),
                        'action_plan': raw_data.get('actionPlan', ''), 'responsibility': raw_data.get('responsibility', ''), 'target_date': raw_data.get('targetDate', ''),
                        'follow_up_comments': raw_data.get('followUpComment', ''), 'status_remark': raw_data.get('statusRemark', '')
                }]
            }
            serializer = MinutesOfMeetingSerializer(data=mapped_data)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "MOM Saved Successfully!", "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveIncomingMaterialInspectionView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            raw_part_no = raw_data.get('part_no', '')
            final_part_no = None if str(raw_part_no).strip() == '' else raw_part_no
            
            mapped_data = {
                'supplier': raw_data.get('supplier', 'ATOMONE TECHNOLOGIES PVT.LTD'), 'customer': raw_data.get('customer', ''), 'part_name': raw_data.get('part_name', ''),
                'part_no': final_part_no, 'date': raw_data.get('date'), 'grade': raw_data.get('grade', ''), 'mtc': raw_data.get('mtc', ''),
                'ga_nga': raw_data.get('ga_nga', ''), 'coil_no': raw_data.get('coil_no', ''), 'invoice_no': raw_data.get('invoice_no', ''),
                'qty': str(raw_data.get('qty', '')), 'inspection_data': raw_data.get('inspection_data', []), 'prepared_by': raw_data.get('prepared_by', ''),
                'checked_by': raw_data.get('checked_by', ''), 'approved_by': raw_data.get('approved_by', '')
            }
            serializer = IncomingMaterialInspectionSerializer(data=mapped_data)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Incoming Material Inspection Saved!", "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class GetInspectionReportView(APIView):
    def get(self, request):
        customer = request.query_params.get('customer', None)
        part_name = request.query_params.get('part_name', None)
        operation = request.query_params.get('operation', None)
        date = request.query_params.get('date', None)

        filters = {}
        if customer: filters['customer_account__icontains'] = customer
        if part_name: filters['part_name__icontains'] = part_name
        if operation: filters['operation__icontains'] = operation
        if date: filters['inspection_date'] = date

        reports = InspectionReport.objects.filter(**filters).order_by('-id')

        if reports.exists():
             serializer = InspectionReportSerializer(reports.first())
             return Response(serializer.data, status=status.HTTP_200_OK)
        else:
             return Response({"message": "No report found for given filters"}, status=status.HTTP_404_NOT_FOUND)



# ==============================================================================
# ✅ QA HUB LOG REPORT API
# =================================================================================================
class SaveReportLogView(APIView):
    permission_classes = []

    def post(self, request):
        username = request.data.get("username")
        report_name = request.data.get("report_name")
        record_id = request.data.get("record_id")

        if not username or not report_name:
            return Response(
                {"error": "username and report_name are required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        log = auto_log_report(
            username=username,
            report_name=report_name,
            record_id=record_id,
        )

        if not log:
            return Response(
                {"error": "Activity log failed. Check backend console."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            {
                "success": True,
                "message": "QA activity log and notification created successfully.",
                "log_id": log.id,
                "form_key": log.form_key,
                "hub": log.hub,
            },
            status=status.HTTP_201_CREATED,
        )

# ==============================================================================
# 🔥 ALL FORMS FETCH API (For View/Approve Mode)
# ==============================================================================
@api_view(['GET'])
def get_single_report_view(request, form_key, report_id):
    try:
        log_entry = get_object_or_404(ReportActivityLog, id=report_id)
        submitted_user = log_entry.username
        rec_id = log_entry.record_id

        if not rec_id:
            return Response({"success": False, "error": "No Record ID attached to this notification."}, status=404)

        if form_key in ['deviation-view', 'deviation']:
            report = get_object_or_404(DeviationApproval, id=rec_id)
            data = {
                "toolNameNo": report.tool_name_no, "location": report.location, "problem": report.problem,
                "reasonForDeviation": report.reason_for_deviation, "date": str(report.date), "duration": report.duration,
                "prodIncharge": report.prod_incharge, "qaIncharge": report.qa_incharge, "remarks": report.remarks,
                "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key == 'good-receipt':
             report = get_object_or_404(GoodReceiptEntry, id=rec_id)
             data = {
                "requestedBy": report.requested_by, "itemName": report.item_name, "specification": report.specification,
                "department": report.department, "qty": report.qty, "remark": report.remark, "receivedBy": report.received_by,
                "receivedDate": str(report.received_date), "submitted_by": submitted_user
             }
             return Response({"success": True, "data": data}, status=200)

        elif form_key in ['incoming-inspection-view', 'incoming']:
             report = get_object_or_404(IncomingMaterialInspection, id=rec_id)
             data = {
                "supplier": report.supplier, "customer": report.customer, "part_name": report.part_name, "part_no": report.part_no,
                "date": str(report.date), "grade": report.grade, "mtc": report.mtc, "ga_nga": report.ga_nga, "coil_no": report.coil_no,
                "invoice_no": report.invoice_no, "qty": report.qty, "inspection_data": report.inspection_data,
                "prepared_by": report.prepared_by, "checked_by": report.checked_by, "approved_by": report.approved_by, "submitted_by": submitted_user
             }
             return Response({"success": True, "data": data}, status=200)

        elif form_key in ['redbin-view', 'redbin']:
            report = get_object_or_404(RedBinAnalysisReport, id=rec_id)
            data = {
                "entry_date": str(report.entry_date), "part_name_model": report.part_name_model, "operation": report.operation,
                "total_rej_qty": report.total_rej_qty, "defect_detail": report.defect_detail, "root_cause_reason": report.root_cause_reason,
                "action_taken": report.action_taken, "responsible_person": report.responsible_person,
                "target_date": str(report.target_date) if report.target_date else "",
                "completion_date": str(report.completion_date) if report.completion_date else "", "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['scrap-note-view', 'scrap']:
            report = get_object_or_404(ScrapNoteEntry, id=rec_id)
            data = {
                "entry_date": str(report.entry_date), "part_name": report.part_name, "part_no": report.part_no,
                "defect_detail": report.defect_detail, "quantity": report.quantity, "remarks": report.remarks, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['process-audit-view', 'process-audit']:
            report = get_object_or_404(ProcessAuditChecksheet, id=rec_id)
            data = {
                "part_name_no": report.part_name_no, "model_name": report.machine_model, "audit_date": str(report.date),
                "auditor_name": report.auditor, "auditee_name": report.auditee, "audit_details": report.audit_details, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['coherence-view', 'coherence']:
            report = get_object_or_404(CoherenceChecklist, id=rec_id)
            data = {
                "partName": report.part_name, "partNo": report.part_no, "date": str(report.date), "model": report.model_name,
                "preparedBy": report.prepared_by, "verifiedBy": report.verified_by, "operations": report.operations, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['layout-inspection-view', 'layout-inspection']:
            report = get_object_or_404(LayoutInspection, id=rec_id)
            data = {
                "partName": report.part_name, "partNo": report.part_no, "model": report.model_name, "customer": report.customer_name,
                "date": str(report.date), "sampleSize": report.sample_size, "preparedBy": report.prepared_by, "verifiedBy": report.verified_by,
                "inspections": report.inspections, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['product-audit-plan-view', 'product-audit-plan']:
            report = get_object_or_404(ProductAuditPlan, id=rec_id)
            data = {
                "doc_no": report.doc_no, "rev_no": report.rev_no, "date": str(report.date), "plan_year": report.plan_year,
                "prepared_by": report.prepared_by, "approved_by": report.approved_by, "rows": report.audit_rows, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['customer-complaint-view', 'customer-complaint']:
            report = get_object_or_404(CustomerComplaint, id=rec_id)
            data = {
                "date": str(report.date), "part_details": report.part_details, "model_name": report.model_name, "customer_name": report.customer_name,
                "problem_description": report.problem_description, "counter_measure": report.counter_measure, "target_date": str(report.target_date),
                "horizontal_action": report.horizontal_action, "status": report.status, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['customer-satisfaction-view', 'customer-satisfaction']:
            report = get_object_or_404(CustomerSatisfaction, id=rec_id)
            data = {
                "customerName": report.customer_name, "monthYear": report.month_year, **report.performance_indicators, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['warranty-claim-view', 'warranty-claim']:
            report = get_object_or_404(WarrantyClaim, id=rec_id)
            data = {
                "date": str(report.date), "customerName": report.customer_name, "partDetails": report.part_details, "claimQty": report.claim_qty,
                "warrantyDefect": report.warranty_defect, "decision": report.decision, "rejectionRootCause": report.rejection_root_cause,
                "disposalAction": report.disposal_action, "capaAnalysis": report.capa_analysis, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['mom-view', 'mom']:
            report = get_object_or_404(MinutesOfMeeting, id=rec_id)
            data = {
                "date": str(report.date), "time": str(report.time), "subject": report.subject, "aotMembers": report.aot_members,
                "supplierMembers": report.supplier_members, "discussions": report.discussions, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['redbin-attendance-view', 'redbin-attendance']:
            report = get_object_or_404(RedBinAttendance, id=rec_id)
            data = {
                "date": str(report.date), "month": report.month, "year": report.year, "employee_name": report.employee_name,
                "designation": report.designation, "status": report.status, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)

        elif form_key in ['inspection-view', 'inspection']:
            report = get_object_or_404(InspectionReport, id=rec_id)
            data = {
                "customer": report.customer_account, "part_name": report.part_name, "operation": report.operation, "part_number": report.part_number,
                "plant_location": report.plant_location, "date": str(report.inspection_date), "operator": report.operator_name,
                "machine": report.machine_number, "inspection_data": report.inspection_data, "submitted_by": submitted_user
            }
            return Response({"success": True, "data": data}, status=200)
        
        elif form_key in ['rework-view', 'rework']:
            report = get_object_or_404(ReworkEntry, id=rec_id)

            details = report.dynamic_details or {}
            status_val = details.get('status', '')
            observations = details.get('observations', [])

            if status_val == 'ok':
                final_status = 'OK'
            elif status_val == 'notok':
                final_status = 'NOT OK'
            else:
                final_status = '—'

            data = {
                "Date": str(report.date),
                "Part Name": report.part_name,
                "Part No": report.part_no,
                "Spec": report.spec,
                "Non Conformance": report.non_conformance,
                "Rework Qty": report.rework_qty,
                "Status": final_status,
                "Inspected By": report.inspected_by or "—",
                "Remark": report.remark or "—",
                "submitted_by": submitted_user,
            }

            for i, val in enumerate(observations):
                data[f"Obs {i + 1}"] = val if val else "—"

            return Response({"success": True, "data": data}, status=200)

        else:
            return Response({"success": False, "error": f"Form '{form_key}' Not Supported Yet"}, status=400)

    except Exception as e:
        import traceback
        print("🔥 ERROR IN GET SINGLE REPORT:", traceback.format_exc())
        return Response({"success": False, "error": str(e)}, status=500)


# ==============================================================================
# 📊 QA DATA FETCH API (General List Views)
# ==============================================================================
@api_view(['GET'])
def qa_data_view(request, form_key):

    # ── 🔥 NAYA MASTER HELPER FUNCTION (Sabhi APIs ko Filter Karne Ke Liye) ──
    def apply_date_filter(queryset, date_field):
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')

        # Agar 'all' pass kiya hai toh pura DB data return kar do
        if start_date == 'all':
            return queryset

        # Default Behavior: Agar filter se kuch na bhejein toh sirf Aaj (Today) ka data aayega
        if not start_date and not end_date:
            ist_tz = pytz.timezone('Asia/Kolkata')
            from django.utils.timezone import now
            today_str = now().astimezone(ist_tz).strftime("%Y-%m-%d")
            if date_field == 'created_at':
                return queryset.filter(**{f"{date_field}__date": today_str})
            return queryset.filter(**{f"{date_field}": today_str})

        # Custom Filters: Last 2 days, Specific Date wagerah
        if start_date and end_date:
            if date_field == 'created_at':
                return queryset.filter(**{f"{date_field}__date__range": [start_date, end_date]})
            return queryset.filter(**{f"{date_field}__range": [start_date, end_date]})
        elif start_date:
            if date_field == 'created_at':
                return queryset.filter(**{f"{date_field}__date__gte": start_date})
            return queryset.filter(**{f"{date_field}__gte": start_date})
        elif end_date:
            if date_field == 'created_at':
                return queryset.filter(**{f"{date_field}__date__lte": end_date})
            return queryset.filter(**{f"{date_field}__lte": end_date})
            
        return queryset

    # ── Helper: AM/PM aur Comma wala Time Generator ──
    def format_to_ampm(raw_time):
        if not raw_time:
            return '—'
        try:
            if hasattr(raw_time, 'strftime'):
                ist_tz = pytz.timezone('Asia/Kolkata')
                from django.utils.timezone import localtime
                local_dt = localtime(raw_time, ist_tz) if getattr(raw_time, 'tzinfo', None) else raw_time
                return local_dt.strftime("%Y-%m-%d , %I:%M %p").lower()
            else:
                time_str = str(raw_time)
                if '+' in time_str:
                    time_str = time_str.split('+')[0].strip() 
                dt_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                return dt_obj.strftime("%Y-%m-%d , %I:%M %p").lower()
        except Exception:
            return str(raw_time)


    # ── Helper: raw SQL table se data fetch karna ──────────────
    def fetch_from_table(table_name, source_tag=None):
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name} ORDER BY id DESC")
                cols = [col[0] for col in cursor.description]
                rows = []
                for row in cursor.fetchall():
                    record = dict(zip(cols, row))
                    for k, v in record.items():
                        if hasattr(v, 'strftime'):
                            record[k] = str(v)
                    if source_tag:
                        record['_source'] = source_tag
                    rows.append(record)
                return rows
        except Exception as e:
            print(f"⚠️ {table_name} error: {e}")
            return []

    # ── 2. INSPECTION (FPIR) ───────────────────────────────────
    if form_key == 'inspection-view':
        all_data = []
        report_id = request.GET.get('id', None) 
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')

        try:
            with connection.cursor() as cursor:
                if report_id:
                    cursor.execute("""
                        SELECT 
                            id, customer_account, part_name, operation,
                            part_number, plant_location, inspection_date,
                            operator_name, machine_number, inspection_data
                        FROM inspection_reports
                        WHERE id = %s
                    """, [report_id])
                else:
                    query = """
                        SELECT 
                            id, customer_account, part_name, operation,
                            part_number, plant_location, inspection_date,
                            operator_name, machine_number, inspection_data
                        FROM inspection_reports
                    """
                    params = []
                    
                    # 🔥 SQL Filtering for Inspection view
                    if start_date != 'all':
                        if not start_date and not end_date:
                            ist_tz = pytz.timezone('Asia/Kolkata')
                            from django.utils.timezone import now
                            today_str = now().astimezone(ist_tz).strftime("%Y-%m-%d")
                            query += " WHERE inspection_date = %s"
                            params.append(today_str)
                        elif start_date and end_date:
                            query += " WHERE inspection_date BETWEEN %s AND %s"
                            params.extend([start_date, end_date])
                        elif start_date:
                            query += " WHERE inspection_date >= %s"
                            params.append(start_date)
                        elif end_date:
                            query += " WHERE inspection_date <= %s"
                            params.append(end_date)
                            
                    query += " ORDER BY id DESC"
                    cursor.execute(query, params)

                cols = [col[0] for col in cursor.description]

                for row in cursor.fetchall():
                    rec = dict(zip(cols, row))
                    for k, v in rec.items():
                        if hasattr(v, 'strftime'):
                            rec[k] = str(v)

                    raw_insp_data = rec.get('inspection_data')
                    
                    if isinstance(raw_insp_data, str):
                        try:
                            insp_data = json.loads(raw_insp_data)
                        except json.JSONDecodeError:
                            insp_data = {} 
                    else:
                        insp_data = raw_insp_data or {}

                    logs       = insp_data.get('logs', [])
                    parameters = insp_data.get('parameters', [])

                    # 🔥 NAYA LOGIC: Ek Parameter = Ek Row, Stages ke val1/val2 side me aayenge
                    for param in parameters:
                        sr_key = str(param.get('sr', param.get('sr_no', '')))
                        
                        row_data = {
                            'Customer':      rec.get('customer_account', '—'),
                            'Part Name':     rec.get('part_name', '—'),
                            'Operation':     rec.get('operation', '—'),
                            'Part Number':   rec.get('part_number', '—'),
                            'Plant':         rec.get('plant_location', '—'),
                            'Insp. Date':    rec.get('inspection_date', '—'),
                            'Operator':      rec.get('operator_name', '—') or '—',
                            'Machine No':    rec.get('machine_number', '—') or '—',
                            'Parameter':     param.get('item', '—'),
                            'Category':      param.get('category', '—'),
                            'Specification': param.get('spec', '—'),
                            'Tolerance':     param.get('tol', '—'),
                            'Instrument':    param.get('instr', '—'),
                        }

                        for log in logs:
                            stage_name = log.get('displayStage', log.get('baseStage', 'STAGE')).upper()
                            readings = log.get('readings', {})
                            vals = readings.get(sr_key, {})
                            
                            val1 = vals.get('val1', '—')
                            val2 = vals.get('val2', '—')
                            
                            row_data[f'{stage_name} VAL 1'] = val1 if val1 else '—'
                            row_data[f'{stage_name} VAL 2'] = val2 if val2 else '—'
                            
                        all_data.append(row_data)

        except Exception as e:
            print(f"⚠️ inspection-view error: {e}")
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

        return JsonResponse({'data': all_data})

    # ── 3. RED BIN ANALYSIS ────────────────────────────────────
    elif form_key == 'redbin-view':
        try:
            base_query = RedBinAnalysisReport.objects.all()
            reports = apply_date_filter(base_query, 'entry_date').order_by('-entry_date', '-created_time')
            data = []
            
            for report in reports:
                raw_time = getattr(report, 'created_time', getattr(report, 'created_at', None)) 
                
                data.append({
                    'Date': str(report.entry_date), 
                    'Part Name & Model': report.part_name_model,
                    'Operation': report.operation,
                    'Total Rejected Qty': report.total_rej_qty,
                    'Defect Detail': report.defect_detail,
                    'Root Cause': report.root_cause_reason,
                    'Action Taken': report.action_taken,
                    'Responsible Person': report.responsible_person,
                    'Target Date': str(report.target_date),
                    'Completion Date': str(report.completion_date) if report.completion_date else 'Pending',
                })
            return JsonResponse({'data': data})
        except Exception as e:
            print("View Error:", str(e))
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 4. RED BIN ATTENDANCE ──────────────────────────────────
    elif form_key == 'redbin-attendance-view':
        try:
            reports = apply_date_filter(RedBinAttendance.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            
            status_map = {'P': 'Present', 'A': 'Absent', '': 'Unmarked'}
            
            for report in reports:
                data.append({
                    'Date': str(report.date),
                    'Month': report.month,
                    'Year': report.year,
                    'Employee Name': report.employee_name,
                    'Designation': report.designation,
                    'Attendance': status_map.get(report.status, report.status),
                })
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 5. SCRAP NOTE ──────────────────────────────────────────
    elif form_key == 'scrap-note-view':
        try:
            reports = apply_date_filter(ScrapNoteEntry.objects.all(), 'entry_date').order_by('-entry_date', '-created_at')
            data = []
            for report in reports:
                data.append({
                    'Date': str(report.entry_date),
                    'Part Name': report.part_name,
                    'Part No': report.part_no,
                    'Defect Detail': report.defect_detail,
                    'Quantity': report.quantity,
                    'Remarks': report.remarks or '—',
                })
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 6. REWORK REPORT ───────────────────────────────────────
    elif form_key == 'rework-view':
        try:
            reports = apply_date_filter(ReworkEntry.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for r in reports:
                details = r.dynamic_details or {}
                status_val = details.get('status', '')
                observations = details.get('observations', [])
                
                if status_val == 'ok':
                    final_status = ' OK'
                elif status_val == 'notok':
                    final_status = 'NOT OK'
                else:
                    final_status = '—'

                row_data = {
                    'Date': str(r.date),
                    'Part Name': r.part_name,
                    'Part No': r.part_no,
                    'Spec': r.spec,
                    'Non Conformance': r.non_conformance,
                    'Rework Qty': r.rework_qty,
                    'Status': final_status,
                    'Inspected By': r.inspected_by or '—',
                    'Remark': r.remark or '—'
                }
                
                for i, val in enumerate(observations):
                    row_data[f'Obs {i+1}'] = val if val else '—'
                    
                data.append(row_data)
                
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 7. DEVIATION APPROVAL ──────────────────────────────────
    elif form_key == 'deviation-view':
        try:
            reports = apply_date_filter(DeviationApproval.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                data.append({
                    'Date': str(report.date),
                    'Tool Name/No.': report.tool_name_no or '—',
                    'Location': report.location or '—',
                    'Problem': report.problem or '—',
                    'Reason for Deviation': report.reason_for_deviation or '—',
                    'Duration': report.duration or '—',
                    'Prod Incharge': report.prod_incharge or '—',
                    'QA Incharge': report.qa_incharge or '—',
                    'Remarks': report.remarks or '—'
                })
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 8. GOOD RECEIPT ENTRY (NEW) ────────────────────────────
    elif form_key == 'good-receipt':
        try:
            reports = apply_date_filter(GoodReceiptEntry.objects.all(), 'received_date').order_by('-received_date', '-created_at')
            data = []
            for report in reports:
                data.append({
                    'Date': str(report.received_date),
                    'Requested By': report.requested_by,
                    'Item Name': report.item_name,
                    'Department': report.department,
                    'Quantity': report.qty,
                    'Specification': report.specification or '—',
                    'Received By': report.received_by,
                    'Remark': report.remark or '—'
                })
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 9. PROCESS AUDIT CHECKSHEET ────────────────────────────
    elif form_key == 'process-audit-view':
        try:
            reports = apply_date_filter(ProcessAuditChecksheet.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_info = {
                    'Date': str(report.date) if report.date else '—',
                    'Part Name & No': report.part_name_no or '—',
                    'Machine Model': report.machine_model or '—',
                    'Auditor': report.auditor or '—',
                    'Auditee': report.auditee or '—',
                }
                
                audit_details = report.audit_details or []
                if not audit_details:
                    data.append({**base_info, 'Detail': 'No details found'})
                else:
                    for detail in audit_details:
                        row = base_info.copy()
                        if isinstance(detail, dict):
                            for k, v in detail.items():
                                row[k.capitalize()] = v
                        else:
                            row['Detail'] = str(detail)
                        data.append(row)
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 10. COHERENCE CHECKLIST ─────────────────────────────────
    elif form_key == 'coherence-view':
        try:
            reports = apply_date_filter(CoherenceChecklist.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_info = {
                    'Date': str(report.date) if report.date else '—',
                    'Part Name': report.part_name or '—',
                    'Part No': report.part_no or '—',
                    'Model Name': report.model_name or '—',
                    'Prepared By': report.prepared_by or '—',
                    'Verified By': report.verified_by or '—',
                }
                
                operations = report.operations or []
                if not operations:
                    data.append({**base_info, 'Operation Detail': 'No operations added'})
                else:
                    for op in operations:
                        row = base_info.copy()
                        if isinstance(op, dict):
                            for k, v in op.items():
                                row[f"Op {k.capitalize()}"] = v
                        data.append(row)
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 11. LAYOUT INSPECTION ───────────────────────────────────
    elif form_key == 'layout-inspection-view':
        try:
            reports = apply_date_filter(LayoutInspection.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_info = {
                    'Date': str(report.date) if report.date else '—',
                    'Customer Name': report.customer_name or '—',
                    'Part Name': report.part_name or '—',
                    'Part No': report.part_no or '—',
                    'Model Name': report.model_name or '—',
                    'Sample Size': report.sample_size or '—',
                    'Prepared By': report.prepared_by or '—',
                    'Verified By': report.verified_by or '—',
                }
                
                inspections = report.inspections or []
                if not inspections:
                    data.append({**base_info, 'Inspection Detail': 'No inspections added'})
                else:
                    for insp in inspections:
                        row = base_info.copy()
                        if isinstance(insp, dict):
                            for k, v in insp.items():
                                row[k.capitalize()] = v
                        data.append(row)
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 12. PRODUCT AUDIT PLAN ──────────────────────────────────
    elif form_key == 'product-audit-plan-view':
        try:
            reports = apply_date_filter(ProductAuditPlan.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_info = {
                    'Date': str(report.date) if report.date else '—',
                    'Doc No': report.doc_no or '—',
                    'Rev No': report.rev_no or '—',
                    'Plan Year': report.plan_year or '—',
                    'Prepared By': report.prepared_by or '—',
                    'Approved By': report.approved_by or '—',
                }
                
                audit_rows = report.audit_rows or []
                if not audit_rows:
                    data.append({**base_info, 'Audit Schedule': 'No schedules added'})
                else:
                    for a_row in audit_rows:
                        row = base_info.copy()
                        if isinstance(a_row, dict):
                            for k, v in a_row.items():
                                row[k.capitalize()] = v
                        data.append(row)
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 13. CUSTOMER COMPLAINT ──────────────────────────────────
    elif form_key == 'customer-complaint-view':
        try:
            reports = apply_date_filter(CustomerComplaint.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                data.append({
                    'Date': str(report.date) if report.date else '—',
                    'Customer Name': report.customer_name or '—',
                    'Part Details': report.part_details or '—',
                    'Model Name': report.model_name or '—',
                    'Problem Description': report.problem_description or '—',
                    'Counter Measure': report.counter_measure or '—',
                    'Target Date': str(report.target_date) if report.target_date else '—',
                    'Horizontal Action': report.horizontal_action or '—',
                    'Status': report.status or '—',
                })
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 14. CUSTOMER SATISFACTION ───────────────────────────────
    elif form_key == 'customer-satisfaction-view':
        try:
            reports = apply_date_filter(CustomerSatisfaction.objects.all(), 'created_at').order_by('-created_at')
            data = []
            for report in reports:
                row = {
                    'Customer Name': report.customer_name or '—',
                    'Month Year': report.month_year or '—',
                }
                
                indicators = report.performance_indicators or {}
                if isinstance(indicators, dict):
                    for k, v in indicators.items():
                        row[k.replace('_', ' ').title()] = v
                        
                data.append(row)
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 15. WARRANTY CLAIM ──────────────────────────────────────
    elif form_key == 'warranty-claim-view':
        try:
            reports = apply_date_filter(WarrantyClaim.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                data.append({
                    'Date': str(report.date) if report.date else '—',
                    'Customer Name': report.customer_name or '—',
                    'Part Details': report.part_details or '—',
                    'Claim Qty': report.claim_qty or '—',
                    'Warranty Defect': report.warranty_defect or '—',
                    'Decision': report.decision or '—',
                    'Rejection Root Cause': report.rejection_root_cause or '—',
                    'Disposal Action': report.disposal_action or '—',
                    'CAPA Analysis': report.capa_analysis or '—',
                })
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 16. MINUTES OF MEETING (MOM) ────────────────────────────
    elif form_key == 'mom-view':
        try:
            reports = apply_date_filter(MinutesOfMeeting.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_info = {
                    'Date': str(report.date) if report.date else '—',
                    'Time': str(report.time) if report.time else '—',
                    'Subject': report.subject or '—',
                    'AOT Members': report.aot_members or '—',
                    'Supplier Members': report.supplier_members or '—',
                }
                
                discussions = report.discussions or []
                if not discussions:
                    data.append({**base_info, 'Discussion': 'No discussions added'})
                else:
                    for disc in discussions:
                        row = base_info.copy()
                        if isinstance(disc, dict):
                            for k, v in disc.items():
                                row[k.capitalize()] = v
                        data.append(row)
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 17. INCOMING MATERIAL INSPECTION (NEW) ──────────────────
    elif form_key == 'incoming-inspection-view':
        try:
            reports = apply_date_filter(IncomingMaterialInspection.objects.all(), 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_info = {
                    'Date': str(report.date) if report.date else '—',
                    'Supplier': report.supplier or '—',
                    'Customer': report.customer or '—',
                    'Part Name': report.part_name or '—',
                    'Part No': report.part_no or '—',
                    'Grade': report.grade or '—',
                    'MTC': report.mtc or '—',
                    'GA/NGA': report.ga_nga or '—',
                    'Coil No': report.coil_no or '—',
                    'Invoice No': report.invoice_no or '—',
                    'QTY': report.qty or '—',
                    'Prepared By': report.prepared_by or '—',
                    'Checked By': report.checked_by or '—',
                    'Approved By': report.approved_by or '—',
                }

                insp_rows = report.inspection_data or []
                if not insp_rows:
                    data.append({**base_info, 'Parameter': 'No parameters added'})
                else:
                    for i_row in insp_rows:
                        row = base_info.copy()
                        row['Parameter'] = i_row.get('parameter', '—')
                        row['Specification'] = i_row.get('specification', '—')
                        row['Insp Method'] = i_row.get('inspMethod', '—')
                        
                        # 5 Observations nikal kar set karna
                        observations = i_row.get('observations', [])
                        for i in range(5):
                            val = observations[i] if i < len(observations) else ''
                            row[f'Obs {i+1}'] = val if val else '—'
                        
                        row['Remark'] = i_row.get('remark', '—')
                        data.append(row)
                        
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # Agar koi aur form_key aati hai toh default error
    return JsonResponse({'data': [], 'error': 'Form type not supported'}, status=400)