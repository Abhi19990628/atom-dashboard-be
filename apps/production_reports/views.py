import json
import traceback
from datetime import datetime
import pytz

from django.db import connection, transaction
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.cache import never_cache

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view

from api.services.report_logging import auto_log_report
from api.utils.date_filters import apply_date_filter

# ==============================================================================
# IMPORTS FROM MAIN API APP
# ==============================================================================
from api.models import (
    BinTrolleyReport, MachineChecksheetReport, MachineChecksheetObservation,
    TipChangeDressing, ReworkEntry, FiveSChecksheetReport, FiveSChecksheetObservation,
    DailyProductionPlan, FourMChangeInspection, FourMChangeRecord,
    MonthlyProductionPlan, OperatorObservanceChecklist, OperatorObservancePlan,
    PMChecklistMHE, ProjectionWelderQual, SpotWelderQual, TigMigWelderQual, ProcessValidation, FourMDisplay, FourMSummary,
    ReportActivityLog, FourMInformationSheet, QANotification
)

from api.serializers import (
    TipChangeDressingSerializer, DailyProductionPlanSerializer,
    FourMChangeInspectionSerializer, FourMChangeRecordSerializer, FourMSummarySerializer,FourMInformationSheetSerializer
)


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def clean_val(val, default=None):
    return val if val != '' and val is not None else default

# ==============================================================================
# ✅ PRODUCTION HUB ROUTE MAPPING + AUTO ACTIVITY LOG & NOTIFICATION ROUTER
# ==============================================================================

# ==============================================================================
# 🏭 DAILY PRODUCTION APIs (Save APIs updated with auto_log_report)
# ==============================================================================

class SaveBinTrolleyReportView(APIView):
    def post(self, request):
        try:
            data = request.data
            report = BinTrolleyReport.objects.create(
                date=data.get('date'),
                week=data.get('week', ''),
                month=data.get('month', ''),
                checkpoints=data.get('checkpoints', {}),
                cleaning_details=data.get('cleaning_details', {}),
                maintenance_details=data.get('maintenance_details', {})
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "Bin Trolley Form", report.id)
            return Response({"success": True, "message": "✅ Bin Trolley Data Saved!", "record_id": report.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveMachineChecksheetView(APIView):
    @transaction.atomic  
    def post(self, request):
        try:
            data = request.data
            report = MachineChecksheetReport.objects.create(
                date=data.get('date', timezone.now().date()),
                plant_name=data.get('plant_name', 'Plant 1'),
                machine_no=data.get('machine_no', ''),
                checked_by_maintenance=data.get('checked_by_maintenance', ''),
                verified_by_production=data.get('verified_by_production', '')
            )

            check_points_data = data.get('check_points', [])
            observations = [
                MachineChecksheetObservation(
                    report=report,
                    s_no=item.get('s_no', index + 1),
                    poka_yoke_detail=item.get('poka_yoke_detail', ''),
                    checking_method=item.get('checking_method', ''),
                    reference_sop=item.get('reference_sop', ''),
                    is_ok=item.get('is_ok', True),
                    remarks=item.get('remarks', '')
                ) for index, item in enumerate(check_points_data)
            ]
            
            if observations:
                MachineChecksheetObservation.objects.bulk_create(observations)

            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "Machine Checksheet", report.id)
            return Response({"success": True, "message": "✅ Daily Checksheet Saved Successfully!", "report_id": report.id, "record_id": report.id}, status=status.HTTP_201_CREATED)

        except Exception as e:
            print("❌ Django Error (Checksheet Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveTipChangeView(APIView):
    def post(self, request):
        try:
            serializer = TipChangeDressingSerializer(data=request.data)
            if serializer.is_valid():
                obj = serializer.save()
                # 🔥 AUTO LOG BANAO
                auto_log_report(request.data.get('submitted_by'), "Tip Change Monitor Form", obj.id)
                return Response({"success": True, "message": "✅ Tip Change & Dressing data saved successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": "Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            print("❌ Django Error (Tip Change Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR) 

class SaveReworkReportView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            date_val = data.get('date')
            remark_val = data.get('remark')
            items = data.get('items', [])
            
            entries_to_create = []
            for row in items:
                dynamic_data = {
                    "status": row.get('status', ''),
                    "observations": row.get('observations', [])
                }
                entries_to_create.append(
                    ReworkEntry(
                        date=date_val, remark=remark_val,
                        part_name=row.get('part_name', ''),
                        part_no=row.get('part_no', ''),
                        spec=row.get('spec', ''),
                        non_conformance=row.get('non_conformance', ''),
                        rework_qty=int(row.get('rework_qty') or 0),
                        inspected_by=row.get('inspected_by', ''),
                        dynamic_details=dynamic_data
                    )
                )
            
            last_id = None
            if entries_to_create:
                ReworkEntry.objects.bulk_create(entries_to_create)
                last_record = ReworkEntry.objects.last()
                last_id = last_record.id if last_record else None
                
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "Rework Report", last_id)
            return Response({"success": True, "message": "✅ Rework Data Saved!", "record_id": last_id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class CheckFiveSStatusView(APIView):
    def get(self, request):
        try:
            date = request.query_params.get('date')
            area_param = request.query_params.get('area')
            is_filled = FiveSChecksheetReport.objects.filter(date=date, area=area_param).exists()
            return Response({'isFilled': is_filled}, status=200)
        except Exception as e:
            return Response({'error': str(e)}, status=500)

class SaveFiveSReportView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            summary = data.get('summary', {})
            
            report = FiveSChecksheetReport.objects.create(
                area=data.get('area', ''),
                zone_leader=data.get('zoneLeader', ''),
                date=data.get('date'),
                language=data.get('language', 'eng'),
                total_checks=int(summary.get('totalChecks', 0)),
                ok_count=int(summary.get('okCount', 0)),
                ng_count=int(summary.get('ngCount', 0))
            )
            
            checks_data = data.get('checks', [])
            obs_to_create = []
            for s_group in checks_data:
                s_category_name = s_group.get('s', '')
                for pt in s_group.get('points', []):
                    obs_to_create.append(
                        FiveSChecksheetObservation(
                            report=report, s_category=s_category_name, check_point=pt.get('point', ''), status=pt.get('status', 'Not Checked')
                        )
                    )
            
            if obs_to_create:
                FiveSChecksheetObservation.objects.bulk_create(obs_to_create)
                
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "5S Checksheet", report.id)
            return Response({"success": True, "message": "✅ 5S Checksheet Saved!", "record_id": report.id}, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            print("Error:", str(e))
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveDailyProductionPlanView(APIView):
    report_name = "Daily Production plan"
    def post(self, request):
        try:
            serializer = DailyProductionPlanSerializer(data=request.data)
            if serializer.is_valid():
                obj = serializer.save()
                # 🔥 AUTO LOG BANAO
                auto_log_report(request.data.get('submitted_by'), "Daily Prod Form", obj.id)
                return Response({"success": True, "message": "Daily Production Plan saved successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": "Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print(" Django Error (Daily Production Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@never_cache
@api_view(['GET'])
def get_today_production_data(request):
    report_name = "Daily_production_plan"
    try:
        plant_name = request.GET.get('plant_name')
        date_str = request.GET.get('date')

        if not plant_name:
            return Response({'success': False, 'error': 'plant_name is required'}, status=400)

        filter_date = datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else datetime.now().date()
        queryset = DailyProductionPlan.objects.filter(plant=plant_name, created_at__date=filter_date)
        filled_machines = list(queryset.values_list('machine_no', flat=True))

        return Response({'success': True, 'filled_machines': filled_machines, 'date': str(filter_date)})
    except Exception as e:
        return Response({'success': False, 'error': str(e)}, status=500)

class UpdateDailyProductionPlanView(APIView):
    def patch(self, request, pk):
        try:
            plan = get_object_or_404(DailyProductionPlan, pk=pk)
            serializer = DailyProductionPlanSerializer(plan, data=request.data, partial=True)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Production details updated successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_200_OK)
            return Response({"success": False, "error": "Update Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print(" Django Error (Daily Production Update): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveFourMChangeInspectionView(APIView):
    def post(self, request):
        try:
            serializer = FourMChangeInspectionSerializer(data=request.data)
            if serializer.is_valid():
                obj = serializer.save()
                # 🔥 AUTO LOG BANAO
                auto_log_report(request.data.get('submitted_by'), "4M Change Inspection", obj.id)
                return Response({"success": True, "message": " 4M Change Inspection data saved successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": "Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print("Django Error (4M Change Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveFourMChangeRecordView(APIView):
    def post(self, request):
        try:
            serializer = FourMChangeRecordSerializer(data=request.data)
            if serializer.is_valid():
                obj = serializer.save() 
                # 🔥 AUTO LOG BANAO
                auto_log_report(request.data.get('submitted_by'), "4M Tracking Record", obj.id)
                return Response({"success": True, "message": " 4M Change Record saved successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": "Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print("Django Error (4M Change Record Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": "Server connection failed: " + str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)  

class SaveFourMDisplayView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            entries = data.get('entries', [])

            for entry in entries:
                FourMDisplay.objects.create(
                    s_no=entry.get('s_no'),
                    machine_no=entry.get('machine_no', ''),
                    operator_name=entry.get('operator_name', ''),
                    man=entry.get('man', ''),
                    machine=entry.get('machine', ''),
                    material=entry.get('material', ''),
                    method=entry.get('method', '')
                )
            
            last_record = FourMDisplay.objects.last()
            last_id = last_record.id if last_record else None
            
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "4M Display Board", last_id)
            return Response({"success": True, "message": " 4M Display Board Saved!", "record_id": last_id}, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            print(f"4M Display Board Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveFourMSummaryView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            prepared_by = data.get('prepared_by', '')
            approved_by = data.get('approved_by', '')
            entries = data.get('entries', [])

            for entry in entries:
                entry['prepared_by'] = prepared_by
                entry['approved_by'] = approved_by
                if not entry.get('date'):
                    entry.pop('date', None)

            serializer = FourMSummarySerializer(data=entries, many=True)
            if serializer.is_valid():
                objs = serializer.save()
                last_id = objs[-1].id if objs else None
                # 🔥 AUTO LOG BANAO
                auto_log_report(prepared_by, "4M Summary Sheet", last_id)
                return Response(
                    {"success": True, "message": "4M Summary Sheet Saved Successfully!", "record_id": last_id}, 
                    status=status.HTTP_201_CREATED
                )
            else:
                return Response(
                    {"success": False, "error": serializer.errors}, 
                    status=status.HTTP_400_BAD_REQUEST
                )

        except Exception as e:
            print(f"4M Summary Board Error: {str(e)}")
            return Response(
                {"success": False, "error": str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# ==============================================================================
# 📅 MONTHLY PRODUCTION APIs (Save APIs updated with auto_log_report)
# ==============================================================================

class SaveMonthlyProdPlanView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            plan = MonthlyProductionPlan.objects.create(
                filled_date=clean_val(data.get('date')),
                part_name=data.get('partName', ''),
                customer_name=data.get('customer', ''),
                opening_stock=clean_val(data.get('openingStock'), 0),
                schedule_qty=clean_val(data.get('scheduleQty'), 0),
                planned_qty=clean_val(data.get('plannedQty'), 0),
                remark=data.get('remark', ''),
                prepared_by=data.get('preparedBy', ''),
                approved_by=data.get('approvedBy', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('preparedBy'), "Monthly Prod Plan", plan.id)
            return Response({"success": True, "message": " Monthly Production Plan Data Saved!", "record_id": plan.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            print(f"Monthly Prod Plan Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveOperatorObservanceChecklistView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            checklist = OperatorObservanceChecklist.objects.create(
                record_date=clean_val(data.get('recordDate')), 
                operator_name=data.get('operatorName', ''),
                model=data.get('model', ''),
                part_operation=data.get('partOperation', ''),
                checkpoints=data.get('formData', []), 
                prepared_by=data.get('preparedBy', ''),
                approved_by=data.get('approvedBy', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('preparedBy'), "Operator Observance Checklist", checklist.id)
            return Response({"success": True, "message": " Operator Observance Checklist Saved!", "record_id": checklist.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            print(f"Observance Checklist Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveOperatorObservancePlanView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            plan = OperatorObservancePlan.objects.create(
                plan_year=data.get('selectedYear', ''),
                plan_month=data.get('selectedMonth', ''),
                operators_data=data.get('operators', []), 
                prepared_by=data.get('preparedBy', ''),
                approved_by=data.get('approvedBy', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('preparedBy'), "Operator Observance Plan", plan.id)
            return Response({"success": True, "message": f" Operator Observance Plan Saved!", "record_id": plan.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            print(f"Observance Plan Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SavePMChecklistMHEView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            pm_form = PMChecklistMHE.objects.create(
                filled_date=clean_val(data.get('filledDate')), 
                part_name=data.get('partName', ''),
                trolley_no=data.get('trolleyNo', ''),
                pm_frequency=data.get('pmFrequency', ''),
                checkpoints=data.get('checkPoints', []), 
                checked_by=data.get('checkedBy', ''),
                verified_by=data.get('verifiedBy', ''),
                general_remarks=data.get('generalRemarks', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('checkedBy'), "PM Checklist MHE", pm_form.id)
            return Response({"success": True, "message": " Preventive Maintenance MHE Data Saved!", "record_id": pm_form.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            print(f"PM Checklist MHE Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveProjectionWelderView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            welder = ProjectionWelderQual.objects.create(
                wps_no=data.get('wpsNo', ''),
                date=clean_val(data.get('date')),
                welding_process=data.get('weldingProcess', 'PROJECTION WELDING'),
                base_metal=data.get('baseMetal', ''),
                base_metal_thickness=data.get('baseMetalThickness', ''),
                machine_no=data.get('machineNo', ''),
                trials=data.get('trials', []),
                welder_name=data.get('welderName', ''),
                conducted_by=data.get('conductedBy', ''),
                verified_by=data.get('verifiedBy', ''),
                qualification_status=data.get('qualificationStatus', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('conductedBy'), "Projection Welder", welder.id)
            return Response({"success": True, "message": "Projection Welder Data Saved!", "record_id": welder.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveSpotWelderView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            welder = SpotWelderQual.objects.create(
                wps_no=data.get('wpsNo', ''),
                date=clean_val(data.get('date')),
                welding_process=data.get('weldingProcess', 'Spot Welding'),
                base_metal=data.get('baseMetal', ''),
                base_metal_thickness=data.get('baseMetalThickness', ''),
                machine_no=data.get('machineNo', ''),
                gun_type=data.get('gunType', ''),
                trials=data.get('trials', []),
                welder_name=data.get('welderName', ''),
                conducted_by=data.get('conductedBy', ''),
                verified_by=data.get('verifiedBy', ''),
                qualification_status=data.get('qualificationStatus', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('conductedBy'), "Spot Welder", welder.id)
            return Response({"success": True, "message": "Spot Welder Data Saved!", "record_id": welder.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveTigMigWelderView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            welder = TigMigWelderQual.objects.create(
                wps_no=data.get('wpsNo', ''),
                testing_date=clean_val(data.get('testingDate')),
                welding_process=data.get('weldingProcess', ''),
                machine_no=data.get('machineNo', ''),
                base_metal=data.get('baseMetal', ''),
                base_metal_thickness=data.get('baseMetalThickness', ''),
                base_metal_size=data.get('baseMetalSize', ''),
                welding_position=data.get('weldingPosition', ''),
                filler_material=data.get('fillerMaterial', ''),
                filler_material_size=data.get('fillerMaterialSize', ''),
                shielding_gas=data.get('shieldingGas', ''),
                wire_feed_speed=data.get('wireFeedSpeed', ''),
                trials=data.get('trials', []),
                test_results=data.get('testResults', {}),
                welder_name=data.get('welderName', ''),
                conducted_by=data.get('conductedBy', ''),
                verified_by=data.get('verifiedBy', ''),
                qualification_status=data.get('qualificationStatus', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('conductedBy'), "TIG/MIG Welder", welder.id)
            return Response({"success": True, "message": "MIG/TIG Welder Data Saved!", "record_id": welder.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveProcessValidationView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            validation = ProcessValidation.objects.create(
                validation_date=clean_val(data.get('validationDate')),
                revalidation_date=clean_val(data.get('revalidationDate')),
                process_name=data.get('processName', ''),
                material_details=data.get('materialDetails', ''),
                machine_no=data.get('machineNo', ''),
                process_owner=data.get('processOwner', ''),
                part_name=data.get('partName', ''),
                fixture_no=data.get('fixtureNo', ''),
                operators=data.get('operators', []),
                parameters=data.get('parameters', []),
                trials=data.get('trials', []),
                final_params=data.get('finalParams', []),
                conclusion=data.get('conclusion', ''),
                prepared_by=data.get('preparedBy', ''),
                approved_by=data.get('approvedBy', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('preparedBy'), "Process Validation", validation.id)
            return Response({"success": True, "message": "Process Validation Data Saved!", "record_id": validation.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
class SaveFourMInformationSheetView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            entries = data.get('entries', [])
            prepared_by_val = data.get('preparedBy', data.get('prepared_by', ''))
            
            last_id = None
            for entry in entries:
                if not entry.get('time'):
                    entry['time'] = None
                entry['prepared_by'] = prepared_by_val
                serializer = FourMInformationSheetSerializer(data=entry)
                
                if serializer.is_valid():
                    saved_instance = serializer.save()
                    last_id = saved_instance.id
                else:
                    return Response({"success": False, "error": str(serializer.errors)}, status=status.HTTP_400_BAD_REQUEST)
            
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by') or prepared_by_val, "4M Information Sheet", last_id)
            return Response({"success": True, "message": "Information Sheet Saved Successfully!", "record_id": last_id}, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            print(f"Information Sheet Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# ==============================================================================
# 🔥 NEW CLEANED UP: SINGLE REPORT FETCH API (REGISTRY PATTERN)
# ==============================================================================
@api_view(['GET'])
def get_single_production_report_view(request, form_key, report_id):
    try:
        log_entry = get_object_or_404(ReportActivityLog, id=report_id)
        submitted_user = log_entry.username
        rec_id = log_entry.record_id

        if not rec_id:
            return Response({"success": False, "error": "No Record ID attached to this notification."}, status=404)

        # ── 🔥 CUSTOM FORMATTERS ──
        def get_machine_checksheet(r):
            obs = MachineChecksheetObservation.objects.filter(report=r)
            obs_data = [{"s_no": o.s_no, "poka_yoke_detail": o.poka_yoke_detail, "checking_method": o.checking_method, "reference_sop": o.reference_sop, "is_ok": o.is_ok, "remarks": o.remarks} for o in obs]
            return {"date": str(r.date), "plant_name": r.plant_name, "machine_no": r.machine_no, "checked_by_maintenance": r.checked_by_maintenance, "verified_by_production": r.verified_by_production, "check_points": obs_data}

        def get_fives_checksheet(r):
            obs = FiveSChecksheetObservation.objects.filter(report=r)
            obs_data = [{"s_category": o.s_category, "check_point": o.check_point, "status": o.status} for o in obs]
            return {"area": r.area, "zoneLeader": r.zone_leader, "date": str(r.date), "language": r.language, "totalChecks": r.total_checks, "okCount": r.ok_count, "ngCount": r.ng_count, "observations": obs_data}

        # ── 🔥 THE REGISTRY ──
        FORM_REGISTRY = {
            'daily-prod-plan': lambda r: DailyProductionPlanSerializer(r).data,
            'four-m-inspection': lambda r: FourMChangeInspectionSerializer(r).data,
            'four-m-record': lambda r: FourMChangeRecordSerializer(r).data,
            'tip-change': lambda r: TipChangeDressingSerializer(r).data,
            'four-m-summary': lambda r: FourMSummarySerializer(r).data,
            'machine-checksheet': get_machine_checksheet,
            'five-s': get_fives_checksheet,
            'five-s-view': get_fives_checksheet,
            'bin-trolley': lambda r: {"date": str(r.date), "week": r.week, "month": r.month, "checkpoints": r.checkpoints, "cleaning_details": r.cleaning_details, "maintenance_details": r.maintenance_details},
            'rework': lambda r: {"date": str(r.date), "remark": r.remark, "part_name": r.part_name, "part_no": r.part_no, "spec": r.spec, "non_conformance": r.non_conformance, "rework_qty": r.rework_qty, "inspected_by": r.inspected_by, "dynamic_details": r.dynamic_details},
            'rework-view': lambda r: {"date": str(r.date), "remark": r.remark, "part_name": r.part_name, "part_no": r.part_no, "spec": r.spec, "non_conformance": r.non_conformance, "rework_qty": r.rework_qty, "inspected_by": r.inspected_by, "dynamic_details": r.dynamic_details},
            'monthly-prod-plan': lambda r: {"date": str(r.filled_date), "partName": r.part_name, "customer": r.customer_name, "openingStock": r.opening_stock, "scheduleQty": r.schedule_qty, "plannedQty": r.planned_qty, "remark": r.remark, "preparedBy": r.prepared_by, "approvedBy": r.approved_by},
            'operator-observance-checklist': lambda r: {"recordDate": str(r.record_date), "operatorName": r.operator_name, "model": r.model, "partOperation": r.part_operation, "formData": r.checkpoints, "preparedBy": r.prepared_by, "approvedBy": r.approved_by},
            'operator-observance-plan': lambda r: {"selectedYear": r.plan_year, "selectedMonth": r.plan_month, "operators": r.operators_data, "preparedBy": r.prepared_by, "approvedBy": r.approved_by},
            'pm-checklist-mhe': lambda r: {"filledDate": str(r.filled_date), "partName": r.part_name, "trolleyNo": r.trolley_no, "pmFrequency": r.pm_frequency, "checkPoints": r.checkpoints, "checkedBy": r.checked_by, "verifiedBy": r.verified_by, "generalRemarks": r.general_remarks},
            'projection-welder': lambda r: {"wpsNo": r.wps_no, "date": str(r.date), "weldingProcess": r.welding_process, "baseMetal": r.base_metal, "baseMetalThickness": r.base_metal_thickness, "machineNo": r.machine_no, "trials": r.trials, "welderName": r.welder_name, "conductedBy": r.conducted_by, "verifiedBy": r.verified_by, "qualificationStatus": r.qualification_status},
            'spot-welder': lambda r: {"wpsNo": r.wps_no, "date": str(r.date), "weldingProcess": r.welding_process, "baseMetal": r.base_metal, "baseMetalThickness": r.base_metal_thickness, "machineNo": r.machine_no, "gunType": r.gun_type, "trials": r.trials, "welderName": r.welder_name, "conductedBy": r.conducted_by, "verifiedBy": r.verified_by, "qualificationStatus": r.qualification_status},
            'tig-mig-welder': lambda r: {"wpsNo": r.wps_no, "testingDate": str(r.testing_date), "weldingProcess": r.welding_process, "machineNo": r.machine_no, "baseMetal": r.base_metal, "baseMetalThickness": r.base_metal_thickness, "baseMetalSize": r.base_metal_size, "weldingPosition": r.welding_position, "fillerMaterial": r.filler_material, "fillerMaterialSize": r.filler_material_size, "shieldingGas": r.shielding_gas, "wireFeedSpeed": r.wire_feed_speed, "trials": r.trials, "testResults": r.test_results, "welderName": r.welder_name, "conductedBy": r.conducted_by, "verifiedBy": r.verified_by, "qualificationStatus": r.qualification_status},
            'process-validation': lambda r: {"validationDate": str(r.validation_date), "revalidationDate": str(r.revalidation_date), "processName": r.process_name, "materialDetails": r.material_details, "machineNo": r.machine_no, "processOwner": r.process_owner, "partName": r.part_name, "fixtureNo": r.fixture_no, "operators": r.operators, "parameters": r.parameters, "trials": r.trials, "finalParams": r.final_params, "conclusion": r.conclusion, "preparedBy": r.prepared_by, "approvedBy": r.approved_by},
            'four-m-display': lambda r: {"s_no": r.s_no, "machine_no": r.machine_no, "operator_name": r.operator_name, "man": r.man, "machine": r.machine, "material": r.material, "method": r.method},
            'information-sheet': lambda r: FourMInformationSheetSerializer(r).data,
            'four-m-information': lambda r: FourMInformationSheetSerializer(r).data,
        }

        MODEL_REGISTRY = {
            'daily-prod-plan': DailyProductionPlan, 'four-m-inspection': FourMChangeInspection,
            'four-m-record': FourMChangeRecord, 'tip-change': TipChangeDressing,
            'four-m-summary': FourMSummary, 'machine-checksheet': MachineChecksheetReport,
            'five-s': FiveSChecksheetReport, 'five-s-view': FiveSChecksheetReport,
            'bin-trolley': BinTrolleyReport, 'rework': ReworkEntry, 'rework-view': ReworkEntry,
            'monthly-prod-plan': MonthlyProductionPlan, 'operator-observance-checklist': OperatorObservanceChecklist,
            'operator-observance-plan': OperatorObservancePlan, 'pm-checklist-mhe': PMChecklistMHE,
            'projection-welder': ProjectionWelderQual, 'spot-welder': SpotWelderQual,
            'tig-mig-welder': TigMigWelderQual, 'process-validation': ProcessValidation,
            'four-m-display': FourMDisplay,
            'information-sheet': FourMInformationSheet,
            'four-m-information': FourMInformationSheet,
        }

        if form_key not in FORM_REGISTRY or form_key not in MODEL_REGISTRY:
            return Response({"success": False, "error": f"Form '{form_key}' Not Supported Yet in Production"}, status=400)

        TargetModel = MODEL_REGISTRY[form_key]
        report = get_object_or_404(TargetModel, id=rec_id)
        
        formatter = FORM_REGISTRY[form_key]
        data = formatter(report)

        status_text = log_entry.status or ""
        
        # Ek chhota helper function taaki email split karna aasan ho jaye
        def clean_name(email_or_name):
            if not email_or_name:
                return "—"
            return email_or_name.split('@')[0].capitalize()

        # 1. Prepared By (Email se naam nikalna)
        prepared_by = clean_name(submitted_user)
        data['Prepared By'] = prepared_by
        data['submitted_by'] = submitted_user  # Original email bhi rakh lete hain backup ke liye
        
        # Default values set kar dete hain
        data['Approved By'] = "—"
        data['Rejected By'] = "—"
        data['approval_status'] = "Pending"
        
        # 2. Status Check (Approved, Rejected, Completed)
        if status_text.startswith('Approved by'):
            raw_approver = status_text.replace('Approved by', '').strip()
            data['Approved By'] = clean_name(raw_approver)
            data['approval_status'] = "Approved"
            
        elif status_text.startswith('Rejected by'):
            raw_rejecter = status_text.replace('Rejected by', '').strip()
            data['Rejected By'] = clean_name(raw_rejecter)
            data['approval_status'] = "Rejected"
            
        elif status_text in ['Completed', 'Approved']:
            data['Approved By'] = prepared_by
            data['approval_status'] = "Approved"

        # 3. Remarks 
        data['Approver_remarks'] = log_entry.remarks if log_entry.remarks else 'Null'
        

        # 4. Timestamp
        data['approved_or_rejected_at'] = log_entry.approved_or_rejected_at or ""

        return Response({"success": True, "data": data}, status=200)
    except Exception as e:
        import traceback
        print("🔥 ERROR IN GET SINGLE PRODUCTION REPORT:", traceback.format_exc())
        return Response({"success": False, "error": str(e)}, status=500)


# ==============================================================================
# 📊 PRODUCTION DATA FETCH API (View Reports - Kept As-Is)
# ==============================================================================

@api_view(['GET'])
def production_data_view(request, form_key):

    # ── 🔥 MASTER HELPER FUNCTION (Sabhi APIs ko Filter Karne Ke Liye) ──
    def apply_date_filter(queryset, date_field):
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')
        
        
        if start_date == 'all':
            return queryset

        if not start_date and not end_date:
            ist_tz = pytz.timezone('Asia/Kolkata')
            from django.utils.timezone import now
            today_str = now().astimezone(ist_tz).strftime("%Y-%m-%d")
            if date_field == 'created_at':
                return queryset.filter(**{f"{date_field}__date": today_str})
            return queryset.filter(**{f"{date_field}": today_str})

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

    # ── Helper: raw SQL table se data fetch karna
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

    # ──  COMMON LOG MAPPING  ──
    log_mapping = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT record_id, username, status 
                FROM user_report_activity_logs
            """)
            for row in cursor.fetchall():
                rec_id, raw_username, status = row[0], row[1], row[2]
                
                # Prepared By ke liye @ se split karne wala logic
                username = raw_username.split('@')[0] if raw_username else '—'
                
                if rec_id not in log_mapping:
                    log_mapping[rec_id] = {'prepared_by': '—', 'approved_by': '—'}
                
                # Status ke hisaab se logic
                if status == 'In Progress' or status == 'Pending':
                    log_mapping[rec_id]['prepared_by'] = username.capitalize()
                    
                # Approved By ke liye logic jo emails ko bhi handle karega
                elif status and status.startswith('Approved'):
                    raw_approver = status.replace('Approved by ', '').strip()
                    approver_name = raw_approver.split('@')[0] if raw_approver else '—'
                    log_mapping[rec_id]['approved_by'] = approver_name.capitalize()
                    
                elif status == 'Completed':
                    log_mapping[rec_id]['approved_by'] = username.capitalize()
    except Exception as e:
        print(f" Activity logs fetch error: {e}")

    # ── 1. DAILY PRODUCTION PLAN ────────────────────────────────
    if form_key == 'daily-prod-plan':
        try:
            base_query = DailyProductionPlan.objects.all()
            records = apply_date_filter(base_query, 'plan_date').order_by('-plan_date', '-created_at')

            data = [{
                'id': r.id,
                'Date': str(r.plan_date),
                'created_at': r.created_at.isoformat() if r.created_at else None,  
                'updated_at': r.updated_at.isoformat() if r.updated_at else None,
                'Plant': r.plant or '',
                'Machine No': r.machine_no or '—',
                'Shift': r.shift or '—',
                'Operator Name': r.operator_name,
                'Part Name': r.part_name,
                'Part No': r.part_no,
                'Operation': r.operation_name,
                'Planned Qty': r.planned_quantity,
                'Achieved Qty.': r.achieved_quantity,
                'Qty Remark': r.qty_remark or '—',
                'Start Time': str(r.production_start_time) if r.production_start_time else '—',
                'End Time': str(r.production_end_time) if r.production_end_time else '—',
                'Total Time': r.total_working_time or '—',
                'Tool Setup (min)': r.tool_setup_time if r.tool_setup_time is not None else 0,
                'Machine B/D (min)': r.machine_bd_time if r.machine_bd_time is not None else 0,
                'Tool B/D (min)': r.tool_bd_time if r.tool_bd_time is not None else 0,
                'RM Coil No': r.rm_coil_no or '—',
                'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
            } for r in records]
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 2. 4M CHANGE INSPECTION REPORT ──────────────────────────
    elif form_key == 'four-m-inspection':
        try:
            base_query = FourMChangeInspection.objects.all()
            records = apply_date_filter(base_query, 'inspection_date').order_by('-inspection_date', '-created_at')
            data = []
            
            for r in records:
                common_data = {
                    'Date': str(r.inspection_date),
                    'Part Name': r.part_name or '',
                    'Part No.': r.part_no or '',
                    'Operation': r.operation or '',
                    'Lot Qty': r.lot_qty if r.lot_qty is not None else '',
                    'OK Qty': r.ok_qty if r.ok_qty is not None else '',
                    'Rej. Qty': r.rej_qty if r.rej_qty is not None else '',    
                    'Parameter/Specs': r.parameter_specs or '',                
                    'Insp. By': r.inspected_by or '',
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }

                checkpoints_list = [
                    ('BEFORE (RETROACTIVE)', 'Parameter 1', getattr(r, 'before_1', '')),
                    ('BEFORE (RETROACTIVE)', 'Parameter 2', getattr(r, 'before_2', '')),
                    ('BEFORE (RETROACTIVE)', 'Parameter 3', getattr(r, 'before_3', '')),
                    ('BEFORE (RETROACTIVE)', 'Parameter 4', getattr(r, 'before_4', '')),
                    ('BEFORE (RETROACTIVE)', 'Parameter 5', getattr(r, 'before_5', '')),
                    ('AFTER (SETUP APPROVAL)', 'Parameter 1', getattr(r, 'after_1', '')),
                    ('AFTER (SETUP APPROVAL)', 'Parameter 2', getattr(r, 'after_2', '')),
                    ('AFTER (SETUP APPROVAL)', 'Parameter 3', getattr(r, 'after_3', '')),
                    ('AFTER (SETUP APPROVAL)', 'Parameter 4', getattr(r, 'after_4', '')),
                    ('AFTER (SETUP APPROVAL)', 'Parameter 5', getattr(r, 'after_5', '')),
                ]

                for category, checkpoint_name, status_value in checkpoints_list:
                    row = common_data.copy() 
                    row['Category'] = category
                    row['Check Point'] = checkpoint_name
                    row['Status/Value'] = status_value or ''
                    data.append(row)
                    
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 3. 4M CHANGE RECORD ─────────────────────────────────────
    elif form_key == 'four-m-record':
        try:
            base_query = FourMChangeRecord.objects.all()
            records = apply_date_filter(base_query, 'created_at').order_by('-created_at')
            data = []
            
            for r in records:
                common_data = {
                    'Date': str(r.date) if getattr(r, 'date', None) else '',
                    'Time': str(r.time) if r.time else '',
                    'Machine No': r.machine_no or '',
                    'Part Info': r.part_info or '',
                    'Operation No': r.operation_no or '',
                    'Nature of Change': r.nature_of_change or '',
                    'Action Taken': r.action_taken or '',
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }

                parameters_list = [
                    ('Change Info', 'Description', r.description or ''),
                    ('4M Status', 'Man', getattr(r, 'status_man', '') or ''),
                    ('4M Status', 'Machine', getattr(r, 'status_machine', '') or ''),
                    ('4M Status', 'Material', getattr(r, 'status_material', '') or ''),
                    ('4M Status', 'Method', getattr(r, 'status_method', '') or ''),
                    ('Approval & Training', 'Setup Approval', r.setup_approval or ''),
                    ('Approval & Training', 'Training Provided', getattr(r, 'training_provided', '') or ''),
                    ('Retroactive', 'Qty Checked', getattr(r, 'retro_qty_checked', '') if getattr(r, 'retro_qty_checked', None) is not None else ''),
                    ('Retroactive', 'Qty OK', getattr(r, 'retro_qty_ok', '') or ''),
                    ('Retroactive', 'R/W', getattr(r, 'retro_rw', '') or ''),
                    ('Retroactive', 'Scrap', getattr(r, 'retro_scrap', '') or ''),
                    ('Containment', 'Qty Checked', getattr(r, 'cont_qty_checked', '') if getattr(r, 'cont_qty_checked', None) is not None else ''),
                    ('Containment', 'Qty OK', getattr(r, 'cont_qty_ok', '') or ''),
                    ('Containment', 'R/W', getattr(r, 'cont_rw', '') or ''),
                    ('Containment', 'Scrap', getattr(r, 'cont_scrap', '') or ''),
                    ('Dispatch', 'Customer', getattr(r, 'customer', '') or ''),
                    ('Dispatch', 'Date', str(r.dispatch_date) if getattr(r, 'dispatch_date', None) else ''),
                    ('Dispatch', 'Invoice No', getattr(r, 'invoice_no', '') or ''),
                    ('Footer', 'Remark', getattr(r, 'remark', '') or ''),
                ]

                for category, parameter, value in parameters_list:
                    row = common_data.copy()
                    row['Category'] = category
                    row['Check Point'] = parameter
                    row['Value / Status'] = value
                    data.append(row)
                    
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 4. TIP CHANGE DRESSING ──────────────────────────────────
    elif form_key == 'tip-change':
        try:
            base_query = TipChangeDressing.objects.all()
            records = apply_date_filter(base_query, 'date').order_by('-date')
            data = [{
                'Date': str(r.date),
                'Plant': r.plant or 'N/A',
                'Machine Name': r.machine_name or 'N/A',
                'Machine No': r.machine_no or 'N/A',
                'Part Name': r.part_name,
                'Operation': r.operation or 'N/A',
                'Prod Qty': r.prd_qty,
                'Tip Change': r.tip_change,
                'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
            } for r in records]
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 5. BIN TROLLEY REPORT ───────────────────────────────────
    elif form_key == 'bin-trolley':
        try:
            base_query = BinTrolleyReport.objects.all()
            records = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for r in records:
                def parse_json(field_data):
                    if isinstance(field_data, str):
                        try: return json.loads(field_data)
                        except: return {}
                    return field_data or {}

                cp = parse_json(r.checkpoints)
                cl = parse_json(r.cleaning_details)
                mn = parse_json(r.maintenance_details)

                common_data = {
                    'Date': str(r.date),
                    'Week': r.week or '',
                    'Month': r.month or '',
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }

                items_list = [
                    ('Checkpoints', '1. Bin/Trolley Should Be Clean Properly', cp.get('cp1', {}).get('status', ''), cp.get('cp1', {}).get('remarks', '')),
                    ('Checkpoints', '2. Bin/Trolley Should be Free From Dust', cp.get('cp2', {}).get('status', ''), cp.get('cp2', {}).get('remarks', '')),
                    ('Checkpoints', '3. Bin/Trolley Should Not Be Damage And Broken', cp.get('cp3', {}).get('status', ''), cp.get('cp3', {}).get('remarks', '')),
                    ('Checkpoints', '4. Bin/Trolley Should be Free From Oil Surface', cp.get('cp4', {}).get('status', ''), cp.get('cp4', {}).get('remarks', '')),
                    ('Checkpoints', '5. Bin/Trolley Should Be Clean In Bin Cleaning Area', cp.get('cp5', {}).get('status', ''), cp.get('cp5', {}).get('remarks', '')),
                    ('Checkpoints', '6. Others (Please Specify)', cp.get('cp6', {}).get('status', ''), cp.get('cp6', {}).get('remarks', '')),
                    ('Cleaning Details', 'Total Bin / Trolley Quantity', cl.get('cd1', {}).get('frequency', ''), cl.get('cd1', {}).get('remarks', '')),
                    ('Cleaning Details', 'Bin / Trolley Clean Quantity', cl.get('cd2', {}).get('frequency', ''), cl.get('cd2', {}).get('remarks', '')),
                    ('Cleaning Details', 'Bin / Trolley Unclean Quantity', cl.get('cd3', {}).get('frequency', ''), cl.get('cd3', {}).get('remarks', '')),
                    ('Maintenance Details', 'Total Maintenance Quantity', mn.get('md1', {}).get('frequency', ''), mn.get('md1', {}).get('remarks', '')),
                    ('Maintenance Details', 'Bin/Trolley Ok Quantity', mn.get('md2', {}).get('frequency', ''), mn.get('md2', {}).get('remarks', '')),
                    ('Maintenance Details', 'Bin/Trolley Reject Quantity', mn.get('md3', {}).get('frequency', ''), mn.get('md3', {}).get('remarks', '')),
                ]

                for category, detail_name, value, remarks in items_list:
                    row = common_data.copy()
                    row['Category'] = category
                    row['Check Point / Detail'] = detail_name
                    row['Status / Qty'] = value
                    row['Remarks'] = remarks
                    data.append(row)
                
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)
            
    # ── 6. 5S CHECKSHEET REPORT ─────────────────────────────────
    elif form_key == 'five-s-view':
        try:
            base_query = FiveSChecksheetReport.objects.prefetch_related('observations')
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                observations = report.observations.all()
                if observations.exists():
                    for obs in observations:
                        data.append({
                            'Date': str(report.date),
                            'Zone Leader': report.zone_leader,
                            'Area': report.area,
                            'Category (S)': obs.s_category,
                            'Check Point': obs.check_point,
                            'Status': obs.status,
                            'OK Count': report.ok_count,
                            'NG Count': report.ng_count,
                            'Prepared By': log_mapping.get(report.id, {}).get('prepared_by', '—'),
                            'Approved By': log_mapping.get(report.id, {}).get('approved_by', '—'),
                        })
                else:
                    data.append({
                        'Date': str(report.date),
                        'Zone Leader': report.zone_leader,
                        'Area': report.area,
                        'Category (S)': '—',
                        'Check Point': '—',
                        'Status': '—',
                        'OK Count': report.ok_count,
                        'NG Count': report.ng_count,
                        'Prepared By': log_mapping.get(report.id, {}).get('prepared_by', '—'),
                        'Approved By': log_mapping.get(report.id, {}).get('approved_by', '—'),
                     })
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 7. MONTHLY PRODUCTION PLAN ──────────────────────────────
    elif form_key == 'monthly-prod-plan':
        try:
            base_query = MonthlyProductionPlan.objects.all()
            reports = apply_date_filter(base_query, 'created_at').order_by('-created_at')
            data = []
            for r in reports:
                data.append({
                    'Date': str(r.filled_date) if r.filled_date else '',
                    'Part Name': r.part_name or '',
                    'Customer Name': r.customer_name or '',
                    'Opening Stock': r.opening_stock,
                    'Schedule Qty': r.schedule_qty,
                    'Planned Qty': r.planned_qty,
                    'Remark': r.remark or '',
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—')
                })
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 8. OPERATOR OBSERVANCE CHECKLIST ────────────────────────
    elif form_key == 'operator-observance-checklist':
        try:
            base_query = OperatorObservanceChecklist.objects.all()
            reports = apply_date_filter(base_query, 'record_date').order_by('-created_at')
            data = []
            effect_map = {'100': 'High', '50': 'Medium', 'low': 'Low'}
            
            for r in reports:
                common_data = {
                    'Date': str(r.record_date) if r.record_date else '',
                    'Operator Name': r.operator_name or '',
                    'Model': r.model or '',
                    'Part / Operation': r.part_operation or '',
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }
                
                checkpoints = r.checkpoints
                if isinstance(checkpoints, str):
                    try: checkpoints = json.loads(checkpoints)
                    except: checkpoints = []
                checkpoints = checkpoints if isinstance(checkpoints, list) else []

                if not checkpoints:
                    data.append(common_data)
                else:
                    for cp in checkpoints:
                        row = common_data.copy()
                        row['Task / Check Point'] = cp.get('task', '')
                        row['Response'] = cp.get('response', '').title()
                        row['Training'] = 'Yes' if cp.get('training') else 'No'
                        raw_effect = str(cp.get('effect', '')).lower()
                        row['Effectiveness'] = effect_map.get(raw_effect, cp.get('effect', ''))
                        row['Remarks'] = cp.get('remarks', '')
                        data.append(row)
                        
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 9. OPERATOR OBSERVANCE PLAN ─────────────────────────────
    elif form_key == 'operator-observance-plan':
        try:
            base_query = OperatorObservancePlan.objects.all()
            reports = apply_date_filter(base_query, 'created_at').order_by('-created_at')
            data = []
            for r in reports:
                common_data = {
                    'Plan Period': f"{r.plan_month} {r.plan_year}",
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }
                
                operators = r.operators_data
                if isinstance(operators, str):
                    try: operators = json.loads(operators)
                    except: operators = []
                operators = operators if isinstance(operators, list) else []

                if not operators:
                    data.append(common_data)
                else:
                    for op in operators:
                        row = common_data.copy()
                        row['Operator Name'] = op.get('name', '')
                        row['Department'] = op.get('dept', '')
                        row['Status'] = op.get('status', '')
                        data.append(row)
                        
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 10. PM CHECKLIST MHE ─────────────────────────────────────
    elif form_key == 'pm-checklist-mhe':
        try:
            base_query = PMChecklistMHE.objects.all()
            reports = apply_date_filter(base_query, 'filled_date').order_by('-created_at')
            data = []
            for r in reports:
                common_data = {
                    'Date': str(r.filled_date) if r.filled_date else '',
                    'Part Name / No': r.part_name or '',
                    'Trolley No': r.trolley_no or '',
                    'PM Frequency': r.pm_frequency or '',
                    'Checked By': r.checked_by or '',
                    'Verified By': r.verified_by or '',
                    'General Remarks': r.general_remarks or '',
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }
                
                checkpoints = r.checkpoints
                if isinstance(checkpoints, str):
                    try: checkpoints = json.loads(checkpoints)
                    except: checkpoints = []
                checkpoints = checkpoints if isinstance(checkpoints, list) else []

                if not checkpoints:
                    data.append(common_data)
                else:
                    for cp in checkpoints:
                        row = common_data.copy()
                        row['Sr No'] = cp.get('id', '')
                        row['Check Point Task'] = cp.get('task', '')
                        row['Done On'] = cp.get('doneOn', '')
                        row['Status'] = cp.get('status', '')
                        row['Remarks'] = cp.get('remarks', '')
                        data.append(row)
                        
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)
    
    # ── 11. PROJECTION WELDER QUALIFICATION ───────────────────────
    elif form_key == 'projection-welder':
        try:
            base_query = ProjectionWelderQual.objects.all()
            records = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for r in records:
                common_data = {
                    'Date': str(r.date) if r.date else '',
                    'WPS No': r.wps_no or '',
                    'Process': r.welding_process or '',
                    'Base Metal': r.base_metal or '',
                    'Thickness': r.base_metal_thickness or '',
                    'Machine No': r.machine_no or '',
                    'Welder Name': r.welder_name or '',
                    'Status': r.qualification_status or '',
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }
                
                trials = r.trials
                if isinstance(trials, str):
                    try: trials = json.loads(trials)
                    except: trials = []
                trials = trials if isinstance(trials, list) else []

                if not trials:
                    data.append(common_data)
                else:
                    for i, t in enumerate(trials):
                        category = f"Trial {i + 1}"
                        parameters_list = [
                            ('Squeeze Time', t.get('squeeze', '')),
                            ('Weld Time', t.get('weld', '')),
                            ('Hold Time', t.get('hold', '')),
                            ('Off Time', t.get('off', '')),
                            ('Current (Amp/KA)', t.get('current', '')),
                            ('Pressure (Bar)', t.get('pressure', '')),
                            ('Torque', t.get('torque', '')),
                            ('Visual', t.get('visual', '')),
                        ]
                        for param, val in parameters_list:
                            row = common_data.copy()
                            row['Category'] = category
                            row['Parameter'] = param
                            row['Value / Status'] = val
                            data.append(row)
                            
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 12. SPOT WELDER QUALIFICATION ─────────────────────────────
    elif form_key == 'spot-welder':
        try:
            base_query = SpotWelderQual.objects.all()
            records = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for r in records:
                common_data = {
                    'Date': str(r.date) if r.date else '',
                    'WPS No': r.wps_no or '',
                    'Gun Type': r.gun_type or '',
                    'Base Metal': r.base_metal or '',
                    'Thickness': r.base_metal_thickness or '',
                    'Machine No': r.machine_no or '',
                    'Welder Name': r.welder_name or '',
                    'Status': r.qualification_status or '',
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }
                
                trials = r.trials
                if isinstance(trials, str):
                    try: trials = json.loads(trials)
                    except: trials = []
                trials = trials if isinstance(trials, list) else []

                if not trials:
                    data.append(common_data)
                else:
                    for i, t in enumerate(trials):
                        category = f"Trial {i + 1}"
                        parameters_list = [
                            ('Squeeze Time', t.get('squeeze', '')),
                            ('Weld Time', t.get('weld', '')),
                            ('Hold Time', t.get('hold', '')),
                            ('Off Time', t.get('off', '')),
                            ('Current (Amp)', t.get('current', '')),
                            ('Pressure (Bar)', t.get('pressure', '')),
                            ('Strength (Nugget)', t.get('strength', '')),
                            ('Visual', t.get('visual', '')),
                        ]
                        for param, val in parameters_list:
                            row = common_data.copy()
                            row['Category'] = category
                            row['Parameter'] = param
                            row['Value / Status'] = val
                            data.append(row)

            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 13. TIG/MIG WELDER QUALIFICATION ──────────────────────────
    elif form_key == 'tig-mig-welder':
        try:
            base_query = TigMigWelderQual.objects.all()
            records = apply_date_filter(base_query, 'testing_date').order_by('-created_at')
            data = []
            for r in records:
                common_data = {
                    'Testing Date': str(r.testing_date) if r.testing_date else '',
                    'WPS No': r.wps_no or '',
                    'Process': r.welding_process or '',
                    'Machine No': r.machine_no or '',
                    'Base Metal': r.base_metal or '',
                    'Thickness': r.base_metal_thickness or '',
                    'Filler Material': r.filler_material or '',
                    'Shielding Gas': r.shielding_gas or '',
                    'Welder Name': r.welder_name or '',
                    'Status': r.qualification_status or '',
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }
                
                trials = r.trials
                if isinstance(trials, str):
                    try: trials = json.loads(trials)
                    except: trials = []
                trials = trials if isinstance(trials, list) else []

                if not trials:
                    data.append(common_data)
                else:
                    for i, t in enumerate(trials):
                        category = f"Trial {i + 1}"
                        parameters_list = [
                            ('Current Range', t.get('currRange', '')),
                            ('Current Actual', t.get('currActual', '')),
                            ('Voltage Range', t.get('voltRange', '')),
                            ('Voltage Actual', t.get('voltActual', '')),
                            ('Gas Flow Range', t.get('gasRange', '')),
                            ('Gas Flow Actual', t.get('gasActual', '')),
                            ('Deposit Thickness', t.get('depositThickness', '')),
                            ('Defect', t.get('defect', '')),
                        ]
                        for param, val in parameters_list:
                            row = common_data.copy()
                            row['Category'] = category
                            row['Parameter'] = param
                            row['Value / Status'] = val
                            data.append(row)

            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 14. PROCESS VALIDATION REPORT ─────────────────────────────
    elif form_key == 'process-validation':
        try:
            base_query = ProcessValidation.objects.all()
            records = apply_date_filter(base_query, 'validation_date').order_by('-created_at')
            data = []
            for r in records:
                ops = r.operators
                if isinstance(ops, str):
                    try: ops = json.loads(ops)
                    except: ops = []
                operators_str = ", ".join([op for op in ops if op]) if isinstance(ops, list) else ""

                common_data = {
                    'Validation Date': str(r.validation_date) if r.validation_date else '',
                    'Process Name': r.process_name or '',
                    'Machine No': r.machine_no or '',
                    'Part Name': r.part_name or '',
                    'Operators': operators_str,
                    'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                    'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                }

                trials = r.trials
                if isinstance(trials, str):
                    try: trials = json.loads(trials)
                    except: trials = []
                trials = trials if isinstance(trials, list) else []

                if not trials:
                    data.append(common_data)
                else:
                    for i, t in enumerate(trials):
                        category = f"Trial {i + 1}"
                        parameters_list = [
                            ('Parameter 1 (P1)', t.get('p1', '')),
                            ('Parameter 2 (P2)', t.get('p2', '')),
                            ('Parameter 3 (P3)', t.get('p3', '')),
                            ('Parameter 4 (P4)', t.get('p4', '')),
                            ('Parameter 5 (P5)', t.get('p5', '')),
                            ('Parameter 6 (P6)', t.get('p6', '')),
                            ('Parameter 7 (P7)', t.get('p7', '')),
                            ('Parameter 8 (P8)', t.get('p8', '')),
                            ('Characteristics Spec', t.get('specified', '')),
                            ('Characteristics Obs', t.get('observed', '')),
                            ('Decision', t.get('decision', '')),
                        ]

                        for param, val in parameters_list:
                            row = common_data.copy()
                            row['Category'] = category
                            row['Parameter'] = param
                            row['Value / Status'] = val
                            data.append(row)

            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 15. 4M DISPLAY BOARD ──────────────────────────────────────
    elif form_key == 'four-m-display':
        try:
            base_query = FourMDisplay.objects.all()
            records = apply_date_filter(base_query, 'date_filled').order_by('-created_at')
            data = [{
                'Machine No': r.machine_no or '',
                'Operator Name': r.operator_name or '',
                'Man': r.man or '',
                'Machine': r.machine or '',
                'Material': r.material or '',
                'Method': r.method or '',
                'Date': str(r.date_filled) if r.date_filled else '',
                'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
            } for r in records]
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 16. 4M SUMMARY SHEET ──────────────────────────────────────
    elif form_key == 'four-m-summary':
        try:
            base_query = FourMSummary.objects.all()
            records = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = [{
                'Date': str(r.date) if r.date else '',
                'Customer': r.customer or '',
                'Part Name & No': r.part_name_no or '',
                'Type of Change': r.type_of_change or '',
                'Change Detail': r.change_detail or '',
                'Retro Total Qty': r.retro_total_qty if r.retro_total_qty is not None else '',
                'Retro OK Qty': r.retro_ok_qty if r.retro_ok_qty is not None else '',
                'Retro Rej. Qty': r.retro_rej_qty if r.retro_rej_qty is not None else '',
                'Status After Final Insp.': r.status_after_final or '',
                'Action for NG Material': r.action_for_ng or '',
                'Sup. Signature': r.sup_signature or '',
                'Sign Prod. Head': r.sign_prod_head or '',
                'Sign QA Head': r.sign_qa_head or '',
                'Remarks': r.remarks or '',
                'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
            } for r in records]
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # ── 17. 4M INFORMATION SHEET ──────────────────────────────────
    elif form_key == 'four-m-information':
        try:
            base_query = FourMInformationSheet.objects.all()
            records = apply_date_filter(base_query, 'date_filled').order_by('-created_at')
            data = [{
                'S.No': r.s_no if r.s_no is not None else '',
                'Time': str(r.time) if r.time else '',
                'Machine No': r.machine_no or '',
                'Operator Name': r.operator_name or '',
                'Man': r.man or '',
                'Machine': r.machine or '',
                'Material': r.material or '',
                'Method': r.method or '',
                'Change Description': r.change_description or '',
                'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
                'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
                'Date': str(r.date_filled) if r.date_filled else '',
                'created_at': r.created_at.isoformat() if r.created_at else None,
            } for r in records]
            return JsonResponse({'data': data})
        except Exception as e:
            return JsonResponse({'data': [], 'error': str(e)}, status=500)

    # Agar koi galat form_key aati hai
    return JsonResponse({'data': [], 'error': 'Production form type not supported'}, status=400)

import json
import traceback
from datetime import datetime
import pytz
import logging
import time

from django.core.exceptions import ObjectDoesNotExist
from django.db import connection, transaction
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.cache import never_cache

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view

from api.services.report_logging import auto_log_report
from api.utils.date_filters import apply_date_filter

# ==============================================================================
# IMPORTS FROM MAIN API APP
# ==============================================================================
from api.models import (
    BinTrolleyReport, MachineChecksheetReport, MachineChecksheetObservation,
    TipChangeDressing, ReworkEntry, FiveSChecksheetReport, FiveSChecksheetObservation,
    DailyProductionPlan, FourMChangeInspection, FourMChangeRecord,
    MonthlyProductionPlan, OperatorObservanceChecklist, OperatorObservancePlan,
    PMChecklistMHE, ProjectionWelderQual, SpotWelderQual, TigMigWelderQual, ProcessValidation, FourMDisplay, FourMSummary,
    ReportActivityLog, FourMInformationSheet, QANotification
)

from api.serializers import (
    TipChangeDressingSerializer, DailyProductionPlanSerializer,
    FourMChangeInspectionSerializer, FourMChangeRecordSerializer, FourMSummarySerializer,FourMInformationSheetSerializer
)


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def clean_val(val, default=None):
    return val if val != '' and val is not None else default

# ==============================================================================
# ✅ PRODUCTION HUB ROUTE MAPPING + AUTO ACTIVITY LOG & NOTIFICATION ROUTER
# ==============================================================================

# ==============================================================================
# 🏭 DAILY PRODUCTION APIs (Save APIs updated with auto_log_report)
# ==============================================================================

class SaveBinTrolleyReportView(APIView):
    def post(self, request):
        try:
            data = request.data
            report = BinTrolleyReport.objects.create(
                date=data.get('date'),
                week=data.get('week', ''),
                month=data.get('month', ''),
                checkpoints=data.get('checkpoints', {}),
                cleaning_details=data.get('cleaning_details', {}),
                maintenance_details=data.get('maintenance_details', {})
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "Bin Trolley Form", report.id)
            return Response({"success": True, "message": "✅ Bin Trolley Data Saved!", "record_id": report.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveMachineChecksheetView(APIView):
    @transaction.atomic  
    def post(self, request):
        try:
            data = request.data
            report = MachineChecksheetReport.objects.create(
                date=data.get('date', timezone.now().date()),
                plant_name=data.get('plant_name', 'Plant 1'),
                machine_no=data.get('machine_no', ''),
                checked_by_maintenance=data.get('checked_by_maintenance', ''),
                verified_by_production=data.get('verified_by_production', '')
            )

            check_points_data = data.get('check_points', [])
            observations = [
                MachineChecksheetObservation(
                    report=report,
                    s_no=item.get('s_no', index + 1),
                    poka_yoke_detail=item.get('poka_yoke_detail', ''),
                    checking_method=item.get('checking_method', ''),
                    reference_sop=item.get('reference_sop', ''),
                    is_ok=item.get('is_ok', True),
                    remarks=item.get('remarks', '')
                ) for index, item in enumerate(check_points_data)
            ]
            
            if observations:
                MachineChecksheetObservation.objects.bulk_create(observations)

            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "Machine Checksheet", report.id)
            return Response({"success": True, "message": "✅ Daily Checksheet Saved Successfully!", "report_id": report.id, "record_id": report.id}, status=status.HTTP_201_CREATED)

        except Exception as e:
            print("❌ Django Error (Checksheet Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveTipChangeView(APIView):
    def post(self, request):
        try:
            serializer = TipChangeDressingSerializer(data=request.data)
            if serializer.is_valid():
                obj = serializer.save()
                # 🔥 AUTO LOG BANAO
                auto_log_report(request.data.get('submitted_by'), "Tip Change Monitor Form", obj.id)
                return Response({"success": True, "message": "✅ Tip Change & Dressing data saved successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": "Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            print("❌ Django Error (Tip Change Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR) 

class SaveReworkReportView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            date_val = data.get('date')
            remark_val = data.get('remark')
            items = data.get('items', [])
            
            entries_to_create = []
            for row in items:
                dynamic_data = {
                    "status": row.get('status', ''),
                    "observations": row.get('observations', [])
                }
                entries_to_create.append(
                    ReworkEntry(
                        date=date_val, remark=remark_val,
                        part_name=row.get('part_name', ''),
                        part_no=row.get('part_no', ''),
                        spec=row.get('spec', ''),
                        non_conformance=row.get('non_conformance', ''),
                        rework_qty=int(row.get('rework_qty') or 0),
                        inspected_by=row.get('inspected_by', ''),
                        dynamic_details=dynamic_data
                    )
                )
            
            last_id = None
            if entries_to_create:
                ReworkEntry.objects.bulk_create(entries_to_create)
                last_record = ReworkEntry.objects.last()
                last_id = last_record.id if last_record else None
                
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "Rework Report", last_id)
            return Response({"success": True, "message": "✅ Rework Data Saved!", "record_id": last_id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class CheckFiveSStatusView(APIView):
    def get(self, request):
        try:
            date = request.query_params.get('date')
            area_param = request.query_params.get('area')
            is_filled = FiveSChecksheetReport.objects.filter(date=date, area=area_param).exists()
            return Response({'isFilled': is_filled}, status=200)
        except Exception as e:
            return Response({'error': str(e)}, status=500)

class SaveFiveSReportView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            summary = data.get('summary', {})
            
            report = FiveSChecksheetReport.objects.create(
                area=data.get('area', ''),
                zone_leader=data.get('zoneLeader', ''),
                date=data.get('date'),
                language=data.get('language', 'eng'),
                total_checks=int(summary.get('totalChecks', 0)),
                ok_count=int(summary.get('okCount', 0)),
                ng_count=int(summary.get('ngCount', 0))
            )
            
            checks_data = data.get('checks', [])
            obs_to_create = []
            for s_group in checks_data:
                s_category_name = s_group.get('s', '')
                for pt in s_group.get('points', []):
                    obs_to_create.append(
                        FiveSChecksheetObservation(
                            report=report, s_category=s_category_name, check_point=pt.get('point', ''), status=pt.get('status', 'Not Checked')
                        )
                    )
            
            if obs_to_create:
                FiveSChecksheetObservation.objects.bulk_create(obs_to_create)
                
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "5S Checksheet", report.id)
            return Response({"success": True, "message": "✅ 5S Checksheet Saved!", "record_id": report.id}, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            print("Error:", str(e))
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveDailyProductionPlanView(APIView):
    report_name = "Daily Production plan"
    def post(self, request):
        try:
            serializer = DailyProductionPlanSerializer(data=request.data)
            if serializer.is_valid():
                obj = serializer.save()
                # 🔥 AUTO LOG BANAO
                auto_log_report(request.data.get('submitted_by'), "Daily Prod Form", obj.id)
                return Response({"success": True, "message": "Daily Production Plan saved successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": "Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print(" Django Error (Daily Production Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@never_cache
@api_view(['GET'])
def get_today_production_data(request):
    report_name = "Daily_production_plan"
    try:
        plant_name = request.GET.get('plant_name')
        date_str = request.GET.get('date')

        if not plant_name:
            return Response({'success': False, 'error': 'plant_name is required'}, status=400)

        filter_date = datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else datetime.now().date()
        queryset = DailyProductionPlan.objects.filter(plant=plant_name, created_at__date=filter_date)
        filled_machines = list(queryset.values_list('machine_no', flat=True))

        return Response({'success': True, 'filled_machines': filled_machines, 'date': str(filter_date)})
    except Exception as e:
        return Response({'success': False, 'error': str(e)}, status=500)

class UpdateDailyProductionPlanView(APIView):
    def patch(self, request, pk):
        try:
            plan = get_object_or_404(DailyProductionPlan, pk=pk)
            serializer = DailyProductionPlanSerializer(plan, data=request.data, partial=True)
            if serializer.is_valid():
                obj = serializer.save()
                return Response({"success": True, "message": "Production details updated successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_200_OK)
            return Response({"success": False, "error": "Update Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print(" Django Error (Daily Production Update): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveFourMChangeInspectionView(APIView):
    def post(self, request):
        try:
            serializer = FourMChangeInspectionSerializer(data=request.data)
            if serializer.is_valid():
                obj = serializer.save()
                # 🔥 AUTO LOG BANAO
                auto_log_report(request.data.get('submitted_by'), "4M Change Inspection", obj.id)
                return Response({"success": True, "message": " 4M Change Inspection data saved successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": "Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print("Django Error (4M Change Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveFourMChangeRecordView(APIView):
    def post(self, request):
        try:
            serializer = FourMChangeRecordSerializer(data=request.data)
            if serializer.is_valid():
                obj = serializer.save() 
                # 🔥 AUTO LOG BANAO
                auto_log_report(request.data.get('submitted_by'), "4M Tracking Record", obj.id)
                return Response({"success": True, "message": " 4M Change Record saved successfully!", "data": serializer.data, "record_id": obj.id}, status=status.HTTP_201_CREATED)
            return Response({"success": False, "error": "Validation failed", "details": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print("Django Error (4M Change Record Save): ", str(e))
            traceback.print_exc()
            return Response({"success": False, "error": "Server connection failed: " + str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)  

class SaveFourMDisplayView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            entries = data.get('entries', [])

            for entry in entries:
                FourMDisplay.objects.create(
                    s_no=entry.get('s_no'),
                    machine_no=entry.get('machine_no', ''),
                    operator_name=entry.get('operator_name', ''),
                    man=entry.get('man', ''),
                    machine=entry.get('machine', ''),
                    material=entry.get('material', ''),
                    method=entry.get('method', '')
                )
            
            last_record = FourMDisplay.objects.last()
            last_id = last_record.id if last_record else None
            
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by'), "4M Display Board", last_id)
            return Response({"success": True, "message": " 4M Display Board Saved!", "record_id": last_id}, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            print(f"4M Display Board Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveFourMSummaryView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            prepared_by = data.get('prepared_by', '')
            approved_by = data.get('approved_by', '')
            entries = data.get('entries', [])

            for entry in entries:
                entry['prepared_by'] = prepared_by
                entry['approved_by'] = approved_by
                if not entry.get('date'):
                    entry.pop('date', None)

            serializer = FourMSummarySerializer(data=entries, many=True)
            if serializer.is_valid():
                objs = serializer.save()
                last_id = objs[-1].id if objs else None
                # 🔥 AUTO LOG BANAO
                auto_log_report(prepared_by, "4M Summary Sheet", last_id)
                return Response(
                    {"success": True, "message": "4M Summary Sheet Saved Successfully!", "record_id": last_id}, 
                    status=status.HTTP_201_CREATED
                )
            else:
                return Response(
                    {"success": False, "error": serializer.errors}, 
                    status=status.HTTP_400_BAD_REQUEST
                )

        except Exception as e:
            print(f"4M Summary Board Error: {str(e)}")
            return Response(
                {"success": False, "error": str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# ==============================================================================
# 📅 MONTHLY PRODUCTION APIs (Save APIs updated with auto_log_report)
# ==============================================================================

class SaveMonthlyProdPlanView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            plan = MonthlyProductionPlan.objects.create(
                filled_date=clean_val(data.get('date')),
                part_name=data.get('partName', ''),
                customer_name=data.get('customer', ''),
                opening_stock=clean_val(data.get('openingStock'), 0),
                schedule_qty=clean_val(data.get('scheduleQty'), 0),
                planned_qty=clean_val(data.get('plannedQty'), 0),
                remark=data.get('remark', ''),
                prepared_by=data.get('preparedBy', ''),
                approved_by=data.get('approvedBy', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('preparedBy'), "Monthly Prod Plan", plan.id)
            return Response({"success": True, "message": " Monthly Production Plan Data Saved!", "record_id": plan.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            print(f"Monthly Prod Plan Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveOperatorObservanceChecklistView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            checklist = OperatorObservanceChecklist.objects.create(
                record_date=clean_val(data.get('recordDate')), 
                operator_name=data.get('operatorName', ''),
                model=data.get('model', ''),
                part_operation=data.get('partOperation', ''),
                checkpoints=data.get('formData', []), 
                prepared_by=data.get('preparedBy', ''),
                approved_by=data.get('approvedBy', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('preparedBy'), "Operator Observance Checklist", checklist.id)
            return Response({"success": True, "message": " Operator Observance Checklist Saved!", "record_id": checklist.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            print(f"Observance Checklist Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveOperatorObservancePlanView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            plan = OperatorObservancePlan.objects.create(
                plan_year=data.get('selectedYear', ''),
                plan_month=data.get('selectedMonth', ''),
                operators_data=data.get('operators', []), 
                prepared_by=data.get('preparedBy', ''),
                approved_by=data.get('approvedBy', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('preparedBy'), "Operator Observance Plan", plan.id)
            return Response({"success": True, "message": f" Operator Observance Plan Saved!", "record_id": plan.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            print(f"Observance Plan Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SavePMChecklistMHEView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            pm_form = PMChecklistMHE.objects.create(
                filled_date=clean_val(data.get('filledDate')), 
                part_name=data.get('partName', ''),
                trolley_no=data.get('trolleyNo', ''),
                pm_frequency=data.get('pmFrequency', ''),
                checkpoints=data.get('checkPoints', []), 
                checked_by=data.get('checkedBy', ''),
                verified_by=data.get('verifiedBy', ''),
                general_remarks=data.get('generalRemarks', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('checkedBy'), "PM Checklist MHE", pm_form.id)
            return Response({"success": True, "message": " Preventive Maintenance MHE Data Saved!", "record_id": pm_form.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            print(f"PM Checklist MHE Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveProjectionWelderView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            welder = ProjectionWelderQual.objects.create(
                wps_no=data.get('wpsNo', ''),
                date=clean_val(data.get('date')),
                welding_process=data.get('weldingProcess', 'PROJECTION WELDING'),
                base_metal=data.get('baseMetal', ''),
                base_metal_thickness=data.get('baseMetalThickness', ''),
                machine_no=data.get('machineNo', ''),
                trials=data.get('trials', []),
                welder_name=data.get('welderName', ''),
                conducted_by=data.get('conductedBy', ''),
                verified_by=data.get('verifiedBy', ''),
                qualification_status=data.get('qualificationStatus', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('conductedBy'), "Projection Welder", welder.id)
            return Response({"success": True, "message": "Projection Welder Data Saved!", "record_id": welder.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveSpotWelderView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            welder = SpotWelderQual.objects.create(
                wps_no=data.get('wpsNo', ''),
                date=clean_val(data.get('date')),
                welding_process=data.get('weldingProcess', 'Spot Welding'),
                base_metal=data.get('baseMetal', ''),
                base_metal_thickness=data.get('baseMetalThickness', ''),
                machine_no=data.get('machineNo', ''),
                gun_type=data.get('gunType', ''),
                trials=data.get('trials', []),
                welder_name=data.get('welderName', ''),
                conducted_by=data.get('conductedBy', ''),
                verified_by=data.get('verifiedBy', ''),
                qualification_status=data.get('qualificationStatus', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('conductedBy'), "Spot Welder", welder.id)
            return Response({"success": True, "message": "Spot Welder Data Saved!", "record_id": welder.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveTigMigWelderView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            welder = TigMigWelderQual.objects.create(
                wps_no=data.get('wpsNo', ''),
                testing_date=clean_val(data.get('testingDate')),
                welding_process=data.get('weldingProcess', ''),
                machine_no=data.get('machineNo', ''),
                base_metal=data.get('baseMetal', ''),
                base_metal_thickness=data.get('baseMetalThickness', ''),
                base_metal_size=data.get('baseMetalSize', ''),
                welding_position=data.get('weldingPosition', ''),
                filler_material=data.get('fillerMaterial', ''),
                filler_material_size=data.get('fillerMaterialSize', ''),
                shielding_gas=data.get('shieldingGas', ''),
                wire_feed_speed=data.get('wireFeedSpeed', ''),
                trials=data.get('trials', []),
                test_results=data.get('testResults', {}),
                welder_name=data.get('welderName', ''),
                conducted_by=data.get('conductedBy', ''),
                verified_by=data.get('verifiedBy', ''),
                qualification_status=data.get('qualificationStatus', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('conductedBy'), "TIG/MIG Welder", welder.id)
            return Response({"success": True, "message": "MIG/TIG Welder Data Saved!", "record_id": welder.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveProcessValidationView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            validation = ProcessValidation.objects.create(
                validation_date=clean_val(data.get('validationDate')),
                revalidation_date=clean_val(data.get('revalidationDate')),
                process_name=data.get('processName', ''),
                material_details=data.get('materialDetails', ''),
                machine_no=data.get('machineNo', ''),
                process_owner=data.get('processOwner', ''),
                part_name=data.get('partName', ''),
                fixture_no=data.get('fixtureNo', ''),
                operators=data.get('operators', []),
                parameters=data.get('parameters', []),
                trials=data.get('trials', []),
                final_params=data.get('finalParams', []),
                conclusion=data.get('conclusion', ''),
                prepared_by=data.get('preparedBy', ''),
                approved_by=data.get('approvedBy', '')
            )
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('preparedBy'), "Process Validation", validation.id)
            return Response({"success": True, "message": "Process Validation Data Saved!", "record_id": validation.id}, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
class SaveFourMInformationSheetView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            entries = data.get('entries', [])
            prepared_by_val = data.get('preparedBy', data.get('prepared_by', ''))
            
            last_id = None
            for entry in entries:
                if not entry.get('time'):
                    entry['time'] = None
                entry['prepared_by'] = prepared_by_val
                serializer = FourMInformationSheetSerializer(data=entry)
                
                if serializer.is_valid():
                    saved_instance = serializer.save()
                    last_id = saved_instance.id
                else:
                    return Response({"success": False, "error": str(serializer.errors)}, status=status.HTTP_400_BAD_REQUEST)
            
            # 🔥 AUTO LOG BANAO
            auto_log_report(data.get('submitted_by') or prepared_by_val, "4M Information Sheet", last_id)
            return Response({"success": True, "message": "Information Sheet Saved Successfully!", "record_id": last_id}, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            print(f"Information Sheet Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# # ==============================================================================
# # 🔥 NEW CLEANED UP: SINGLE REPORT FETCH API (REGISTRY PATTERN)
# # ==============================================================================
# @api_view(['GET'])
# def get_single_production_report_view(request, form_key, report_id):
#     try:
#         log_entry = get_object_or_404(ReportActivityLog, id=report_id)
#         submitted_user = log_entry.username
#         rec_id = log_entry.record_id

#         if not rec_id:
#             return Response({"success": False, "error": "No Record ID attached to this notification."}, status=404)

#         # ── 🔥 CUSTOM FORMATTERS ──
#         def get_machine_checksheet(r):
#             obs = MachineChecksheetObservation.objects.filter(report=r)
#             obs_data = [{"s_no": o.s_no, "poka_yoke_detail": o.poka_yoke_detail, "checking_method": o.checking_method, "reference_sop": o.reference_sop, "is_ok": o.is_ok, "remarks": o.remarks} for o in obs]
#             return {"date": str(r.date), "plant_name": r.plant_name, "machine_no": r.machine_no, "checked_by_maintenance": r.checked_by_maintenance, "verified_by_production": r.verified_by_production, "check_points": obs_data}

#         def get_fives_checksheet(r):
#             obs = FiveSChecksheetObservation.objects.filter(report=r)
#             obs_data = [{"s_category": o.s_category, "check_point": o.check_point, "status": o.status} for o in obs]
#             return {"area": r.area, "zoneLeader": r.zone_leader, "date": str(r.date), "language": r.language, "totalChecks": r.total_checks, "okCount": r.ok_count, "ngCount": r.ng_count, "observations": obs_data}

#         # ── 🔥 THE REGISTRY ──
#         FORM_REGISTRY = {
#             'daily-prod-plan': lambda r: DailyProductionPlanSerializer(r).data,
#             'four-m-inspection': lambda r: FourMChangeInspectionSerializer(r).data,
#             'four-m-record': lambda r: FourMChangeRecordSerializer(r).data,
#             'tip-change': lambda r: TipChangeDressingSerializer(r).data,
#             'four-m-summary': lambda r: FourMSummarySerializer(r).data,
#             'machine-checksheet': get_machine_checksheet,
#             'five-s': get_fives_checksheet,
#             'five-s-view': get_fives_checksheet,
#             'bin-trolley': lambda r: {"date": str(r.date), "week": r.week, "month": r.month, "checkpoints": r.checkpoints, "cleaning_details": r.cleaning_details, "maintenance_details": r.maintenance_details},
#             'rework': lambda r: {"date": str(r.date), "remark": r.remark, "part_name": r.part_name, "part_no": r.part_no, "spec": r.spec, "non_conformance": r.non_conformance, "rework_qty": r.rework_qty, "inspected_by": r.inspected_by, "dynamic_details": r.dynamic_details},
#             'rework-view': lambda r: {"date": str(r.date), "remark": r.remark, "part_name": r.part_name, "part_no": r.part_no, "spec": r.spec, "non_conformance": r.non_conformance, "rework_qty": r.rework_qty, "inspected_by": r.inspected_by, "dynamic_details": r.dynamic_details},
#             'monthly-prod-plan': lambda r: {"date": str(r.filled_date), "partName": r.part_name, "customer": r.customer_name, "openingStock": r.opening_stock, "scheduleQty": r.schedule_qty, "plannedQty": r.planned_qty, "remark": r.remark, "preparedBy": r.prepared_by, "approvedBy": r.approved_by},
#             'operator-observance-checklist': lambda r: {"recordDate": str(r.record_date), "operatorName": r.operator_name, "model": r.model, "partOperation": r.part_operation, "formData": r.checkpoints, "preparedBy": r.prepared_by, "approvedBy": r.approved_by},
#             'operator-observance-plan': lambda r: {"selectedYear": r.plan_year, "selectedMonth": r.plan_month, "operators": r.operators_data, "preparedBy": r.prepared_by, "approvedBy": r.approved_by},
#             'pm-checklist-mhe': lambda r: {"filledDate": str(r.filled_date), "partName": r.part_name, "trolleyNo": r.trolley_no, "pmFrequency": r.pm_frequency, "checkPoints": r.checkpoints, "checkedBy": r.checked_by, "verifiedBy": r.verified_by, "generalRemarks": r.general_remarks},
#             'projection-welder': lambda r: {"wpsNo": r.wps_no, "date": str(r.date), "weldingProcess": r.welding_process, "baseMetal": r.base_metal, "baseMetalThickness": r.base_metal_thickness, "machineNo": r.machine_no, "trials": r.trials, "welderName": r.welder_name, "conductedBy": r.conducted_by, "verifiedBy": r.verified_by, "qualificationStatus": r.qualification_status},
#             'spot-welder': lambda r: {"wpsNo": r.wps_no, "date": str(r.date), "weldingProcess": r.welding_process, "baseMetal": r.base_metal, "baseMetalThickness": r.base_metal_thickness, "machineNo": r.machine_no, "gunType": r.gun_type, "trials": r.trials, "welderName": r.welder_name, "conductedBy": r.conducted_by, "verifiedBy": r.verified_by, "qualificationStatus": r.qualification_status},
#             'tig-mig-welder': lambda r: {"wpsNo": r.wps_no, "testingDate": str(r.testing_date), "weldingProcess": r.welding_process, "machineNo": r.machine_no, "baseMetal": r.base_metal, "baseMetalThickness": r.base_metal_thickness, "baseMetalSize": r.base_metal_size, "weldingPosition": r.welding_position, "fillerMaterial": r.filler_material, "fillerMaterialSize": r.filler_material_size, "shieldingGas": r.shielding_gas, "wireFeedSpeed": r.wire_feed_speed, "trials": r.trials, "testResults": r.test_results, "welderName": r.welder_name, "conductedBy": r.conducted_by, "verifiedBy": r.verified_by, "qualificationStatus": r.qualification_status},
#             'process-validation': lambda r: {"validationDate": str(r.validation_date), "revalidationDate": str(r.revalidation_date), "processName": r.process_name, "materialDetails": r.material_details, "machineNo": r.machine_no, "processOwner": r.process_owner, "partName": r.part_name, "fixtureNo": r.fixture_no, "operators": r.operators, "parameters": r.parameters, "trials": r.trials, "finalParams": r.final_params, "conclusion": r.conclusion, "preparedBy": r.prepared_by, "approvedBy": r.approved_by},
#             'four-m-display': lambda r: {"s_no": r.s_no, "machine_no": r.machine_no, "operator_name": r.operator_name, "man": r.man, "machine": r.machine, "material": r.material, "method": r.method},
#             'information-sheet': lambda r: FourMInformationSheetSerializer(r).data,
#             'four-m-information': lambda r: FourMInformationSheetSerializer(r).data,
#         }

#         MODEL_REGISTRY = {
#             'daily-prod-plan': DailyProductionPlan, 'four-m-inspection': FourMChangeInspection,
#             'four-m-record': FourMChangeRecord, 'tip-change': TipChangeDressing,
#             'four-m-summary': FourMSummary, 'machine-checksheet': MachineChecksheetReport,
#             'five-s': FiveSChecksheetReport, 'five-s-view': FiveSChecksheetReport,
#             'bin-trolley': BinTrolleyReport, 'rework': ReworkEntry, 'rework-view': ReworkEntry,
#             'monthly-prod-plan': MonthlyProductionPlan, 'operator-observance-checklist': OperatorObservanceChecklist,
#             'operator-observance-plan': OperatorObservancePlan, 'pm-checklist-mhe': PMChecklistMHE,
#             'projection-welder': ProjectionWelderQual, 'spot-welder': SpotWelderQual,
#             'tig-mig-welder': TigMigWelderQual, 'process-validation': ProcessValidation,
#             'four-m-display': FourMDisplay,
#             'information-sheet': FourMInformationSheet,
#             'four-m-information': FourMInformationSheet,
#         }

#         if form_key not in FORM_REGISTRY or form_key not in MODEL_REGISTRY:
#             return Response({"success": False, "error": f"Form '{form_key}' Not Supported Yet in Production"}, status=400)

#         TargetModel = MODEL_REGISTRY[form_key]
#         report = get_object_or_404(TargetModel, id=rec_id)
        
#         formatter = FORM_REGISTRY[form_key]
#         data = formatter(report)
#         data['submitted_by'] = submitted_user

#         return Response({"success": True, "data": data}, status=200)

#     except Exception as e:
#         import traceback
#         print("🔥 ERROR IN GET SINGLE PRODUCTION REPORT:", traceback.format_exc())
#         return Response({"success": False, "error": str(e)}, status=500)


# # ==============================================================================
# # 📊 PRODUCTION DATA FETCH API (View Reports - Kept As-Is)
# # ==============================================================================

# @api_view(['GET'])
# def production_data_view(request, form_key):

#     # ── 🔥 MASTER HELPER FUNCTION (Sabhi APIs ko Filter Karne Ke Liye) ──
#     def apply_date_filter(queryset, date_field):
#         start_date = request.GET.get('start_date')
#         end_date = request.GET.get('end_date')
        
        
#         if start_date == 'all':
#             return queryset

#         if not start_date and not end_date:
#             ist_tz = pytz.timezone('Asia/Kolkata')
#             from django.utils.timezone import now
#             today_str = now().astimezone(ist_tz).strftime("%Y-%m-%d")
#             if date_field == 'created_at':
#                 return queryset.filter(**{f"{date_field}__date": today_str})
#             return queryset.filter(**{f"{date_field}": today_str})

#         if start_date and end_date:
#             if date_field == 'created_at':
#                 return queryset.filter(**{f"{date_field}__date__range": [start_date, end_date]})
#             return queryset.filter(**{f"{date_field}__range": [start_date, end_date]})
#         elif start_date:
#             if date_field == 'created_at':
#                 return queryset.filter(**{f"{date_field}__date__gte": start_date})
#             return queryset.filter(**{f"{date_field}__gte": start_date})
#         elif end_date:
#             if date_field == 'created_at':
#                 return queryset.filter(**{f"{date_field}__date__lte": end_date})
#             return queryset.filter(**{f"{date_field}__lte": end_date})
            
#         return queryset

#     # ── Helper: raw SQL table se data fetch karna
#     def fetch_from_table(table_name, source_tag=None):
#         try:
#             with connection.cursor() as cursor:
#                 cursor.execute(f"SELECT * FROM {table_name} ORDER BY id DESC")
#                 cols = [col[0] for col in cursor.description]
#                 rows = []
#                 for row in cursor.fetchall():
#                     record = dict(zip(cols, row))
#                     for k, v in record.items():
#                         if hasattr(v, 'strftime'):
#                             record[k] = str(v)
#                     if source_tag:
#                         record['_source'] = source_tag
#                     rows.append(record)
#                 return rows
#         except Exception as e:
#             print(f"⚠️ {table_name} error: {e}")
#             return []

#     # ──  COMMON LOG MAPPING  ──
#     log_mapping = {}
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT record_id, username, status 
#                 FROM user_report_activity_logs
#             """)
#             for row in cursor.fetchall():
#                 rec_id, raw_username, status = row[0], row[1], row[2]
                
#                 # Prepared By ke liye @ se split karne wala logic
#                 username = raw_username.split('@')[0] if raw_username else '—'
                
#                 if rec_id not in log_mapping:
#                     log_mapping[rec_id] = {'prepared_by': '—', 'approved_by': '—'}
                
#                 # Status ke hisaab se logic
#                 if status == 'In Progress' or status == 'Pending':
#                     log_mapping[rec_id]['prepared_by'] = username.capitalize()
                    
#                 # Approved By ke liye logic jo emails ko bhi handle karega
#                 elif status and status.startswith('Approved'):
#                     raw_approver = status.replace('Approved by ', '').strip()
#                     approver_name = raw_approver.split('@')[0] if raw_approver else '—'
#                     log_mapping[rec_id]['approved_by'] = approver_name.capitalize()
                    
#                 elif status == 'Completed':
#                     log_mapping[rec_id]['approved_by'] = username.capitalize()
#     except Exception as e:
#         print(f" Activity logs fetch error: {e}")

#     # ── 1. DAILY PRODUCTION PLAN ────────────────────────────────
#     if form_key == 'daily-prod-plan':
#         try:
#             base_query = DailyProductionPlan.objects.all()
#             records = apply_date_filter(base_query, 'plan_date').order_by('-plan_date', '-created_at')

#             data = [{
#                 'id': r.id,
#                 'Date': str(r.plan_date),
#                 'created_at': r.created_at.isoformat() if r.created_at else None,  
#                 'updated_at': r.updated_at.isoformat() if r.updated_at else None,
#                 'Plant': r.plant or '',
#                 'Machine No': r.machine_no or '—',
#                 'Shift': r.shift or '—',
#                 'Operator Name': r.operator_name,
#                 'Part Name': r.part_name,
#                 'Part No': r.part_no,
#                 'Operation': r.operation_name,
#                 'Planned Qty': r.planned_quantity,
#                 'Achieved Qty.': r.achieved_quantity,
#                 'Qty Remark': r.qty_remark or '—',
#                 'Start Time': str(r.production_start_time) if r.production_start_time else '—',
#                 'End Time': str(r.production_end_time) if r.production_end_time else '—',
#                 'Total Time': r.total_working_time or '—',
#                 'Tool Setup (min)': r.tool_setup_time if r.tool_setup_time is not None else 0,
#                 'Machine B/D (min)': r.machine_bd_time if r.machine_bd_time is not None else 0,
#                 'Tool B/D (min)': r.tool_bd_time if r.tool_bd_time is not None else 0,
#                 'RM Coil No': r.rm_coil_no or '—',
#                 'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                 'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#             } for r in records]
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 2. 4M CHANGE INSPECTION REPORT ──────────────────────────
#     elif form_key == 'four-m-inspection':
#         try:
#             base_query = FourMChangeInspection.objects.all()
#             records = apply_date_filter(base_query, 'inspection_date').order_by('-inspection_date', '-created_at')
#             data = []
            
#             for r in records:
#                 common_data = {
#                     'Date': str(r.inspection_date),
#                     'Part Name': r.part_name or '',
#                     'Part No.': r.part_no or '',
#                     'Operation': r.operation or '',
#                     'Lot Qty': r.lot_qty if r.lot_qty is not None else '',
#                     'OK Qty': r.ok_qty if r.ok_qty is not None else '',
#                     'Rej. Qty': r.rej_qty if r.rej_qty is not None else '',    
#                     'Parameter/Specs': r.parameter_specs or '',                
#                     'Insp. By': r.inspected_by or '',
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }

#                 checkpoints_list = [
#                     ('BEFORE (RETROACTIVE)', 'Parameter 1', getattr(r, 'before_1', '')),
#                     ('BEFORE (RETROACTIVE)', 'Parameter 2', getattr(r, 'before_2', '')),
#                     ('BEFORE (RETROACTIVE)', 'Parameter 3', getattr(r, 'before_3', '')),
#                     ('BEFORE (RETROACTIVE)', 'Parameter 4', getattr(r, 'before_4', '')),
#                     ('BEFORE (RETROACTIVE)', 'Parameter 5', getattr(r, 'before_5', '')),
#                     ('AFTER (SETUP APPROVAL)', 'Parameter 1', getattr(r, 'after_1', '')),
#                     ('AFTER (SETUP APPROVAL)', 'Parameter 2', getattr(r, 'after_2', '')),
#                     ('AFTER (SETUP APPROVAL)', 'Parameter 3', getattr(r, 'after_3', '')),
#                     ('AFTER (SETUP APPROVAL)', 'Parameter 4', getattr(r, 'after_4', '')),
#                     ('AFTER (SETUP APPROVAL)', 'Parameter 5', getattr(r, 'after_5', '')),
#                 ]

#                 for category, checkpoint_name, status_value in checkpoints_list:
#                     row = common_data.copy() 
#                     row['Category'] = category
#                     row['Check Point'] = checkpoint_name
#                     row['Status/Value'] = status_value or ''
#                     data.append(row)
                    
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 3. 4M CHANGE RECORD ─────────────────────────────────────
#     elif form_key == 'four-m-record':
#         try:
#             base_query = FourMChangeRecord.objects.all()
#             records = apply_date_filter(base_query, 'created_at').order_by('-created_at')
#             data = []
            
#             for r in records:
#                 common_data = {
#                     'Date': str(r.date) if getattr(r, 'date', None) else '',
#                     'Time': str(r.time) if r.time else '',
#                     'Machine No': r.machine_no or '',
#                     'Part Info': r.part_info or '',
#                     'Operation No': r.operation_no or '',
#                     'Nature of Change': r.nature_of_change or '',
#                     'Action Taken': r.action_taken or '',
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }

#                 parameters_list = [
#                     ('Change Info', 'Description', r.description or ''),
#                     ('4M Status', 'Man', getattr(r, 'status_man', '') or ''),
#                     ('4M Status', 'Machine', getattr(r, 'status_machine', '') or ''),
#                     ('4M Status', 'Material', getattr(r, 'status_material', '') or ''),
#                     ('4M Status', 'Method', getattr(r, 'status_method', '') or ''),
#                     ('Approval & Training', 'Setup Approval', r.setup_approval or ''),
#                     ('Approval & Training', 'Training Provided', getattr(r, 'training_provided', '') or ''),
#                     ('Retroactive', 'Qty Checked', getattr(r, 'retro_qty_checked', '') if getattr(r, 'retro_qty_checked', None) is not None else ''),
#                     ('Retroactive', 'Qty OK', getattr(r, 'retro_qty_ok', '') or ''),
#                     ('Retroactive', 'R/W', getattr(r, 'retro_rw', '') or ''),
#                     ('Retroactive', 'Scrap', getattr(r, 'retro_scrap', '') or ''),
#                     ('Containment', 'Qty Checked', getattr(r, 'cont_qty_checked', '') if getattr(r, 'cont_qty_checked', None) is not None else ''),
#                     ('Containment', 'Qty OK', getattr(r, 'cont_qty_ok', '') or ''),
#                     ('Containment', 'R/W', getattr(r, 'cont_rw', '') or ''),
#                     ('Containment', 'Scrap', getattr(r, 'cont_scrap', '') or ''),
#                     ('Dispatch', 'Customer', getattr(r, 'customer', '') or ''),
#                     ('Dispatch', 'Date', str(r.dispatch_date) if getattr(r, 'dispatch_date', None) else ''),
#                     ('Dispatch', 'Invoice No', getattr(r, 'invoice_no', '') or ''),
#                     ('Footer', 'Remark', getattr(r, 'remark', '') or ''),
#                 ]

#                 for category, parameter, value in parameters_list:
#                     row = common_data.copy()
#                     row['Category'] = category
#                     row['Check Point'] = parameter
#                     row['Value / Status'] = value
#                     data.append(row)
                    
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 4. TIP CHANGE DRESSING ──────────────────────────────────
#     elif form_key == 'tip-change':
#         try:
#             base_query = TipChangeDressing.objects.all()
#             records = apply_date_filter(base_query, 'date').order_by('-date')
#             data = [{
#                 'Date': str(r.date),
#                 'Plant': r.plant or 'N/A',
#                 'Machine Name': r.machine_name or 'N/A',
#                 'Machine No': r.machine_no or 'N/A',
#                 'Part Name': r.part_name,
#                 'Operation': r.operation or 'N/A',
#                 'Prod Qty': r.prd_qty,
#                 'Tip Change': r.tip_change,
#                 'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                 'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#             } for r in records]
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 5. BIN TROLLEY REPORT ───────────────────────────────────
#     elif form_key == 'bin-trolley':
#         try:
#             base_query = BinTrolleyReport.objects.all()
#             records = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
#             data = []
#             for r in records:
#                 def parse_json(field_data):
#                     if isinstance(field_data, str):
#                         try: return json.loads(field_data)
#                         except: return {}
#                     return field_data or {}

#                 cp = parse_json(r.checkpoints)
#                 cl = parse_json(r.cleaning_details)
#                 mn = parse_json(r.maintenance_details)

#                 common_data = {
#                     'Date': str(r.date),
#                     'Week': r.week or '',
#                     'Month': r.month or '',
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }

#                 items_list = [
#                     ('Checkpoints', '1. Bin/Trolley Should Be Clean Properly', cp.get('cp1', {}).get('status', ''), cp.get('cp1', {}).get('remarks', '')),
#                     ('Checkpoints', '2. Bin/Trolley Should be Free From Dust', cp.get('cp2', {}).get('status', ''), cp.get('cp2', {}).get('remarks', '')),
#                     ('Checkpoints', '3. Bin/Trolley Should Not Be Damage And Broken', cp.get('cp3', {}).get('status', ''), cp.get('cp3', {}).get('remarks', '')),
#                     ('Checkpoints', '4. Bin/Trolley Should be Free From Oil Surface', cp.get('cp4', {}).get('status', ''), cp.get('cp4', {}).get('remarks', '')),
#                     ('Checkpoints', '5. Bin/Trolley Should Be Clean In Bin Cleaning Area', cp.get('cp5', {}).get('status', ''), cp.get('cp5', {}).get('remarks', '')),
#                     ('Checkpoints', '6. Others (Please Specify)', cp.get('cp6', {}).get('status', ''), cp.get('cp6', {}).get('remarks', '')),
#                     ('Cleaning Details', 'Total Bin / Trolley Quantity', cl.get('cd1', {}).get('frequency', ''), cl.get('cd1', {}).get('remarks', '')),
#                     ('Cleaning Details', 'Bin / Trolley Clean Quantity', cl.get('cd2', {}).get('frequency', ''), cl.get('cd2', {}).get('remarks', '')),
#                     ('Cleaning Details', 'Bin / Trolley Unclean Quantity', cl.get('cd3', {}).get('frequency', ''), cl.get('cd3', {}).get('remarks', '')),
#                     ('Maintenance Details', 'Total Maintenance Quantity', mn.get('md1', {}).get('frequency', ''), mn.get('md1', {}).get('remarks', '')),
#                     ('Maintenance Details', 'Bin/Trolley Ok Quantity', mn.get('md2', {}).get('frequency', ''), mn.get('md2', {}).get('remarks', '')),
#                     ('Maintenance Details', 'Bin/Trolley Reject Quantity', mn.get('md3', {}).get('frequency', ''), mn.get('md3', {}).get('remarks', '')),
#                 ]

#                 for category, detail_name, value, remarks in items_list:
#                     row = common_data.copy()
#                     row['Category'] = category
#                     row['Check Point / Detail'] = detail_name
#                     row['Status / Qty'] = value
#                     row['Remarks'] = remarks
#                     data.append(row)
                
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)
            
#     # ── 6. 5S CHECKSHEET REPORT ─────────────────────────────────
#     elif form_key == 'five-s-view':
#         try:
#             base_query = FiveSChecksheetReport.objects.prefetch_related('observations')
#             reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
#             data = []
#             for report in reports:
#                 observations = report.observations.all()
#                 if observations.exists():
#                     for obs in observations:
#                         data.append({
#                             'Date': str(report.date),
#                             'Zone Leader': report.zone_leader,
#                             'Area': report.area,
#                             'Category (S)': obs.s_category,
#                             'Check Point': obs.check_point,
#                             'Status': obs.status,
#                             'OK Count': report.ok_count,
#                             'NG Count': report.ng_count,
#                             'Prepared By': log_mapping.get(report.id, {}).get('prepared_by', '—'),
#                             'Approved By': log_mapping.get(report.id, {}).get('approved_by', '—'),
#                         })
#                 else:
#                     data.append({
#                         'Date': str(report.date),
#                         'Zone Leader': report.zone_leader,
#                         'Area': report.area,
#                         'Category (S)': '—',
#                         'Check Point': '—',
#                         'Status': '—',
#                         'OK Count': report.ok_count,
#                         'NG Count': report.ng_count,
#                         'Prepared By': log_mapping.get(report.id, {}).get('prepared_by', '—'),
#                         'Approved By': log_mapping.get(report.id, {}).get('approved_by', '—'),
#                      })
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 7. MONTHLY PRODUCTION PLAN ──────────────────────────────
#     elif form_key == 'monthly-prod-plan':
#         try:
#             base_query = MonthlyProductionPlan.objects.all()
#             reports = apply_date_filter(base_query, 'created_at').order_by('-created_at')
#             data = []
#             for r in reports:
#                 data.append({
#                     'Date': str(r.filled_date) if r.filled_date else '',
#                     'Part Name': r.part_name or '',
#                     'Customer Name': r.customer_name or '',
#                     'Opening Stock': r.opening_stock,
#                     'Schedule Qty': r.schedule_qty,
#                     'Planned Qty': r.planned_qty,
#                     'Remark': r.remark or '',
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—')
#                 })
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 8. OPERATOR OBSERVANCE CHECKLIST ────────────────────────
#     elif form_key == 'operator-observance-checklist':
#         try:
#             base_query = OperatorObservanceChecklist.objects.all()
#             reports = apply_date_filter(base_query, 'record_date').order_by('-created_at')
#             data = []
#             effect_map = {'100': 'High', '50': 'Medium', 'low': 'Low'}
            
#             for r in reports:
#                 common_data = {
#                     'Date': str(r.record_date) if r.record_date else '',
#                     'Operator Name': r.operator_name or '',
#                     'Model': r.model or '',
#                     'Part / Operation': r.part_operation or '',
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }
                
#                 checkpoints = r.checkpoints
#                 if isinstance(checkpoints, str):
#                     try: checkpoints = json.loads(checkpoints)
#                     except: checkpoints = []
#                 checkpoints = checkpoints if isinstance(checkpoints, list) else []

#                 if not checkpoints:
#                     data.append(common_data)
#                 else:
#                     for cp in checkpoints:
#                         row = common_data.copy()
#                         row['Task / Check Point'] = cp.get('task', '')
#                         row['Response'] = cp.get('response', '').title()
#                         row['Training'] = 'Yes' if cp.get('training') else 'No'
#                         raw_effect = str(cp.get('effect', '')).lower()
#                         row['Effectiveness'] = effect_map.get(raw_effect, cp.get('effect', ''))
#                         row['Remarks'] = cp.get('remarks', '')
#                         data.append(row)
                        
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 9. OPERATOR OBSERVANCE PLAN ─────────────────────────────
#     elif form_key == 'operator-observance-plan':
#         try:
#             base_query = OperatorObservancePlan.objects.all()
#             reports = apply_date_filter(base_query, 'created_at').order_by('-created_at')
#             data = []
#             for r in reports:
#                 common_data = {
#                     'Plan Period': f"{r.plan_month} {r.plan_year}",
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }
                
#                 operators = r.operators_data
#                 if isinstance(operators, str):
#                     try: operators = json.loads(operators)
#                     except: operators = []
#                 operators = operators if isinstance(operators, list) else []

#                 if not operators:
#                     data.append(common_data)
#                 else:
#                     for op in operators:
#                         row = common_data.copy()
#                         row['Operator Name'] = op.get('name', '')
#                         row['Department'] = op.get('dept', '')
#                         row['Status'] = op.get('status', '')
#                         data.append(row)
                        
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 10. PM CHECKLIST MHE ─────────────────────────────────────
#     elif form_key == 'pm-checklist-mhe':
#         try:
#             base_query = PMChecklistMHE.objects.all()
#             reports = apply_date_filter(base_query, 'filled_date').order_by('-created_at')
#             data = []
#             for r in reports:
#                 common_data = {
#                     'Date': str(r.filled_date) if r.filled_date else '',
#                     'Part Name / No': r.part_name or '',
#                     'Trolley No': r.trolley_no or '',
#                     'PM Frequency': r.pm_frequency or '',
#                     'Checked By': r.checked_by or '',
#                     'Verified By': r.verified_by or '',
#                     'General Remarks': r.general_remarks or '',
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }
                
#                 checkpoints = r.checkpoints
#                 if isinstance(checkpoints, str):
#                     try: checkpoints = json.loads(checkpoints)
#                     except: checkpoints = []
#                 checkpoints = checkpoints if isinstance(checkpoints, list) else []

#                 if not checkpoints:
#                     data.append(common_data)
#                 else:
#                     for cp in checkpoints:
#                         row = common_data.copy()
#                         row['Sr No'] = cp.get('id', '')
#                         row['Check Point Task'] = cp.get('task', '')
#                         row['Done On'] = cp.get('doneOn', '')
#                         row['Status'] = cp.get('status', '')
#                         row['Remarks'] = cp.get('remarks', '')
#                         data.append(row)
                        
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)
    
#     # ── 11. PROJECTION WELDER QUALIFICATION ───────────────────────
#     elif form_key == 'projection-welder':
#         try:
#             base_query = ProjectionWelderQual.objects.all()
#             records = apply_date_filter(base_query, 'date').order_by('-created_at')
#             data = []
#             for r in records:
#                 common_data = {
#                     'Date': str(r.date) if r.date else '',
#                     'WPS No': r.wps_no or '',
#                     'Process': r.welding_process or '',
#                     'Base Metal': r.base_metal or '',
#                     'Thickness': r.base_metal_thickness or '',
#                     'Machine No': r.machine_no or '',
#                     'Welder Name': r.welder_name or '',
#                     'Status': r.qualification_status or '',
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }
                
#                 trials = r.trials
#                 if isinstance(trials, str):
#                     try: trials = json.loads(trials)
#                     except: trials = []
#                 trials = trials if isinstance(trials, list) else []

#                 if not trials:
#                     data.append(common_data)
#                 else:
#                     for i, t in enumerate(trials):
#                         category = f"Trial {i + 1}"
#                         parameters_list = [
#                             ('Squeeze Time', t.get('squeeze', '')),
#                             ('Weld Time', t.get('weld', '')),
#                             ('Hold Time', t.get('hold', '')),
#                             ('Off Time', t.get('off', '')),
#                             ('Current (Amp/KA)', t.get('current', '')),
#                             ('Pressure (Bar)', t.get('pressure', '')),
#                             ('Torque', t.get('torque', '')),
#                             ('Visual', t.get('visual', '')),
#                         ]
#                         for param, val in parameters_list:
#                             row = common_data.copy()
#                             row['Category'] = category
#                             row['Parameter'] = param
#                             row['Value / Status'] = val
#                             data.append(row)
                            
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 12. SPOT WELDER QUALIFICATION ─────────────────────────────
#     elif form_key == 'spot-welder':
#         try:
#             base_query = SpotWelderQual.objects.all()
#             records = apply_date_filter(base_query, 'date').order_by('-created_at')
#             data = []
#             for r in records:
#                 common_data = {
#                     'Date': str(r.date) if r.date else '',
#                     'WPS No': r.wps_no or '',
#                     'Gun Type': r.gun_type or '',
#                     'Base Metal': r.base_metal or '',
#                     'Thickness': r.base_metal_thickness or '',
#                     'Machine No': r.machine_no or '',
#                     'Welder Name': r.welder_name or '',
#                     'Status': r.qualification_status or '',
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }
                
#                 trials = r.trials
#                 if isinstance(trials, str):
#                     try: trials = json.loads(trials)
#                     except: trials = []
#                 trials = trials if isinstance(trials, list) else []

#                 if not trials:
#                     data.append(common_data)
#                 else:
#                     for i, t in enumerate(trials):
#                         category = f"Trial {i + 1}"
#                         parameters_list = [
#                             ('Squeeze Time', t.get('squeeze', '')),
#                             ('Weld Time', t.get('weld', '')),
#                             ('Hold Time', t.get('hold', '')),
#                             ('Off Time', t.get('off', '')),
#                             ('Current (Amp)', t.get('current', '')),
#                             ('Pressure (Bar)', t.get('pressure', '')),
#                             ('Strength (Nugget)', t.get('strength', '')),
#                             ('Visual', t.get('visual', '')),
#                         ]
#                         for param, val in parameters_list:
#                             row = common_data.copy()
#                             row['Category'] = category
#                             row['Parameter'] = param
#                             row['Value / Status'] = val
#                             data.append(row)

#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 13. TIG/MIG WELDER QUALIFICATION ──────────────────────────
#     elif form_key == 'tig-mig-welder':
#         try:
#             base_query = TigMigWelderQual.objects.all()
#             records = apply_date_filter(base_query, 'testing_date').order_by('-created_at')
#             data = []
#             for r in records:
#                 common_data = {
#                     'Testing Date': str(r.testing_date) if r.testing_date else '',
#                     'WPS No': r.wps_no or '',
#                     'Process': r.welding_process or '',
#                     'Machine No': r.machine_no or '',
#                     'Base Metal': r.base_metal or '',
#                     'Thickness': r.base_metal_thickness or '',
#                     'Filler Material': r.filler_material or '',
#                     'Shielding Gas': r.shielding_gas or '',
#                     'Welder Name': r.welder_name or '',
#                     'Status': r.qualification_status or '',
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }
                
#                 trials = r.trials
#                 if isinstance(trials, str):
#                     try: trials = json.loads(trials)
#                     except: trials = []
#                 trials = trials if isinstance(trials, list) else []

#                 if not trials:
#                     data.append(common_data)
#                 else:
#                     for i, t in enumerate(trials):
#                         category = f"Trial {i + 1}"
#                         parameters_list = [
#                             ('Current Range', t.get('currRange', '')),
#                             ('Current Actual', t.get('currActual', '')),
#                             ('Voltage Range', t.get('voltRange', '')),
#                             ('Voltage Actual', t.get('voltActual', '')),
#                             ('Gas Flow Range', t.get('gasRange', '')),
#                             ('Gas Flow Actual', t.get('gasActual', '')),
#                             ('Deposit Thickness', t.get('depositThickness', '')),
#                             ('Defect', t.get('defect', '')),
#                         ]
#                         for param, val in parameters_list:
#                             row = common_data.copy()
#                             row['Category'] = category
#                             row['Parameter'] = param
#                             row['Value / Status'] = val
#                             data.append(row)

#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 14. PROCESS VALIDATION REPORT ─────────────────────────────
#     elif form_key == 'process-validation':
#         try:
#             base_query = ProcessValidation.objects.all()
#             records = apply_date_filter(base_query, 'validation_date').order_by('-created_at')
#             data = []
#             for r in records:
#                 ops = r.operators
#                 if isinstance(ops, str):
#                     try: ops = json.loads(ops)
#                     except: ops = []
#                 operators_str = ", ".join([op for op in ops if op]) if isinstance(ops, list) else ""

#                 common_data = {
#                     'Validation Date': str(r.validation_date) if r.validation_date else '',
#                     'Process Name': r.process_name or '',
#                     'Machine No': r.machine_no or '',
#                     'Part Name': r.part_name or '',
#                     'Operators': operators_str,
#                     'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                     'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 }

#                 trials = r.trials
#                 if isinstance(trials, str):
#                     try: trials = json.loads(trials)
#                     except: trials = []
#                 trials = trials if isinstance(trials, list) else []

#                 if not trials:
#                     data.append(common_data)
#                 else:
#                     for i, t in enumerate(trials):
#                         category = f"Trial {i + 1}"
#                         parameters_list = [
#                             ('Parameter 1 (P1)', t.get('p1', '')),
#                             ('Parameter 2 (P2)', t.get('p2', '')),
#                             ('Parameter 3 (P3)', t.get('p3', '')),
#                             ('Parameter 4 (P4)', t.get('p4', '')),
#                             ('Parameter 5 (P5)', t.get('p5', '')),
#                             ('Parameter 6 (P6)', t.get('p6', '')),
#                             ('Parameter 7 (P7)', t.get('p7', '')),
#                             ('Parameter 8 (P8)', t.get('p8', '')),
#                             ('Characteristics Spec', t.get('specified', '')),
#                             ('Characteristics Obs', t.get('observed', '')),
#                             ('Decision', t.get('decision', '')),
#                         ]

#                         for param, val in parameters_list:
#                             row = common_data.copy()
#                             row['Category'] = category
#                             row['Parameter'] = param
#                             row['Value / Status'] = val
#                             data.append(row)

#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 15. 4M DISPLAY BOARD ──────────────────────────────────────
#     elif form_key == 'four-m-display':
#         try:
#             base_query = FourMDisplay.objects.all()
#             records = apply_date_filter(base_query, 'date_filled').order_by('-created_at')
#             data = [{
#                 'Machine No': r.machine_no or '',
#                 'Operator Name': r.operator_name or '',
#                 'Man': r.man or '',
#                 'Machine': r.machine or '',
#                 'Material': r.material or '',
#                 'Method': r.method or '',
#                 'Date': str(r.date_filled) if r.date_filled else '',
#                 'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                 'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#             } for r in records]
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 16. 4M SUMMARY SHEET ──────────────────────────────────────
#     elif form_key == 'four-m-summary':
#         try:
#             base_query = FourMSummary.objects.all()
#             records = apply_date_filter(base_query, 'date').order_by('-created_at')
#             data = [{
#                 'Date': str(r.date) if r.date else '',
#                 'Customer': r.customer or '',
#                 'Part Name & No': r.part_name_no or '',
#                 'Type of Change': r.type_of_change or '',
#                 'Change Detail': r.change_detail or '',
#                 'Retro Total Qty': r.retro_total_qty if r.retro_total_qty is not None else '',
#                 'Retro OK Qty': r.retro_ok_qty if r.retro_ok_qty is not None else '',
#                 'Retro Rej. Qty': r.retro_rej_qty if r.retro_rej_qty is not None else '',
#                 'Status After Final Insp.': r.status_after_final or '',
#                 'Action for NG Material': r.action_for_ng or '',
#                 'Sup. Signature': r.sup_signature or '',
#                 'Sign Prod. Head': r.sign_prod_head or '',
#                 'Sign QA Head': r.sign_qa_head or '',
#                 'Remarks': r.remarks or '',
#                 'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                 'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#             } for r in records]
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # ── 17. 4M INFORMATION SHEET ──────────────────────────────────
#     elif form_key == 'four-m-information':
#         try:
#             base_query = FourMInformationSheet.objects.all()
#             records = apply_date_filter(base_query, 'date_filled').order_by('-created_at')
#             data = [{
#                 'S.No': r.s_no if r.s_no is not None else '',
#                 'Time': str(r.time) if r.time else '',
#                 'Machine No': r.machine_no or '',
#                 'Operator Name': r.operator_name or '',
#                 'Man': r.man or '',
#                 'Machine': r.machine or '',
#                 'Material': r.material or '',
#                 'Method': r.method or '',
#                 'Change Description': r.change_description or '',
#                 'Prepared By': log_mapping.get(r.id, {}).get('prepared_by', '—'),
#                 'Approved By': log_mapping.get(r.id, {}).get('approved_by', '—'),
#                 'Date': str(r.date_filled) if r.date_filled else '',
#                 'created_at': r.created_at.isoformat() if r.created_at else None,
#             } for r in records]
#             return JsonResponse({'data': data})
#         except Exception as e:
#             return JsonResponse({'data': [], 'error': str(e)}, status=500)

#     # Agar koi galat form_key aati hai
#     return JsonResponse({'data': [], 'error': 'Production form type not supported'}, status=400)





# # ============================================================
# # NEW API: /api/get-record/<form_key>/<record_id>/
# # Ye API sirf record_id se exact record fetch karti hai
# # Koi date filter nahi — direct filter(id=record_id)
# # ============================================================

# logger = logging.getLogger(__name__)

# # ══════════════════════════════════════════════════════════
# # 1. UTILITY FUNCTIONS (Global Scope)
# # ══════════════════════════════════════════════════════════
# def parse_json_field(field_data):
#     if isinstance(field_data, str):
#         try:
#             return json.loads(field_data)
#         except Exception:
#             return []
#     elif isinstance(field_data, dict):
#         return field_data
#     return field_data or []

# def get_log_details(rid):
#     prepared_by = '—'
#     approved_by = '—'
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT username, status 
#                 FROM user_report_activity_logs 
#                 WHERE record_id = %s
#                 ORDER BY id ASC
#             """, [rid])
            
#             rows = cursor.fetchall()
#             for index, row in enumerate(rows):
#                 raw_username = row[0]
#                 status = row[1]
                
#                 formatted_name = raw_username.split('@')[0].capitalize() if raw_username else '—'
                
#                 if index == 0 and formatted_name != '—':
#                     prepared_by = formatted_name
                    
#                 if status and status.startswith('Approved'):
#                     raw_approver = status.replace('Approved by ', '').strip()
#                     approver_name = raw_approver.split('@')[0].capitalize() if raw_approver else '—'
#                     approved_by = approver_name
#                 elif status == 'Completed':
#                     approved_by = formatted_name
                    
#     except Exception as e:
#         logger.error(f"⚠️ Log fetch error: {e}")
        
#     return prepared_by, approved_by


# # ══════════════════════════════════════════════════════════
# # 2. SEPARATE HANDLER FUNCTIONS FOR EACH FORM
# # ══════════════════════════════════════════════════════════

# def handle_daily_prod_plan(rid, prep_by, app_by):
#     r = DailyProductionPlan.objects.get(id=rid)
#     data = {
#         'Date': str(r.plan_date),
#         'Plant': r.plant or '',
#         'Machine No': r.machine_no or '—',
#         'Shift': r.shift or '—',
#         'Operator Name': r.operator_name,
#         'Part Name': r.part_name,
#         'Part No': r.part_no,
#         'Operation': r.operation_name,
#         'Planned Qty': r.planned_quantity,
#         'Achieved Qty': r.achieved_quantity,
#         'Qty Remark': r.qty_remark or '—',
#         'Start Time': str(r.production_start_time) if r.production_start_time else '—',
#         'End Time': str(r.production_end_time) if r.production_end_time else '—',
#         'Total Time': r.total_working_time or '—',
#         'Tool Setup (min)': r.tool_setup_time if r.tool_setup_time is not None else 0,
#         'Machine B/D (min)': r.machine_bd_time if r.machine_bd_time is not None else 0,
#         'Tool B/D (min)': r.tool_bd_time if r.tool_bd_time is not None else 0,
#         'RM Coil No': r.rm_coil_no or '—',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     return {'data': data}


# def handle_four_m_inspection(rid, prep_by, app_by):
#     r = FourMChangeInspection.objects.get(id=rid)
#     checkpoints_list = [
#         ('BEFORE (RETROACTIVE)', 'Parameter 1', getattr(r, 'before_1', '')),
#         ('BEFORE (RETROACTIVE)', 'Parameter 2', getattr(r, 'before_2', '')),
#         ('BEFORE (RETROACTIVE)', 'Parameter 3', getattr(r, 'before_3', '')),
#         ('BEFORE (RETROACTIVE)', 'Parameter 4', getattr(r, 'before_4', '')),
#         ('BEFORE (RETROACTIVE)', 'Parameter 5', getattr(r, 'before_5', '')),
#         ('AFTER (SETUP APPROVAL)', 'Parameter 1', getattr(r, 'after_1', '')),
#         ('AFTER (SETUP APPROVAL)', 'Parameter 2', getattr(r, 'after_2', '')),
#         ('AFTER (SETUP APPROVAL)', 'Parameter 3', getattr(r, 'after_3', '')),
#         ('AFTER (SETUP APPROVAL)', 'Parameter 4', getattr(r, 'after_4', '')),
#         ('AFTER (SETUP APPROVAL)', 'Parameter 5', getattr(r, 'after_5', '')),
#     ]
#     common = {
#         'Date': str(r.inspection_date),
#         'Part Name': r.part_name or '',
#         'Part No': r.part_no or '',
#         'Operation': r.operation or '',
#         'Lot Qty': r.lot_qty if r.lot_qty is not None else '',
#         'OK Qty': r.ok_qty if r.ok_qty is not None else '',
#         'Rej Qty': r.rej_qty if r.rej_qty is not None else '',
#         'Parameter/Specs': r.parameter_specs or '',
#         'Insp By': r.inspected_by or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     rows = []
#     for category, cp_name, val in checkpoints_list:
#         row = common.copy()
#         row['Category'] = category
#         row['Check Point'] = cp_name
#         row['Status/Value'] = val or ''
#         rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_four_m_record(rid, prep_by, app_by):
#     r = FourMChangeRecord.objects.get(id=rid)
#     common = {
#         'Date': str(r.date) if getattr(r, 'date', None) else '',
#         'Time': str(r.time) if r.time else '',
#         'Machine No': r.machine_no or '',
#         'Part Info': r.part_info or '',
#         'Operation No': r.operation_no or '',
#         'Nature of Change': r.nature_of_change or '',
#         'Action Taken': r.action_taken or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     parameters_list = [
#         ('Change Info', 'Description', r.description or ''),
#         ('4M Status', 'Man', getattr(r, 'status_man', '') or ''),
#         ('4M Status', 'Machine', getattr(r, 'status_machine', '') or ''),
#         ('4M Status', 'Material', getattr(r, 'status_material', '') or ''),
#         ('4M Status', 'Method', getattr(r, 'status_method', '') or ''),
#         ('Approval & Training', 'Setup Approval', r.setup_approval or ''),
#         ('Approval & Training', 'Training Provided', getattr(r, 'training_provided', '') or ''),
#         ('Retroactive', 'Qty Checked', getattr(r, 'retro_qty_checked', '') if getattr(r, 'retro_qty_checked', None) is not None else ''),
#         ('Retroactive', 'Qty OK', getattr(r, 'retro_qty_ok', '') or ''),
#         ('Retroactive', 'R/W', getattr(r, 'retro_rw', '') or ''),
#         ('Retroactive', 'Scrap', getattr(r, 'retro_scrap', '') or ''),
#         ('Containment', 'Qty Checked', getattr(r, 'cont_qty_checked', '') if getattr(r, 'cont_qty_checked', None) is not None else ''),
#         ('Containment', 'Qty OK', getattr(r, 'cont_qty_ok', '') or ''),
#         ('Containment', 'R/W', getattr(r, 'cont_rw', '') or ''),
#         ('Containment', 'Scrap', getattr(r, 'cont_scrap', '') or ''),
#         ('Dispatch', 'Customer', getattr(r, 'customer', '') or ''),
#         ('Dispatch', 'Date', str(r.dispatch_date) if getattr(r, 'dispatch_date', None) else ''),
#         ('Dispatch', 'Invoice No', getattr(r, 'invoice_no', '') or ''),
#         ('Footer', 'Remark', getattr(r, 'remark', '') or ''),
#     ]
#     rows = []
#     for category, parameter, value in parameters_list:
#         row = common.copy()
#         row['Category'] = category
#         row['Check Point'] = parameter
#         row['Value / Status'] = value
#         rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_tip_change(rid, prep_by, app_by):
#     r = TipChangeDressing.objects.get(id=rid)
#     data = {
#         'Date': str(r.date),
#         'Plant': r.plant or '',
#         'Machine Name': r.machine_name or '',
#         'Machine No': r.machine_no or '',
#         'Part Name': r.part_name,
#         'Operation': r.operation or '',
#         'Prod Qty': r.prd_qty,
#         'Tip Change': r.tip_change,
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     return {'data': data}


# def handle_bin_trolley(rid, prep_by, app_by):
#     r = BinTrolleyReport.objects.get(id=rid)
#     cp = parse_json_field(r.checkpoints)
#     cl = parse_json_field(r.cleaning_details)
#     mn = parse_json_field(r.maintenance_details)
    
#     common = {
#         'Date': str(r.date),
#         'Week': r.week or '',
#         'Month': r.month or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
    
#     items_list = [
#         ('Checkpoints', '1. Bin/Trolley Should Be Clean Properly', cp.get('cp1', {}).get('status', ''), cp.get('cp1', {}).get('remarks', '')),
#         ('Checkpoints', '2. Bin/Trolley Should be Free From Dust', cp.get('cp2', {}).get('status', ''), cp.get('cp2', {}).get('remarks', '')),
#         ('Checkpoints', '3. Should Not Be Damage And Broken', cp.get('cp3', {}).get('status', ''), cp.get('cp3', {}).get('remarks', '')),
#         ('Checkpoints', '4. Should be Free From Oil Surface', cp.get('cp4', {}).get('status', ''), cp.get('cp4', {}).get('remarks', '')),
#         ('Checkpoints', '5. Should Be Clean In Bin Cleaning Area', cp.get('cp5', {}).get('status', ''), cp.get('cp5', {}).get('remarks', '')),
#         ('Checkpoints', '6. Others (Please Specify)', cp.get('cp6', {}).get('status', ''), cp.get('cp6', {}).get('remarks', '')),
#         ('Cleaning Details', 'Total Bin / Trolley Quantity', cl.get('cd1', {}).get('frequency', ''), cl.get('cd1', {}).get('remarks', '')),
#         ('Cleaning Details', 'Bin / Trolley Clean Quantity', cl.get('cd2', {}).get('frequency', ''), cl.get('cd2', {}).get('remarks', '')),
#         ('Cleaning Details', 'Bin / Trolley Unclean Quantity', cl.get('cd3', {}).get('frequency', ''), cl.get('cd3', {}).get('remarks', '')),
#         ('Maintenance Details', 'Total Maintenance Quantity', mn.get('md1', {}).get('frequency', ''), mn.get('md1', {}).get('remarks', '')),
#         ('Maintenance Details', 'Bin/Trolley Ok Quantity', mn.get('md2', {}).get('frequency', ''), mn.get('md2', {}).get('remarks', '')),
#         ('Maintenance Details', 'Bin/Trolley Reject Quantity', mn.get('md3', {}).get('frequency', ''), mn.get('md3', {}).get('remarks', '')),
#     ]
#     rows = []
#     for category, detail_name, value, remarks in items_list:
#         row = common.copy()
#         row['Category'] = category
#         row['Check Point / Detail'] = detail_name
#         row['Status / Qty'] = value
#         row['Remarks'] = remarks
#         rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_five_s(rid, prep_by, app_by):
#     r = FiveSChecksheetReport.objects.prefetch_related('observations').get(id=rid)
#     common = {
#         'Date': str(r.date),
#         'Zone Leader': r.zone_leader,
#         'Area': r.area,
#         'OK Count': r.ok_count,
#         'NG Count': r.ng_count,
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     observations = r.observations.all()
#     if observations.exists():
#         rows = []
#         for obs in observations:
#             row = common.copy()
#             row['Category (S)'] = obs.s_category
#             row['Check Point'] = obs.check_point
#             row['Status'] = obs.status
#             rows.append(row)
#         return {'data': rows, 'common': common}
#     return {'data': [common], 'common': common}


# def handle_monthly_prod_plan(rid, prep_by, app_by):
#     r = MonthlyProductionPlan.objects.get(id=rid)
#     data = {
#         'Date': str(r.filled_date) if r.filled_date else '',
#         'Part Name': r.part_name or '',
#         'Customer Name': r.customer_name or '',
#         'Opening Stock': r.opening_stock,
#         'Schedule Qty': r.schedule_qty,
#         'Planned Qty': r.planned_qty,
#         'Remark': r.remark or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     return {'data': data}


# def handle_operator_observance_checklist(rid, prep_by, app_by):
#     r = OperatorObservanceChecklist.objects.get(id=rid)
#     effect_map = {'100': 'High', '50': 'Medium', 'low': 'Low'}
#     common = {
#         'Date': str(r.record_date) if r.record_date else '',
#         'Operator Name': r.operator_name or '',
#         'Model': r.model or '',
#         'Part / Operation': r.part_operation or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     checkpoints = parse_json_field(r.checkpoints)
#     if not checkpoints:
#         return {'data': [common], 'common': common}
    
#     rows = []
#     for cp in checkpoints:
#         row = common.copy()
#         row['Task / Check Point'] = cp.get('task', '')
#         row['Response'] = cp.get('response', '').title()
#         row['Training'] = 'Yes' if cp.get('training') else 'No'
#         raw_effect = str(cp.get('effect', '')).lower()
#         row['Effectiveness'] = effect_map.get(raw_effect, cp.get('effect', ''))
#         row['Remarks'] = cp.get('remarks', '')
#         rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_operator_observance_plan(rid, prep_by, app_by):
#     r = OperatorObservancePlan.objects.get(id=rid)
#     common = {
#         'Plan Period': f"{r.plan_month} {r.plan_year}",
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     operators = parse_json_field(r.operators_data)
#     if not operators:
#         return {'data': [common], 'common': common}
    
#     rows = []
#     for op in operators:
#         row = common.copy()
#         row['Operator Name'] = op.get('name', '')
#         row['Department'] = op.get('dept', '')
#         row['Status'] = op.get('status', '')
#         rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_pm_checklist_mhe(rid, prep_by, app_by):
#     r = PMChecklistMHE.objects.get(id=rid)
#     common = {
#         'Date': str(r.filled_date) if r.filled_date else '',
#         'Part Name / No': r.part_name or '',
#         'Trolley No': r.trolley_no or '',
#         'PM Frequency': r.pm_frequency or '',
#         'Checked By': r.checked_by or '',
#         'Verified By': r.verified_by or '',
#         'General Remarks': r.general_remarks or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     checkpoints = parse_json_field(r.checkpoints)
#     if not checkpoints:
#         return {'data': [common], 'common': common}
    
#     rows = []
#     for cp in checkpoints:
#         row = common.copy()
#         row['Sr No'] = cp.get('id', '')
#         row['Check Point Task'] = cp.get('task', '')
#         row['Done On'] = cp.get('doneOn', '')
#         row['Status'] = cp.get('status', '')
#         row['Remarks'] = cp.get('remarks', '')
#         rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_projection_welder(rid, prep_by, app_by):
#     r = ProjectionWelderQual.objects.get(id=rid)
#     common = {
#         'Date': str(r.date) if r.date else '',
#         'WPS No': r.wps_no or '',
#         'Process': r.welding_process or '',
#         'Base Metal': r.base_metal or '',
#         'Thickness': r.base_metal_thickness or '',
#         'Machine No': r.machine_no or '',
#         'Welder Name': r.welder_name or '',
#         'Status': r.qualification_status or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     trials = parse_json_field(r.trials)
#     if not trials:
#         return {'data': [common], 'common': common}
        
#     rows = []
#     for i, t in enumerate(trials):
#         for param, val in [('Squeeze Time', t.get('squeeze', '')), ('Weld Time', t.get('weld', '')), ('Hold Time', t.get('hold', '')), ('Off Time', t.get('off', '')), ('Current (Amp/KA)', t.get('current', '')), ('Pressure (Bar)', t.get('pressure', '')), ('Torque', t.get('torque', '')), ('Visual', t.get('visual', ''))]:
#             row = common.copy()
#             row['Category'] = f"Trial {i+1}"
#             row['Parameter'] = param
#             row['Value / Status'] = val
#             rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_spot_welder(rid, prep_by, app_by):
#     r = SpotWelderQual.objects.get(id=rid)
#     common = {
#         'Date': str(r.date) if r.date else '',
#         'WPS No': r.wps_no or '',
#         'Gun Type': r.gun_type or '',
#         'Base Metal': r.base_metal or '',
#         'Thickness': r.base_metal_thickness or '',
#         'Machine No': r.machine_no or '',
#         'Welder Name': r.welder_name or '',
#         'Status': r.qualification_status or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     trials = parse_json_field(r.trials)
#     if not trials:
#         return {'data': [common], 'common': common}
        
#     rows = []
#     for i, t in enumerate(trials):
#         for param, val in [('Squeeze Time', t.get('squeeze', '')), ('Weld Time', t.get('weld', '')), ('Hold Time', t.get('hold', '')), ('Off Time', t.get('off', '')), ('Current (Amp)', t.get('current', '')), ('Pressure (Bar)', t.get('pressure', '')), ('Strength (Nugget)', t.get('strength', '')), ('Visual', t.get('visual', ''))]:
#             row = common.copy()
#             row['Category'] = f"Trial {i+1}"
#             row['Parameter'] = param
#             row['Value / Status'] = val
#             rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_tig_mig_welder(rid, prep_by, app_by):
#     r = TigMigWelderQual.objects.get(id=rid)
#     common = {
#         'Testing Date': str(r.testing_date) if r.testing_date else '',
#         'WPS No': r.wps_no or '',
#         'Process': r.welding_process or '',
#         'Machine No': r.machine_no or '',
#         'Base Metal': r.base_metal or '',
#         'Thickness': r.base_metal_thickness or '',
#         'Filler Material': r.filler_material or '',
#         'Shielding Gas': r.shielding_gas or '',
#         'Welder Name': r.welder_name or '',
#         'Status': r.qualification_status or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     trials = parse_json_field(r.trials)
#     if not trials:
#         return {'data': [common], 'common': common}
        
#     rows = []
#     for i, t in enumerate(trials):
#         for param, val in [('Current Range', t.get('currRange', '')), ('Current Actual', t.get('currActual', '')), ('Voltage Range', t.get('voltRange', '')), ('Voltage Actual', t.get('voltActual', '')), ('Gas Flow Range', t.get('gasRange', '')), ('Gas Flow Actual', t.get('gasActual', '')), ('Deposit Thickness', t.get('depositThickness', '')), ('Defect', t.get('defect', ''))]:
#             row = common.copy()
#             row['Category'] = f"Trial {i+1}"
#             row['Parameter'] = param
#             row['Value / Status'] = val
#             rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_process_validation(rid, prep_by, app_by):
#     r = ProcessValidation.objects.get(id=rid)
#     ops = parse_json_field(r.operators)
#     operators_str = ", ".join([op for op in ops if op]) if isinstance(ops, list) else ""
#     common = {
#         'Validation Date': str(r.validation_date) if r.validation_date else '',
#         'Process Name': r.process_name or '',
#         'Machine No': r.machine_no or '',
#         'Part Name': r.part_name or '',
#         'Operators': operators_str,
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     trials = parse_json_field(r.trials)
#     if not trials:
#         return {'data': [common], 'common': common}
        
#     rows = []
#     for i, t in enumerate(trials):
#         for param, val in [('Parameter 1 (P1)', t.get('p1', '')), ('Parameter 2 (P2)', t.get('p2', '')), ('Parameter 3 (P3)', t.get('p3', '')), ('Parameter 4 (P4)', t.get('p4', '')), ('Parameter 5 (P5)', t.get('p5', '')), ('Parameter 6 (P6)', t.get('p6', '')), ('Parameter 7 (P7)', t.get('p7', '')), ('Parameter 8 (P8)', t.get('p8', '')), ('Characteristics Spec', t.get('specified', '')), ('Characteristics Obs', t.get('observed', '')), ('Decision', t.get('decision', ''))]:
#             row = common.copy()
#             row['Category'] = f"Trial {i+1}"
#             row['Parameter'] = param
#             row['Value / Status'] = val
#             rows.append(row)
#     return {'data': rows, 'common': common}


# def handle_four_m_display(rid, prep_by, app_by):
#     r = FourMDisplay.objects.get(id=rid)
#     data = {
#         'Machine No': r.machine_no or '',
#         'Operator Name': r.operator_name or '',
#         'Man': r.man or '',
#         'Machine': r.machine or '',
#         'Material': r.material or '',
#         'Method': r.method or '',
#         'Date': str(r.date_filled) if r.date_filled else '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     return {'data': data}


# def handle_four_m_summary(rid, prep_by, app_by):
#     r = FourMSummary.objects.get(id=rid)
#     data = {
#         'Date': str(r.date) if r.date else '',
#         'Customer': r.customer or '',
#         'Part Name & No': r.part_name_no or '',
#         'Type of Change': r.type_of_change or '',
#         'Change Detail': r.change_detail or '',
#         'Retro Total Qty': r.retro_total_qty if r.retro_total_qty is not None else '',
#         'Retro OK Qty': r.retro_ok_qty if r.retro_ok_qty is not None else '',
#         'Retro Rej Qty': r.retro_rej_qty if r.retro_rej_qty is not None else '',
#         'Status After Final Insp': r.status_after_final or '',
#         'Action for NG Material': r.action_for_ng or '',
#         'Sup Signature': r.sup_signature or '',
#         'Sign Prod Head': r.sign_prod_head or '',
#         'Sign QA Head': r.sign_qa_head or '',
#         'Remarks': r.remarks or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#     }
#     return {'data': data}


# def handle_four_m_information(rid, prep_by, app_by):
#     r = FourMInformationSheet.objects.get(id=rid)
#     data = {
#         'Time': str(r.time) if r.time else '',
#         'Machine No': r.machine_no or '',
#         'Operator Name': r.operator_name or '',
#         'Man': r.man or '',
#         'Machine': r.machine or '',
#         'Material': r.material or '',
#         'Method': r.method or '',
#         'Change Description': r.change_description or '',
#         'Prepared By': prep_by,
#         'Approved By': app_by,
#         'Date': str(r.date_filled) if r.date_filled else '',
#     }
#     return {'data': data}


# # ══════════════════════════════════════════════════════════
# # 3. REGISTRY (Mapping Form Keys to Functions)
# # ══════════════════════════════════════════════════════════
# FORM_HANDLERS = {
#     'daily-prod-plan': handle_daily_prod_plan,
#     'four-m-inspection': handle_four_m_inspection,
#     'four-m-record': handle_four_m_record,
#     'tip-change': handle_tip_change,
#     'bin-trolley': handle_bin_trolley,
#     'five-s': handle_five_s,
#     'monthly-prod-plan': handle_monthly_prod_plan,
#     'operator-observance-checklist': handle_operator_observance_checklist,
#     'operator-observance-plan': handle_operator_observance_plan,
#     'pm-checklist-mhe': handle_pm_checklist_mhe,
#     'projection-welder': handle_projection_welder,
#     'spot-welder': handle_spot_welder,
#     'tig-mig-welder': handle_tig_mig_welder,
#     'process-validation': handle_process_validation,
#     'four-m-display': handle_four_m_display,
#     'four-m-summary': handle_four_m_summary,
#     'four-m-information': handle_four_m_information,
# }


# # ══════════════════════════════════════════════════════════
# # 4. THE MAIN VIEW (Clean & Modular)
# # ══════════════════════════════════════════════════════════
# @api_view(['GET'])
# def get_record_by_id(request, form_key, record_id):
#     """
#     Ek specific record ko uske ID se fetch karta hai.
#     user_report_activity_logs mein jo record_id hai, 
#     wahi is table ka primary key (id) hai.
#     """
#     # ⏱️ Timer Start
#     start_time = time.perf_counter() 

#     try:
#         rid = int(record_id)
#     except (ValueError, TypeError):
#         return JsonResponse({'data': None, 'error': 'Invalid record_id'}, status=400)

#     # 1. Check if the form key exists in our registry
#     handler_func = FORM_HANDLERS.get(form_key)
#     if not handler_func:
#         return JsonResponse({'data': None, 'error': f'Form key "{form_key}" not supported'}, status=400)

#     # 2. Fetch Logs for 'Prepared By' & 'Approved By'
#     prep_by, app_by = get_log_details(rid)

#     # 3. Execute the handler and return response
#     try:
#         response_data = handler_func(rid, prep_by, app_by)
        
#         # ⏱️ Calculate Time
#         end_time = time.perf_counter()
#         execution_time = (end_time - start_time) * 1000
#         print(f"🔥 YEH API ({form_key}) CHALNE MEIN {execution_time:.4f} ms LAGE! 🔥", flush=True)
        
#         return JsonResponse(response_data)
        
#     except ObjectDoesNotExist:
#         return JsonResponse({'data': None, 'error': f'Record #{rid} not found'}, status=404)
        
#     except Exception as e:
#         logger.exception(f"Error in {form_key} for ID {rid}: {e}")
#         return JsonResponse({'data': None, 'error': str(e)}, status=500)