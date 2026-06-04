import json
import traceback
from datetime import datetime

from django.db import connection, transaction
from django.shortcuts import get_object_or_404
from django.http import JsonResponse

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view

# ==============================================================================
# IMPORTS FROM MAIN API APP
# ==============================================================================
from api.models import (
    DailyPowerPressChecksheet, MachineHistoryCard, MachineBreakdownIntimation,
    ToolHistoryReport, ToolPreventiveMaintenance, MachineBreakdown, ToolBreakdown,
    MachineCriticalSpare, ToolCriticalSpare, MachineChecksheetReport
)

from api.serializers import (
    MachineBreakdownSerializer, ToolBreakdownSerializer,
    MachineCriticalSpareSerializer, ToolCriticalSpareSerializer
)

# ==============================================================================
# HELPER FUNCTIONS
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

# =========================================================
# 🛠️ MAINTENANCE SAVE APIs
# =========================================================

class SaveDailyPowerPressView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get('items', []) if 'items' in data else [data]
            else:
                items = []

            entries_to_create = []
            for row in items:
                entries_to_create.append(
                    DailyPowerPressChecksheet(
                        plant=row.get('plant', ''),
                        operator_name=row.get('operator_name', ''),
                        machine_no=row.get('machine_no', ''),
                        shift=row.get('shift', ''),
                        date=row.get('date'),
                        checkpoints=row.get('checkpoints', []) 
                    )
                )
            
            if entries_to_create:
                DailyPowerPressChecksheet.objects.bulk_create(entries_to_create)
                return Response({"success": True, "message": "✅ Daily Power Press Checksheet Data Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": "Bhai, koi data nahi mila save karne ke liye."}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            print(f"Backend Crash: {str(e)}") 
            return Response({"success": False, "error": f"Server Error: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveMachineHistoryCardView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            machine_details = data.get('machineDetails', {})
            history_data = data.get('historyData', [])
            signatures = data.get('signatures', {})

            if not machine_details.get('machineName') or not machine_details.get('machineNo'):
                return Response({
                    "success": False, 
                    "error": "Machine Name and No. are required."
                }, status=status.HTTP_400_BAD_REQUEST)

            card = MachineHistoryCard.objects.create(
                machine_name=machine_details.get('machineName', ''),
                machine_no=machine_details.get('machineNo', ''),
                machine_specs=machine_details.get('machineSpecs', ''),
                location=machine_details.get('location', ''),
                history_records=history_data,
                prepared_by=signatures.get('preparedBy', ''),
                approved_by=signatures.get('approvedBy', '')
            )

            return Response({
                "success": True, 
                "message": "Machine History Card Saved Successfully",
                "id": card.id
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            print(f"Backend Crash: {str(e)}")
            return Response({"success": False, "error": f"Server Error: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveMachineBreakdownView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data

            def get_val(key, default=None):
                val = data.get(key)
                return val if val != '' else default

            intimation = MachineBreakdownIntimation.objects.create(
                given_date=get_val('givenDate'),
                given_time=get_val('givenTime'),
                machine_name_no=data.get('machineNameNo', ''),
                breakdown_name=data.get('breakdownName', ''),
                part_made_after_inspection=data.get('partMadeAfterInspection', ''),
                breakdown_desc=data.get('breakdownDesc', ''),
                repair_date=get_val('repairDate'),
                repair_time=get_val('repairTime'),
                repair_hours=get_val('repairHours'),
                mechanics_count=get_val('mechanicsCount'),
                repair_desc=data.get('repairDesc', ''),
                status=data.get('status', 'OK'),
                verification_date=get_val('verificationDate'),
                verification_time=get_val('verificationTime'),
                language=data.get('language', 'english')
            )

            return Response({
                "success": True, 
                "message": "✅ Machine Breakdown Intimation Saved Successfully!",
                "id": intimation.id
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            print(f"Backend Crash: {str(e)}")
            traceback.print_exc()
            return Response({"success": False, "error": f"Server Error: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveToolHistoryView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            tool_info = data.get('toolInformation', {})
            history_rec = data.get('historyRecord', {})

            report = ToolHistoryReport.objects.create(
                filled_date=clean_val(data.get('filledDate')),
                part_name=tool_info.get('partName', ''),
                part_no=tool_info.get('partNo', ''),
                tool_name=tool_info.get('toolName', ''),
                model=tool_info.get('model', ''),
                customer_name=tool_info.get('customerName', ''),
                estimated_tool_life=tool_info.get('estimatedToolLife', ''),
                estimated_maintenance_frequency=tool_info.get('estimatedMaintenanceFrequency', ''),
                date=clean_val(history_rec.get('date')),
                prod=history_rec.get('prod', ''),
                resharpening_stroke=history_rec.get('resharpeningStroke', ''),
                cumulative_prod=history_rec.get('cumulativeProd', ''),
                problem_reported=history_rec.get('problemReported', ''),
                action_taken=history_rec.get('actionTaken', ''),
                updated_in_4m=history_rec.get('updatedIn4M', ''),
                remarks=history_rec.get('remarks', '')
            )

            return Response({"success": True, "message": "✅ Tool History Data Saved!"}, status=status.HTTP_201_CREATED)

        except Exception as e:
            print(f"Tool History Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveToolPreventiveMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            
            ToolPreventiveMaintenance.objects.create(
                tool_name=data.get('toolName', ''),
                part_name=data.get('partName', ''),
                part_no=data.get('partNo', ''),
                operation_no=data.get('operationNo', ''),
                maintenance_person=data.get('maintenancePerson', ''),
                maintenance_data=data.get('formData', {})
            )
            
            return Response({"success": True, "message": "✅ Tool PM Record Saved!"}, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            print(f"Tool PM Save Error: {str(e)}")
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveMachineBreakdownSummaryView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'date': parse_date(raw_data.get('date')),
                'machine_type_no': raw_data.get('machineTypeNo', ''),
                'details': {
                    'problem_description': raw_data.get('problemDescription', ''),
                    'time_period_maintenance': raw_data.get('timePeriodMaintenance', ''),
                    'status_after_period': raw_data.get('statusAfterPeriod', ''),
                    'updated_in_4m': raw_data.get('updatedIn4m', ''),
                    'sign': raw_data.get('sign', ''),
                    'remarks': raw_data.get('remarks', '')
                }
            }

            serializer = MachineBreakdownSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Machine Breakdown Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveToolBreakdownSummaryView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'date': parse_date(raw_data.get('date')),
                'tool_name': raw_data.get('toolName', ''),
                'details': {
                    'process_name': raw_data.get('processName', ''),
                    'problem': raw_data.get('problem', ''),
                    'action_taken': raw_data.get('actionTaken', ''),
                    'total_time_taken': raw_data.get('totalTimeTaken', ''),
                    'checked_by': raw_data.get('checkedBy', ''),
                    'history_card_status': raw_data.get('historyCardStatus', ''),
                    'updated_in_4m': raw_data.get('updatedIn4M', ''),
                    'sign': raw_data.get('sign', ''),
                    'remarks': raw_data.get('remarks', '')
                }
            }

            serializer = ToolBreakdownSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Tool Breakdown Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveMachineCriticalSpareView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'date': parse_date(raw_data.get('date', raw_data.get('currentDate'))),
                'spare_description': raw_data.get('spareDescription', ''),
                'model_description': raw_data.get('modelDescription', ''),
                'box_location': raw_data.get('boxLocation', 'STORE ROOM'),
                'prepared_by': raw_data.get('preparedBy', ''),
                'approved_by': raw_data.get('approvedBy', ''),
                'spare_details': {
                    'spare_type': raw_data.get('spareType', 'REPLACEMENT'),
                    'uom': raw_data.get('uom', ''),
                    'opening_stock': raw_data.get('openingStock', ''),
                    'minimum_level': raw_data.get('minimumLevel', ''),
                    'maximum_level': raw_data.get('maximumLevel', ''),
                    'reorder_level': raw_data.get('reorderLevel', ''),
                    'lead_time': raw_data.get('leadTime', ''),
                    'closing_stock': raw_data.get('closingStock', ''),
                    'pr_status': raw_data.get('prStatus', '')
                }
            }

            serializer = MachineCriticalSpareSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Machine Critical Spare Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveToolCriticalSpareView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            # 🛠️ BUG FIXED: request.dat ko request.data kiya
            raw_data = request.data
            
            mapped_data = {
                'date': parse_date(raw_data.get('date')),
                'spare_description': raw_data.get('spareDescription', ''),
                'model_description': raw_data.get('modelDescription', ''),
                'box_location': raw_data.get('boxLocation', 'STORE ROOM'),
                'spare_details': {
                    'spare_type': raw_data.get('spareType', 'REPLACEMENT'),
                    'uom': raw_data.get('uom', ''),
                    'opening_stock': raw_data.get('openingStock', ''),
                    'minimum_level': raw_data.get('minimumLevel', ''),
                    'lead_time': raw_data.get('leadTime', '')
                }
            }

            serializer = ToolCriticalSpareSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Tool Critical Spare Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# ==============================================================================
# 📊 MAINTENANCE DATA FETCH API
# ==============================================================================

@api_view(['GET'])
def maintenance_data_view(request, form_key):
    try:
        # ── 1. MACHINE HISTORY CARD ─────────────────────────────
        if form_key == 'mc_history':
            cards = MachineHistoryCard.objects.all().order_by('-created_at')
            data = []
            for card in cards:
                history_list = card.history_records or []
                if not history_list:
                    data.append({
                        'Machine Name': card.machine_name,
                        'Machine No.': card.machine_no,
                        'Machine Specs': card.machine_specs or '—',
                        'Location': card.location or '—',
                        'Date': '—', 'Problem': '—', 'Action Taken': '—',
                        '4M Update': '—', 'Signature': '—', 'Remarks': '—',
                        'Prepared By': card.prepared_by or '—',
                        'Approved By': card.approved_by or '—',
                    })
                else:
                    for i, record in enumerate(history_list):
                        if not record.get('date') and not record.get('problem'):
                            continue
                        show_base = (i == 0)
                        data.append({
                            'Machine Name': card.machine_name if show_base else '',
                            'Machine No.': card.machine_no if show_base else '',
                            'Machine Specs': (card.machine_specs or '—') if show_base else '',
                            'Location': (card.location or '—') if show_base else '',
                            'Date': record.get('date', '—'),
                            'Problem': record.get('problem', '—'),
                            'Action Taken': record.get('actionTaken', '—'),
                            '4M Update': record.get('update4M', '—'),
                            'Signature': record.get('signature', '—'),
                            'Remarks': record.get('remarks', '—'),
                            'Prepared By': (card.prepared_by or '—') if show_base else '',
                            'Approved By': (card.approved_by or '—') if show_base else '',
                        })
            return JsonResponse({'data': data})
            
        # ── 2. MACHINE BREAKDOWN INTIMATION ────────────
        elif form_key == 'mc_breakdown':
            reports = MachineBreakdownIntimation.objects.all().order_by('-created_at')
            data = []
            for report in reports:
                data.append({
                    'Given Date': str(report.given_date) if report.given_date else '—',
                    'Given Time': str(report.given_time) if report.given_time else '—',
                    'Machine Name & No.': report.machine_name_no or '—',
                    'Breakdown Name': report.breakdown_name or '—',
                    'Part Made': report.part_made_after_inspection or '—',
                    'Breakdown Desc': report.breakdown_desc or '—',
                    'Repair Date': str(report.repair_date) if report.repair_date else '—',
                    'Repair Time': str(report.repair_time) if report.repair_time else '—',
                    'Repair Hours': str(report.repair_hours) if report.repair_hours else '—',
                    'Mechanics Involved': str(report.mechanics_count) if report.mechanics_count else '—',
                    'Repair Desc': report.repair_desc or '—',
                    'Quality Status': report.status or '—',
                    'Verification Date': str(report.verification_date) if report.verification_date else '—',
                    'Verification Time': str(report.verification_time) if report.verification_time else '—',
                })
            return JsonResponse({'data': data})

        # ── 3. POWER PRESS CHECKSHEET ───────────────────────────
        elif form_key == 'power_press_check':
            checks = DailyPowerPressChecksheet.objects.all().order_by('-date', '-created_at')
            data = []
            for check in checks:
                base_data = {
                    'Date': str(check.date) if check.date else '—',
                    'Shift': check.shift or '—',
                    'Plant': check.plant or '—',
                    'Machine No': check.machine_no or '—',
                    'Operator Name': check.operator_name or '—',
                }

                checkpoint_list = check.checkpoints
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except: checkpoint_list = []

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        show_base = (i == 0)
                        if isinstance(cp, dict):
                            is_ok_val = cp.get('status', cp.get('is_ok', cp.get('value')))
                            if is_ok_val is True or str(is_ok_val).upper() == 'OK': cp_status = 'OK'
                            elif is_ok_val is False or str(is_ok_val).upper() in ['NG', 'NOT OK']: cp_status = 'NOT OK'
                            else: cp_status = str(is_ok_val) if is_ok_val is not None else '—'

                            data.append({
                                'Date': base_data['Date'] if show_base else '',
                                'Shift': base_data['Shift'] if show_base else '',
                                'Plant': base_data['Plant'] if show_base else '',
                                'Machine No': base_data['Machine No'] if show_base else '',
                                'Operator Name': base_data['Operator Name'] if show_base else '',
                                'Check Point': cp.get('Check Point', cp.get('checkPoint', cp.get('check_point', cp.get('checkpoint', cp.get('name', '—'))))),
                                'Specification': cp.get('Specification', cp.get('specification', cp.get('spec', '—'))),
                                'Method': cp.get('Method', cp.get('method', cp.get('checkingMethod', cp.get('checking_method', '—')))),
                                'Observed Value': cp.get('Observed Value', cp.get('observedValue', cp.get('observed_value', cp.get('observed', cp.get('value', '—'))))),
                                'Status': cp_status,
                            })
                        else:
                            data.append({
                                **{k: v if show_base else '' for k, v in base_data.items()},
                                'Check Point': str(cp), 'Specification': '—', 'Method': '—', 'Observed Value': '—', 'Status': '—'
                            })
                else:
                    row_data = base_data.copy()
                    row_data.update({'Check Point': 'No checkpoints recorded', 'Specification': '—', 'Method': '—', 'Observed Value': '—', 'Status': '—'})
                    data.append(row_data)

            return JsonResponse({'data': data})

        # ── 4. TOOL HISTORY CARD ────────────────────────────────────────────
        elif form_key == 'tool_history':
            reports = ToolHistoryReport.objects.all().order_by('-created_at')
            data = []
            for r in reports:
                data.append({
                    'Date': str(r.date) if r.date else '—',
                    'Tool Name': r.tool_name or '—',
                    'Part Name & No': f"{r.part_name} ({r.part_no})" if r.part_name else '—',
                    'Customer': r.customer_name or '—',
                    'Prod Count': r.prod or '—',
                    'Resharp Stroke': r.resharpening_stroke or '—',
                    'Cumulative Prod': r.cumulative_prod or '—',
                    'Problem Reported': r.problem_reported or '—',
                    'Action Taken': r.action_taken or '—',
                    '4M Update': r.updated_in_4m or '—',
                    'Remarks': r.remarks or '—',
                })
            return JsonResponse({'data': data})

        # ── 5. TOOL PREVENTIVE MAINTENANCE ───────
        elif form_key == 'tool_pm_check':
            reports = ToolPreventiveMaintenance.objects.all().order_by('-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date),
                    'Tool Name': report.tool_name,
                    'Part Name': report.part_name or '—',
                    'Part No.': report.part_no or '—',
                    'Op No.': report.operation_no or '—',
                    'Maint. Person': report.maintenance_person or '—',
                }

                maint_data = report.maintenance_data
                if maint_data and isinstance(maint_data, dict):
                    i = 0
                    for key, vals in maint_data.items():
                        parts = key.split('-', 1)
                        item_name = parts[0] if len(parts) > 0 else 'Unknown'
                        checkpoint_name = parts[1] if len(parts) > 1 else 'Unknown'
                        show_base = (i == 0)
                        
                        row_data = {
                            **{k: v if show_base else '' for k, v in base_data.items()},
                            'Item': item_name,
                            'Checkpoint': checkpoint_name,
                            'Before Maint.': vals.get('beforeMaint', '—'),
                            'After Maint.': vals.get('afterMaint', '—'),
                            'Remarks': vals.get('remark', '—'),
                        }
                        data.append(row_data)
                        i += 1
                else:
                    row_data = base_data.copy()
                    row_data.update({
                        'Item': 'No data recorded', 'Checkpoint': '—', 'Before Maint.': '—', 'After Maint.': '—', 'Remarks': '—'
                    })
                    data.append(row_data)

            return JsonResponse({'data': data})

        # ── 6. MACHINE BREAKDOWN SUMMARY ──────────────────────────────
        elif form_key == 'mc_breakdown_summary':
            reports = MachineBreakdown.objects.all().order_by('-created_at')
            data = []
            for r in reports:
                details = r.details or {}
                data.append({
                    'Date': str(r.date) if r.date else '—',
                    'Machine Type & No.': r.machine_type_no or '—',
                    'Problem Description': details.get('problem_description', '—'),
                    'Time Period': details.get('time_period_maintenance', '—'),
                    'Status': details.get('status_after_period', '—'),
                    '4M Update': details.get('updated_in_4m', '—'),
                    'Sign': details.get('sign', '—'),
                    'Remarks': details.get('remarks', '—'),
                })
            return JsonResponse({'data': data})

        # ── 7. TOOL BREAKDOWN SUMMARY ─────────────────────────────────
        elif form_key == 'tool_breakdown_summary':
            reports = ToolBreakdown.objects.all().order_by('-created_at')
            data = []
            for r in reports:
                details = r.details or {}
                data.append({
                    'Date': str(r.date) if r.date else '—',
                    'Tool Name': r.tool_name or '—',
                    'Process Name': details.get('process_name', '—'),
                    'Problem': details.get('problem', '—'),
                    'Action Taken': details.get('action_taken', '—'),
                    'Total Time': details.get('total_time_taken', '—'),
                    'Checked By': details.get('checked_by', '—'),
                    'History Card': details.get('history_card_status', '—'),
                    '4M Update': details.get('updated_in_4m', '—'),
                    'Sign': details.get('sign', '—'),
                    'Remarks': details.get('remarks', '—'),
                })
            return JsonResponse({'data': data})

        # ── 8. MACHINE CRITICAL SPARE ─────────────────────────────────
        elif form_key == 'critical_spares':
            reports = MachineCriticalSpare.objects.all().order_by('-created_at')
            data = []
            for r in reports:
                details = r.spare_details or {}
                data.append({
                    'Date': str(r.date) if r.date else '—',
                    'Spare Description': r.spare_description or '—',
                    'Model / Box No.': r.model_description or '—',
                    'Location': r.box_location or '—',
                    'Prepared By': r.prepared_by or '—',
                    'Approved By': r.approved_by or '—',
                    'Spare Type': details.get('spare_type', '—'),
                    'UOM': details.get('uom', '—'),
                    'Opening Stock': details.get('opening_stock', '—'),
                    'Minimum Level': details.get('minimum_level', '—'),
                    'Maximum Level': details.get('maximum_level', '—'),
                    'Reorder Level': details.get('reorder_level', '—'),
                    'Lead Time': details.get('lead_time', '—'),
                    'Closing Stock': details.get('closing_stock', '—'),
                    'PR Status': details.get('pr_status', '—'),
                })
            return JsonResponse({'data': data})

        # ── 9. TOOL CRITICAL SPARE ────────────────────────────────────
        elif form_key == 'tool_critical_spares':
            reports = ToolCriticalSpare.objects.all().order_by('-created_at')
            data = []
            for r in reports:
                details = r.spare_details or {}
                data.append({
                    'Date': str(r.date) if r.date else '—',
                    'Spare Description': r.spare_description or '—',
                    'Model / Box No.': r.model_description or '—',
                    'Location': r.box_location or '—',
                    'Spare Type': details.get('spare_type', '—'),
                    'UOM': details.get('uom', '—'),
                    'Opening Stock': details.get('opening_stock', '—'),
                    'Minimum Level': details.get('minimum_level', '—'),
                    'Lead Time': details.get('lead_time', '—'),
                })
            return JsonResponse({'data': data})

        # ── 10. POKAYOKE ──────────────────────────────────────────────
        elif form_key == 'Poka-Yoke':
            reports = MachineChecksheetReport.objects.prefetch_related(
                'check_points'
            ).order_by('-date', '-created_at')

            data = []
            for report in reports:
                for obs in report.check_points.all():
                    data.append({
                        'Poka Yoke Detail': obs.poka_yoke_detail,
                        'Checking Method':  obs.checking_method,
                        'Result':           'OK' if obs.is_ok else 'NOT OK',
                        'Plant':            report.plant_name,
                        'Machine No':       report.machine_no,
                        'Checked By':       report.checked_by_maintenance or '—',
                        'Verified By':      report.verified_by_production or '—',
                        'Remarks':          obs.remarks or '—',
                        'Date':             str(report.date),
                    })
            return JsonResponse({'data': data})

        else:
            return JsonResponse({'data': [], 'error': 'Maintenance form type not supported yet'}, status=400)

    except Exception as e:
        print(f"⚠️ Maintenance View Error: {e}")
        traceback.print_exc()
        return JsonResponse({'data': [], 'error': str(e)}, status=500)