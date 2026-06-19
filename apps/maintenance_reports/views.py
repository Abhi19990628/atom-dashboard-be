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
    MachineCriticalSpare, ToolCriticalSpare, MachineChecksheetReport,ToolBreakdownIntimation,
    TigWeldingMaintenance, SpotWeldingMaintenance, CompressorMaintenance, 
    LatheMachineMaintenance, VerticalDrillMachineMaintenance, SurfaceGrinderMaintenance, 
    BaseGrinderMaintenance, BeltGrinderMaintenance, PipeCuttingMaintenance, 
    VibraMaintenance, DipMoldingMaintenance, ServoPressMaintenance,
    MachinePreventiveMaintenance, CNCMaintenanceReport, VerticalMillingMachineCheckSheet, 
    ProjectionWeldingPMCheckSheet, PowerPressPMCheckSheet, HydraulicPMCheckSheet,
    PartMaster,FixtureMaintenanceRecord
    
)

from api.serializers import (
    MachineBreakdownSerializer, ToolBreakdownSerializer,
    MachineCriticalSpareSerializer, ToolCriticalSpareSerializer,
    SpotWeldingMaintenanceSerializer,
    CompressorMaintenanceSerializer,
    LatheMachineMaintenanceSerializer,
    VerticalDrillMachineMaintenanceSerializer,
    SurfaceGrinderMaintenanceSerializer,
    TigWeldingMaintenanceSerializer,BaseGrinderMaintenanceSerializer,
    BeltGrinderMaintenanceSerializer,
    PipeCuttingMaintenanceSerializer,
    VibraMaintenanceSerializer,
    DipMoldingMaintenanceSerializer,
    ServoPressMaintenanceSerializer,
    MachinePreventiveMaintenanceSerializer,
   CNCMaintenanceReportSerializer,
   VerticalMillingMachineCheckSheetSerializer,
   ProjectionWeldingPMCheckSheetSerializer,
   PowerPressPMCheckSheetSerializer,
   HydraulicPMCheckSheetSerializer, FixtureMaintenanceRecordSerializer
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


class SaveToolBreakdownSlipView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data
            # helper to check multiple key variants and nested containers
            def get_field(*keys, default=None):
                # check top-level keys and common nested containers used by frontends
                containers = [data]
                for c in ('formData', 'form_data', 'data', 'payload'):
                    if isinstance(data, dict) and c in data and isinstance(data[c], dict):
                        containers.append(data[c])

                for k in keys:
                    for cont in containers:
                        if k in cont:
                            val = cont.get(k)
                            return val if val != '' else default
                return default

            # accept camelCase and snake_case keys
            intimation = ToolBreakdownIntimation.objects.create(
                doc_no=get_field('docNo', 'doc_no', default='AOT-F-BD-01'),

                # Production Section
                reporter_name=get_field('reporterName', 'reporter_name', default=''),
                report_date=get_field('reportDate', 'report_date', default=None),
                machine_name_no=get_field('machineNameNo', 'machine_name_no', default=''),
                report_time=get_field('reportTime', 'report_time', default=None),
                breakdown_details=get_field('breakdownDetails', 'breakdown_details', default=''),
                prod_supervisor_name=get_field('prodSupervisorName', 'prod_supervisor_name', default=''),

                # Maintenance Section
                maint_date=get_field('maintDate', 'maint_date', default=None),
                maint_time=get_field('maintTime', 'maint_time', default=None),
                time_taken_to_rectify=get_field('timeTakenToRectify', 'time_taken_to_rectify', default=''),
                men_engaged=get_field('menEngaged', 'men_engaged', default=None),
                action_taken_details=get_field('actionTakenDetails', 'action_taken_details', default=''),
                maint_incharge_name=get_field('maintInchargeName', 'maint_incharge_name', default=''),

                # Quality Section
                status=get_field('status', default='OK'),
                qa_date=get_field('qaDate', 'qa_date', default=None),
                qa_time=get_field('qaTime', 'qa_time', default=None),
                nc_verification=get_field('ncVerification', 'nc_verification', default=''),
                qa_incharge_name=get_field('qaInchargeName', 'qa_incharge_name', default=''),

                # Extra Info
                language=get_field('language', default='hindi')
            )

            return Response({
                "success": True, 
                "message": "✅ Tool Breakdown Intimation Saved Successfully!",
                "id": intimation.id
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            print(f"Backend Crash: {str(e)}")
            traceback.print_exc()
            return Response({"success": False, "error": f"Server Error: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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


##############################################
#Machine maintenance weekly view 
##############################################



class SaveSpotWeldingMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'SPOT WELDING M/C'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),  # JSON Field mapped from tableData
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = SpotWeldingMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Spot Welding Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveCompressorMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'Compressor'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = CompressorMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Compressor Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveLatheMachineMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'LATHE MACHINE'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = LatheMachineMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Lathe Machine Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveVerticalDrillMachineMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'VERTICAL DRILL MACHINE'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = VerticalDrillMachineMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Vertical Drill Machine Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveSurfaceGrinderMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'SURFACE GRINDER'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = SurfaceGrinderMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Surface Grinder Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveTigWeldingMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            # NOTE: TigWeldingMaintenance model mein 'specification' field nahi hai, isliye map nahi kiya.
            mapped_data = {
                'machine_name': raw_data.get('machineName', ''),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = TigWeldingMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Tig Welding Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
class SaveBaseGrinderMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'Base Grinder'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = BaseGrinderMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Base Grinder Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveBeltGrinderMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'BELT GRINDER'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = BeltGrinderMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Belt Grinder Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



class SavePipeCuttingMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'Pipe Cutter'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = PipeCuttingMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Pipe Cutting Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveVibraMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'Vibra'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = VibraMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Vibra Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveDipMoldingMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'Dip Molding Machine'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = DipMoldingMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Dip Molding Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SaveServoPressMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            mapped_data = {
                'machine_name': raw_data.get('machineName', 'Servo Press'),
                'date': parse_date(raw_data.get('date')) if raw_data.get('date') else None,
                'machine_no': raw_data.get('machineNo', ''),
                'location': raw_data.get('location', ''),
                'specification': raw_data.get('specification', ''),
                'maintenance_personnel': raw_data.get('maintenancePersonnel', ''),
                'checkpoints': raw_data.get('tableData', []),
                'prepared_by': raw_data.get('preparedBy', ''),
                'checked_by': raw_data.get('checkedBy', '')
            }

            serializer = ServoPressMaintenanceSerializer(data=mapped_data)
            if serializer.is_valid():
                serializer.save()
                return Response({"success": True, "message": "Servo Press Maintenance Saved!"}, status=status.HTTP_201_CREATED)
            
            return Response({"success": False, "error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveMachinePreventiveMaintenanceView(APIView):
    def post(self, request):
        try:
            serializer = MachinePreventiveMaintenanceSerializer(data=request.data)

            if not serializer.is_valid():
                return Response(
                    {"success": False, "errors": serializer.errors},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            with transaction.atomic():
                report = serializer.save()

            return Response(
                {
                    "success": True,
                    "message": "Machine preventive maintenance saved successfully",
                    "id": report.id,
                },
                status=status.HTTP_201_CREATED,
            )
        except Exception as e:
            traceback.print_exc()
            return Response(
                {"success": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
class SaveCNCMaintenanceReportView(APIView):
    def post(self, request):
        try:
            serializer = CNCMaintenanceReportSerializer(data=request.data)

            if not serializer.is_valid():
                return Response(
                    {"success": False, "errors": serializer.errors},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            with transaction.atomic():
                report = serializer.save()

            return Response(
                {
                    "success": True,
                    "message": "CNC maintenance report saved successfully",
                    "id": report.id,
                },
                status=status.HTTP_201_CREATED,
            )

        except Exception as e:
            traceback.print_exc()
            return Response(
                {"success": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        

class SaveVerticalMillingMachineCheckSheetView(APIView):
    def post(self, request):
        try:
            serializer = VerticalMillingMachineCheckSheetSerializer(data=request.data)

            if not serializer.is_valid():
                return Response(
                    {"success": False, "errors": serializer.errors},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            with transaction.atomic():
                report = serializer.save()

            return Response(
                {
                    "success": True,
                    "message": "Vertical milling machine check sheet saved successfully",
                    "id": report.id,
                },
                status=status.HTTP_201_CREATED,
            )

        except Exception as e:
            traceback.print_exc()
            return Response(
                {"success": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )    

class SaveProjectionWeldingPMCheckSheetView(APIView):
    def post(self, request):
        try:
            serializer = ProjectionWeldingPMCheckSheetSerializer(data=request.data)

            if not serializer.is_valid():
                return Response(
                    {"success": False, "errors": serializer.errors},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            with transaction.atomic():
                report = serializer.save()

            return Response(
                {
                    "success": True,
                    "message": "Projection welding PM check sheet saved successfully",
                    "id": report.id,
                },
                status=status.HTTP_201_CREATED,
            )

        except Exception as e:
            traceback.print_exc()
            return Response(
                {"success": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )    
        
class SavePowerPressPMCheckSheetView(APIView):
    def post(self, request):
        try:
            serializer = PowerPressPMCheckSheetSerializer(data=request.data)

            if not serializer.is_valid():
                return Response(
                    {"success": False, "errors": serializer.errors},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            with transaction.atomic():
                report = serializer.save()

            return Response(
                {
                    "success": True,
                    "message": "Power press PM check sheet saved successfully",
                    "id": report.id,
                },
                status=status.HTTP_201_CREATED,
            )

        except Exception as e:
            traceback.print_exc()
            return Response(
                {"success": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )   
     
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



class SaveHydraulicPMCheckSheetView(APIView):
    def post(self, request):
        try:
            # Passing data to our verified serializer setup
            serializer = HydraulicPMCheckSheetSerializer(data=request.data)

            if not serializer.is_valid():
                return Response(
                    {"success": False, "errors": serializer.errors},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Safeguarding database state safely
            with transaction.atomic():
                report = serializer.save()

            return Response(
                {
                    "success": True,
                    "message": "Hydraulic machine PM check sheet saved successfully",
                    "id": report.id,
                },
                status=status.HTTP_201_CREATED,
            )

        except Exception as e:
            traceback.print_exc()
            return Response(
                {"success": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


# ==============================================================================
# 📊 MAINTENANCE DATA FETCH API
# ==============================================================================
@api_view(['GET'])
def maintenance_data_view(request, form_key):
    # ── 🔥 MASTER DATE FILTER FUNCTION ──
    def apply_date_filter(queryset, date_field):
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')

        if start_date == 'all':
            return queryset

        if not start_date and not end_date:
            ist_tz = pytz.timezone('Asia/Kolkata')
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

    try:
        # ── 1. MACHINE HISTORY CARD ──
        if form_key == 'mc_history':
            base_query = MachineHistoryCard.objects.all()
            cards = apply_date_filter(base_query, 'created_at').order_by('-created_at')
            data = []
            for card in cards:
                history_list = card.history_records or []
                if not history_list:
                    data.append({
                        'Machine Name': card.machine_name, 'Machine No.': card.machine_no, 'Machine Specs': card.machine_specs or 'N/A', 'Location': card.location or 'N/A', 'Date': 'N/A', 'Problem': 'N/A', 'Action Taken': 'N/A', '4M Update': 'N/A', 'Signature': 'N/A', 'Remarks': 'N/A', 'Prepared By': card.prepared_by or 'N/A', 'Approved By': card.approved_by or 'N/A',
                    })
                else:
                    for i, record in enumerate(history_list):
                        if not record.get('date') and not record.get('problem'):
                            continue
                        data.append({
                            'Machine Name': card.machine_name, 'Machine No.': card.machine_no, 'Machine Specs': card.machine_specs or 'N/A', 'Location': card.location or 'N/A', 'Date': record.get('date', 'N/A'), 'Problem': record.get('problem', 'N/A'), 'Action Taken': record.get('actionTaken', 'N/A'), '4M Update': record.get('update4M', 'N/A'), 'Signature': record.get('signature', 'N/A'), 'Remarks': record.get('remarks', 'N/A'), 'Prepared By': card.prepared_by or 'N/A', 'Approved By': card.approved_by or 'N/A',
                        })
            return JsonResponse({'data': data})
            
        # ── 2. MACHINE BREAKDOWN INTIMATION ──
        elif form_key == 'mc_breakdown':
            base_query = MachineBreakdownIntimation.objects.all()
            reports = apply_date_filter(base_query, 'given_date').order_by('-created_at')
            data = []
            for report in reports:
                data.append({
                    'Given Date': str(report.given_date) if report.given_date else 'N/A', 'Given Time': str(report.given_time) if report.given_time else 'N/A', 'Machine Name & No.': report.machine_name_no or 'N/A', 'Breakdown Name': report.breakdown_name or 'N/A', 'Part Made': report.part_made_after_inspection or 'N/A', 'Breakdown Desc': report.breakdown_desc or 'N/A', 'Repair Date': str(report.repair_date) if report.repair_date else 'N/A', 'Repair Time': str(report.repair_time) if report.repair_time else 'N/A', 'Repair Hours': str(report.repair_hours) if report.repair_hours else 'N/A', 'Mechanics Involved': str(report.mechanics_count) if report.mechanics_count else 'N/A', 'Repair Desc': report.repair_desc or 'N/A', 'Quality Status': report.status or 'N/A', 'Verification Date': str(report.verification_date) if report.verification_date else 'N/A', 'Verification Time': str(report.verification_time) if report.verification_time else 'N/A',
                })
            return JsonResponse({'data': data})

        # ── 3. DAILY POWER PRESS CHECKSHEET ──
        elif form_key == 'power_press_check':
            base_query = DailyPowerPressChecksheet.objects.all()
            checks = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for check in checks:
                base_data = {
                    'Date': str(check.date) if check.date else 'N/A', 'Shift': check.shift or 'N/A', 'Plant': check.plant or 'N/A', 'Machine No': check.machine_no or 'N/A', 'Operator Name': check.operator_name or 'N/A',
                }
                checkpoint_list = check.checkpoints
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except: checkpoint_list = []

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            is_ok_val = cp.get('status', cp.get('is_ok', cp.get('value')))
                            if is_ok_val is True or str(is_ok_val).upper() == 'OK': cp_status = 'OK'
                            elif is_ok_val is False or str(is_ok_val).upper() in ['NG', 'NOT OK']: cp_status = 'NOT OK'
                            else: cp_status = str(is_ok_val) if is_ok_val is not None else 'N/A'

                            data.append({
                                **base_data,
                                'Sr.': str(i+1),
                                'Check Points': cp.get('Check Point', cp.get('checkPoint', cp.get('check_point', cp.get('checkpoint', cp.get('name', 'N/A'))))), 
                                'Checking Parameter': cp.get('Specification', cp.get('specification', cp.get('spec', 'N/A'))), 
                                'Method': cp.get('Method', cp.get('method', cp.get('checkingMethod', cp.get('checking_method', 'N/A')))), 
                                'Before Maint.': cp.get('Observed Value', cp.get('observedValue', cp.get('observed_value', cp.get('observed', cp.get('value', 'N/A'))))), 
                                'After Maint': cp_status,
                                'Remarks': cp.get('Remarks', cp.get('remarks', cp.get('remark', ''))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 4. TOOL HISTORY CARD ──
        elif form_key == 'tool_history':
            base_query = ToolHistoryReport.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for r in reports:
                data.append({
                    'Date': str(r.date) if r.date else 'N/A', 'Tool Name': r.tool_name or 'N/A', 'Part Name & No': f"{r.part_name} ({r.part_no})" if r.part_name else 'N/A', 'Customer': r.customer_name or 'N/A', 'Prod Count': r.prod or 'N/A', 'Resharp Stroke': r.resharpening_stroke or 'N/A', 'Cumulative Prod': r.cumulative_prod or 'N/A', 'Problem Reported': r.problem_reported or 'N/A', 'Action Taken': r.action_taken or 'N/A', '4M Update': r.updated_in_4m or 'N/A', 'Remarks': r.remarks or 'N/A',
                })
            return JsonResponse({'data': data})

        # ── 5. TOOL PREVENTIVE MAINTENANCE ──
        elif form_key == 'tool_pm_check':
            base_query = ToolPreventiveMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date), 'Tool Name': report.tool_name, 'Part Name': report.part_name or 'N/A', 'Part No.': report.part_no or 'N/A', 'Op No.': report.operation_no or 'N/A', 'Maint. Person': report.maintenance_person or 'N/A',
                }
                maint_data = report.maintenance_data
                if maint_data and isinstance(maint_data, dict):
                    i = 0
                    for key, vals in maint_data.items():
                        parts = key.split('-', 1)
                        item_name = parts[0] if len(parts) > 0 else 'Unknown'
                        checkpoint_name = parts[1] if len(parts) > 1 else 'Unknown'
                        
                        row_data = {
                            **base_data, 
                            'Sr.': str(i+1),
                            'Check Points': f"{item_name} - {checkpoint_name}", 
                            'Checking Parameter': vals.get('parameter', vals.get('specification', 'N/A')),
                            'Method': vals.get('method', vals.get('checkingMethod', 'N/A')), 
                            'Before Maint.': vals.get('beforeMaint', 'N/A'), 
                            'After Maint': vals.get('afterMaint', 'N/A'), 
                            'Remarks': vals.get('remark', 'N/A')
                        }
                        data.append(row_data)
                        i += 1
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No data recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 6. MACHINE BREAKDOWN SUMMARY ──
        elif form_key == 'mc_breakdown_summary':
            base_query = MachineBreakdown.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for r in reports:
                details = r.details or {}
                data.append({
                    'Date': str(r.date) if r.date else 'N/A', 'Machine Type & No.': r.machine_type_no or 'N/A', 'Problem Description': details.get('problem_description', 'N/A'), 'Time Period': details.get('time_period_maintenance', 'N/A'), 'Status': details.get('status_after_period', 'N/A'), '4M Update': details.get('updated_in_4m', 'N/A'), 'Sign': details.get('sign', 'N/A'), 'Remarks': details.get('remarks', 'N/A'),
                })
            return JsonResponse({'data': data})

        # ── 7. TOOL BREAKDOWN SUMMARY ──
        elif form_key == 'tool_breakdown_summary':
            base_query = ToolBreakdown.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for r in reports:
                details = r.details or {}
                data.append({
                    'Date': str(r.date) if r.date else 'N/A', 'Tool Name': r.tool_name or 'N/A', 'Process Name': details.get('process_name', 'N/A'), 'Problem': details.get('problem', 'N/A'), 'Action Taken': details.get('action_taken', 'N/A'), 'Total Time': details.get('total_time_taken', 'N/A'), 'Checked By': details.get('checked_by', 'N/A'), 'History Card': details.get('history_card_status', 'N/A'), '4M Update': details.get('updated_in_4m', 'N/A'), 'Sign': details.get('sign', 'N/A'), 'Remarks': details.get('remarks', 'N/A'),
                })
            return JsonResponse({'data': data})

        # ── 8. MACHINE CRITICAL SPARE ──
        elif form_key == 'critical_spares':
            base_query = MachineCriticalSpare.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for r in reports:
                details = r.spare_details or {}
                data.append({
                    'Date': str(r.date) if r.date else 'N/A', 'Spare Description': r.spare_description or 'N/A', 'Model / Box No.': r.model_description or 'N/A', 'Location': r.box_location or 'N/A', 'Prepared By': r.prepared_by or 'N/A', 'Approved By': r.approved_by or 'N/A', 'Spare Type': details.get('spare_type', 'N/A'), 'UOM': details.get('uom', 'N/A'), 'Opening Stock': details.get('opening_stock', 'N/A'), 'Minimum Level': details.get('minimum_level', 'N/A'), 'Maximum Level': details.get('maximum_level', 'N/A'), 'Reorder Level': details.get('reorder_level', 'N/A'), 'Lead Time': details.get('lead_time', 'N/A'), 'Closing Stock': details.get('closing_stock', 'N/A'), 'PR Status': details.get('pr_status', 'N/A'),
                })
            return JsonResponse({'data': data})

        # ── 9. TOOL CRITICAL SPARE ──
        elif form_key == 'tool_critical_spares':
            base_query = ToolCriticalSpare.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for r in reports:
                details = r.spare_details or {}
                data.append({
                    'Date': str(r.date) if r.date else 'N/A', 'Spare Description': r.spare_description or 'N/A', 'Model / Box No.': r.model_description or 'N/A', 'Location': r.box_location or 'N/A', 'Spare Type': details.get('spare_type', 'N/A'), 'UOM': details.get('uom', 'N/A'), 'Opening Stock': details.get('opening_stock', 'N/A'), 'Minimum Level': details.get('minimum_level', 'N/A'), 'Lead Time': details.get('lead_time', 'N/A'),
                })
            return JsonResponse({'data': data})

        # ── 10. POKAYOKE ──
        elif form_key == 'Poka-Yoke' or form_key == 'pokayoke-view':
            base_query = MachineChecksheetReport.objects.prefetch_related('check_points')
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                for obs in report.check_points.all():
                    data.append({
                        'Poka Yoke Detail': obs.poka_yoke_detail, 'Checking Method':  obs.checking_method, 'Result': 'OK' if obs.is_ok else 'NOT OK', 'Plant': report.plant_name, 'Machine No': report.machine_no, 'Checked By': report.checked_by_maintenance or 'N/A', 'Verified By': report.verified_by_production or 'N/A', 'Remarks': obs.remarks or 'N/A', 'Date': str(report.date),
                    })
            return JsonResponse({'data': data})

        # ── 11. TOOL BREAKDOWN SLIP ──
        elif form_key == 'tool_breakdown_slip':
            base_query = ToolBreakdownIntimation.objects.all()
            reports = apply_date_filter(base_query, 'report_date').order_by('-created_at')
            data = []
            for report in reports:
                data.append({
                    'Reporter Name': report.reporter_name or 'N/A', 'Report Date': str(report.report_date) if report.report_date else 'N/A', 'Machine Name & No.': report.machine_name_no or 'N/A', 'Report Time': str(report.report_time) if report.report_time else 'N/A', 'Breakdown Details': report.breakdown_details or 'N/A', 'Prod Supervisor': report.prod_supervisor_name or 'N/A', 'Maint Date': str(report.maint_date) if report.maint_date else 'N/A', 'Maint Time': str(report.maint_time) if report.maint_time else 'N/A', 'Time Taken': report.time_taken_to_rectify or 'N/A', 'Men Engaged': str(report.men_engaged) if report.men_engaged else 'N/A', 'Action Taken': report.action_taken_details or 'N/A', 'Maint Incharge': report.maint_incharge_name or 'N/A', 'Quality Status': report.status or 'N/A', 'QA Date': str(report.qa_date) if report.qa_date else 'N/A', 'QA Time': str(report.qa_time) if report.qa_time else 'N/A', 'NC Verification': report.nc_verification or 'N/A', 'QA Incharge': report.qa_incharge_name or 'N/A',
                })
            return JsonResponse({'data': data})


        # =========================================================================
        # 🔥🔥🔥 SEPARATED WORKING MACHINES (12 to 29) 🔥🔥🔥
        # =========================================================================

        # ── 12. SURFACE GRINDER ──
        elif form_key == 'surface_grinder':
            base_query = SurfaceGrinderMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 13. TIG WELDING ──
        elif form_key == 'tig_welding':
            base_query = TigWeldingMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 14. SPOT WELDING ──
        elif form_key == 'spot_welding':
            base_query = SpotWeldingMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 15. COMPRESSOR ──
        elif form_key == 'compressor':
            base_query = CompressorMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 16. LATHE MACHINE ──
        elif form_key == 'lathe_machine':
            base_query = LatheMachineMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 17. VERTICAL DRILL ──
        elif form_key == 'vertical_drill':
            base_query = VerticalDrillMachineMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 18. BASE GRINDER ──
        elif form_key == 'base_grinder':
            base_query = BaseGrinderMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 19. BELT GRINDER ──
        elif form_key == 'belt_grinder':
            base_query = BeltGrinderMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 20. PIPE CUTTING ──
        elif form_key == 'pipe_cutting':
            base_query = PipeCuttingMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 21. VIBRA ──
        elif form_key == 'vibra':
            base_query = VibraMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 22. DIP MOLDING ──
        elif form_key == 'dip_molding':
            base_query = DipMoldingMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 23. SERVO PRESS ──
        elif form_key == 'servo_press':
            base_query = ServoPressMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', 'N/A'))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', 'N/A')), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', 'N/A')), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 24. PROJECTION WELD (WEEKLY) ──
        elif form_key == 'projection_weld':
            base_query = ProjectionWeldingPMCheckSheet.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', cp.get('Check Point', 'N/A')))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', cp.get('checkingMethod', 'N/A'))), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', cp.get('observed_value', cp.get('observedValue', 'N/A')))), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 25. VMM (WEEKLY) ──
        elif form_key == 'vmm':
            base_query = VerticalMillingMachineCheckSheet.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', cp.get('Check Point', 'N/A')))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', cp.get('checkingMethod', 'N/A'))), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', cp.get('observed_value', cp.get('observedValue', 'N/A')))), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 26. CNC (WEEKLY) ──
        elif form_key == 'cnc':
            base_query = CNCMaintenanceReport.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                # 🔥 CNC uses "checklist" primarily
                checkpoint_list = getattr(report, 'checklist', getattr(report, 'checkpoints', []))
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', cp.get('Check Point', 'N/A')))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', cp.get('checkingMethod', 'N/A'))), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', cp.get('observed_value', cp.get('observedValue', 'N/A')))), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 27. HYDRAULIC MIG (WEEKLY) ──
        elif form_key == 'hydraulic_mig':
            base_query = HydraulicPMCheckSheet.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', cp.get('Check Point', 'N/A')))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', cp.get('checkingMethod', 'N/A'))), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', cp.get('observed_value', cp.get('observedValue', 'N/A')))), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 28. POWER PRESS (WEEKLY) ──
        elif form_key == 'power_press':
            base_query = PowerPressPMCheckSheet.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', cp.get('Check Point', 'N/A')))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', cp.get('checkingMethod', 'N/A'))), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', cp.get('observed_value', cp.get('observedValue', 'N/A')))), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 29. VMC (WEEKLY) ──
        elif form_key == 'vmc':
            base_query = MachinePreventiveMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else 'N/A', 
                    'Machine Name': getattr(report, 'machine_name', 'N/A') or 'N/A', 
                    'Machine No': getattr(report, 'machine_no', 'N/A') or 'N/A', 
                    'Location': getattr(report, 'location', 'N/A') or 'N/A', 
                    'Specification': getattr(report, 'specification', 'N/A') or 'N/A', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', 'N/A') or 'N/A', 
                    'Prepared By': getattr(report, 'prepared_by', 'N/A') or 'N/A', 
                    'Checked By': getattr(report, 'checked_by', 'N/A') or 'N/A',
                }
                checkpoint_list = getattr(report, 'checkpoints', [])
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except:
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_after = cp.get('after_maintenance', cp.get('after', cp.get('status', cp.get('is_ok', cp.get('value')))))
                            cp_status = 'OK' if (raw_after is True or str(raw_after).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_after is False or str(raw_after).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_after or 'N/A'))
                            data.append({
                                **base_data, 
                                'Sr.': str(cp.get('sr_no', cp.get('id', i+1))), 
                                'Check Points': cp.get('check_point', cp.get('point', cp.get('checkPoint', cp.get('Check Point', 'N/A')))), 
                                'Checking Parameter': cp.get('parameter', cp.get('specification', cp.get('Specification', cp.get('Checking Parameter', cp.get('checking_parameter', 'N/A'))))), 
                                'Method': cp.get('checking_method', cp.get('method', cp.get('checkingMethod', 'N/A'))), 
                                'Before Maint.': cp.get('before_maintenance', cp.get('before', cp.get('observed_value', cp.get('observedValue', 'N/A')))), 
                                'After Maint': cp_status, 
                                'Remarks': cp.get('remarks', cp.get('remark', cp.get('Remarks', cp.get('Remark', '')))) or 'N/A'
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Checking Parameter': 'N/A', 'Method': 'N/A', 'Before Maint.': 'N/A', 'After Maint': 'N/A', 'Remarks': 'N/A'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 30. WELDING FIXTURE MAINTENANCE ──
        elif form_key in ['welding_fixture', 'weekly_pm_welding_fixture']:
            # Aapke exact FixtureMaintenanceRecord model ka use
            base_query = FixtureMaintenanceRecord.objects.all()
            
            # done_on_date field ka istemal
            reports = apply_date_filter(base_query, 'done_on_date').order_by('-done_on_date', '-created_at')
            data = []
            
            for report in reports:
                base_data = {
                    'Date': str(report.done_on_date) if report.done_on_date else 'N/A',
                    'Part Name': report.part_name or 'N/A',
                    'Part No': report.part_no or 'N/A',
                    'Fixture No': report.fixture_no or 'N/A',
                    'Operation': report.operation_name or 'N/A',
                    'Inspected By': report.inspected_by or 'N/A',
                }
                
                # Naye model ki checklist_data field
                checkpoint_list = report.checklist_data
                
               
                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        if isinstance(cp, dict):
                            raw_status = cp.get('status', 'N/A')
                            cp_status = 'OK' if (raw_status is True or str(raw_status).upper() in ['OK', 'YES']) else ('NOT OK' if (raw_status is False or str(raw_status).upper() in ['NG', 'NOT OK', 'NO']) else str(raw_status or 'N/A'))
                            
                            data.append({
                                **base_data,
                                'Sr.': str(i + 1),
                                'Check Points': cp.get('parameter', 'N/A'),
                                'Status': cp_status,
                                'Remarks': cp.get('remarks', 'N/A'),
                                'Corrective Action': cp.get('correctiveAction', cp.get('corrective_action', 'N/A'))
                            })
                        else:
                            data.append({**base_data, 'Sr.': str(i+1), 'Check Points': str(cp), 'Status': 'N/A', 'Remarks': 'N/A', 'Corrective Action': 'N/A'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': 'N/A', 'Check Points': 'No checkpoints recorded', 'Status': 'N/A', 'Remarks': 'N/A', 'Corrective Action': 'N/A'})
                    data.append(row_data)
                    
            return JsonResponse({'data': data})

        else:
            return JsonResponse({'data': [], 'error': f'Maintenance form type "{form_key}" not supported yet'}, status=400)
        
    except Exception as e:
        print(f"⚠️ Maintenance View Error: {e}")
        traceback.print_exc()
        return JsonResponse({'data': [], 'error': str(e)}, status=500)
class SaveFixtureMaintenanceView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            raw_data = request.data
            
            # React payload ko model fields ke sath map kar rahe hain
            mapped_data = {
                'part_name': raw_data.get('part_name', ''),
                'part_no': raw_data.get('part_no', ''),
                'done_on_date': parse_date(raw_data.get('done_on_date')) if raw_data.get('done_on_date') else None,
                'fixture_no': raw_data.get('fixture_no', ''),
                'operation_name': raw_data.get('operation_name', ''),
                'checklist_data': raw_data.get('checklist_data', []),      # JSON Field
                'pin_chart_data': raw_data.get('pin_chart_data', []),      # JSON Field
                'bush_chart_data': raw_data.get('bush_chart_data', []),    # JSON Field
                'inspected_by': raw_data.get('inspected_by', '')
            }

            serializer = FixtureMaintenanceRecordSerializer(data=mapped_data)
            
            if serializer.is_valid():
                serializer.save()
                return Response(
                    {"success": True, "message": "Fixture Maintenance Record Saved!"}, 
                    status=status.HTTP_201_CREATED
                )
            
            # Validation fail hone par errors return karega
            return Response(
                {"success": False, "error": serializer.errors}, 
                status=status.HTTP_400_BAD_REQUEST
            )
            
        except Exception as e:
            return Response(
                {"success": False, "error": str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )