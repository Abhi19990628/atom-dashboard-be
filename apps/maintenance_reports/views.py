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
    PartMaster,
    
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
print("INSIDE SaveDailyPowerPressView")
class SaveDailyPowerPressView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            print("INSIDE SaveDailyPowerPressView")

            data = request.data
            print("REQUEST DATA =", data)

            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get('items', []) if 'items' in data else [data]
            else:
                items = []

            print("ITEMS =", items)

            entries_to_create = []

            for row in items:
                print("ROW =", row)

                entries_to_create.append(
                    DailyPowerPressChecksheet(
                        plant=row.get('plant', ''),
                        operator_name=row.get('operator_name', ''),
                        machine_no=row.get('machine_no', ''),
                        prepared_by=row.get('prepared_by', ''),
                        shift=row.get('shift', ''),
                        date=row.get('date'),
                        checkpoints=row.get('checkpoints', [])
                    )
                )

            if entries_to_create:
                DailyPowerPressChecksheet.objects.bulk_create(entries_to_create)
                return Response(
                    {
                        "success": True,
                        "message": "✅ Daily Power Press Checksheet Data Saved!"
                    },
                    status=status.HTTP_201_CREATED
                )

            return Response(
                {
                    "success": False,
                    "error": "Bhai, koi data nahi mila save karne ke liye."
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        except Exception as e:
            print("Backend Crash:", str(e))
            traceback.print_exc()

            return Response(
                {
                    "success": False,
                    "error": f"Server Error: {str(e)}"
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
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


class SaveMachineBreakdownView(APIView):
    @transaction.atomic
    def post(self, request):
        try:
            data = request.data

            def get_val(key, default=None):
                val = data.get(key)
                return val if val != '' else default
            intimation = MachineBreakdownIntimation.objects.create(
                given_date=get_val('given_date'),
                given_time=get_val('given_time'),
                machine_name_no=data.get('machine_name_no', ''),
                breakdown_name=data.get('breakdown_name', ''),
                prepared_by=data.get('prepared_by', ''),
                part_made_after_inspection=data.get('part_made_after_inspection', ''),
                breakdown_desc=data.get('breakdown_desc', ''),
                repair_date=get_val('repair_date'),
                repair_time=get_val('repair_time'),
                repair_hours=get_val('repair_hours'),
                mechanics_count=get_val('mechanics_count'),
                repair_desc=data.get('repair_desc', ''),
                status=data.get('status', 'OK'),
                verification_date=get_val('verification_date'),
                verification_time=get_val('verification_time'),
                language=data.get('language', 'english')
            )

            print("SAVED ID:", intimation.id)
            
            saved_record = MachineBreakdownIntimation.objects.filter(
                id=intimation.id
            ).first()
            
            print("FOUND IN DB:", saved_record)
            
            return Response({
                "success": True,
                "message": "Saved",
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
                prepared_by=tool_info.get('preparedBy', ''),
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
                prepared_by=data.get('preparedBy', ''),
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
          
            data = request.data.copy()
            data['prepared_by'] = data.get('prepared_by')
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
                # Production Section
                reporter_name=get_field('reporterName', 'reporter_name', default=''),
                report_date=get_field('reportDate', 'report_date', default=None),
                machine_name_no=get_field('machineNameNo', 'machine_name_no', default=''),
                report_time=get_field('reportTime', 'report_time', default=None),
                breakdown_details=get_field('breakdownDetails', 'breakdown_details', default=''),
                prepared_by=get_field('preparedBy', 'prepared_by', default=''),
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
            data = request.data.copy()
            data['prepared_by'] = data.get('prepared_by')
            serializer = MachinePreventiveMaintenanceSerializer(data=data)

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
            data = request.data.copy()
            data['prepared_by'] = data.get('prepared_by')
            serializer = CNCMaintenanceReportSerializer(data=data)

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
            data = request.data.copy()
            data['prepared_by'] = data.get('prepared_by')
            serializer = PowerPressPMCheckSheetSerializer(data=data)

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
                        'Machine Name': card.machine_name, 'Machine No.': card.machine_no, 'Machine Specs': card.machine_specs or '—', 'Location': card.location or '—', 'Date': '—', 'Problem': '—', 'Action Taken': '—', '4M Update': '—', 'Signature': '—', 'Remarks': '—', 'Prepared By': card.prepared_by or '—', 'Approved By': card.approved_by or '—',
                    })
                else:
                    for i, record in enumerate(history_list):
                        if not record.get('date') and not record.get('problem'):
                            continue
                        show_base = (i == 0)
                        data.append({
                            'Machine Name': card.machine_name if show_base else '', 'Machine No.': card.machine_no if show_base else '', 'Machine Specs': (card.machine_specs or '—') if show_base else '', 'Location': (card.location or '—') if show_base else '', 'Date': record.get('date', '—'), 'Problem': record.get('problem', '—'), 'Action Taken': record.get('actionTaken', '—'), '4M Update': record.get('update4M', '—'), 'Signature': record.get('signature', '—'), 'Remarks': record.get('remarks', '—'), 'Prepared By': (card.prepared_by or '—') if show_base else '', 'Approved By': (card.approved_by or '—') if show_base else '',
                        })
            return JsonResponse({'data': data})
            
        # ── 2. MACHINE BREAKDOWN INTIMATION ──
        elif form_key == 'mc_breakdown':
            base_query = MachineBreakdownIntimation.objects.all()
            reports = apply_date_filter(base_query, 'given_date').order_by('-created_at')
            data = []
            for report in reports:
                data.append({
                    'Given Date': str(report.given_date) if report.given_date else '—', 'Given Time': str(report.given_time) if report.given_time else '—', 'Machine Name & No.': report.machine_name_no or '—', 'Breakdown Name': report.breakdown_name or '—', 'Part Made': report.part_made_after_inspection or '—', 'Breakdown Desc': report.breakdown_desc or '—', 'Repair Date': str(report.repair_date) if report.repair_date else '—', 'Repair Time': str(report.repair_time) if report.repair_time else '—', 'Repair Hours': str(report.repair_hours) if report.repair_hours else '—', 'Mechanics Involved': str(report.mechanics_count) if report.mechanics_count else '—', 'Repair Desc': report.repair_desc or '—', 'Quality Status': report.status or '—', 'Verification Date': str(report.verification_date) if report.verification_date else '—', 'Verification Time': str(report.verification_time) if report.verification_time else '—',
                })
            return JsonResponse({'data': data})

        # ── 3. DAILY POWER PRESS CHECKSHEET ──
        elif form_key == 'power_press_check':
            base_query = DailyPowerPressChecksheet.objects.all()
            checks = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for check in checks:
                base_data = {
                    'Date': str(check.date) if check.date else '—', 'Shift': check.shift or '—', 'Plant': check.plant or '—', 'Machine No': check.machine_no or '—', 'Operator Name': check.operator_name or '—',
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
                                'Date': base_data['Date'] if show_base else '', 'Shift': base_data['Shift'] if show_base else '', 'Plant': base_data['Plant'] if show_base else '', 'Machine No': base_data['Machine No'] if show_base else '', 'Operator Name': base_data['Operator Name'] if show_base else '', 'Check Point': cp.get('Check Point', cp.get('checkPoint', cp.get('check_point', cp.get('checkpoint', cp.get('name', '—'))))), 'Specification': cp.get('Specification', cp.get('specification', cp.get('spec', '—'))), 'Method': cp.get('Method', cp.get('method', cp.get('checkingMethod', cp.get('checking_method', '—')))), 'Observed Value': cp.get('Observed Value', cp.get('observedValue', cp.get('observed_value', cp.get('observed', cp.get('value', '—'))))), 'Status': cp_status,
                            })
                        else:
                            data.append({**{k: v if show_base else '' for k, v in base_data.items()}, 'Check Point': str(cp), 'Specification': '—', 'Method': '—', 'Observed Value': '—', 'Status': '—'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Check Point': 'No checkpoints recorded', 'Specification': '—', 'Method': '—', 'Observed Value': '—', 'Status': '—'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 4. TOOL HISTORY CARD ──
        elif form_key == 'tool_history':
            base_query = ToolHistoryReport.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for r in reports:
                data.append({
                    'Date': str(r.date) if r.date else '—', 'Tool Name': r.tool_name or '—', 'Part Name & No': f"{r.part_name} ({r.part_no})" if r.part_name else '—', 'Customer': r.customer_name or '—', 'Prod Count': r.prod or '—', 'Resharp Stroke': r.resharpening_stroke or '—', 'Cumulative Prod': r.cumulative_prod or '—', 'Problem Reported': r.problem_reported or '—', 'Action Taken': r.action_taken or '—', '4M Update': r.updated_in_4m or '—', 'Remarks': r.remarks or '—',
                })
            return JsonResponse({'data': data})

        # ── 5. TOOL PREVENTIVE MAINTENANCE ──
        elif form_key == 'tool_pm_check':
            base_query = ToolPreventiveMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date), 'Tool Name': report.tool_name, 'Part Name': report.part_name or '—', 'Part No.': report.part_no or '—', 'Op No.': report.operation_no or '—', 'Maint. Person': report.maintenance_person or '—',
                }
                maint_data = report.maintenance_data
                if maint_data and isinstance(maint_data, dict):
                    i = 0
                    for key, vals in maint_data.items():
                        parts = key.split('-', 1)
                        item_name = parts[0] if len(parts) > 0 else 'Unknown'
                        checkpoint_name = parts[1] if len(parts) > 1 else 'Unknown'
                        show_base = (i == 0)
                        
                        row_data = {**{k: v if show_base else '' for k, v in base_data.items()}, 'Item': item_name, 'Checkpoint': checkpoint_name, 'Before Maint.': vals.get('beforeMaint', '—'), 'After Maint.': vals.get('afterMaint', '—'), 'Remarks': vals.get('remark', '—')}
                        data.append(row_data)
                        i += 1
                else:
                    row_data = base_data.copy()
                    row_data.update({'Item': 'No data recorded', 'Checkpoint': '—', 'Before Maint.': '—', 'After Maint.': '—', 'Remarks': '—'})
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
                    'Date': str(r.date) if r.date else '—', 'Machine Type & No.': r.machine_type_no or '—', 'Problem Description': details.get('problem_description', '—'), 'Time Period': details.get('time_period_maintenance', '—'), 'Status': details.get('status_after_period', '—'), '4M Update': details.get('updated_in_4m', '—'), 'Sign': details.get('sign', '—'), 'Remarks': details.get('remarks', '—'),
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
                    'Date': str(r.date) if r.date else '—', 'Tool Name': r.tool_name or '—', 'Process Name': details.get('process_name', '—'), 'Problem': details.get('problem', '—'), 'Action Taken': details.get('action_taken', '—'), 'Total Time': details.get('total_time_taken', '—'), 'Checked By': details.get('checked_by', '—'), 'History Card': details.get('history_card_status', '—'), '4M Update': details.get('updated_in_4m', '—'), 'Sign': details.get('sign', '—'), 'Remarks': details.get('remarks', '—'),
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
                    'Date': str(r.date) if r.date else '—', 'Spare Description': r.spare_description or '—', 'Model / Box No.': r.model_description or '—', 'Location': r.box_location or '—', 'Prepared By': r.prepared_by or '—', 'Approved By': r.approved_by or '—', 'Spare Type': details.get('spare_type', '—'), 'UOM': details.get('uom', '—'), 'Opening Stock': details.get('opening_stock', '—'), 'Minimum Level': details.get('minimum_level', '—'), 'Maximum Level': details.get('maximum_level', '—'), 'Reorder Level': details.get('reorder_level', '—'), 'Lead Time': details.get('lead_time', '—'), 'Closing Stock': details.get('closing_stock', '—'), 'PR Status': details.get('pr_status', '—'),
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
                    'Date': str(r.date) if r.date else '—', 'Spare Description': r.spare_description or '—', 'Model / Box No.': r.model_description or '—', 'Location': r.box_location or '—', 'Spare Type': details.get('spare_type', '—'), 'UOM': details.get('uom', '—'), 'Opening Stock': details.get('opening_stock', '—'), 'Minimum Level': details.get('minimum_level', '—'), 'Lead Time': details.get('lead_time', '—'),
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
                        'Poka Yoke Detail': obs.poka_yoke_detail, 'Checking Method':  obs.checking_method, 'Result': 'OK' if obs.is_ok else 'NOT OK', 'Plant': report.plant_name, 'Machine No': report.machine_no, 'Checked By': report.checked_by_maintenance or '—', 'Verified By': report.verified_by_production or '—', 'Remarks': obs.remarks or '—', 'Date': str(report.date),
                    })
            return JsonResponse({'data': data})

        # ── 11. TOOL BREAKDOWN SLIP ──
        elif form_key == 'tool_breakdown_slip':
            base_query = ToolBreakdownIntimation.objects.all()
            reports = apply_date_filter(base_query, 'report_date').order_by('-created_at')
            data = []
            for report in reports:
                data.append({
                    'Reporter Name': report.reporter_name or '—', 'Report Date': str(report.report_date) if report.report_date else '—', 'Machine Name & No.': report.machine_name_no or '—', 'Report Time': str(report.report_time) if report.report_time else '—', 'Breakdown Details': report.breakdown_details or '—', 'Prod Supervisor': report.prod_supervisor_name or '—', 'Maint Date': str(report.maint_date) if report.maint_date else '—', 'Maint Time': str(report.maint_time) if report.maint_time else '—', 'Time Taken': report.time_taken_to_rectify or '—', 'Men Engaged': str(report.men_engaged) if report.men_engaged else '—', 'Action Taken': report.action_taken_details or '—', 'Maint Incharge': report.maint_incharge_name or '—', 'Quality Status': report.status or '—', 'QA Date': str(report.qa_date) if report.qa_date else '—', 'QA Time': str(report.qa_time) if report.qa_time else '—', 'NC Verification': report.nc_verification or '—', 'QA Incharge': report.qa_incharge_name or '—',
                })
            return JsonResponse({'data': data})

        # =========================================================================
        # 🔥🔥🔥 12 TO 23: GROUPED WORKING MACHINES (CLEAN & FAST) 🔥🔥🔥
        # =========================================================================
        
        elif form_key in ['surface_grinder', 'tig_welding', 'spot_welding', 'compressor', 'lathe_machine', 'vertical_drill', 'base_grinder', 'belt_grinder', 'pipe_cutting', 'vibra', 'dip_molding', 'servo_press']:
            model_map = {
                'surface_grinder': SurfaceGrinderMaintenance, 'tig_welding': TigWeldingMaintenance, 'spot_welding': SpotWeldingMaintenance,
                'compressor': CompressorMaintenance, 'lathe_machine': LatheMachineMaintenance, 'vertical_drill': VerticalDrillMachineMaintenance,
                'base_grinder': BaseGrinderMaintenance, 'belt_grinder': BeltGrinderMaintenance, 'pipe_cutting': PipeCuttingMaintenance,
                'vibra': VibraMaintenance, 'dip_molding': DipMoldingMaintenance, 'servo_press': ServoPressMaintenance
            }
            base_query = model_map[form_key].objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            
            for report in reports:
                base_data = {
                    'Date': str(report.date) if getattr(report, 'date', None) else '—', 
                    'Machine Name': getattr(report, 'machine_name', '—') or '—', 
                    'Machine No': getattr(report, 'machine_no', '—') or '—', 
                    'Location': getattr(report, 'location', '—') or '—', 
                    'Specification': getattr(report, 'specification', '—') or '—', 
                    'Maint. Personnel': getattr(report, 'maintenance_personnel', '—') or '—', 
                    'Prepared By': getattr(report, 'prepared_by', '—') or '—', 
                    'Checked By': getattr(report, 'checked_by', '—') or '—',
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
                        show_base = (i == 0)
                        if isinstance(cp, dict):
                            data.append({**{k: (v if show_base else '') for k, v in base_data.items()}, 'Sr.': cp.get('id', str(i+1)), 'Check Point': cp.get('point', cp.get('checkPoint', '—')), 'Parameter': cp.get('parameter', '—'), 'Method': cp.get('method', '—'), 'Before': cp.get('before', '—'), 'After': cp.get('after', '—'), 'Remarks': cp.get('remarks', '—')})
                        else:
                            data.append({**{k: (v if show_base else '') for k, v in base_data.items()}, 'Sr.': str(i+1), 'Check Point': str(cp), 'Parameter': '—', 'Method': '—', 'Before': '—', 'After': '—', 'Remarks': '—'})
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': '—', 'Check Point': 'No checkpoints recorded', 'Parameter': '—', 'Method': '—', 'Before': '—', 'After': '—', 'Remarks': '—'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # =========================================================================
        # 🔥🔥🔥 24 TO 29: BULLETPROOF FIX BASED EXACTLY ON YOUR MODELS 🔥🔥🔥
        # =========================================================================

        # ── 24. PROJECTION WELDING ──
        elif form_key == 'projection_weld':
            base_query = ProjectionWeldingPMCheckSheet.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if report.date else '—', 
                    'Machine Name': report.machine_name or '—', 
                    'Machine No': report.machine_no or '—', 
                    'Location': report.location or '—', 
                    'Specification': report.specification or '—', 
                    'Maint. Personnel': report.maintenance_personnel or '—', 
                    'Prepared By': report.prepared_by or '—', 
                    'Checked By': report.checked_by or '—',
                }
                
                checkpoint_list = report.checkpoints
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except: 
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        show_base = (i == 0)
                        if isinstance(cp, dict):
                            is_ok_val = cp.get('status', cp.get('is_ok', cp.get('value', cp.get('after'))))
                            cp_status = 'OK' if (is_ok_val is True or str(is_ok_val).upper() in ['OK', 'YES']) else ('NOT OK' if (is_ok_val is False or str(is_ok_val).upper() in ['NG', 'NOT OK', 'NO']) else str(is_ok_val or '—'))
                            
                            data.append({
                                **{k: (v if show_base else '') for k, v in base_data.items()}, 
                                'Sr.': cp.get('id', str(i+1)), 
                                'Check Point': cp.get('point', cp.get('checkPoint', cp.get('Check Point', '—'))), 
                                'Parameter': cp.get('parameter', cp.get('specification', '—')), 
                                'Method': cp.get('method', cp.get('checkingMethod', '—')), 
                                'Before': cp.get('observed_value', cp.get('observedValue', cp.get('before', '—'))), 
                                'After': cp_status, 
                                'Remarks': cp.get('remarks', '—')
                            })
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': '—', 'Check Point': 'No checkpoints recorded', 'Parameter': '—', 'Method': '—', 'Before': '—', 'After': '—', 'Remarks': '—'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 25. VMM (Vertical Milling Machine) ──
        elif form_key == 'vmm':
            base_query = VerticalMillingMachineCheckSheet.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if report.date else '—', 
                    'Machine Name': report.machine_name or '—', 
                    'Machine No': report.machine_no or '—', 
                    'Location': report.location or '—', 
                    'Specification': report.specification or '—', 
                    'Maint. Personnel': report.maintenance_personnel or '—', 
                    'Prepared By': report.prepared_by or '—', 
                    'Checked By': report.checked_by or '—',
                }
                
                checkpoint_list = report.checkpoints
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except: 
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        show_base = (i == 0)
                        if isinstance(cp, dict):
                            is_ok_val = cp.get('status', cp.get('is_ok', cp.get('value', cp.get('after'))))
                            cp_status = 'OK' if (is_ok_val is True or str(is_ok_val).upper() in ['OK', 'YES']) else ('NOT OK' if (is_ok_val is False or str(is_ok_val).upper() in ['NG', 'NOT OK', 'NO']) else str(is_ok_val or '—'))
                            data.append({
                                **{k: (v if show_base else '') for k, v in base_data.items()}, 
                                'Sr.': cp.get('id', str(i+1)), 
                                'Check Point': cp.get('point', cp.get('checkPoint', cp.get('Check Point', '—'))), 
                                'Parameter': cp.get('parameter', cp.get('specification', '—')), 
                                'Method': cp.get('method', cp.get('checkingMethod', '—')), 
                                'Before': cp.get('observed_value', cp.get('observedValue', cp.get('before', '—'))), 
                                'After': cp_status, 
                                'Remarks': cp.get('remarks', '—')
                            })
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': '—', 'Check Point': 'No checkpoints recorded', 'Parameter': '—', 'Method': '—', 'Before': '—', 'After': '—', 'Remarks': '—'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 26. CNC (Notice: Model uses 'checklist' instead of 'checkpoints') ──
        elif form_key == 'cnc':
            base_query = CNCMaintenanceReport.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if report.date else '—', 
                    'Machine Name': report.machine_name or '—', 
                    'Machine No': report.machine_no or '—', 
                    'Location': report.location or '—', 
                    'Specification': report.specification or '—', 
                    'Maint. Personnel': report.maintenance_personnel or '—',
                    'Prepared By': '—', # Missing in model
                    'Checked By': '—',  # Missing in model
                }
                
                # USING 'checklist' as defined in your CNC model
                checkpoint_list = report.checklist 
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except: 
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        show_base = (i == 0)
                        if isinstance(cp, dict):
                            is_ok_val = cp.get('status', cp.get('is_ok', cp.get('value', cp.get('after'))))
                            cp_status = 'OK' if (is_ok_val is True or str(is_ok_val).upper() in ['OK', 'YES']) else ('NOT OK' if (is_ok_val is False or str(is_ok_val).upper() in ['NG', 'NOT OK', 'NO']) else str(is_ok_val or '—'))
                            data.append({
                                **{k: (v if show_base else '') for k, v in base_data.items()}, 
                                'Sr.': cp.get('id', str(i+1)), 
                                'Check Point': cp.get('point', cp.get('checkPoint', cp.get('Check Point', '—'))), 
                                'Parameter': cp.get('parameter', cp.get('specification', '—')), 
                                'Method': cp.get('method', cp.get('checkingMethod', '—')), 
                                'Before': cp.get('observed_value', cp.get('observedValue', cp.get('before', '—'))), 
                                'After': cp_status, 
                                'Remarks': cp.get('remarks', '—')
                            })
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': '—', 'Check Point': 'No checkpoints recorded', 'Parameter': '—', 'Method': '—', 'Before': '—', 'After': '—', 'Remarks': '—'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 27. HYDRAULIC MIG ──
        elif form_key == 'hydraulic_mig':
            base_query = HydraulicPMCheckSheet.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if report.date else '—', 
                    'Machine Name': report.machine_name or '—', 
                    'Machine No': report.machine_no or '—', 
                    'Location': report.location or '—', 
                    'Specification': report.specification or '—', 
                    'Maint. Personnel': report.maintenance_personnel or '—', 
                    'Prepared By': report.prepared_by or '—', 
                    'Checked By': report.checked_by or '—',
                }
                
                checkpoint_list = report.checkpoints
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except: 
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        show_base = (i == 0)
                        if isinstance(cp, dict):
                            is_ok_val = cp.get('status', cp.get('is_ok', cp.get('value', cp.get('after'))))
                            cp_status = 'OK' if (is_ok_val is True or str(is_ok_val).upper() in ['OK', 'YES']) else ('NOT OK' if (is_ok_val is False or str(is_ok_val).upper() in ['NG', 'NOT OK', 'NO']) else str(is_ok_val or '—'))
                            data.append({
                                **{k: (v if show_base else '') for k, v in base_data.items()}, 
                                'Sr.': cp.get('id', str(i+1)), 
                                'Check Point': cp.get('point', cp.get('checkPoint', cp.get('Check Point', '—'))), 
                                'Parameter': cp.get('parameter', cp.get('specification', '—')), 
                                'Method': cp.get('method', cp.get('checkingMethod', '—')), 
                                'Before': cp.get('observed_value', cp.get('observedValue', cp.get('before', '—'))), 
                                'After': cp_status, 
                                'Remarks': cp.get('remarks', '—')
                            })
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': '—', 'Check Point': 'No checkpoints recorded', 'Parameter': '—', 'Method': '—', 'Before': '—', 'After': '—', 'Remarks': '—'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 28. WEEKLY POWER PRESS (Notice: Model misses maint_personnel, prepared_by, checked_by) ──
        elif form_key == 'power_press':
            base_query = PowerPressPMCheckSheet.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if report.date else '—', 
                    'Machine Name': report.machine_name or '—', 
                    'Machine No': report.machine_no or '—', 
                    'Location': report.location or '—', 
                    'Specification': report.specification or '—',
                    'Maint. Personnel': '—', # Missing in model
                    'Prepared By': '—',      # Missing in model
                    'Checked By': '—',       # Missing in model
                }
                
                checkpoint_list = report.checkpoints
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except: 
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        show_base = (i == 0)
                        if isinstance(cp, dict):
                            is_ok_val = cp.get('status', cp.get('is_ok', cp.get('value', cp.get('after'))))
                            cp_status = 'OK' if (is_ok_val is True or str(is_ok_val).upper() in ['OK', 'YES']) else ('NOT OK' if (is_ok_val is False or str(is_ok_val).upper() in ['NG', 'NOT OK', 'NO']) else str(is_ok_val or '—'))
                            data.append({
                                **{k: (v if show_base else '') for k, v in base_data.items()}, 
                                'Sr.': cp.get('id', str(i+1)), 
                                'Check Point': cp.get('point', cp.get('checkPoint', cp.get('Check Point', '—'))), 
                                'Parameter': cp.get('parameter', cp.get('specification', '—')), 
                                'Method': cp.get('method', cp.get('checkingMethod', '—')), 
                                'Before': cp.get('observed_value', cp.get('observedValue', cp.get('before', '—'))), 
                                'After': cp_status, 
                                'Remarks': cp.get('remarks', '—')
                            })
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': '—', 'Check Point': 'No checkpoints recorded', 'Parameter': '—', 'Method': '—', 'Before': '—', 'After': '—', 'Remarks': '—'})
                    data.append(row_data)
            return JsonResponse({'data': data})

        # ── 29. MACHINE PREVENTIVE MAINTENANCE / VMC (Notice: Model misses prepared_by, checked_by) ──
        elif form_key == 'vmc': 
            base_query = MachinePreventiveMaintenance.objects.all()
            reports = apply_date_filter(base_query, 'date').order_by('-date', '-created_at')
            data = []
            for report in reports:
                base_data = {
                    'Date': str(report.date) if report.date else '—', 
                    'Machine Name': report.machine_name or '—', 
                    'Machine No': report.machine_no or '—', 
                    'Location': report.location or '—', 
                    'Specification': report.specification or '—', 
                    'Maint. Personnel': report.maintenance_personnel or '—',
                    'Prepared By': '—', # Missing in model
                    'Checked By': '—',  # Missing in model
                }
                
                checkpoint_list = report.checkpoints
                if isinstance(checkpoint_list, str):
                    try: checkpoint_list = json.loads(checkpoint_list)
                    except: 
                        try: checkpoint_list = ast.literal_eval(checkpoint_list)
                        except: checkpoint_list = []
                elif isinstance(checkpoint_list, dict):
                    checkpoint_list = checkpoint_list.get('data', [])

                if isinstance(checkpoint_list, list) and len(checkpoint_list) > 0:
                    for i, cp in enumerate(checkpoint_list):
                        show_base = (i == 0)
                        if isinstance(cp, dict):
                            is_ok_val = cp.get('status', cp.get('is_ok', cp.get('value', cp.get('after'))))
                            cp_status = 'OK' if (is_ok_val is True or str(is_ok_val).upper() in ['OK', 'YES']) else ('NOT OK' if (is_ok_val is False or str(is_ok_val).upper() in ['NG', 'NOT OK', 'NO']) else str(is_ok_val or '—'))
                            data.append({
                                **{k: (v if show_base else '') for k, v in base_data.items()}, 
                                'Sr.': cp.get('id', str(i+1)), 
                                'Check Point': cp.get('point', cp.get('checkPoint', cp.get('Check Point', '—'))), 
                                'Parameter': cp.get('parameter', cp.get('specification', '—')), 
                                'Method': cp.get('method', cp.get('checkingMethod', '—')), 
                                'Before': cp.get('observed_value', cp.get('observedValue', cp.get('before', '—'))), 
                                'After': cp_status, 
                                'Remarks': cp.get('remarks', '—')
                            })
                else:
                    row_data = base_data.copy()
                    row_data.update({'Sr.': '—', 'Check Point': 'No checkpoints recorded', 'Parameter': '—', 'Method': '—', 'Before': '—', 'After': '—', 'Remarks': '—'})
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