from rest_framework import serializers
from .models import (
    # Purane Models
    Operator, 
    OperatorAssignment, 
    IdleReport, 
    Plant2HourlyIdletime, 
    InspectionReport,
    # Naye Models
    L1_PartInfoMaster, 
    L2_ProcessReportMaster, 
    L3_ParameterDetailMaster,
    IncomingInspectionReport, 
    IncomingInspectionObservation,
    MachineChecksheetReport, 
    MachineChecksheetObservation,
    DailyProductionReport,
    TipChangeDressing, 
    FiveSChecksheetReport, 
    FiveSChecksheetObservation,
    BinTrolleyReport,
    RedBinAnalysisReport,
    RedBinAttendance,
    ScrapNoteEntry,
    ReworkEntry,
    DeviationApproval,
    DailyPowerPressChecksheet,
    MachineHistoryCard,
    MachineBreakdownIntimation,
    ToolBreakdownIntimation,
    ToolHistoryReport,
    ToolPreventiveMaintenance,
    GoodReceiptEntry,DailyProductionPlan,
     FourMChangeInspection,
     FourMChangeRecord,
     MonthlyProductionPlan, 
    OperatorObservanceChecklist, 
    OperatorObservancePlan, 
    PMChecklistMHE,ProjectionWelderQual, SpotWelderQual, TigMigWelderQual, ProcessValidation,
    ProcessAuditChecksheet,CoherenceChecklist, LayoutInspection, ProductAuditPlan, CustomerComplaint,
    CustomerSatisfaction, WarrantyClaim, MinutesOfMeeting,
    MachineBreakdown, ToolBreakdown, MachineCriticalSpare, ToolCriticalSpare,ToolBreakdownIntimation,
    SpotWeldingMaintenance,
    CompressorMaintenance,
    LatheMachineMaintenance,
    VerticalDrillMachineMaintenance,
    SurfaceGrinderMaintenance,
    TigWeldingMaintenance,
    BaseGrinderMaintenance,
    BeltGrinderMaintenance,
    PipeCuttingMaintenance,
    VibraMaintenance,
    DipMoldingMaintenance,
    ServoPressMaintenance,
    MachinePreventiveMaintenance,
    CNCMaintenanceReport,
    VerticalMillingMachineCheckSheet,
    ProjectionWeldingPMCheckSheet,
    PowerPressPMCheckSheet,
    HydraulicPMCheckSheet,FourMDisplay,FourMSummary, FixtureMaintenanceRecord,IncomingMaterialInspection
    ,FourMInformationSheet
)

# ==========================================
# 🟢 PURANE SERIALIZERS (Jo aapne bheje the)
# ==========================================

# 1. Operator Serializer
class OperatorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Operator
        fields = '__all__'

# 2. Operator Assignment Serializer
class OperatorAssignmentSerializer(serializers.ModelSerializer):
    class Meta:
        model = OperatorAssignment
        fields = '__all__'

# 3. Idle Report Serializer
class IdleReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = IdleReport
        fields = '__all__'

# 4. Hourly Idle Time Serializer
class Plant2HourlyIdletimeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Plant2HourlyIdletime
        fields = '__all__'

# 5. QMS INSPECTION REPORT SERIALIZER
class InspectionReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = InspectionReport
        fields = '__all__'  

# ==========================================
# 🔵 NAYE SERIALIZERS (Jo humne abhi banaye)
# ==========================================

# --- MASTER DATA (Excel Uploads) ---
class L1PartInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = L1_PartInfoMaster
        fields = '__all__'

class L2ProcessReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = L2_ProcessReportMaster
        fields = '__all__'

class L3ParameterDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = L3_ParameterDetailMaster
        fields = '__all__'


# --- INCOMING INSPECTION (Master-Detail Nested) ---
class IncomingInspectionObservationSerializer(serializers.ModelSerializer):
    class Meta:
        model = IncomingInspectionObservation
        exclude = ('inspection_report',) # Frontend ko ID bhejne ki zaroorat nahi

class IncomingInspectionReportSerializer(serializers.ModelSerializer):
    observations = IncomingInspectionObservationSerializer(many=True)

    class Meta:
        model = IncomingInspectionReport
        fields = '__all__'

    # Custom Save Logic: Header + Rows ek sath save karne ke liye
    def create(self, validated_data):
        observations_data = validated_data.pop('observations', [])
        report = IncomingInspectionReport.objects.create(**validated_data)
        
        for obs_data in observations_data:
            IncomingInspectionObservation.objects.create(inspection_report=report, **obs_data)
        return report


# --- DAILY MACHINE CHECKSHEET (Nested) ---
class MachineChecksheetObservationSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineChecksheetObservation
        exclude = ('report',)

class MachineChecksheetReportSerializer(serializers.ModelSerializer):
    check_points = MachineChecksheetObservationSerializer(many=True)

    class Meta:
        model = MachineChecksheetReport
        fields = '__all__'

    def create(self, validated_data):
        check_points_data = validated_data.pop('check_points', [])
        report = MachineChecksheetReport.objects.create(**validated_data)
        
        for point_data in check_points_data:
            MachineChecksheetObservation.objects.create(report=report, **point_data)
        return report


# --- DAILY PRODUCTION REPORT / MES (DPR) ---
class DailyProductionReportSerializer(serializers.ModelSerializer):
    # Frontend mein IDs ki jagah actual Naam dikhane ke liye:
    part_name = serializers.CharField(source='part_info.part_name', read_only=True)
    operation_name = serializers.CharField(source='operation.report_name', read_only=True)
    operator_name = serializers.CharField(source='operator.name', read_only=True)

    class Meta:
        model = DailyProductionReport
        fields = '__all__'
        


class TipChangeDressingSerializer(serializers.ModelSerializer):
    class Meta:
        model = TipChangeDressing
        fields = '__all__'  





# =====================================================================
# 5S CHECKSHEET SERIALIZERS (Nested)
# =====================================================================
class FiveSChecksheetObservationSerializer(serializers.ModelSerializer):
    class Meta:
        model = FiveSChecksheetObservation
        # Report field ko exclude kiya hai taaki frontend se bhejna na pade, 
        # ye backend par automatically attach ho jayega.
        fields = ['id', 's_category', 'check_point', 'status']

class FiveSChecksheetReportSerializer(serializers.ModelSerializer):
    observations = FiveSChecksheetObservationSerializer(many=True)

    class Meta:
        model = FiveSChecksheetReport
        fields = '__all__'

    # Custom create method for Nested Data Save
    def create(self, validated_data):
        observations_data = validated_data.pop('observations', [])
        report = FiveSChecksheetReport.objects.create(**validated_data)
        
        # Report id ke sath observations save kar rahe hain
        for obs_data in observations_data:
            FiveSChecksheetObservation.objects.create(report=report, **obs_data)
            
        return report

# =====================================================================
# STANDARD SERIALIZERS
# =====================================================================
class BinTrolleyReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = BinTrolleyReport
        fields = '__all__'

class RedBinAnalysisReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = RedBinAnalysisReport
        fields = '__all__'

class RedBinAttendanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = RedBinAttendance
        fields = '__all__'

class ScrapNoteEntrySerializer(serializers.ModelSerializer):
    class Meta:
        model = ScrapNoteEntry
        fields = '__all__'

class ReworkEntrySerializer(serializers.ModelSerializer):
    class Meta:
        model = ReworkEntry
        fields = '__all__'


class DeviationApprovalSerializer(serializers.ModelSerializer):
    class Meta:
        model = DeviationApproval
        fields = '__all__'


class GoodReceiptEntrySerializer(serializers.ModelSerializer):
    class Meta:
        model = GoodReceiptEntry
        fields = '__all__'
class DailyPowerPressChecksheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = DailyPowerPressChecksheet
        fields = '__all__'


class MachineHistoryCardSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineHistoryCard
        fields = '__all__'

class MachineBreakdownIntimationSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineBreakdownIntimation
        fields = '__all__'



class ToolHistoryReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolHistoryReport
        fields = '__all__'
class ToolPreventiveMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolPreventiveMaintenance
        fields = '__all__'


class ToolBreakdownIntimationSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolBreakdownIntimation
        fields = '__all__'

class DailyProductionPlanSerializer(serializers.ModelSerializer):
    class Meta:
        model = DailyProductionPlan
        fields = '__all__'

class FourMChangeInspectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMChangeInspection
        fields = '__all__'
        
        
# serializers.py
class FourMChangeRecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMChangeRecord
        fields = '__all__'

class FourMDisplaySerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMDisplay
        fields = '__all__'
        
class FourMSummarySerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMSummary
        fields = '__all__' 

class FourMInformationSheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMInformationSheet
        fields = '__all__'      
###################################
#
#       Prodcution monthly 
###################################

class MonthlyProductionPlanSerializer(serializers.ModelSerializer):
    class Meta:
        model = MonthlyProductionPlan
        fields = '__all__'

class OperatorObservanceChecklistSerializer(serializers.ModelSerializer):
    class Meta:
        model = OperatorObservanceChecklist
        fields = '__all__'

class OperatorObservancePlanSerializer(serializers.ModelSerializer):
    class Meta:
        model = OperatorObservancePlan
        fields = '__all__'

class PMChecklistMHESerializer(serializers.ModelSerializer):
    class Meta:
        model = PMChecklistMHE
        fields = '__all__'
        
class ProjectionWelderQualSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProjectionWelderQual
        fields = '__all__'

class SpotWelderQualSerializer(serializers.ModelSerializer):
    class Meta:
        model = SpotWelderQual
        fields = '__all__'

class TigMigWelderQualSerializer(serializers.ModelSerializer):
    class Meta:
        model = TigMigWelderQual
        fields = '__all__'

class ProcessValidationSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProcessValidation
        fields = '__all__'
 
###################################
#
#       QA monthly 
###################################
class ProcessAuditChecksheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProcessAuditChecksheet
        fields = '__all__'

class CoherenceChecklistSerializer(serializers.ModelSerializer):
    class Meta:
        model = CoherenceChecklist
        fields = '__all__'

class LayoutInspectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = LayoutInspection
        fields = '__all__'

class ProductAuditPlanSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProductAuditPlan
        fields = '__all__'

class CustomerComplaintSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomerComplaint
        fields = '__all__'


class CustomerSatisfactionSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomerSatisfaction
        fields = '__all__'

class WarrantyClaimSerializer(serializers.ModelSerializer):
    class Meta:
        model = WarrantyClaim
        fields = '__all__'

class MinutesOfMeetingSerializer(serializers.ModelSerializer):
    class Meta:
        model = MinutesOfMeeting
        fields = '__all__'

 ###################################
#
#       Maintance  monthly 
###################################

class MachineBreakdownSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineBreakdown
        fields = '__all__'

class ToolBreakdownSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolBreakdown
        fields = '__all__'


class MachineCriticalSpareSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineCriticalSpare
        fields = '__all__'

class ToolCriticalSpareSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolCriticalSpare
        fields = '__all__'


from rest_framework import serializers
from .models import Notification

class NotificationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Notification
        fields = ['id', 'machine_no', 'message', 'is_read', 'created_at']
        
 ###################################
#
#   machine   Maintance  weekly
###################################


class SpotWeldingMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = SpotWeldingMaintenance
        fields = '__all__'

class CompressorMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = CompressorMaintenance
        fields = '__all__'

class LatheMachineMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = LatheMachineMaintenance
        fields = '__all__'

class VerticalDrillMachineMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = VerticalDrillMachineMaintenance
        fields = '__all__'

class SurfaceGrinderMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = SurfaceGrinderMaintenance
        fields = '__all__'

class TigWeldingMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = TigWeldingMaintenance
        fields = '__all__'
        
class BaseGrinderMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = BaseGrinderMaintenance
        fields = '__all__'

class BeltGrinderMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = BeltGrinderMaintenance
        fields = '__all__'

class PipeCuttingMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = PipeCuttingMaintenance
        fields = '__all__'

class VibraMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = VibraMaintenance
        fields = '__all__'

class DipMoldingMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = DipMoldingMaintenance
        fields = '__all__'

class ServoPressMaintenanceSerializer(serializers.ModelSerializer):
    date = serializers.DateField(required=False, allow_null=True)
    class Meta:
        model = ServoPressMaintenance
        fields = '__all__'
class MachinePreventiveMaintenanceSerializer(serializers.ModelSerializer):
    checklist = serializers.JSONField(source='checkpoints')

    class Meta:
        model = MachinePreventiveMaintenance
        fields = [
            'id',
            'machine_name',
            'machine_no',
            'date',
            'location',
            'specification',
            'maintenance_personnel',
            'checklist',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']

    def validate_checklist(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checklist must be a non-empty list.")

        required_fields = {
            'sr_no',
            'check_point',
            'checking_method',
            'before_maintenance',
            'after_maintenance',
            'remarks',
        }

        for index, item in enumerate(value, start=1):
            if not isinstance(item, dict) or not required_fields.issubset(item):
                raise serializers.ValidationError(
                    f"Checklist row {index} must contain sr_no, check_point, checking_method, before_maintenance, after_maintenance and remarks."
                )

        return value
class CNCMaintenanceReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = CNCMaintenanceReport
        fields = [
            'id',
            'machine_name',
            'machine_no',
            'date',
            'location',
            'specification',
            'maintenance_personnel',
            'checklist',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']

    def validate_checklist(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checklist must be a non-empty list.")

        required_fields = {
            'sr_no',
            'check_point',
            'checking_method',
            'before_maintenance',
            'after_maintenance',
            'remarks',
        }

        for index, item in enumerate(value, start=1):
            if not isinstance(item, dict) or not required_fields.issubset(item):
                raise serializers.ValidationError(
                    f"Checklist row {index} must contain sr_no, check_point, checking_method, before_maintenance, after_maintenance and remarks."
                )

        return value

from rest_framework import serializers
class VerticalMillingMachineCheckSheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = VerticalMillingMachineCheckSheet
        fields = [
            'id',
            'machine_name',
            'machine_no',
            'date',
            'location',
            'specification',
            'maintenance_personnel',
            'checkpoints',
            'prepared_by',
            'checked_by',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']

    def validate_checkpoints(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checkpoints must be a non-empty list.")

        required_fields = {
            'sr_no',
            'check_point',
            'checking_parameter',
            'method',
            'before_maintenance',
            'after_maintenance',
            'remarks',
        }

        for index, item in enumerate(value, start=1):
            if not isinstance(item, dict) or not required_fields.issubset(item):
                raise serializers.ValidationError(
                    f"Checkpoint row {index} must contain sr_no, check_point, checking_parameter, method, before_maintenance, after_maintenance and remarks."
                )

        return value
class ProjectionWeldingPMCheckSheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProjectionWeldingPMCheckSheet
        fields = [
            'id',
            'machine_name',
            'machine_no',
            'date',
            'location',
            'specification',
            'maintenance_personnel',
            'checkpoints',
            'prepared_by',
            'checked_by',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']

    def validate_checkpoints(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checkpoints must be a non-empty list.")

        required_fields = {
            'sr_no',
            'check_point',
            'checking_parameter',
            'checking_method',
            'before_maintenance',
            'after_maintenance',
            'spare_used_remarks',
        }

        for index, item in enumerate(value, start=1):
            if not isinstance(item, dict) or not required_fields.issubset(item):
                raise serializers.ValidationError(
                    f"Checkpoint row {index} must contain sr_no, check_point, checking_parameter, checking_method, before_maintenance, after_maintenance and spare_used_remarks."
                )

        return value 
class PowerPressPMCheckSheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = PowerPressPMCheckSheet
        fields = [
            'id',
            'machine_name',
            'machine_no',
            'date',
            'location',
            'specification',
            'checkpoints',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']

    def validate_checkpoints(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checkpoints must be a non-empty list.")

        required_fields = {
            'sr_no',
            'check_point',
            'checking_method',
            'checking_parameter',
            'before_maintenance',
            'after_maintenance',
            'remarks',
        }

        for index, item in enumerate(value, start=1):
            if not isinstance(item, dict) or not required_fields.issubset(item):
                raise serializers.ValidationError(
                    f"Checkpoint row {index} must contain sr_no, check_point, checking_method, checking_parameter, before_maintenance, after_maintenance and remarks."
                )

        return value       


class HydraulicPMCheckSheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = HydraulicPMCheckSheet
        fields = [
            'id',
            'machine_name',
            'machine_no',
            'date',
            'location',
            'specification',
            'maintenance_personnel',
            'checkpoints',
            'prepared_by',
            'checked_by',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']

    def validate_checkpoints(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checkpoints must be a non-empty list.")

        # Fields perfectly matching your exact UI screenshot columns
        required_fields = {
            'sr_no',
            'check_point',
            'checking_parameter',
            'checking_method',
            'before_maintenance',
            'after_maintenance',
            'remarks',
        }

        for index, item in enumerate(value, start=1):
            if not isinstance(item, dict) or not required_fields.issubset(item):
                raise serializers.ValidationError(
                    f"Checkpoint row {index} must contain sr_no, check_point, checking_parameter, checking_method, before_maintenance, after_maintenance, and remarks."
                )

        return value
    
    


class FixtureMaintenanceRecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = FixtureMaintenanceRecord
        fields = '__all__'


class IncomingMaterialInspectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = IncomingMaterialInspection
        fields = '__all__'
        
from rest_framework import serializers
from .models import UserProfile

class UserDepartmentProfileSerializer(serializers.ModelSerializer):
    username = serializers.CharField(source="user.username", read_only=True)
    email = serializers.EmailField(source="user.email", read_only=True)
    role = serializers.SerializerMethodField()
    fullName = serializers.CharField(source="full_name", read_only=True)
    profileImage = serializers.ImageField(source="profile_image", read_only=True)
    # profile_image = serializers.ImageField(required=False, allow_null=True)
    class Meta:
        model = UserProfile
        fields = [
            "id",
            "user",
            "username",
            "email",
            "employee_id",
            "mobile_no",
            "contact_email",
            "full_name",
            "location",
            "department",
            "designation",
            "profile_image",
            "role",
        ]
        read_only_fields = ["id", "user", "username", "email", "role" ]

    def get_role(self, obj):
        if obj.user.is_superuser:
            return "Admin"
        if obj.user.is_staff:
            return "Staff"
        return "User"