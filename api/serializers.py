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
    GoodReceiptEntry,
    DailyProductionPlan,
    FourMChangeInspection,
    FourMChangeRecord,
    MonthlyProductionPlan,
    OperatorObservanceChecklist,
    OperatorObservancePlan,
    PMChecklistMHE,
    ProjectionWelderQual,
    SpotWelderQual,
    TigMigWelderQual,
    ProcessValidation,
    ProcessAuditChecksheet,
    CoherenceChecklist,
    LayoutInspection,
    ProductAuditPlan,
    CustomerComplaint,
    CustomerSatisfaction,
    WarrantyClaim,
    MinutesOfMeeting,
    MachineBreakdown,
    ToolBreakdown,
    MachineCriticalSpare,
    ToolCriticalSpare,
    ToolBreakdownIntimation,
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
    HydraulicPMCheckSheet,
    FourMDisplay,
    FourMSummary,
    FixtureMaintenanceRecord,
    IncomingMaterialInspection,
    FourMInformationSheet,
)

# ==========================================
# 🟢 PURANE SERIALIZERS (Jo aapne bheje the)
# ==========================================


# 1. Operator Serializer
class OperatorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Operator
        fields = "__all__"


# 2. Operator Assignment Serializer
class OperatorAssignmentSerializer(serializers.ModelSerializer):
    class Meta:
        model = OperatorAssignment
        fields = "__all__"


# 3. Idle Report Serializer
class IdleReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = IdleReport
        fields = "__all__"


# 4. Hourly Idle Time Serializer
class Plant2HourlyIdletimeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Plant2HourlyIdletime
        fields = "__all__"


# 5. QMS INSPECTION REPORT SERIALIZER
class InspectionReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = InspectionReport
        fields = "__all__"


# ==========================================
# 🔵 NAYE SERIALIZERS (Jo humne abhi banaye)
# ==========================================


# --- MASTER DATA (Excel Uploads) ---
class L1PartInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = L1_PartInfoMaster
        fields = "__all__"


class L2ProcessReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = L2_ProcessReportMaster
        fields = "__all__"


class L3ParameterDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = L3_ParameterDetailMaster
        fields = "__all__"


# --- INCOMING INSPECTION (Master-Detail Nested) ---
class IncomingInspectionObservationSerializer(serializers.ModelSerializer):
    class Meta:
        model = IncomingInspectionObservation
        exclude = ("inspection_report",)  # Frontend ko ID bhejne ki zaroorat nahi


class IncomingInspectionReportSerializer(serializers.ModelSerializer):
    observations = IncomingInspectionObservationSerializer(many=True)

    class Meta:
        model = IncomingInspectionReport
        fields = "__all__"

    # Custom Save Logic: Header + Rows ek sath save karne ke liye
    def create(self, validated_data):
        observations_data = validated_data.pop("observations", [])
        report = IncomingInspectionReport.objects.create(**validated_data)

        for obs_data in observations_data:
            IncomingInspectionObservation.objects.create(
                inspection_report=report, **obs_data
            )
        return report


# --- DAILY MACHINE CHECKSHEET (Nested) ---
class MachineChecksheetObservationSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineChecksheetObservation
        exclude = ("report",)


class MachineChecksheetReportSerializer(serializers.ModelSerializer):
    check_points = MachineChecksheetObservationSerializer(many=True)

    class Meta:
        model = MachineChecksheetReport
        fields = "__all__"

    def create(self, validated_data):
        check_points_data = validated_data.pop("check_points", [])
        report = MachineChecksheetReport.objects.create(**validated_data)

        for point_data in check_points_data:
            MachineChecksheetObservation.objects.create(report=report, **point_data)
        return report


# --- DAILY PRODUCTION REPORT / MES (DPR) ---
class DailyProductionReportSerializer(serializers.ModelSerializer):
    # Frontend mein IDs ki jagah actual Naam dikhane ke liye:
    part_name = serializers.CharField(source="part_info.part_name", read_only=True)
    operation_name = serializers.CharField(
        source="operation.report_name", read_only=True
    )
    operator_name = serializers.CharField(source="operator.name", read_only=True)

    class Meta:
        model = DailyProductionReport
        fields = "__all__"


class TipChangeDressingSerializer(serializers.ModelSerializer):
    class Meta:
        model = TipChangeDressing
        fields = "__all__"


# =====================================================================
# 5S CHECKSHEET SERIALIZERS (Nested)
# =====================================================================
class FiveSChecksheetObservationSerializer(serializers.ModelSerializer):
    class Meta:
        model = FiveSChecksheetObservation
        # Report field ko exclude kiya hai taaki frontend se bhejna na pade,
        # ye backend par automatically attach ho jayega.
        fields = ["id", "s_category", "check_point", "status"]


class FiveSChecksheetReportSerializer(serializers.ModelSerializer):
    observations = FiveSChecksheetObservationSerializer(many=True)

    class Meta:
        model = FiveSChecksheetReport
        fields = "__all__"

    # Custom create method for Nested Data Save
    def create(self, validated_data):
        observations_data = validated_data.pop("observations", [])
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
        fields = "__all__"


class RedBinAnalysisReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = RedBinAnalysisReport
        fields = "__all__"


class RedBinAttendanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = RedBinAttendance
        fields = "__all__"


class ScrapNoteEntrySerializer(serializers.ModelSerializer):
    class Meta:
        model = ScrapNoteEntry
        fields = "__all__"


class ReworkEntrySerializer(serializers.ModelSerializer):
    class Meta:
        model = ReworkEntry
        fields = "__all__"


class DeviationApprovalSerializer(serializers.ModelSerializer):
    class Meta:
        model = DeviationApproval
        fields = "__all__"


class GoodReceiptEntrySerializer(serializers.ModelSerializer):
    class Meta:
        model = GoodReceiptEntry
        fields = "__all__"


class DailyPowerPressChecksheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = DailyPowerPressChecksheet
        fields = "__all__"


class MachineHistoryCardSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineHistoryCard
        fields = "__all__"


class MachineBreakdownIntimationSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineBreakdownIntimation
        fields = "__all__"


class ToolHistoryReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolHistoryReport
        fields = "__all__"


class ToolPreventiveMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolPreventiveMaintenance
        fields = "__all__"


class ToolBreakdownIntimationSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolBreakdownIntimation
        fields = "__all__"


class DailyProductionPlanSerializer(serializers.ModelSerializer):
    class Meta:
        model = DailyProductionPlan
        fields = "__all__"


class FourMChangeInspectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMChangeInspection
        fields = "__all__"


# serializers.py
class FourMChangeRecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMChangeRecord
        fields = "__all__"


class FourMDisplaySerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMDisplay
        fields = "__all__"


class FourMSummarySerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMSummary
        fields = "__all__"


class FourMInformationSheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = FourMInformationSheet
        fields = "__all__"


###################################
#
#       Prodcution monthly
###################################


class MonthlyProductionPlanSerializer(serializers.ModelSerializer):
    class Meta:
        model = MonthlyProductionPlan
        fields = "__all__"


class OperatorObservanceChecklistSerializer(serializers.ModelSerializer):
    class Meta:
        model = OperatorObservanceChecklist
        fields = "__all__"


class OperatorObservancePlanSerializer(serializers.ModelSerializer):
    class Meta:
        model = OperatorObservancePlan
        fields = "__all__"


class PMChecklistMHESerializer(serializers.ModelSerializer):
    class Meta:
        model = PMChecklistMHE
        fields = "__all__"


class ProjectionWelderQualSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProjectionWelderQual
        fields = "__all__"


class SpotWelderQualSerializer(serializers.ModelSerializer):
    class Meta:
        model = SpotWelderQual
        fields = "__all__"


class TigMigWelderQualSerializer(serializers.ModelSerializer):
    class Meta:
        model = TigMigWelderQual
        fields = "__all__"


class ProcessValidationSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProcessValidation
        fields = "__all__"


###################################
#
#       QA monthly
###################################
class ProcessAuditChecksheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProcessAuditChecksheet
        fields = "__all__"


class CoherenceChecklistSerializer(serializers.ModelSerializer):
    class Meta:
        model = CoherenceChecklist
        fields = "__all__"


class LayoutInspectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = LayoutInspection
        fields = "__all__"


class ProductAuditPlanSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProductAuditPlan
        fields = "__all__"


class CustomerComplaintSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomerComplaint
        fields = "__all__"


class CustomerSatisfactionSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomerSatisfaction
        fields = "__all__"


class WarrantyClaimSerializer(serializers.ModelSerializer):
    class Meta:
        model = WarrantyClaim
        fields = "__all__"


class MinutesOfMeetingSerializer(serializers.ModelSerializer):
    class Meta:
        model = MinutesOfMeeting
        fields = "__all__"


###################################
#
#       Maintance  monthly
###################################


class MachineBreakdownSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineBreakdown
        fields = "__all__"


class ToolBreakdownSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolBreakdown
        fields = "__all__"


class MachineCriticalSpareSerializer(serializers.ModelSerializer):
    class Meta:
        model = MachineCriticalSpare
        fields = "__all__"


class ToolCriticalSpareSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToolCriticalSpare
        fields = "__all__"


from rest_framework import serializers
from .models import Notification


class NotificationSerializer(serializers.ModelSerializer):
    """
    FINAL-COMPATIBLE NOTIFICATION SERIALIZER

    Final Notification DB fields:
        id
        message
        is_read
        created_at
        user_id

    All machine / Ideal / reason information
    IdealTimeSegmentReason se derive hogi.

    Frontend compatibility ke liye old response keys
    temporarily maintain kiye ja rahe hain.
    """

    # ==========================================================
    # FINAL NOTIFICATION FIELD
    # ==========================================================
    id = serializers.IntegerField(
        source="pk",
        read_only=True,
    )

    user_id = serializers.IntegerField(
        read_only=True,
        allow_null=True,
    )

    # ==========================================================
    # COMPATIBILITY / IDEAL-DERIVED FIELDS
    # ==========================================================

    ideal_event_id = serializers.SerializerMethodField()

    machine_no = serializers.SerializerMethodField()
    notification_type = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()

    plant_location = serializers.SerializerMethodField()
    ideal_mode = serializers.SerializerMethodField()

    idle_started_at = serializers.SerializerMethodField()
    idle_ended_at = serializers.SerializerMethodField()

    reason_category = serializers.SerializerMethodField()
    specific_reason = serializers.SerializerMethodField()
    remark = serializers.SerializerMethodField()

    submitted_by = serializers.SerializerMethodField()
    submitted_at = serializers.SerializerMethodField()

    updated_at = serializers.SerializerMethodField()

    class Meta:
        model = Notification

        fields = [
            "id",
            # Exact Ideal event ID
            "ideal_event_id",
            # Machine / compatibility
            "machine_no",
            # Final Notification fields
            "message",
            "is_read",
            "created_at",
            "user_id",
            # Frontend compatibility
            "notification_type",
            "status",
            # Ideal event details
            "plant_location",
            "ideal_mode",
            "idle_started_at",
            "idle_ended_at",
            # Reason details
            "reason_category",
            "specific_reason",
            "remark",
            # Submission details
            "submitted_by",
            "submitted_at",
            # Temporary response compatibility
            "updated_at",
        ]

        read_only_fields = fields

    # ==========================================================
    # HELPER
    # ==========================================================

    @staticmethod
    def _ideal(obj):

        try:
            return obj.ideal_event
        except Exception:
            return None

    # ==========================================================
    # EXACT EVENT ID
    # ==========================================================

    def get_ideal_event_id(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.id

        # Temporary legacy safety
        return getattr(
            obj,
            "ideal_event_id",
            None,
        )

    # ==========================================================
    # MACHINE / PLANT / EVENT
    # ==========================================================

    def get_machine_no(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return str(ideal.machine_no)

        return getattr(
            obj,
            "machine_no",
            None,
        )

    def get_plant_location(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.plant_location

        return getattr(
            obj,
            "plant_location",
            None,
        )

    def get_ideal_mode(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.ideal_mode

        return getattr(
            obj,
            "ideal_mode",
            None,
        )

    # ==========================================================
    # TYPE / STATUS
    # ==========================================================

    def get_notification_type(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return "IDLE_REASON"

        return getattr(
            obj,
            "notification_type",
            "GENERAL",
        )

    def get_status(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.report_status

        return getattr(
            obj,
            "status",
            None,
        )

    # ==========================================================
    # IDEAL TIMING
    # ==========================================================

    def get_idle_started_at(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.ideal_start_at

        return getattr(
            obj,
            "idle_started_at",
            None,
        )

    def get_idle_ended_at(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.ideal_end_at

        return getattr(
            obj,
            "idle_ended_at",
            None,
        )

    # ==========================================================
    # REASON
    # ==========================================================

    def get_reason_category(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.reason

        return getattr(
            obj,
            "reason_category",
            None,
        )

    def get_specific_reason(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.specific_reason

        return getattr(
            obj,
            "specific_reason",
            None,
        )

    def get_remark(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.remark

        return getattr(
            obj,
            "remark",
            None,
        )

    # ==========================================================
    # SUBMISSION
    # ==========================================================

    def get_submitted_by(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.submitted_by

        return getattr(
            obj,
            "submitted_by",
            None,
        )

    def get_submitted_at(self, obj):

        ideal = self._ideal(obj)

        if ideal is not None:
            return ideal.submitted_at

        return getattr(
            obj,
            "submitted_at",
            None,
        )

    # ==========================================================
    # TEMP FRONTEND COMPATIBILITY
    # ==========================================================

    def get_updated_at(self, obj):

        # Final Notification model me updated_at nahi hoga.
        # Frontend compatibility ke liye created_at return karte hain.
        return getattr(
            obj,
            "created_at",
            None,
        )


###################################
#
#   machine   Maintance  weekly
###################################


class SpotWeldingMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = SpotWeldingMaintenance
        fields = "__all__"


class CompressorMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = CompressorMaintenance
        fields = "__all__"


class LatheMachineMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = LatheMachineMaintenance
        fields = "__all__"


class VerticalDrillMachineMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = VerticalDrillMachineMaintenance
        fields = "__all__"


class SurfaceGrinderMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = SurfaceGrinderMaintenance
        fields = "__all__"


class TigWeldingMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = TigWeldingMaintenance
        fields = "__all__"


class BaseGrinderMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = BaseGrinderMaintenance
        fields = "__all__"


class BeltGrinderMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = BeltGrinderMaintenance
        fields = "__all__"


class PipeCuttingMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = PipeCuttingMaintenance
        fields = "__all__"


class VibraMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = VibraMaintenance
        fields = "__all__"


class DipMoldingMaintenanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = DipMoldingMaintenance
        fields = "__all__"


class ServoPressMaintenanceSerializer(serializers.ModelSerializer):
    date = serializers.DateField(required=False, allow_null=True)

    class Meta:
        model = ServoPressMaintenance
        fields = "__all__"


class MachinePreventiveMaintenanceSerializer(serializers.ModelSerializer):
    checklist = serializers.JSONField(source="checkpoints")

    class Meta:
        model = MachinePreventiveMaintenance
        fields = [
            "id",
            "machine_name",
            "machine_no",
            "date",
            "location",
            "specification",
            "maintenance_personnel",
            "checklist",
            "created_at",
        ]
        read_only_fields = ["id", "created_at"]

    def validate_checklist(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checklist must be a non-empty list.")

        required_fields = {
            "sr_no",
            "check_point",
            "checking_method",
            "before_maintenance",
            "after_maintenance",
            "remarks",
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
            "id",
            "machine_name",
            "machine_no",
            "date",
            "location",
            "specification",
            "maintenance_personnel",
            "checklist",
            "created_at",
        ]
        read_only_fields = ["id", "created_at"]

    def validate_checklist(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checklist must be a non-empty list.")

        required_fields = {
            "sr_no",
            "check_point",
            "checking_method",
            "before_maintenance",
            "after_maintenance",
            "remarks",
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
            "id",
            "machine_name",
            "machine_no",
            "date",
            "location",
            "specification",
            "maintenance_personnel",
            "checkpoints",
            "prepared_by",
            "checked_by",
            "created_at",
        ]
        read_only_fields = ["id", "created_at"]

    def validate_checkpoints(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checkpoints must be a non-empty list.")

        required_fields = {
            "sr_no",
            "check_point",
            "checking_parameter",
            "method",
            "before_maintenance",
            "after_maintenance",
            "remarks",
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
            "id",
            "machine_name",
            "machine_no",
            "date",
            "location",
            "specification",
            "maintenance_personnel",
            "checkpoints",
            "prepared_by",
            "checked_by",
            "created_at",
        ]
        read_only_fields = ["id", "created_at"]

    def validate_checkpoints(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checkpoints must be a non-empty list.")

        required_fields = {
            "sr_no",
            "check_point",
            "checking_parameter",
            "checking_method",
            "before_maintenance",
            "after_maintenance",
            "spare_used_remarks",
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
            "id",
            "machine_name",
            "machine_no",
            "date",
            "location",
            "specification",
            "checkpoints",
            "created_at",
        ]
        read_only_fields = ["id", "created_at"]

    def validate_checkpoints(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checkpoints must be a non-empty list.")

        required_fields = {
            "sr_no",
            "check_point",
            "checking_method",
            "checking_parameter",
            "before_maintenance",
            "after_maintenance",
            "remarks",
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
            "id",
            "machine_name",
            "machine_no",
            "date",
            "location",
            "specification",
            "maintenance_personnel",
            "checkpoints",
            "prepared_by",
            "checked_by",
            "created_at",
        ]
        read_only_fields = ["id", "created_at"]

    def validate_checkpoints(self, value):
        if not isinstance(value, list) or not value:
            raise serializers.ValidationError("checkpoints must be a non-empty list.")

        # Fields perfectly matching your exact UI screenshot columns
        required_fields = {
            "sr_no",
            "check_point",
            "checking_parameter",
            "checking_method",
            "before_maintenance",
            "after_maintenance",
            "remarks",
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
        fields = "__all__"


class IncomingMaterialInspectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = IncomingMaterialInspection
        fields = "__all__"


from rest_framework import serializers
from .models import UserProfile


class UserDepartmentProfileSerializer(serializers.ModelSerializer):

    username = serializers.CharField(source="user.username", read_only=True)

    email = serializers.EmailField(source="user.email", read_only=True)

    role = serializers.SerializerMethodField()

    fullName = serializers.CharField(source="full_name", read_only=True)

    profileImage = serializers.ImageField(source="profile_image", read_only=True)

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
            # Database/original fields
            "full_name",
            "profile_image",
            # Frontend compatibility fields
            "fullName",
            "profileImage",
            "location",
            "department",
            "designation",
            "role",
        ]

        read_only_fields = [
            "id",
            "user",
            "username",
            "email",
            "role",
            "fullName",
            "profileImage",
        ]

    def get_role(self, obj):

        if obj.user.is_superuser:
            return "Admin"

        if obj.user.is_staff:
            return "Staff"

        return "User"
