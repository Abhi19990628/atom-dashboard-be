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
    MachineBreakdown, ToolBreakdown, MachineCriticalSpare, ToolCriticalSpare
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