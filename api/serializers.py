# from rest_framework import serializers
# from .models import (
#     Operator, 
#     OperatorAssignment, 
#     IdleReport, 
#     Plant2HourlyIdletime, 
#     InspectionReport
# )

# # 1. Operator Serializer
# class OperatorSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Operator
#         fields = '__all__'

# # 2. Operator Assignment Serializer
# class OperatorAssignmentSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = OperatorAssignment
#         fields = '__all__'

# # 3. Idle Report Serializer
# class IdleReportSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = IdleReport
#         fields = '__all__'

# # 4. Hourly Idle Time Serializer
# class Plant2HourlyIdletimeSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Plant2HourlyIdletime
#         fields = '__all__'

# # ==========================================
# # 5. QMS INSPECTION REPORT SERIALIZER (NEW)
# # ==========================================
# class InspectionReportSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = InspectionReport
#         fields = '__all__'  
#         # '__all__' ka matlab hai ye model ke saare fields 
#         # (customer, part, JSON data sab kuch) automatically API ke liye set kar dega.


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
    DailyProductionReport
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