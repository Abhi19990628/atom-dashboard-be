# from django.db import models
# from django.utils import timezone

# class Operator(models.Model):
#     PLANT_CHOICES = [
#         ('plant_1', 'Plant 1'),
#         ('plant_2', 'Plant 2'),
#     ]
#     name = models.CharField(max_length=100)
#     plant = models.CharField(max_length=20, choices=PLANT_CHOICES)
#     is_active = models.BooleanField(default=True)
#     created_at = models.DateTimeField(auto_now_add=True)
    
#     class Meta:
#         managed = False  # ✅ Django is purane table ko touch nahi karega
#         db_table = 'operators'
#         ordering = ['name']
#         unique_together = ['name', 'plant']
    
#     def __str__(self):
#         return f"{self.name} - {self.plant}"

# class OperatorAssignment(models.Model):
#     SHIFT_CHOICES = [('A', 'Shift A'), ('B', 'Shift B')]
#     PLANT_CHOICES = [('plant_1', 'Plant 1'), ('plant_2', 'Plant 2')]
    
#     id = models.AutoField(primary_key=True)
#     plant = models.CharField(max_length=20, choices=PLANT_CHOICES, default='plant_2')
#     machine_no = models.CharField(max_length=10)
#     operator_name = models.CharField(max_length=100)
#     shift = models.CharField(max_length=1, choices=SHIFT_CHOICES)
#     start_time = models.DateTimeField(default=timezone.now) 
#     created_at = models.DateTimeField(default=timezone.now)
    
#     class Meta:
#         managed = False  # ✅ Data safe rahega
#         db_table = 'operator_assignments'

#     def __str__(self):
#         return f"{self.plant}: {self.operator_name} -> Machine {self.machine_no}"

# class IdleReport(models.Model):
#     IDLE_REASON_CHOICES = [
#         ('TOOL_BD', 'Tool Breakdown'),
#         ('MC_BD', 'Machine Breakdown'),
#         ('MAINT', 'Scheduled Maintenance'),
#         ('CHANGEOVER', 'Changeover'),
#         ('NO_MATERIAL', 'Material Shortage'),
#         ('QUALITY_ISSUE', 'Quality Issue'),  
#         ('OTHER', 'Other'),
#     ]
#     PLANT_CHOICES = [('plant_1', 'Plant 1'), ('plant_2', 'Plant 2')]
    
#     id = models.AutoField(primary_key=True)
#     plant = models.CharField(max_length=20, choices=PLANT_CHOICES, default='plant_2')
#     machine_no = models.CharField(max_length=10)
#     operator_name = models.CharField(max_length=100)
#     tool_id = models.CharField(max_length=100)
#     reason = models.CharField(max_length=20, choices=IDLE_REASON_CHOICES)
#     created_at = models.DateTimeField(auto_now_add=True)
    
#     class Meta:
#         managed = False  # ✅ Yahi table error de rahi thi, ab nahi degi
#         db_table = 'idle_reports'

#     def __str__(self):
#         return f"{self.plant} - Idle: Machine {self.machine_no} - {self.reason}"

# class Plant2HourlyIdletime(models.Model):
#     timestamp = models.DateTimeField()
#     tool_id = models.CharField(max_length=50)
#     machine_no = models.CharField(max_length=10)
#     idle_time = models.IntegerField(default=0)
#     shut_height = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
#     shift = models.CharField(max_length=5)
    
#     class Meta:
#         # ❌ Yahan managed = False NAHI aayega kyunki isko create karna hai
#         db_table = 'Plant2_hourly_idle'
#         unique_together = [('timestamp', 'machine_no')]
#         indexes = [
#             models.Index(fields=['timestamp', 'machine_no']),
#             models.Index(fields=['machine_no', 'timestamp']),
#         ]
#         ordering = ['-timestamp', 'machine_no']
    
#     def __str__(self):
#         return f"M{self.machine_no} - {self.timestamp.strftime('%Y-%m-%d %H:%M')} - {self.idle_time}min"

# class InspectionReport(models.Model):
#     customer_account = models.CharField(max_length=255)
#     part_name = models.CharField(max_length=255)
#     operation = models.CharField(max_length=255)
#     part_number = models.CharField(max_length=255, blank=True, null=True)
    
#     plant_location = models.CharField(max_length=100)
#     inspection_date = models.DateField()
#     operator_name = models.CharField(max_length=100)
#     machine_number = models.CharField(max_length=50)
    
#     inspection_data = models.JSONField() 
#     created_at = models.DateTimeField(auto_now_add=True)

#     class Meta:
#         managed = False  # ✅ Data safe rahega
#         db_table = 'inspection_reports'
#         ordering = ['-created_at']

#     def __str__(self):
#         return f"{self.customer_account} - {self.part_name} - {self.inspection_date}"


# # ==========================================
# # 🆕 NAYA MASTER DATA DESIGN (L1, L2, L3)
# # Inme ab managed = False laga diya gaya hai
# # ==========================================

# class L1_PartInfoMaster(models.Model):
#     customer_name = models.CharField(max_length=100)
#     part_name = models.CharField(max_length=100)
#     model_name = models.CharField(max_length=100)
#     part_no = models.CharField(max_length=100)

#     class Meta:
#         managed = False  # ✅ Ise add kar diya
#         db_table = 'L1_part_info_master'

#     def __str__(self):
#         return f"{self.customer_name} | {self.part_name} | {self.part_no}"

# class L2_ProcessReportMaster(models.Model):
#     part_info = models.ForeignKey(L1_PartInfoMaster, related_name='process_reports', on_delete=models.CASCADE)
#     report_name = models.CharField(max_length=150)

#     class Meta:
#         managed = False  # ✅ Ise add kar diya
#         db_table = 'L2_process_report_master'

#     def __str__(self):
#         return f"{self.part_info.part_name} -> {self.report_name}"

# class L3_ParameterDetailMaster(models.Model):
#     CATEGORY_CHOICES = [
#         ('PRODUCT', 'PRODUCT'),
#         ('PROCESS', 'PROCESS')
#     ]
    
#     process_report = models.ForeignKey(L2_ProcessReportMaster, related_name='parameters', on_delete=models.CASCADE)
#     category = models.CharField(max_length=50, choices=CATEGORY_CHOICES)
#     parameter_name = models.CharField(max_length=200)
#     specification = models.CharField(max_length=200)
#     instrument = models.CharField(max_length=200)

#     class Meta:
#         managed = False  # ✅ Ise add kar diya
#         db_table = 'L3_parameter_detail_master'

#     def __str__(self):
#         return f"{self.category} | {self.parameter_name} | {self.specification}"
    
    
    
from django.db import models
from django.utils import timezone

# =====================================================================
# 🗄️ 1. LEGACY & BASE TABLES (Data Safe Rahega - Managed = False)
# =====================================================================

class Operator(models.Model):
    PLANT_CHOICES = [
        ('plant_1', 'Plant 1'),
        ('plant_2', 'Plant 2'),
    ]
    name = models.CharField(max_length=100)
    plant = models.CharField(max_length=20, choices=PLANT_CHOICES)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        managed = False  
        db_table = 'operators'
        ordering = ['name']
        unique_together = ['name', 'plant']
    
    def __str__(self):
        return f"{self.name} - {self.plant}"

class OperatorAssignment(models.Model):
    SHIFT_CHOICES = [('A', 'Shift A'), ('B', 'Shift B')]
    PLANT_CHOICES = [('plant_1', 'Plant 1'), ('plant_2', 'Plant 2')]
    
    id = models.AutoField(primary_key=True)
    plant = models.CharField(max_length=20, choices=PLANT_CHOICES, default='plant_2')
    machine_no = models.CharField(max_length=10)
    operator_name = models.CharField(max_length=100)
    shift = models.CharField(max_length=1, choices=SHIFT_CHOICES)
    start_time = models.DateTimeField(default=timezone.now) 
    created_at = models.DateTimeField(default=timezone.now)
    
    class Meta:
        managed = False  
        db_table = 'operator_assignments'

    def __str__(self):
        return f"{self.plant}: {self.operator_name} -> Machine {self.machine_no}"

class IdleReport(models.Model):
    IDLE_REASON_CHOICES = [
        ('TOOL_BD', 'Tool Breakdown'),
        ('MC_BD', 'Machine Breakdown'),
        ('MAINT', 'Scheduled Maintenance'),
        ('CHANGEOVER', 'Changeover'),
        ('NO_MATERIAL', 'Material Shortage'),
        ('QUALITY_ISSUE', 'Quality Issue'),  
        ('OTHER', 'Other'),
    ]
    PLANT_CHOICES = [('plant_1', 'Plant 1'), ('plant_2', 'Plant 2')]
    
    id = models.AutoField(primary_key=True)
    plant = models.CharField(max_length=20, choices=PLANT_CHOICES, default='plant_2')
    machine_no = models.CharField(max_length=10)
    operator_name = models.CharField(max_length=100)
    tool_id = models.CharField(max_length=100)
    reason = models.CharField(max_length=20, choices=IDLE_REASON_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        managed = False  
        db_table = 'idle_reports'

    def __str__(self):
        return f"{self.plant} - Idle: Machine {self.machine_no} - {self.reason}"

class Plant2HourlyIdletime(models.Model):
    timestamp = models.DateTimeField()
    tool_id = models.CharField(max_length=50)
    machine_no = models.CharField(max_length=10)
    idle_time = models.IntegerField(default=0)
    shut_height = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    shift = models.CharField(max_length=5)
    
    class Meta:
        db_table = 'Plant2_hourly_idle'
        unique_together = [('timestamp', 'machine_no')]
        indexes = [
            models.Index(fields=['timestamp', 'machine_no']),
            models.Index(fields=['machine_no', 'timestamp']),
        ]
        ordering = ['-timestamp', 'machine_no']
    
    def __str__(self):
        return f"M{self.machine_no} - {self.timestamp.strftime('%Y-%m-%d %H:%M')} - {self.idle_time}min"

class InspectionReport(models.Model):
    customer_account = models.CharField(max_length=255)
    part_name = models.CharField(max_length=255)
    operation = models.CharField(max_length=255)
    part_number = models.CharField(max_length=255, blank=True, null=True)
    
    plant_location = models.CharField(max_length=100)
    inspection_date = models.DateField()
    operator_name = models.CharField(max_length=100)
    machine_number = models.CharField(max_length=50)
    
    inspection_data = models.JSONField() 
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False  
        db_table = 'inspection_reports'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.customer_account} - {self.part_name} - {self.inspection_date}"


# =====================================================================
# 📚 2. MASTER DATA TABLES (Excel se Upload hone wale)
# =====================================================================

class L1_PartInfoMaster(models.Model):
    customer_name = models.CharField(max_length=100)
    part_name = models.CharField(max_length=100)
    model_name = models.CharField(max_length=100)
    part_no = models.CharField(max_length=100)

    class Meta:
        managed = False  
        db_table = 'L1_part_info_master'

    def __str__(self):
        return f"{self.customer_name} | {self.part_name} | {self.part_no}"

class L2_ProcessReportMaster(models.Model):
    part_info = models.ForeignKey(L1_PartInfoMaster, related_name='process_reports', on_delete=models.CASCADE)
    report_name = models.CharField(max_length=150)

    class Meta:
        managed = False  
        db_table = 'L2_process_report_master'

    def __str__(self):
        return f"{self.part_info.part_name} -> {self.report_name}"

class L3_ParameterDetailMaster(models.Model):
    CATEGORY_CHOICES = [
        ('PRODUCT', 'PRODUCT'),
        ('PROCESS', 'PROCESS')
    ]
    
    process_report = models.ForeignKey(L2_ProcessReportMaster, related_name='parameters', on_delete=models.CASCADE)
    category = models.CharField(max_length=50, choices=CATEGORY_CHOICES)
    parameter_name = models.CharField(max_length=200)
    specification = models.CharField(max_length=200)
    instrument = models.CharField(max_length=200)

    class Meta:
        managed = False  
        db_table = 'L3_parameter_detail_master'

    def __str__(self):
        return f"{self.category} | {self.parameter_name} | {self.specification}"


# =====================================================================
# 📥 3. INCOMING INSPECTION REPORT (Master-Detail)
# =====================================================================

class IncomingInspectionReport(models.Model):
    part_info = models.ForeignKey(L1_PartInfoMaster, on_delete=models.CASCADE)
    report_no = models.CharField(max_length=50, unique=True, blank=True, null=True)
    inspection_date = models.DateField(default=timezone.now)
    supplier_name = models.CharField(max_length=255)
    
    grn_no = models.CharField(max_length=100)
    qty_received = models.PositiveIntegerField()
    qty_inspected = models.PositiveIntegerField()
    qty_accepted = models.PositiveIntegerField()
    qty_rejected = models.PositiveIntegerField(default=0)
    
    inspected_by = models.CharField(max_length=100)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True  
        db_table = "incoming_inspection_report"

class IncomingInspectionObservation(models.Model):
    inspection_report = models.ForeignKey(IncomingInspectionReport, on_delete=models.CASCADE, related_name='observations')
    parameter_name = models.CharField(max_length=255)
    specification = models.CharField(max_length=255)
    
    obs_1 = models.CharField(max_length=100, blank=True, null=True)
    obs_2 = models.CharField(max_length=100, blank=True, null=True)
    obs_3 = models.CharField(max_length=100, blank=True, null=True)
    obs_4 = models.CharField(max_length=100, blank=True, null=True)
    obs_5 = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = True
        db_table = "incoming_inspection_observation"


# =====================================================================
# ⚙️ 4. DAILY MACHINE CHECK SHEET (Poka-Yoke / PM) - UPDATED 
# =====================================================================

class MachineChecksheetReport(models.Model):
    # UI: Plant & Machine Selection
    date = models.DateField(default=timezone.now)
    plant_name = models.CharField(max_length=100, default="Plant 1") 
    machine_no = models.CharField(max_length=50)
    
    # UI: Signatures 
    checked_by_maintenance = models.CharField(max_length=100, blank=True, null=True) 
    verified_by_production = models.CharField(max_length=100, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "machine_checksheet_report"

    def __str__(self):
        return f"{self.machine_no} - {self.date}"


class MachineChecksheetObservation(models.Model):
    report = models.ForeignKey(MachineChecksheetReport, on_delete=models.CASCADE, related_name='check_points')
    
    # UI: Daily Check Parameters
    s_no = models.PositiveIntegerField() 
    poka_yoke_detail = models.CharField(max_length=255) 
    checking_method = models.CharField(max_length=255) 
    reference_sop = models.CharField(max_length=255, blank=True, null=True) 
    
    is_ok = models.BooleanField(default=True) 
    remarks = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = True
        db_table = "machine_checksheet_observation"
        ordering = ['s_no']


# =====================================================================
# 📊 5. DAILY PRODUCTION REPORT / MES PLAN (DPR)
# =====================================================================

class DailyProductionReport(models.Model):
    report_date = models.DateField(default=timezone.now)
    shift = models.CharField(max_length=5) 
    plant = models.CharField(max_length=20) 
    
    operator = models.ForeignKey(Operator, on_delete=models.SET_NULL, null=True, related_name='production_reports')
    machine_no = models.CharField(max_length=50) 
    
    part_info = models.ForeignKey(L1_PartInfoMaster, on_delete=models.RESTRICT, related_name='production_reports')
    operation = models.ForeignKey(L2_ProcessReportMaster, on_delete=models.RESTRICT, related_name='production_reports')
    
    start_time = models.DateTimeField(blank=True, null=True)
    end_time = models.DateTimeField(blank=True, null=True)
    working_time_min = models.IntegerField(default=0)
    
    total_production = models.IntegerField(default=0)
    target_production = models.IntegerField(default=0)
    
    ok_quantity = models.IntegerField(default=0)
    rejection_qty = models.IntegerField(default=0)
    rework_qty = models.IntegerField(default=0)
    not_confirm_qty = models.IntegerField(default=0, verbose_name="Not Confirm / Hold Qty")
    
    tool_setup_min = models.IntegerField(default=0)
    machine_bd_min = models.IntegerField(default=0, verbose_name="Machine Breakdown Min")
    tool_bd_min = models.IntegerField(default=0, verbose_name="Tool Breakdown Min")
    
    coil_no = models.CharField(max_length=100, blank=True, null=True)
    remarks = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "daily_production_report"
        unique_together = ['report_date', 'shift', 'machine_no', 'part_info']

    def __str__(self):
        return f"{self.report_date} | M-{self.machine_no} | {self.part_info.part_name} | OK: {self.ok_quantity}"

    @property
    def oee_quality(self):
        if self.total_production == 0:
            return 0
        return round((self.ok_quantity / self.total_production) * 100, 2)

    @property
    def total_loss_time(self):
        return self.tool_setup_min + self.machine_bd_min + self.tool_bd_min

    def save(self, *args, **kwargs):
        if self.ok_quantity == 0 and self.total_production > 0:
            self.ok_quantity = self.total_production - (self.rejection_qty + self.rework_qty + self.not_confirm_qty)
        super().save(*args, **kwargs)