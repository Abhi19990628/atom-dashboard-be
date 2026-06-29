from django.db import models
from django.utils import timezone
from django.utils.timezone import now

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
        managed = False
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
class InspectionItem(models.Model):
    report = models.ForeignKey(InspectionReport, on_delete=models.CASCADE, related_name='items')
    sr_no = models.IntegerField(null=True, blank=True)
    item = models.CharField(max_length=200, null=True, blank=True)
    special_char = models.CharField(max_length=100, blank=True, null=True)
    spec = models.CharField(max_length=100, null=True, blank=True)
    tolerance = models.CharField(max_length=50, null=True, blank=True)
    inst = models.CharField(max_length=100, null=True, blank=True)

    class Meta:
        managed = False  # Connects to your existing DB table
        db_table = 'inspection_items'


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
        
# =====================================================================
# 🛠️ 6. TIP CHANGE & DRESSING MONITORING (React Form Model)
# =====================================================================

class TipChangeDressing(models.Model):
    # Nayi Fields
    plant = models.CharField(max_length=50, blank=True, null=True)
    machine_name = models.CharField(max_length=50, blank=True, null=True)
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    
    # Form wali Fields
    part_name = models.CharField(max_length=100)
    operation = models.CharField(max_length=150, blank=True, null=True)
    prd_qty = models.IntegerField()
    tip_change = models.CharField(max_length=10) 
    
    # ✅ NAYA: Time aur Date track karne ke liye (Auto-save)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    
    # Purana sirf date wala field (agar aapko alag se chahiye toh, warna created_at me dono aa jate hain)
    date = models.DateField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tip_change_monitoring"

    def __str__(self):
        return f"{self.machine_name} ({self.machine_no}) - {self.part_name}"
    
    

class PushSubscription(models.Model):
    endpoint = models.URLField(max_length=500, unique=True)
    auth = models.CharField(max_length=100)
    p256dh = models.CharField(max_length=100)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Push Subscription {self.id}"
# =====================================================================
#  6. 5S CHECKSHEET REPORT 
# =====================================================================

class FiveSChecksheetReport(models.Model):
    area = models.CharField(max_length=100, default="P.Shop & Parking area")
    zone_leader = models.CharField(max_length=100)
    date = models.DateField(default=timezone.now)
    language = models.CharField(max_length=10)
    
    # Summary stats
    total_checks = models.IntegerField(default=0)
    ok_count = models.IntegerField(default=0)
    ng_count = models.IntegerField(default=0)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "five_s_checksheet_report"

    def __str__(self):
        return f"{self.zone_leader} - {self.area} - {self.date}"


class FiveSChecksheetObservation(models.Model):
    report = models.ForeignKey(FiveSChecksheetReport, on_delete=models.CASCADE, related_name='observations')
    s_category = models.CharField(max_length=10)  # e.g., "1'S'", "2'S'"
    check_point = models.TextField()
    status = models.CharField(max_length=20)  # "OK", "NG", "Not Checked"

    class Meta:
        managed = True
        db_table = "five_s_observation"
# =====================================================================
#  BinTrolley REPORT 
# =====================================================================
class BinTrolleyReport(models.Model):
    # Basic Info
    date = models.DateField(default=timezone.now)
    week = models.CharField(max_length=10)  # e.g., 'W1'
    month = models.CharField(max_length=20) # e.g., 'Apr'
    
    # 🌟 Saara data in 3 columns mein aayega JSON ban kar
    checkpoints = models.JSONField(default=dict, blank=True, null=True)
    cleaning_details = models.JSONField(default=dict, blank=True, null=True)
    maintenance_details = models.JSONField(default=dict, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "bin_trolley_report"
        ordering = ['-date', '-created_at']

    def __str__(self):
        return f"{self.date} - {self.week} ({self.month})"
    

# =====================================================================
# RED BIN ANALYSIS REPORT (RedBinForm.js)
# =====================================================================
from django.db import models
from django.utils import timezone

class RedBinAnalysisReport(models.Model):
    entry_date = models.DateField(default=timezone.now) 
    part_name_model = models.CharField(max_length=255)
    operation = models.CharField(max_length=255)
    total_rej_qty = models.PositiveIntegerField()
    
    defect_detail = models.TextField()
    root_cause_reason = models.TextField()
    action_taken = models.TextField()
    
    responsible_person = models.CharField(max_length=150)
    target_date = models.DateField()
    completion_date = models.DateField(null=True, blank=True)
    
    created_time = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        managed = True
        db_table = "red_bin_analysis_report"
# =====================================================================
#  RED BIN ATTENDANCE (RedbinAttendance.js) - SINGLE TABLE
# =====================================================================
class RedBinAttendance(models.Model):
    # JS choices se match karne ke liye
    ATTENDANCE_CHOICES = [
        ('P', 'Present'),
        ('A', 'Absent'),
        ('', 'Unmarked'),
    ]

    date = models.DateField(default=timezone.now)
    month = models.CharField(max_length=20)
    year = models.IntegerField()
    
    employee_name = models.CharField(max_length=150)
    designation = models.CharField(max_length=100)
    
    # Ab choices yahan apply kar di hain
    status = models.CharField(
        max_length=1, 
        choices=ATTENDANCE_CHOICES, 
        default='', 
        blank=True
    )
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "red_bin_attendance"
        unique_together = ['date', 'employee_name']

# =====================================================================
# SCRAP NOTE ENTRY (ScrapNoteForm.js)
# =====================================================================
class ScrapNoteEntry(models.Model):
    entry_date = models.DateField(default=timezone.now)
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100)
    
    defect_detail = models.CharField(max_length=255)
    quantity = models.PositiveIntegerField()
    
    remarks = models.TextField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "scrap_note_entry"

class ReworkEntry(models.Model):
    # Common Data (Har row ke sath save hoga)
    date = models.DateField(default=timezone.now)
    remark = models.TextField(blank=True, null=True)
    
    # Standard Columns (Table mein dikhne wale main data)
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100)
    spec = models.CharField(max_length=255)
    non_conformance = models.CharField(max_length=255)
    rework_qty = models.PositiveIntegerField(default=0)
    inspected_by = models.CharField(max_length=150, blank=True, null=True)
    
    # 🔥 JSONField: Isme status ('ok'/'notok') aur observations ka array jayega
    dynamic_details = models.JSONField(default=dict, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "rework_entry"
        ordering = ['-date', '-created_at']

    def __str__(self):
        return f"{self.date} | {self.part_name} - Qty: {self.rework_qty}"
    

class DeviationApproval(models.Model):
    tool_name_no = models.CharField(max_length=255, verbose_name="Tool Name/No.")
    location = models.CharField(max_length=255)
    problem = models.TextField()
    reason_for_deviation = models.TextField()
    
    date = models.DateField(default=timezone.now)
    duration = models.CharField(max_length=100)
    
    prod_incharge = models.CharField(max_length=150, blank=True, null=True)
    qa_incharge = models.CharField(max_length=150, blank=True, null=True)
    
    remarks = models.TextField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "deviation_approval"
        ordering = ['-created_at']

    def __str__(self):
        return f"Deviation: {self.tool_name_no} - {self.date}" 
    
class GoodReceiptEntry(models.Model):
    DEPARTMENT_CHOICES = [
        ('QA', 'QA'),
        ('IT', 'IT'),
        ('IOT', 'IOT'),  # <-- IOT added here
        ('PRODUCTION', 'PRODUCTION'),
        ('HR', 'HR'),
        ('MAINTENANCE', 'MAINTENANCE'),
        ('PURCHASE', 'PURCHASE'),
    ]

    requested_by = models.CharField(max_length=255)
    item_name = models.CharField(max_length=255)
    specification = models.CharField(max_length=500, blank=True, null=True)
    department = models.CharField(max_length=50, choices=DEPARTMENT_CHOICES)
    qty = models.CharField(max_length=100) # CharField to support "2 Boxes"
    remark = models.TextField(blank=True, null=True)
    received_by = models.CharField(max_length=255)
    received_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.item_name} - {self.department} ({self.received_date})"


# =====================================================================
#  MACHINE HISTORY & BREAKDOWN REPORTS
# =====================================================================

class MachineHistoryCard(models.Model):
    machine_name = models.CharField(max_length=255)
    machine_no = models.CharField(max_length=100)
    machine_specs = models.CharField(max_length=255, blank=True, null=True)
    location = models.CharField(max_length=255, blank=True, null=True)
    
    # React se aane wale history array ko hum seedha JSON me save kar lenge (Aapke pattern ke hisaab se)
    history_records = models.JSONField(default=list)
    
    prepared_by = models.CharField(max_length=150, blank=True, null=True)
    approved_by = models.CharField(max_length=150, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "machine_history_card"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.machine_no} - {self.machine_name}"


class MachineBreakdownIntimation(models.Model):
    # Breakdown Details
    given_date = models.DateField(blank=True, null=True)
    given_time = models.TimeField(blank=True, null=True)
    machine_name_no = models.CharField(max_length=255)
    breakdown_name = models.CharField(max_length=255)
    part_made_after_inspection = models.CharField(max_length=255)
    breakdown_desc = models.TextField()

    # Maintenance Dept Details
    repair_date = models.DateField(blank=True, null=True)
    repair_time = models.TimeField(blank=True, null=True)
    repair_hours = models.DecimalField(max_digits=6, decimal_places=2, blank=True, null=True)
    mechanics_count = models.IntegerField(blank=True, null=True)
    repair_desc = models.TextField(blank=True, null=True)

    # Quality Verification
    status = models.CharField(max_length=10, default='OK')  # OK or NG
    verification_date = models.DateField(blank=True, null=True)
    verification_time = models.TimeField(blank=True, null=True)

    language = models.CharField(max_length=50, default='english')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "machine_breakdown_intimation"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.machine_name_no} - {self.breakdown_name}"
    
 
class DailyPowerPressChecksheet(models.Model):
    plant = models.CharField(max_length=50)
    operator_name = models.CharField(max_length=100)
    machine_no = models.CharField(max_length=50)
    shift = models.CharField(max_length=20)
    date = models.DateField()
    
    # Saare 10 checkpoints is ek column me JSON ban ke jayenge
    checkpoints = models.JSONField(default=list) 
    
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.machine_no} - {self.date} ({self.shift})"  
    

# =====================================================================
# 🛠️ TOOL HISTORY & BREAKDOWN REPORTS
# =====================================================================
class ToolHistoryReport(models.Model):
    # Header Information (Top Right Date)
    filled_date = models.DateField(blank=True, null=True)

    # Tool Information
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100)
    tool_name = models.CharField(max_length=255)
    model = models.CharField(max_length=255, blank=True, null=True)
    customer_name = models.CharField(max_length=255, blank=True, null=True)
    estimated_tool_life = models.CharField(max_length=100, blank=True, null=True)
    estimated_maintenance_frequency = models.CharField(max_length=100, blank=True, null=True)
    
    # History Record
    date = models.DateField(blank=True, null=True)
    prod = models.CharField(max_length=100, blank=True, null=True)
    resharpening_stroke = models.CharField(max_length=100, blank=True, null=True)
    cumulative_prod = models.CharField(max_length=100, blank=True, null=True)
    problem_reported = models.TextField(blank=True, null=True)
    action_taken = models.TextField(blank=True, null=True)
    updated_in_4m = models.CharField(max_length=10, blank=True, null=True)
    remarks = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tool_history_report"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.tool_name} - Form Filled: {self.filled_date}"
# =====================================================================
# 🛠️ TOOL PREVENTIVE MAINTENANCE REPORT
# =====================================================================
class ToolPreventiveMaintenance(models.Model):
    date = models.DateField(default=timezone.now)
    tool_name = models.CharField(max_length=255)
    part_name = models.CharField(max_length=255, blank=True, null=True)
    part_no = models.CharField(max_length=100, blank=True, null=True)
    operation_no = models.CharField(max_length=100, blank=True, null=True)
    maintenance_person = models.CharField(max_length=150)

    # User ne jitne bhi checkpoints bhare hain (Before, After, Remark) wo sab yahan JSON mein aayenge
    maintenance_data = models.JSONField(default=dict)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tool_prev_maintenance"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.tool_name} - {self.date}"
    
class ToolBreakdownIntimation(models.Model):
    # Header / Document Details
    doc_no = models.CharField(max_length=50, default='AOT-F-BD-01', blank=True, null=True)

    # 1. Production Section (ब्रेकडाउन देने वाले की जानकारी)
    reporter_name = models.CharField(max_length=255)
    report_date = models.DateField(blank=True, null=True)
    machine_name_no = models.CharField(max_length=255)
    report_time = models.TimeField(blank=True, null=True)
    breakdown_details = models.TextField()
    prod_supervisor_name = models.CharField(max_length=255, blank=True, null=True)

    # 2. Maintenance Deptt Details (रखरखाव विभाग की जानकारी)
    maint_date = models.DateField(blank=True, null=True)
    maint_time = models.TimeField(blank=True, null=True)
    time_taken_to_rectify = models.CharField(max_length=100, blank=True, null=True) # CharField रखा है ताकि '2 Hours' जैसा टेक्स्ट भी सेव हो सके
    men_engaged = models.IntegerField(blank=True, null=True)
    action_taken_details = models.TextField(blank=True, null=True)
    maint_incharge_name = models.CharField(max_length=255, blank=True, null=True)

    # 3. Quality Verification (क्वालिटी द्वारा जाँच)
    status = models.CharField(max_length=10, default='OK')  # OK or NG
    qa_date = models.DateField(blank=True, null=True)
    qa_time = models.TimeField(blank=True, null=True)
    nc_verification = models.CharField(max_length=255, blank=True, null=True)
    qa_incharge_name = models.CharField(max_length=255, blank=True, null=True)

    # Extra Metadata
    language = models.CharField(max_length=50, default='hindi')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tool_breakdown_slip"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.machine_name_no} - {self.reporter_name} ({self.report_date})"
    
from datetime import date
from django.db import models
from datetime import date

class DailyProductionPlan(models.Model):
    plant = models.CharField(max_length=50, blank=True, null=True) 
    shift = models.CharField(max_length=50, blank=True, null=True) 
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    operator_name = models.CharField(max_length=150)
    part_name = models.CharField(max_length=150)
    part_no = models.CharField(max_length=100)
    operation_name = models.CharField(max_length=150)
    planned_quantity = models.PositiveIntegerField()
    achieved_quantity = models.PositiveIntegerField(default=0, blank=True, null=True) 
    qty_remark = models.TextField(blank=True, null=True)
    
    production_start_time = models.TimeField(blank=True, null=True)
    production_end_time = models.TimeField(blank=True, null=True)
    total_working_time = models.CharField(max_length=100, blank=True, null=True) 
    tool_setup_time = models.PositiveIntegerField(default=0, blank=True, null=True)
    machine_bd_time = models.PositiveIntegerField(default=0, blank=True, null=True)
    tool_bd_time = models.PositiveIntegerField(default=0, blank=True, null=True)
    rm_coil_no = models.CharField(max_length=150, blank=True, null=True)
   
    plan_date = models.DateField(default=date.today) 
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'daily_production_plan'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.plant} - {self.shift} - {self.part_name}"

# -----------------------------------------------------
# 2. 4M Change Inspection Report Model
# -----------------------------------------------------
class FourMChangeInspection(models.Model):
    # Basic Details
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100)
    operation = models.CharField(max_length=255)

    # Quantities & Specifications
    lot_qty = models.PositiveIntegerField(blank=True, null=True)
    ok_qty = models.PositiveIntegerField(blank=True, null=True)
    rej_qty = models.PositiveIntegerField(blank=True, null=True)
    parameter_specs = models.CharField(max_length=255, blank=True, null=True)

    # Before (Retroactive) Values
    before_1 = models.CharField(max_length=100, blank=True, null=True)
    before_2 = models.CharField(max_length=100, blank=True, null=True)
    before_3 = models.CharField(max_length=100, blank=True, null=True)
    before_4 = models.CharField(max_length=100, blank=True, null=True)
    before_5 = models.CharField(max_length=100, blank=True, null=True)

    # After / Setup Approval Values
    after_1 = models.CharField(max_length=100, blank=True, null=True)
    after_2 = models.CharField(max_length=100, blank=True, null=True)
    after_3 = models.CharField(max_length=100, blank=True, null=True)
    after_4 = models.CharField(max_length=100, blank=True, null=True)
    after_5 = models.CharField(max_length=100, blank=True, null=True)

    # Footer Info
    inspected_by = models.CharField(max_length=150)
    remarks = models.TextField(blank=True, null=True)

    # Meta Fields
    # ✅ YAHAN CHANGE KIYA HAI (timezone.now ki jagah date.today laga diya)
    inspection_date = models.DateField(default=date.today) 
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'four_m_change_inspection'
        ordering = ['-created_at']

    def __str__(self):
        return f"4M Inspection: {self.part_name} | {self.inspection_date}"
    
class FourMChangeRecord(models.Model):
    # --- 4M Status (OK/Change/Not Set) ---
    STATUS_CHOICES = [('OK', 'No Change'), ('CHANGE', 'Change Implemented'), ('NOT_SET', 'Not Set')]
    status_man = models.CharField(max_length=10, choices=STATUS_CHOICES, default='NOT_SET')
    status_machine = models.CharField(max_length=10, choices=STATUS_CHOICES, default='NOT_SET')
    status_material = models.CharField(max_length=10, choices=STATUS_CHOICES, default='NOT_SET')
    status_method = models.CharField(max_length=10, choices=STATUS_CHOICES, default='NOT_SET')

    # --- Change Details ---
    time = models.TimeField()
    machine_no = models.CharField(max_length=50)
    description = models.TextField()
    nature_of_change = models.CharField(max_length=100) # Planned/Unplanned
    action_taken = models.CharField(max_length=255)
    part_info = models.CharField(max_length=255) # Part Name/Number
    operation_no = models.CharField(max_length=100)
    setup_approval = models.CharField(max_length=50) # OK/Not OK
    training_provided = models.CharField(max_length=50)

    # --- Retroactive Section ---
    retro_qty_checked = models.IntegerField(null=True, blank=True)
    # retro_entry_qty = models.IntegerField(null=True, blank=True)
    retro_qty_ok = models.CharField(max_length=20, null=True, blank=True) 
    retro_rw = models.CharField(max_length=20, null=True, blank=True)
    retro_scrap = models.CharField(max_length=20, null=True, blank=True)

    # --- Containment Suspected Section ---
    cont_qty_checked = models.IntegerField(null=True, blank=True)
    # cont_entry_qty = models.IntegerField(null=True, blank=True)
    cont_qty_ok = models.CharField(max_length=20, null=True, blank=True)
    cont_rw = models.CharField(max_length=20, null=True, blank=True)
    cont_scrap = models.CharField(max_length=20, null=True, blank=True)

    # --- Dispatch Detail ---
    customer = models.CharField(max_length=255)
    dispatch_date = models.DateField(default=date.today)
    invoice_no = models.CharField(max_length=100, blank=True, null=True)

    remark = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'four_m_change_records'




###################################
#
#       Prodcution monthly Datatable 
###################################

class MonthlyProductionPlan(models.Model):
    filled_date = models.DateField(null=True, blank=True)
    part_name = models.CharField(max_length=255, blank=True, null=True)
    customer_name = models.CharField(max_length=255, blank=True, null=True)
    opening_stock = models.IntegerField(default=0, null=True, blank=True)
    schedule_qty = models.IntegerField(default=0, null=True, blank=True)
    planned_qty = models.IntegerField(default=0, null=True, blank=True)
    remark = models.TextField(blank=True, null=True)
    prepared_by = models.CharField(max_length=255, blank=True, null=True)
    approved_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = "monthly_production_plan"

    def __str__(self):
        return f"{self.part_name} - {self.filled_date}"

class OperatorObservanceChecklist(models.Model):
    record_date = models.DateField(null=True, blank=True) # filled_date equivalent
    operator_name = models.CharField(max_length=255, blank=True, null=True)
    model = models.CharField(max_length=255, blank=True, null=True)
    part_operation = models.CharField(max_length=255, blank=True, null=True)
    
    # JSONField for storing the evaluation criteria rows
    checkpoints = models.JSONField(default=list, blank=True) 
    
    prepared_by = models.CharField(max_length=255, blank=True, null=True)
    approved_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "operator_observance_checklist"

    def __str__(self):
        return f"Observance: {self.operator_name} - {self.record_date}"

class OperatorObservancePlan(models.Model):
    filled_date = models.DateField(auto_now_add=True) # Automatically tracks when the plan was submitted
    plan_year = models.CharField(max_length=10, blank=True, null=True)
    plan_month = models.CharField(max_length=20, blank=True, null=True)
    
    # JSONField to store multiple operators and their status (Plan vs Actual)
    operators_data = models.JSONField(default=list, blank=True)
    
    prepared_by = models.CharField(max_length=255, blank=True, null=True)
    approved_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "operator_observance_plan"

    def __str__(self):
        return f"Plan: {self.plan_month} {self.plan_year}"

class PMChecklistMHE(models.Model):
    filled_date = models.DateField(null=True, blank=True)
    part_name = models.CharField(max_length=255, blank=True, null=True)
    trolley_no = models.CharField(max_length=255, blank=True, null=True)
    pm_frequency = models.CharField(max_length=100, blank=True, null=True)
    
    # JSONField for saving Sr No 1 to 11 checklist points
    checkpoints = models.JSONField(default=list, blank=True)
    
    checked_by = models.CharField(max_length=255, blank=True, null=True)
    verified_by = models.CharField(max_length=255, blank=True, null=True)
    general_remarks = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pm_checklist_mhe"

    def __str__(self):
        return f"PM MHE: {self.trolley_no} - {self.filled_date}"
    

class ProjectionWelderQual(models.Model):
    # Table Name in DB will be appname_projectionwelderqual
    wps_no = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    welding_process = models.CharField(max_length=100, default="PROJECTION WELDING")
    
    base_metal = models.CharField(max_length=100, blank=True, null=True)
    base_metal_thickness = models.CharField(max_length=100, blank=True, null=True)
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    
    trials = models.JSONField(default=list) # Saves the dynamic table rows
    
    welder_name = models.CharField(max_length=100, blank=True, null=True)
    conducted_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    qualification_status = models.CharField(max_length=50, blank=True, null=True) # Qualified / Not Qualified
    # welder_photo = models.ImageField(upload_to='welder_photos/projection/', blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'projection_welder_qualification'

class SpotWelderQual(models.Model):
    # Table Name in DB will be appname_spotwelderqual
    wps_no = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    welding_process = models.CharField(max_length=100, default="Spot Welding")
    
    base_metal = models.CharField(max_length=100, blank=True, null=True)
    base_metal_thickness = models.CharField(max_length=100, blank=True, null=True)
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    gun_type = models.CharField(max_length=100, blank=True, null=True)
    
    trials = models.JSONField(default=list)
    
    welder_name = models.CharField(max_length=100, blank=True, null=True)
    conducted_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    qualification_status = models.CharField(max_length=50, blank=True, null=True)
    # welder_photo = models.ImageField(upload_to='welder_photos/spot/', blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'spot_welder_qualification'

class TigMigWelderQual(models.Model):
    # Table Name in DB will be appname_tigmigwelderqual
    wps_no = models.CharField(max_length=100, blank=True, null=True)
    testing_date = models.DateField(blank=True, null=True)
    welding_process = models.CharField(max_length=100, blank=True, null=True) # MIG / TIG
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    
    base_metal = models.CharField(max_length=100, blank=True, null=True)
    base_metal_thickness = models.CharField(max_length=100, blank=True, null=True)
    base_metal_size = models.CharField(max_length=100, blank=True, null=True)
    welding_position = models.CharField(max_length=100, blank=True, null=True)
    
    filler_material = models.CharField(max_length=100, blank=True, null=True)
    filler_material_size = models.CharField(max_length=100, blank=True, null=True)
    shielding_gas = models.CharField(max_length=100, blank=True, null=True)
    wire_feed_speed = models.CharField(max_length=100, blank=True, null=True)
    
    trials = models.JSONField(default=list)
    test_results = models.JSONField(default=dict) # To save visual, defect, strength bend tests
    
    welder_name = models.CharField(max_length=100, blank=True, null=True)
    conducted_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    qualification_status = models.CharField(max_length=50, blank=True, null=True)
    # welder_photo = models.ImageField(upload_to='welder_photos/tig_mig/', blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'tig_mig_welder_qualification'

class ProcessValidation(models.Model):
    # Table Name in DB will be appname_processvalidation
    validation_date = models.DateField(blank=True, null=True)
    revalidation_date = models.DateField(blank=True, null=True)
    process_name = models.CharField(max_length=200, blank=True, null=True)
    material_details = models.CharField(max_length=200, blank=True, null=True)
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    process_owner = models.CharField(max_length=100, blank=True, null=True)
    part_name = models.CharField(max_length=100, blank=True, null=True)
    fixture_no = models.CharField(max_length=100, blank=True, null=True)
    
    operators = models.JSONField(default=list) # Array of operator names
    parameters = models.JSONField(default=list) # P1 to P8 spec table
    trials = models.JSONField(default=list) # Big trials table
    final_params = models.JSONField(default=list) # Final selected params table
    
    conclusion = models.TextField(blank=True, null=True)
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'manufacturing_process_validation'



###################################
#
#       QA monthly Datatable 
###################################

class ProcessAuditChecksheet(models.Model):
    part_name_no = models.CharField(max_length=200, blank=True, null=True)
    machine_model = models.CharField(max_length=200, blank=True, null=True) 
    date = models.DateField(blank=True, null=True)
    auditor = models.CharField(max_length=100, blank=True, null=True)
    auditee = models.CharField(max_length=100, blank=True, null=True)
    
    # Ye naya field pura data (parameter, spec, observation, remark) ek sath save karega
    audit_details = models.JSONField(default=list, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'process_audit_checksheet'

class CoherenceChecklist(models.Model):
    part_name = models.CharField(max_length=200, blank=True, null=True)
    part_no = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    model_name = models.CharField(max_length=100, blank=True, null=True)
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    
    operations = models.JSONField(default=list) # Array of operations
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_coherence_checklist'


class LayoutInspection(models.Model):
    part_name = models.CharField(max_length=200, blank=True, null=True)
    part_no = models.CharField(max_length=100, blank=True, null=True)
    model_name = models.CharField(max_length=100, blank=True, null=True)
    customer_name = models.CharField(max_length=200, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    sample_size = models.CharField(max_length=50, blank=True, null=True)
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    
    inspections = models.JSONField(default=list) # Array of inspection parameters
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_layout_inspection'


class ProductAuditPlan(models.Model):
    doc_no = models.CharField(max_length=100, blank=True, null=True)
    rev_no = models.CharField(max_length=50, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    plan_year = models.CharField(max_length=50, blank=True, null=True)
    
    # Naye fields add kiye gaye hain
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)
    
    audit_rows = models.JSONField(default=list) # Array of audit schedule grids
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'monthly_product_audit_plan'

class CustomerComplaint(models.Model):
    date = models.DateField(blank=True, null=True)
    part_details = models.CharField(max_length=200, blank=True, null=True)
    model_name = models.CharField(max_length=100, blank=True, null=True)
    customer_name = models.CharField(max_length=200, blank=True, null=True)
    problem_description = models.TextField(blank=True, null=True)
    counter_measure = models.TextField(blank=True, null=True)
    target_date = models.DateField(blank=True, null=True)
    horizontal_action = models.CharField(max_length=200, blank=True, null=True)
    status = models.CharField(max_length=50, default='OPEN')
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_customer_complaint'


class CustomerSatisfaction(models.Model):
    customer_name = models.CharField(max_length=255, blank=True, null=True)
    month_year = models.CharField(max_length=50, blank=True, null=True)
    
    # Storing all 8 Performance Indicators as a JSON Object
    performance_indicators = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_customer_satisfaction'


class WarrantyClaim(models.Model):
    date = models.DateField(blank=True, null=True)
    customer_name = models.CharField(max_length=255, blank=True, null=True)
    part_details = models.CharField(max_length=255, blank=True, null=True)
    claim_qty = models.CharField(max_length=50, blank=True, null=True)
    warranty_defect = models.TextField(blank=True, null=True)
    decision = models.CharField(max_length=50, blank=True, null=True)
    rejection_root_cause = models.TextField(blank=True, null=True)
    disposal_action = models.CharField(max_length=255, blank=True, null=True)
    capa_analysis = models.TextField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_warranty_claim'


class MinutesOfMeeting(models.Model):
    date = models.DateField(blank=True, null=True)
    time = models.TimeField(blank=True, null=True)
    subject = models.CharField(max_length=255, blank=True, null=True)
    aot_members = models.TextField(blank=True, null=True)
    supplier_members = models.TextField(blank=True, null=True)
    
    # Storing SR. NO, PART NAME, DEFECTS, ACTION PLAN, etc., as an Array of JSON objects
    discussions = models.JSONField(default=list) 
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_minutes_of_meeting'


# ###################################
# #
# # Machine Maintenance monthly Datatable 
# ###################################

class MachineBreakdown(models.Model):
    date = models.DateField(blank=True, null=True)
    machine_type_no = models.CharField(max_length=255, blank=True, null=True)
    
    # Store Problem, Time, Status, 4M, Sign, Remarks in this JSON field
    details = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_machine_breakdownsummary'


#  MACHINE Critical Spare Model
class MachineCriticalSpare(models.Model):
    date = models.DateField(blank=True, null=True)
    spare_description = models.CharField(max_length=255, blank=True, null=True)
    model_description = models.CharField(max_length=255, blank=True, null=True)
    box_location = models.CharField(max_length=255, blank=True, null=True)
    prepared_by = models.CharField(max_length=150, blank=True, null=True)
    approved_by = models.CharField(max_length=150, blank=True, null=True)
    
    # JSON field for Spare Type, Levels, UOM, Lead Time, Status, etc.
    spare_details = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_machine_critical_spare'

class ToolBreakdown(models.Model):
    date = models.DateField(blank=True, null=True)
    tool_name = models.CharField(max_length=255, blank=True, null=True)
    
    # Store Process, Problem, Action, Time, CheckedBy, HistoryCard, 4M, Sign, Remarks here
    details = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_tool_breakdownsummary'
        
#  TOOL Critical Spare Model
class ToolCriticalSpare(models.Model):
    date = models.DateField(blank=True, null=True)
    spare_description = models.CharField(max_length=255, blank=True, null=True)
    model_description = models.CharField(max_length=255, blank=True, null=True)
    box_location = models.CharField(max_length=255, blank=True, null=True)
    
    # JSON field for Spare Type, UOM, Opening Stock, Min Level, Lead Time
    spare_details = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_tool_critical_spare'





class ReportTrackHistory(models.Model):
    username = models.CharField(max_length=150)       
    department = models.CharField(max_length=100)     
    report_name = models.CharField(max_length=150)    
    action_time = models.DateTimeField(default=now)   

    def __str__(self):
        return f"{self.username} submitted {self.report_name} at {self.action_time}"
    




class MachineEventLog(models.Model):
    plant_no = models.IntegerField(help_text="1 for Plant 1, 2 for Plant 2")
    machine_no = models.CharField(max_length=10)
    event_type = models.CharField(max_length=50)  # Seedha backend se aayega (ON, OFF, etc.)
    timestamp = models.DateTimeField()
    shift = models.CharField(max_length=5)
    details = models.TextField(blank=True, null=True)

    class Meta:
        # Table ka exact naam jo DB mein banega
        db_table = 'Machine_Event_Logs' 
        
        # Super fast search/filter ke liye Index (Bohot zaroori)
        indexes = [
            models.Index(fields=['plant_no', 'machine_no', 'timestamp']),
        ]
        
        # By default latest event sabse upar aayega
        ordering = ['-timestamp']

    def __str__(self):
        return f"P{self.plant_no}-M{self.machine_no} | {self.event_type} | {self.timestamp}"
    

class HourlyDowntimeLog(models.Model):
    
    STATUS_CHOICES = [
        ('ONLINE', 'Machine is Online (But Idle)'),
        ('OFFLINE', 'Machine is Offline (No Signal)'),
    ]

    timestamp = models.DateTimeField()
    machine_no = models.CharField(max_length=10)
    idle_time = models.IntegerField(default=0)
    shift = models.CharField(max_length=5)
    
    # Ye column saaf bata dega machine Online thi ya Offline
    machine_status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='ONLINE')
    
    reason_category = models.CharField(max_length=100, default='Uncategorized')
    specific_reason = models.CharField(
        max_length=255, 
        default='Supervisor not save the information'
    )
    
    class Meta:
        db_table = 'hourly_downtime_logs'
        indexes = [
            models.Index(fields=['timestamp', 'machine_no']),
            models.Index(fields=['machine_no', 'timestamp']),
        ]
        ordering = ['-timestamp', 'machine_no']
    
    def __str__(self):
        return f"M{self.machine_no} - {self.timestamp.strftime('%H:%M')} - Status: {self.machine_status} - {self.idle_time}m"




from django.db import models
from django.contrib.auth.models import User  

class Notification(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='notifications')
    machine_no = models.CharField(max_length=50, null=True, blank=True)
    message = models.TextField()
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"Notification for {self.user.username} - {self.machine_no}"
    
    
    
    
##############################################
# machine maintenance weekly report model 
##############################################


class TigWeldingMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the 12 Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tig_welding_maintenance" # Database table name

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"   

class SpotWeldingMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='SPOT WELDING M/C', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "spot_welding_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class CompressorMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Compressor', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "compressor_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class LatheMachineMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='LATHE MACHINE', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "lathe_machine_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class VerticalDrillMachineMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='VERTICAL DRILL MACHINE', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "vertical_drill_machine_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class SurfaceGrinderMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='SURFACE GRINDER', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "surface_grinder_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"
    
class BaseGrinderMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Base Grinder', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "base_grinder_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class BeltGrinderMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='BELT GRINDER', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "belt_grinder_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


# class TappingMaintenance(models.Model):
#     # Meta Data Fields
#     machine_name = models.CharField(max_length=100, default='Tapping Machine', blank=True, null=True)
#     date = models.DateField()
#     machine_no = models.CharField(max_length=50, blank=True, null=True)
#     location = models.CharField(max_length=100, blank=True, null=True)
#     specification = models.CharField(max_length=100, blank=True, null=True)
#     maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
#     # JSON Field for the Checkpoints (tableData)
#     checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
#     # Signatures
#     prepared_by = models.CharField(max_length=100, blank=True, null=True)
#     checked_by = models.CharField(max_length=100, blank=True, null=True)
    
#     # Auto Timestamps
#     created_at = models.DateTimeField(auto_now_add=True)

#     class Meta:
#         managed = True
#         db_table = "tapping_maintenance"

#     def __str__(self):
#         return f"{self.machine_name} - {self.machine_no} ({self.date})"


class PipeCuttingMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Pipe Cutter', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "pipe_cutting_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class VibraMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Vibra', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "vibra_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class DipMoldingMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Dip Molding Machine', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "dip_molding_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class ServoPressMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Servo Press', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "servo_press_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"
    
class MachinePreventiveMaintenance(models.Model):
    machine_name = models.CharField(max_length=150)
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    checkpoints = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'machine_preventive_maintenance'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.machine_no} - {self.date}"
    
class CNCMaintenanceReport(models.Model):
    machine_name = models.CharField(max_length=150)
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    checklist = models.JSONField(default=list, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "cnc_maintenance_report"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"

class VerticalMillingMachineCheckSheet(models.Model):
    machine_name = models.CharField(max_length=150, default="VERTICAL MILLING MACHINE")
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    checkpoints = models.JSONField(default=list, blank=True)
    prepared_by = models.CharField(max_length=150)
    checked_by = models.CharField(max_length=150, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "vertical_milling_machine_checksheet"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"
class ProjectionWeldingPMCheckSheet(models.Model):
    machine_name = models.CharField(max_length=150, default="PROJECTION WELDING")
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    checkpoints = models.JSONField(default=list, blank=True)
    prepared_by = models.CharField(max_length=150)
    checked_by = models.CharField(max_length=150, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "projection_welding_pm_checksheet"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"
class PowerPressPMCheckSheet(models.Model):
    machine_name = models.CharField(max_length=150, default="POWER PRESS")
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    checkpoints = models.JSONField(default=list, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "power_press_pm_checksheet"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"
    
class HydraulicPMCheckSheet(models.Model):
    machine_name = models.CharField(max_length=150, default="HYDRAULIC MACHINE")
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    
    # Store all 7 checklist items inside this JSON structure
    checkpoints = models.JSONField(default=list, blank=True)
    
    prepared_by = models.CharField(max_length=150)
    checked_by = models.CharField(max_length=150)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "hydraulic_pm_checksheet"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"


class PartMaster(models.Model):
    customer_name = models.CharField(max_length=255)
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=255, null=True, blank=True)
    # Python me 'model' keyword hota hai, isliye column ka naam 'part_model' rakha hai
    part_model = models.CharField(max_length=255, null=True, blank=True) 
    inspection_data = models.JSONField()

    class Meta:
        # Yahan aap apna custom table name set kar sakte hain
        db_table = 'master_data_incoming_material_inspection'

    def __str__(self):
        return f"{self.customer_name} - {self.part_name}"
    
class FourMDisplay(models.Model):
    # s_no can be helpful to keep track of the row order submitted from frontend
    s_no = models.IntegerField(blank=True, null=True) 
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    operator_name = models.CharField(max_length=100, blank=True, null=True)
    
    # 4M Details
    man = models.CharField(max_length=255, blank=True, null=True)
    machine = models.CharField(max_length=255, blank=True, null=True)
    material = models.CharField(max_length=255, blank=True, null=True)
    method = models.CharField(max_length=255, blank=True, null=True)
    
    # Tracking Dates (As requested)
    date_filled = models.DateField(auto_now_add=True)      # Only the date (e.g., 2026-06-06)
    created_at = models.DateTimeField(auto_now_add=True)   # Date AND exact time

    class Meta:
        db_table = 'four_m_display_board'

    def __str__(self):
        return f"{self.machine_no} - {self.operator_name} ({self.date_filled})"
    
class FourMSummary(models.Model):
    # Header Info (Prepared & Approved By)
    
    
    # Basic Row Details
    s_no = models.IntegerField(blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    part_name_no = models.CharField(max_length=255, blank=True, null=True)
    type_of_change = models.CharField(max_length=255, blank=True, null=True)
    change_detail = models.TextField(blank=True, null=True)
    
    # Retroactive Inspection Status
    retro_total_qty = models.IntegerField(blank=True, null=True)
    retro_ok_qty = models.IntegerField(blank=True, null=True)
    retro_rej_qty = models.IntegerField(blank=True, null=True)
    
    # Actions, Customers & Signatures
    status_after_final = models.CharField(max_length=255, blank=True, null=True)
    action_for_ng = models.CharField(max_length=255, blank=True, null=True)
    customer = models.CharField(max_length=255, blank=True, null=True)
    sup_signature = models.CharField(max_length=255, blank=True, null=True)
    sign_prod_head = models.CharField(max_length=255, blank=True, null=True)
    sign_qa_head = models.CharField(max_length=255, blank=True, null=True)
    remarks = models.TextField(blank=True, null=True)
    # Header Info (Prepared & Approved By)
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)
    # Tracking Dates
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'four_m_summary_sheet'

    def __str__(self):
        return f"{self.part_name_no} - {self.date}"
    
    
class FixtureMaintenanceRecord(models.Model):
    
    # 1. Basic Top Level Fields
    part_name = models.CharField(max_length=255, help_text="Assembly / Part Name")
    part_no = models.CharField(max_length=100, blank=True, null=True, help_text="Part Number")
    done_on_date = models.DateField()
    fixture_no = models.CharField(max_length=100)
    operation_name = models.CharField(max_length=255)

    # 2. Checklist Data (JSON Format)
     
    checklist_data = models.JSONField(default=list, help_text="Stores the 8 checklist points data")

    # 3. Technical Chart Data (JSON Format)
    
    pin_chart_data = models.JSONField(default=list, help_text="Stores 12 rows of Pin data")
    bush_chart_data = models.JSONField(default=list, help_text="Stores 12 rows of Bush data")

    # 4. Inspected By (Alag se)
    inspected_by = models.CharField(max_length=255, help_text="Engineer Sign / Name")

    # Audit Trail (Kab create hua)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.fixture_no} - {self.part_name} on {self.done_on_date}"

    class Meta:
        db_table = 'fixture_maintenance_records'
        verbose_name = 'Fixture Maintenance Record'
        verbose_name_plural = 'Fixture Maintenance Records'
        
class IncomingMaterialInspection(models.Model):
    # --- Header Information ---
    supplier = models.CharField(max_length=255, default="ATOMONE TECHNOLOGIES PVT.LTD")
    customer = models.CharField(max_length=255)
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    
    # --- Material Details ---
    grade = models.CharField(max_length=100, blank=True, null=True)
    mtc = models.CharField(max_length=10, blank=True, null=True)     # Options: YES, NO, N/A
    ga_nga = models.CharField(max_length=10, blank=True, null=True)  # Options: GA, NGA, N/A
    coil_no = models.CharField(max_length=100, blank=True, null=True)
    invoice_no = models.CharField(max_length=100, blank=True, null=True)
    qty = models.CharField(max_length=50, blank=True, null=True)     # CharField in case of unit inclusion (e.g. '50 kgs')

    # --- Inspection Parameters (Table Rows in JSON) ---
    inspection_data = models.JSONField(
        default=list, 
        help_text="Stores array of objects containing parameter, specification, inspMethod, observations array, and remark"
    )

    # --- Authorization ---
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)

    # --- Timestamps ---
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "incoming_material_inspection_report"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.supplier} | {self.part_name} ({self.part_no}) - {self.date}"
    
    
    
    
# from django.db import models
# from django.contrib.auth.models import User

# # 1. User Profile: Kaunsa user kis Department/Plant ka hai
# class UserProfile(models.Model):
#     user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    
#     # Department / Plant ke options
#     DEPARTMENT_CHOICES = (
#         ('Plant 1', 'Plant 1'),
#         ('Plant 2', 'Plant 2'),
#         ('QA Hub', 'QA Hub'),
#         ('Production Hub', 'Production Hub'),
#         ('Maintenance Hub', 'Maintenance Hub'),
#     )
#     department_name = models.CharField(max_length=50, choices=DEPARTMENT_CHOICES, default='Plant 1')

#     class Meta:
#         # 🔥 Puraani default table ki jagah ye naam DB mein aayega
#         db_table = 'user_department_profiles' 

#     def __str__(self):
#         return f"{self.user.username} - {self.department_name}"

from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver

# ==============================================================================
# 🏭 ENTERPRISE USER PROFILE MASTER
# ==============================================================================
class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    
    # 📌 Nayi Fields Jo Aapne Maangi Thi
    employee_id = models.CharField(max_length=50, unique=True, null=True, blank=True, help_text="Unique Employee Code")
    mobile_no = models.CharField(max_length=15, null=True, blank=True)
    contact_email = models.EmailField(null=True, blank=True, help_text="Alternate/Direct Email")
    
    # 📌 Location (Konsa Plant Hai)
    LOCATION_CHOICES = (
        ('Plant 1', 'Plant 1'),
        ('Plant 2', 'Plant 2'),
        ('HQ', 'Headquarters'),
    )
    location = models.CharField(max_length=50, choices=LOCATION_CHOICES, default='Plant 1')

    # 📌 Department
    DEPARTMENT_CHOICES = (
        ('QA', 'Quality Assurance (QA)'),
        ('Production', 'Production'),
        ('Maintenance', 'Maintenance'),
        ('Management', 'Management'),
    )
    department = models.CharField(max_length=50, choices=DEPARTMENT_CHOICES, default='QA')

    class Meta:
        db_table = 'user_department_profiles' 

    def __str__(self):
        emp_code = self.employee_id if self.employee_id else "NO-ID"
        return f"{self.user.username} | ID: {emp_code} | {self.location} - {self.department}"
    

from django.db import models
from django.contrib.auth.models import User


class ReportActivityLog(models.Model):
    username = models.CharField(max_length=255)
    department_name = models.CharField(max_length=100) 
    report_name = models.CharField(max_length=255)
    record_id = models.IntegerField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=255, default="In Progress") 
    approved_or_rejected_at = models.CharField(max_length=19, null=True, blank=True)
    remarks = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'user_report_activity_logs'

    def __str__(self):
        return f"{self.username} | {self.report_name} | Status: {self.status}"


# ==========================================
# 3. 🔥 NAYA MODEL: SIRF QA HUB NOTIFICATIONS KE LIYE
# ==========================================
class QANotification(models.Model):
    # Jisko notification bhejna hai (e.g., rajeshdhiman)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='qa_notifications')
    
    # Kis report ke baare mein hai ye notification
    report_log = models.ForeignKey(ReportActivityLog, on_delete=models.CASCADE)
    
    message = models.TextField()
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_hub_notifications' # 🔥 Table ka naam clear rakha hai
        ordering = ['-created_at']

    def __str__(self):
        return f"QA Alert for {self.user.username}: {self.report_log.report_name}"


from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver

# ==============================================================================
# 🏭 ENTERPRISE USER PROFILE MASTER
# ==============================================================================
class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    
    # 📌 Nayi Fields Jo Aapne Maangi Thi
    employee_id = models.CharField(max_length=50, unique=True, null=True, blank=True, help_text="Unique Employee Code")
    mobile_no = models.CharField(max_length=15, null=True, blank=True)
    contact_email = models.EmailField(null=True, blank=True, help_text="Alternate/Direct Email")
    full_name = models.CharField(max_length=100, null=True, blank=True, help_text="Full Name of the User")
    designation = models.CharField(max_length=100, null=True, blank=True, help_text="Designation of the User")
    profile_image = models.ImageField(upload_to="profile_images/", blank=True, null=True)
    # 📌 Location (Konsa Plant Hai)
    LOCATION_CHOICES = (
        ('Plant 1', 'Plant 1'),
        ('Plant 2', 'Plant 2'),
        ('HQ', 'Headquarters'),
    )
    location = models.CharField(max_length=50, choices=LOCATION_CHOICES, default='Plant 1')

    # 📌 Department
    DEPARTMENT_CHOICES = (
        ('QA', 'Quality Assurance (QA)'),
        ('Production', 'Production'),
        ('Maintenance', 'Maintenance'),
        ('Management', 'Management'),
    )
    department = models.CharField(max_length=50, choices=DEPARTMENT_CHOICES, default='QA')

    class Meta:
        db_table = "user_department_profiles"

    def __str__(self):
        emp_code = self.employee_id if self.employee_id else "NO-ID"
        return f"{self.user.username} | ID: {emp_code} | {self.location} - {self.department}"
    

class FourMInformationSheet(models.Model):

    # Tracking row order from frontend

    s_no = models.IntegerField(blank=True, null=True)

    # Specifically added for the Information Sheet form

    time = models.TimeField(blank=True, null=True) 

    # Basic Machine & Operator Details

    machine_no = models.CharField(max_length=100, blank=True, null=True)
    operator_name = models.CharField(max_length=100, blank=True, null=True)


    # 4M Details (Expects "Change" or "No Change")

    man = models.CharField(max_length=50, blank=True, null=True)
    machine = models.CharField(max_length=50, blank=True, null=True)
    material = models.CharField(max_length=50, blank=True, null=True)
    method = models.CharField(max_length=50, blank=True, null=True)

    # Description of the change

    change_description = models.TextField(blank=True, null=True)

    # Person who prepared/submitted the report

    prepared_by = models.CharField(max_length=100, blank=True, null=True)

    # Tracking Dates

    date_filled = models.DateField(auto_now_add=True)      # Only the date

    created_at = models.DateTimeField(auto_now_add=True)   # Date AND exact time

    class Meta:

        db_table = 'four_m_information_sheet'

    def __str__(self):

        return f"{self.machine_no} - {self.operator_name} ({self.date_filled})"
    
from django.db import models
from django.utils import timezone
from django.utils.timezone import now

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
        managed = False
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
    model_name = models.CharField(max_length=50, blank=True, null=True)

    plant_location = models.CharField(max_length=100)
    inspection_date = models.DateField()
    operator_name = models.CharField(max_length=100)
    machine_number = models.CharField(max_length=50)
    
    inspection_data = models.JSONField() 
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True 
        db_table = 'inspection_reports'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.customer_account} - {self.part_name} - {self.inspection_date}"
class InspectionItem(models.Model):
    report = models.ForeignKey(InspectionReport, on_delete=models.CASCADE, related_name='items')
    sr_no = models.IntegerField(null=True, blank=True)
    item = models.CharField(max_length=200, null=True, blank=True)
    special_char = models.CharField(max_length=100, blank=True, null=True)
    spec = models.CharField(max_length=100, null=True, blank=True)
    tolerance = models.CharField(max_length=50, null=True, blank=True)
    inst = models.CharField(max_length=100, null=True, blank=True)

    class Meta:
        managed = False  # Connects to your existing DB table
        db_table = 'inspection_items'


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
        
# =====================================================================
# 🛠️ 6. TIP CHANGE & DRESSING MONITORING (React Form Model)
# =====================================================================

class TipChangeDressing(models.Model):
    # Nayi Fields
    plant = models.CharField(max_length=50, blank=True, null=True)
    machine_name = models.CharField(max_length=50, blank=True, null=True)
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    
    # Form wali Fields
    part_name = models.CharField(max_length=100)
    operation = models.CharField(max_length=150, blank=True, null=True)
    prd_qty = models.IntegerField()
    tip_change = models.CharField(max_length=10) 
    
    # ✅ NAYA: Time aur Date track karne ke liye (Auto-save)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    
    # Purana sirf date wala field (agar aapko alag se chahiye toh, warna created_at me dono aa jate hain)
    date = models.DateField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tip_change_monitoring"

    def __str__(self):
        return f"{self.machine_name} ({self.machine_no}) - {self.part_name}"
    
    

class PushSubscription(models.Model):
    endpoint = models.URLField(max_length=500, unique=True)
    auth = models.CharField(max_length=100)
    p256dh = models.CharField(max_length=100)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Push Subscription {self.id}"
# =====================================================================
#  6. 5S CHECKSHEET REPORT 
# =====================================================================

class FiveSChecksheetReport(models.Model):
    area = models.CharField(max_length=100, default="P.Shop & Parking area")
    zone_leader = models.CharField(max_length=100)
    date = models.DateField(default=timezone.now)
    language = models.CharField(max_length=10)
    
    # Summary stats
    total_checks = models.IntegerField(default=0)
    ok_count = models.IntegerField(default=0)
    ng_count = models.IntegerField(default=0)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "five_s_checksheet_report"

    def __str__(self):
        return f"{self.zone_leader} - {self.area} - {self.date}"


class FiveSChecksheetObservation(models.Model):
    report = models.ForeignKey(FiveSChecksheetReport, on_delete=models.CASCADE, related_name='observations')
    s_category = models.CharField(max_length=10)  # e.g., "1'S'", "2'S'"
    check_point = models.TextField()
    status = models.CharField(max_length=20)  # "OK", "NG", "Not Checked"

    class Meta:
        managed = True
        db_table = "five_s_observation"
# =====================================================================
#  BinTrolley REPORT 
# =====================================================================
class BinTrolleyReport(models.Model):
    # Basic Info
    date = models.DateField(default=timezone.now)
    week = models.CharField(max_length=10)  # e.g., 'W1'
    month = models.CharField(max_length=20) # e.g., 'Apr'
    
    # 🌟 Saara data in 3 columns mein aayega JSON ban kar
    checkpoints = models.JSONField(default=dict, blank=True, null=True)
    cleaning_details = models.JSONField(default=dict, blank=True, null=True)
    maintenance_details = models.JSONField(default=dict, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "bin_trolley_report"
        ordering = ['-date', '-created_at']

    def __str__(self):
        return f"{self.date} - {self.week} ({self.month})"
    

# =====================================================================
# RED BIN ANALYSIS REPORT (RedBinForm.js)
# =====================================================================
from django.db import models
from django.utils import timezone

class RedBinAnalysisReport(models.Model):
    entry_date = models.DateField(default=timezone.now) 
    part_name_model = models.CharField(max_length=255)
    operation = models.CharField(max_length=255)
    total_rej_qty = models.PositiveIntegerField()
    
    defect_detail = models.TextField()
    root_cause_reason = models.TextField()
    action_taken = models.TextField()
    
    responsible_person = models.CharField(max_length=150)
    target_date = models.DateField()
    completion_date = models.DateField(null=True, blank=True)
    
    created_time = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        managed = True
        db_table = "red_bin_analysis_report"
# =====================================================================
#  RED BIN ATTENDANCE (RedbinAttendance.js) - SINGLE TABLE
# =====================================================================
class RedBinAttendance(models.Model):
    # JS choices se match karne ke liye
    ATTENDANCE_CHOICES = [
        ('P', 'Present'),
        ('A', 'Absent'),
        ('', 'Unmarked'),
    ]

    date = models.DateField(default=timezone.now)
    month = models.CharField(max_length=20)
    year = models.IntegerField()
    
    employee_name = models.CharField(max_length=150)
    designation = models.CharField(max_length=100)
    
    # Ab choices yahan apply kar di hain
    status = models.CharField(
        max_length=1, 
        choices=ATTENDANCE_CHOICES, 
        default='', 
        blank=True
    )
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "red_bin_attendance"
        unique_together = ['date', 'employee_name']

# =====================================================================
# SCRAP NOTE ENTRY (ScrapNoteForm.js)
# =====================================================================
class ScrapNoteEntry(models.Model):
    entry_date = models.DateField(default=timezone.now)
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100)
    
    defect_detail = models.CharField(max_length=255)
    quantity = models.PositiveIntegerField()
    
    remarks = models.TextField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "scrap_note_entry"

class ReworkEntry(models.Model):
    # Common Data (Har row ke sath save hoga)
    date = models.DateField(default=timezone.now)
    remark = models.TextField(blank=True, null=True)
    
    # Standard Columns (Table mein dikhne wale main data)
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100)
    spec = models.CharField(max_length=255)
    non_conformance = models.CharField(max_length=255)
    rework_qty = models.PositiveIntegerField(default=0)
    inspected_by = models.CharField(max_length=150, blank=True, null=True)
    
    # 🔥 JSONField: Isme status ('ok'/'notok') aur observations ka array jayega
    dynamic_details = models.JSONField(default=dict, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "rework_entry"
        ordering = ['-date', '-created_at']

    def __str__(self):
        return f"{self.date} | {self.part_name} - Qty: {self.rework_qty}"
    

class DeviationApproval(models.Model):
    tool_name_no = models.CharField(max_length=255, verbose_name="Tool Name/No.")
    location = models.CharField(max_length=255)
    problem = models.TextField()
    reason_for_deviation = models.TextField()
    
    date = models.DateField(default=timezone.now)
    duration = models.CharField(max_length=100)
    
    prod_incharge = models.CharField(max_length=150, blank=True, null=True)
    qa_incharge = models.CharField(max_length=150, blank=True, null=True)
    
    remarks = models.TextField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "deviation_approval"
        ordering = ['-created_at']

    def __str__(self):
        return f"Deviation: {self.tool_name_no} - {self.date}" 
    
class GoodReceiptEntry(models.Model):
    DEPARTMENT_CHOICES = [
        ('QA', 'QA'),
        ('IT', 'IT'),
        ('IOT', 'IOT'),  # <-- IOT added here
        ('PRODUCTION', 'PRODUCTION'),
        ('HR', 'HR'),
        ('MAINTENANCE', 'MAINTENANCE'),
        ('PURCHASE', 'PURCHASE'),
    ]

    requested_by = models.CharField(max_length=255)
    item_name = models.CharField(max_length=255)
    specification = models.CharField(max_length=500, blank=True, null=True)
    department = models.CharField(max_length=50, choices=DEPARTMENT_CHOICES)
    qty = models.CharField(max_length=100) # CharField to support "2 Boxes"
    remark = models.TextField(blank=True, null=True)
    received_by = models.CharField(max_length=255)
    received_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.item_name} - {self.department} ({self.received_date})"


# =====================================================================
#  MACHINE HISTORY & BREAKDOWN REPORTS
# =====================================================================

class MachineHistoryCard(models.Model):
    machine_name = models.CharField(max_length=255)
    machine_no = models.CharField(max_length=100)
    machine_specs = models.CharField(max_length=255, blank=True, null=True)
    location = models.CharField(max_length=255, blank=True, null=True)
    
    # React se aane wale history array ko hum seedha JSON me save kar lenge (Aapke pattern ke hisaab se)
    history_records = models.JSONField(default=list)
    
    prepared_by = models.CharField(max_length=150, blank=True, null=True)
    approved_by = models.CharField(max_length=150, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "machine_history_card"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.machine_no} - {self.machine_name}"


class MachineBreakdownIntimation(models.Model):
    # Breakdown Details
    given_date = models.DateField(blank=True, null=True)
    given_time = models.TimeField(blank=True, null=True)
    machine_name_no = models.CharField(max_length=255)
    breakdown_name = models.CharField(max_length=255)
    part_made_after_inspection = models.CharField(max_length=255)
    breakdown_desc = models.TextField()

    # Maintenance Dept Details
    repair_date = models.DateField(blank=True, null=True)
    repair_time = models.TimeField(blank=True, null=True)
    repair_hours = models.DecimalField(max_digits=6, decimal_places=2, blank=True, null=True)
    mechanics_count = models.IntegerField(blank=True, null=True)
    repair_desc = models.TextField(blank=True, null=True)

    # Quality Verification
    status = models.CharField(max_length=10, default='OK')  # OK or NG
    verification_date = models.DateField(blank=True, null=True)
    verification_time = models.TimeField(blank=True, null=True)

    language = models.CharField(max_length=50, default='english')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "machine_breakdown_intimation"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.machine_name_no} - {self.breakdown_name}"
    
 
class DailyPowerPressChecksheet(models.Model):
    plant = models.CharField(max_length=50)
    operator_name = models.CharField(max_length=100)
    machine_no = models.CharField(max_length=50)
    shift = models.CharField(max_length=20)
    date = models.DateField()
    
    # Saare 10 checkpoints is ek column me JSON ban ke jayenge
    checkpoints = models.JSONField(default=list) 
    
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.machine_no} - {self.date} ({self.shift})"  
    

# =====================================================================
# 🛠️ TOOL HISTORY & BREAKDOWN REPORTS
# =====================================================================
class ToolHistoryReport(models.Model):
    # Header Information (Top Right Date)
    filled_date = models.DateField(blank=True, null=True)

    # Tool Information
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100)
    tool_name = models.CharField(max_length=255)
    model = models.CharField(max_length=255, blank=True, null=True)
    customer_name = models.CharField(max_length=255, blank=True, null=True)
    estimated_tool_life = models.CharField(max_length=100, blank=True, null=True)
    estimated_maintenance_frequency = models.CharField(max_length=100, blank=True, null=True)
    
    # History Record
    date = models.DateField(blank=True, null=True)
    prod = models.CharField(max_length=100, blank=True, null=True)
    resharpening_stroke = models.CharField(max_length=100, blank=True, null=True)
    cumulative_prod = models.CharField(max_length=100, blank=True, null=True)
    problem_reported = models.TextField(blank=True, null=True)
    action_taken = models.TextField(blank=True, null=True)
    updated_in_4m = models.CharField(max_length=10, blank=True, null=True)
    remarks = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tool_history_report"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.tool_name} - Form Filled: {self.filled_date}"
# =====================================================================
# 🛠️ TOOL PREVENTIVE MAINTENANCE REPORT
# =====================================================================
class ToolPreventiveMaintenance(models.Model):
    date = models.DateField(default=timezone.now)
    tool_name = models.CharField(max_length=255)
    part_name = models.CharField(max_length=255, blank=True, null=True)
    part_no = models.CharField(max_length=100, blank=True, null=True)
    operation_no = models.CharField(max_length=100, blank=True, null=True)
    maintenance_person = models.CharField(max_length=150)

    # User ne jitne bhi checkpoints bhare hain (Before, After, Remark) wo sab yahan JSON mein aayenge
    maintenance_data = models.JSONField(default=dict)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tool_prev_maintenance"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.tool_name} - {self.date}"
    
class ToolBreakdownIntimation(models.Model):
    # Header / Document Details
    doc_no = models.CharField(max_length=50, default='AOT-F-BD-01', blank=True, null=True)

    # 1. Production Section (ब्रेकडाउन देने वाले की जानकारी)
    reporter_name = models.CharField(max_length=255)
    report_date = models.DateField(blank=True, null=True)
    machine_name_no = models.CharField(max_length=255)
    report_time = models.TimeField(blank=True, null=True)
    breakdown_details = models.TextField()
    prod_supervisor_name = models.CharField(max_length=255, blank=True, null=True)

    # 2. Maintenance Deptt Details (रखरखाव विभाग की जानकारी)
    maint_date = models.DateField(blank=True, null=True)
    maint_time = models.TimeField(blank=True, null=True)
    time_taken_to_rectify = models.CharField(max_length=100, blank=True, null=True) # CharField रखा है ताकि '2 Hours' जैसा टेक्स्ट भी सेव हो सके
    men_engaged = models.IntegerField(blank=True, null=True)
    action_taken_details = models.TextField(blank=True, null=True)
    maint_incharge_name = models.CharField(max_length=255, blank=True, null=True)

    # 3. Quality Verification (क्वालिटी द्वारा जाँच)
    status = models.CharField(max_length=10, default='OK')  # OK or NG
    qa_date = models.DateField(blank=True, null=True)
    qa_time = models.TimeField(blank=True, null=True)
    nc_verification = models.CharField(max_length=255, blank=True, null=True)
    qa_incharge_name = models.CharField(max_length=255, blank=True, null=True)

    # Extra Metadata
    language = models.CharField(max_length=50, default='hindi')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tool_breakdown_slip"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.machine_name_no} - {self.reporter_name} ({self.report_date})"
    
from datetime import date
from django.db import models
from datetime import date

class DailyProductionPlan(models.Model):
    plant = models.CharField(max_length=50, blank=True, null=True) 
    shift = models.CharField(max_length=50, blank=True, null=True) 
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    operator_name = models.CharField(max_length=150)
    part_name = models.CharField(max_length=150)
    part_no = models.CharField(max_length=100)
    operation_name = models.CharField(max_length=150)
    planned_quantity = models.PositiveIntegerField()
    achieved_quantity = models.PositiveIntegerField(default=0, blank=True, null=True) 
    qty_remark = models.TextField(blank=True, null=True)
    
    production_start_time = models.TimeField(blank=True, null=True)
    production_end_time = models.TimeField(blank=True, null=True)
    total_working_time = models.CharField(max_length=100, blank=True, null=True) 
    tool_setup_time = models.PositiveIntegerField(default=0, blank=True, null=True)
    machine_bd_time = models.PositiveIntegerField(default=0, blank=True, null=True)
    tool_bd_time = models.PositiveIntegerField(default=0, blank=True, null=True)
    rm_coil_no = models.CharField(max_length=150, blank=True, null=True)
   
    plan_date = models.DateField(default=date.today) 
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'daily_production_plan'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.plant} - {self.shift} - {self.part_name}"

# -----------------------------------------------------
# 2. 4M Change Inspection Report Model
# -----------------------------------------------------
class FourMChangeInspection(models.Model):
    # Basic Details
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100)
    operation = models.CharField(max_length=255)

    # Quantities & Specifications
    lot_qty = models.PositiveIntegerField(blank=True, null=True)
    ok_qty = models.PositiveIntegerField(blank=True, null=True)
    rej_qty = models.PositiveIntegerField(blank=True, null=True)
    parameter_specs = models.CharField(max_length=255, blank=True, null=True)

    # Before (Retroactive) Values
    before_1 = models.CharField(max_length=100, blank=True, null=True)
    before_2 = models.CharField(max_length=100, blank=True, null=True)
    before_3 = models.CharField(max_length=100, blank=True, null=True)
    before_4 = models.CharField(max_length=100, blank=True, null=True)
    before_5 = models.CharField(max_length=100, blank=True, null=True)

    # After / Setup Approval Values
    after_1 = models.CharField(max_length=100, blank=True, null=True)
    after_2 = models.CharField(max_length=100, blank=True, null=True)
    after_3 = models.CharField(max_length=100, blank=True, null=True)
    after_4 = models.CharField(max_length=100, blank=True, null=True)
    after_5 = models.CharField(max_length=100, blank=True, null=True)

    # Footer Info
    inspected_by = models.CharField(max_length=150)
    remarks = models.TextField(blank=True, null=True)

    # Meta Fields
    # ✅ YAHAN CHANGE KIYA HAI (timezone.now ki jagah date.today laga diya)
    inspection_date = models.DateField(default=date.today) 
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'four_m_change_inspection'
        ordering = ['-created_at']

    def __str__(self):
        return f"4M Inspection: {self.part_name} | {self.inspection_date}"
    
class FourMChangeRecord(models.Model):
    # --- 4M Status (OK/Change/Not Set) ---
    STATUS_CHOICES = [('OK', 'No Change'), ('CHANGE', 'Change Implemented'), ('NOT_SET', 'Not Set')]
    status_man = models.CharField(max_length=10, choices=STATUS_CHOICES, default='NOT_SET')
    status_machine = models.CharField(max_length=10, choices=STATUS_CHOICES, default='NOT_SET')
    status_material = models.CharField(max_length=10, choices=STATUS_CHOICES, default='NOT_SET')
    status_method = models.CharField(max_length=10, choices=STATUS_CHOICES, default='NOT_SET')

    # --- Change Details ---
    date = models.DateField(null=True, blank=True)
    time = models.TimeField()
    machine_no = models.CharField(max_length=50)
    description = models.TextField()
    nature_of_change = models.CharField(max_length=100) # Planned/Unplanned
    action_taken = models.CharField(max_length=255)
    part_info = models.CharField(max_length=255) # Part Name/Number
    operation_no = models.CharField(max_length=100)
    setup_approval = models.CharField(max_length=50) # OK/Not OK
    training_provided = models.CharField(max_length=50)

    # --- Retroactive Section ---
    retro_qty_checked = models.IntegerField(null=True, blank=True)
    # retro_entry_qty = models.IntegerField(null=True, blank=True)
    retro_qty_ok = models.CharField(max_length=20, null=True, blank=True) 
    retro_rw = models.CharField(max_length=20, null=True, blank=True)
    retro_scrap = models.CharField(max_length=20, null=True, blank=True)

    # --- Containment Suspected Section ---
    cont_qty_checked = models.IntegerField(null=True, blank=True)
    # cont_entry_qty = models.IntegerField(null=True, blank=True)
    cont_qty_ok = models.CharField(max_length=20, null=True, blank=True)
    cont_rw = models.CharField(max_length=20, null=True, blank=True)
    cont_scrap = models.CharField(max_length=20, null=True, blank=True)

    # --- Dispatch Detail ---
    customer = models.CharField(max_length=255)
    dispatch_date = models.DateField(null=True, blank=True)
    invoice_no = models.CharField(max_length=100, blank=True, null=True)

    remark = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'four_m_change_records'




###################################
#
#       Prodcution monthly Datatable 
###################################

class MonthlyProductionPlan(models.Model):
    filled_date = models.DateField(null=True, blank=True)
    part_name = models.CharField(max_length=255, blank=True, null=True)
    customer_name = models.CharField(max_length=255, blank=True, null=True)
    opening_stock = models.IntegerField(default=0, null=True, blank=True)
    schedule_qty = models.IntegerField(default=0, null=True, blank=True)
    planned_qty = models.IntegerField(default=0, null=True, blank=True)
    remark = models.TextField(blank=True, null=True)
    prepared_by = models.CharField(max_length=255, blank=True, null=True)
    approved_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = "monthly_production_plan"

    def __str__(self):
        return f"{self.part_name} - {self.filled_date}"

class OperatorObservanceChecklist(models.Model):
    record_date = models.DateField(null=True, blank=True) # filled_date equivalent
    operator_name = models.CharField(max_length=255, blank=True, null=True)
    model = models.CharField(max_length=255, blank=True, null=True)
    part_operation = models.CharField(max_length=255, blank=True, null=True)
    
    # JSONField for storing the evaluation criteria rows
    checkpoints = models.JSONField(default=list, blank=True) 
    
    prepared_by = models.CharField(max_length=255, blank=True, null=True)
    approved_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "operator_observance_checklist"

    def __str__(self):
        return f"Observance: {self.operator_name} - {self.record_date}"

class OperatorObservancePlan(models.Model):
    filled_date = models.DateField(auto_now_add=True) # Automatically tracks when the plan was submitted
    plan_year = models.CharField(max_length=10, blank=True, null=True)
    plan_month = models.CharField(max_length=20, blank=True, null=True)
    
    # JSONField to store multiple operators and their status (Plan vs Actual)
    operators_data = models.JSONField(default=list, blank=True)
    
    prepared_by = models.CharField(max_length=255, blank=True, null=True)
    approved_by = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "operator_observance_plan"

    def __str__(self):
        return f"Plan: {self.plan_month} {self.plan_year}"

class PMChecklistMHE(models.Model):
    filled_date = models.DateField(null=True, blank=True)
    part_name = models.CharField(max_length=255, blank=True, null=True)
    trolley_no = models.CharField(max_length=255, blank=True, null=True)
    pm_frequency = models.CharField(max_length=100, blank=True, null=True)
    
    # JSONField for saving Sr No 1 to 11 checklist points
    checkpoints = models.JSONField(default=list, blank=True)
    
    checked_by = models.CharField(max_length=255, blank=True, null=True)
    verified_by = models.CharField(max_length=255, blank=True, null=True)
    general_remarks = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pm_checklist_mhe"

    def __str__(self):
        return f"PM MHE: {self.trolley_no} - {self.filled_date}"
    

class ProjectionWelderQual(models.Model):
    # Table Name in DB will be appname_projectionwelderqual
    wps_no = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    welding_process = models.CharField(max_length=100, default="PROJECTION WELDING")
    
    base_metal = models.CharField(max_length=100, blank=True, null=True)
    base_metal_thickness = models.CharField(max_length=100, blank=True, null=True)
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    
    trials = models.JSONField(default=list) # Saves the dynamic table rows
    
    welder_name = models.CharField(max_length=100, blank=True, null=True)
    conducted_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    qualification_status = models.CharField(max_length=50, blank=True, null=True) # Qualified / Not Qualified
    # welder_photo = models.ImageField(upload_to='welder_photos/projection/', blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'projection_welder_qualification'

class SpotWelderQual(models.Model):
    # Table Name in DB will be appname_spotwelderqual
    wps_no = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    welding_process = models.CharField(max_length=100, default="Spot Welding")
    
    base_metal = models.CharField(max_length=100, blank=True, null=True)
    base_metal_thickness = models.CharField(max_length=100, blank=True, null=True)
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    gun_type = models.CharField(max_length=100, blank=True, null=True)
    
    trials = models.JSONField(default=list)
    
    welder_name = models.CharField(max_length=100, blank=True, null=True)
    conducted_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    qualification_status = models.CharField(max_length=50, blank=True, null=True)
    # welder_photo = models.ImageField(upload_to='welder_photos/spot/', blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'spot_welder_qualification'

class TigMigWelderQual(models.Model):
    # Table Name in DB will be appname_tigmigwelderqual
    wps_no = models.CharField(max_length=100, blank=True, null=True)
    testing_date = models.DateField(blank=True, null=True)
    welding_process = models.CharField(max_length=100, blank=True, null=True) # MIG / TIG
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    
    base_metal = models.CharField(max_length=100, blank=True, null=True)
    base_metal_thickness = models.CharField(max_length=100, blank=True, null=True)
    base_metal_size = models.CharField(max_length=100, blank=True, null=True)
    welding_position = models.CharField(max_length=100, blank=True, null=True)
    
    filler_material = models.CharField(max_length=100, blank=True, null=True)
    filler_material_size = models.CharField(max_length=100, blank=True, null=True)
    shielding_gas = models.CharField(max_length=100, blank=True, null=True)
    wire_feed_speed = models.CharField(max_length=100, blank=True, null=True)
    
    trials = models.JSONField(default=list)
    test_results = models.JSONField(default=dict) # To save visual, defect, strength bend tests
    
    welder_name = models.CharField(max_length=100, blank=True, null=True)
    conducted_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    qualification_status = models.CharField(max_length=50, blank=True, null=True)
    # welder_photo = models.ImageField(upload_to='welder_photos/tig_mig/', blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'tig_mig_welder_qualification'

class ProcessValidation(models.Model):
    # Table Name in DB will be appname_processvalidation
    validation_date = models.DateField(blank=True, null=True)
    revalidation_date = models.DateField(blank=True, null=True)
    process_name = models.CharField(max_length=200, blank=True, null=True)
    material_details = models.CharField(max_length=200, blank=True, null=True)
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    process_owner = models.CharField(max_length=100, blank=True, null=True)
    part_name = models.CharField(max_length=100, blank=True, null=True)
    fixture_no = models.CharField(max_length=100, blank=True, null=True)
    
    operators = models.JSONField(default=list) # Array of operator names
    parameters = models.JSONField(default=list) # P1 to P8 spec table
    trials = models.JSONField(default=list) # Big trials table
    final_params = models.JSONField(default=list) # Final selected params table
    
    conclusion = models.TextField(blank=True, null=True)
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'manufacturing_process_validation'



###################################
#
#       QA monthly Datatable 
###################################

class ProcessAuditChecksheet(models.Model):
    part_name_no = models.CharField(max_length=200, blank=True, null=True)
    machine_model = models.CharField(max_length=200, blank=True, null=True) 
    date = models.DateField(blank=True, null=True)
    auditor = models.CharField(max_length=100, blank=True, null=True)
    auditee = models.CharField(max_length=100, blank=True, null=True)
    
    # Ye naya field pura data (parameter, spec, observation, remark) ek sath save karega
    audit_details = models.JSONField(default=list, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'process_audit_checksheet'

class CoherenceChecklist(models.Model):
    part_name = models.CharField(max_length=200, blank=True, null=True)
    part_no = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    model_name = models.CharField(max_length=100, blank=True, null=True)
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    
    operations = models.JSONField(default=list) # Array of operations
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_coherence_checklist'


class LayoutInspection(models.Model):
    part_name = models.CharField(max_length=200, blank=True, null=True)
    part_no = models.CharField(max_length=100, blank=True, null=True)
    model_name = models.CharField(max_length=100, blank=True, null=True)
    customer_name = models.CharField(max_length=200, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    sample_size = models.CharField(max_length=50, blank=True, null=True)
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    verified_by = models.CharField(max_length=100, blank=True, null=True)
    
    inspections = models.JSONField(default=list) # Array of inspection parameters
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_layout_inspection'


class ProductAuditPlan(models.Model):
    doc_no = models.CharField(max_length=100, blank=True, null=True)
    rev_no = models.CharField(max_length=50, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    plan_year = models.CharField(max_length=50, blank=True, null=True)
    
    # Naye fields add kiye gaye hain
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)
    
    audit_rows = models.JSONField(default=list) # Array of audit schedule grids
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'monthly_product_audit_plan'

class CustomerComplaint(models.Model):
    date = models.DateField(blank=True, null=True)
    part_details = models.CharField(max_length=200, blank=True, null=True)
    model_name = models.CharField(max_length=100, blank=True, null=True)
    customer_name = models.CharField(max_length=200, blank=True, null=True)
    problem_description = models.TextField(blank=True, null=True)
    counter_measure = models.TextField(blank=True, null=True)
    target_date = models.DateField(blank=True, null=True)
    horizontal_action = models.CharField(max_length=200, blank=True, null=True)
    status = models.CharField(max_length=50, default='OPEN')
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_customer_complaint'


class CustomerSatisfaction(models.Model):
    customer_name = models.CharField(max_length=255, blank=True, null=True)
    month_year = models.CharField(max_length=50, blank=True, null=True)
    
    # Storing all 8 Performance Indicators as a JSON Object
    performance_indicators = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_customer_satisfaction'


class WarrantyClaim(models.Model):
    date = models.DateField(blank=True, null=True)
    customer_name = models.CharField(max_length=255, blank=True, null=True)
    part_details = models.CharField(max_length=255, blank=True, null=True)
    claim_qty = models.CharField(max_length=50, blank=True, null=True)
    warranty_defect = models.TextField(blank=True, null=True)
    decision = models.CharField(max_length=50, blank=True, null=True)
    rejection_root_cause = models.TextField(blank=True, null=True)
    disposal_action = models.CharField(max_length=255, blank=True, null=True)
    capa_analysis = models.TextField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_warranty_claim'


class MinutesOfMeeting(models.Model):
    date = models.DateField(blank=True, null=True)
    time = models.TimeField(blank=True, null=True)
    subject = models.CharField(max_length=255, blank=True, null=True)
    aot_members = models.TextField(blank=True, null=True)
    supplier_members = models.TextField(blank=True, null=True)
    
    # Storing SR. NO, PART NAME, DEFECTS, ACTION PLAN, etc., as an Array of JSON objects
    discussions = models.JSONField(default=list) 
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_minutes_of_meeting'


# ###################################
# #
# # Machine Maintenance monthly Datatable 
# ###################################

class MachineBreakdown(models.Model):
    date = models.DateField(blank=True, null=True)
    machine_type_no = models.CharField(max_length=255, blank=True, null=True)
    
    # Store Problem, Time, Status, 4M, Sign, Remarks in this JSON field
    details = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_machine_breakdownsummary'


#  MACHINE Critical Spare Model
class MachineCriticalSpare(models.Model):
    date = models.DateField(blank=True, null=True)
    spare_description = models.CharField(max_length=255, blank=True, null=True)
    model_description = models.CharField(max_length=255, blank=True, null=True)
    box_location = models.CharField(max_length=255, blank=True, null=True)
    prepared_by = models.CharField(max_length=150, blank=True, null=True)
    approved_by = models.CharField(max_length=150, blank=True, null=True)
    
    # JSON field for Spare Type, Levels, UOM, Lead Time, Status, etc.
    spare_details = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_machine_critical_spare'

class ToolBreakdown(models.Model):
    date = models.DateField(blank=True, null=True)
    tool_name = models.CharField(max_length=255, blank=True, null=True)
    
    # Store Process, Problem, Action, Time, CheckedBy, HistoryCard, 4M, Sign, Remarks here
    details = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_tool_breakdownsummary'
        
#  TOOL Critical Spare Model
class ToolCriticalSpare(models.Model):
    date = models.DateField(blank=True, null=True)
    spare_description = models.CharField(max_length=255, blank=True, null=True)
    model_description = models.CharField(max_length=255, blank=True, null=True)
    box_location = models.CharField(max_length=255, blank=True, null=True)
    
    # JSON field for Spare Type, UOM, Opening Stock, Min Level, Lead Time
    spare_details = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_tool_critical_spare'





class ReportTrackHistory(models.Model):
    username = models.CharField(max_length=150)       
    department = models.CharField(max_length=100)     
    report_name = models.CharField(max_length=150)    
    action_time = models.DateTimeField(default=now)   

    def __str__(self):
        return f"{self.username} submitted {self.report_name} at {self.action_time}"
    




class MachineEventLog(models.Model):
    plant_no = models.IntegerField(help_text="1 for Plant 1, 2 for Plant 2")
    machine_no = models.CharField(max_length=10)
    event_type = models.CharField(max_length=50)  # Seedha backend se aayega (ON, OFF, etc.)
    timestamp = models.DateTimeField()
    shift = models.CharField(max_length=5)
    details = models.TextField(blank=True, null=True)

    class Meta:
        # Table ka exact naam jo DB mein banega
        db_table = 'Machine_Event_Logs' 
        
        # Super fast search/filter ke liye Index (Bohot zaroori)
        indexes = [
            models.Index(fields=['plant_no', 'machine_no', 'timestamp']),
        ]
        
        # By default latest event sabse upar aayega
        ordering = ['-timestamp']

    def __str__(self):
        return f"P{self.plant_no}-M{self.machine_no} | {self.event_type} | {self.timestamp}"
    

class HourlyDowntimeLog(models.Model):
    
    STATUS_CHOICES = [
        ('ONLINE', 'Machine is Online (But Idle)'),
        ('OFFLINE', 'Machine is Offline (No Signal)'),
    ]

    timestamp = models.DateTimeField()
    machine_no = models.CharField(max_length=10)
    idle_time = models.IntegerField(default=0)
    shift = models.CharField(max_length=5)
    
    # Ye column saaf bata dega machine Online thi ya Offline
    machine_status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='ONLINE')
    
    reason_category = models.CharField(max_length=100, default='Uncategorized')
    specific_reason = models.CharField(
        max_length=255, 
        default='Supervisor not save the information'
    )
    
    class Meta:
        db_table = 'hourly_downtime_logs'
        indexes = [
            models.Index(fields=['timestamp', 'machine_no']),
            models.Index(fields=['machine_no', 'timestamp']),
        ]
        ordering = ['-timestamp', 'machine_no']
    
    def __str__(self):
        return f"M{self.machine_no} - {self.timestamp.strftime('%H:%M')} - Status: {self.machine_status} - {self.idle_time}m"




from django.db import models
from django.contrib.auth.models import User  

class Notification(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='notifications')
    machine_no = models.CharField(max_length=50, null=True, blank=True)
    message = models.TextField()
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"Notification for {self.user.username} - {self.machine_no}"
    
    
    
    
##############################################
# machine maintenance weekly report model 
##############################################


class TigWeldingMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the 12 Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "tig_welding_maintenance" # Database table name

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"   

class SpotWeldingMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='SPOT WELDING M/C', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "spot_welding_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class CompressorMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Compressor', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "compressor_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class LatheMachineMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='LATHE MACHINE', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "lathe_machine_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class VerticalDrillMachineMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='VERTICAL DRILL MACHINE', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "vertical_drill_machine_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class SurfaceGrinderMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='SURFACE GRINDER', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "surface_grinder_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"
    
class BaseGrinderMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Base Grinder', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "base_grinder_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class BeltGrinderMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='BELT GRINDER', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "belt_grinder_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


# class TappingMaintenance(models.Model):
#     # Meta Data Fields
#     machine_name = models.CharField(max_length=100, default='Tapping Machine', blank=True, null=True)
#     date = models.DateField()
#     machine_no = models.CharField(max_length=50, blank=True, null=True)
#     location = models.CharField(max_length=100, blank=True, null=True)
#     specification = models.CharField(max_length=100, blank=True, null=True)
#     maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
#     # JSON Field for the Checkpoints (tableData)
#     checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
#     # Signatures
#     prepared_by = models.CharField(max_length=100, blank=True, null=True)
#     checked_by = models.CharField(max_length=100, blank=True, null=True)
    
#     # Auto Timestamps
#     created_at = models.DateTimeField(auto_now_add=True)

#     class Meta:
#         managed = True
#         db_table = "tapping_maintenance"

#     def __str__(self):
#         return f"{self.machine_name} - {self.machine_no} ({self.date})"


class PipeCuttingMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Pipe Cutter', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "pipe_cutting_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class VibraMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Vibra', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "vibra_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class DipMoldingMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Dip Molding Machine', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "dip_molding_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"


class ServoPressMaintenance(models.Model):
    # Meta Data Fields
    machine_name = models.CharField(max_length=100, default='Servo Press', blank=True, null=True)
    date = models.DateField()
    machine_no = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    specification = models.CharField(max_length=100, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=100, blank=True, null=True)
    
    # JSON Field for the Checkpoints (tableData)
    checkpoints = models.JSONField(default=list, help_text="Stores the checklist array")
    
    # Signatures
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    
    # Auto Timestamps
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = "servo_press_maintenance"

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} ({self.date})"
    
class MachinePreventiveMaintenance(models.Model):
    machine_name = models.CharField(max_length=150)
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    checkpoints = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'machine_preventive_maintenance'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.machine_no} - {self.date}"
    
class CNCMaintenanceReport(models.Model):
    machine_name = models.CharField(max_length=150)
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    checklist = models.JSONField(default=list, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "cnc_maintenance_report"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"

class VerticalMillingMachineCheckSheet(models.Model):
    machine_name = models.CharField(max_length=150, default="VERTICAL MILLING MACHINE")
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    checkpoints = models.JSONField(default=list, blank=True)
    prepared_by = models.CharField(max_length=150)
    checked_by = models.CharField(max_length=150, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "vertical_milling_machine_checksheet"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"
class ProjectionWeldingPMCheckSheet(models.Model):
    machine_name = models.CharField(max_length=150, default="PROJECTION WELDING")
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    checkpoints = models.JSONField(default=list, blank=True)
    prepared_by = models.CharField(max_length=150)
    checked_by = models.CharField(max_length=150, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "projection_welding_pm_checksheet"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"
class PowerPressPMCheckSheet(models.Model):
    machine_name = models.CharField(max_length=150, default="POWER PRESS")
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    checkpoints = models.JSONField(default=list, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "power_press_pm_checksheet"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"
    
class HydraulicPMCheckSheet(models.Model):
    machine_name = models.CharField(max_length=150, default="HYDRAULIC MACHINE")
    machine_no = models.CharField(max_length=50)
    date = models.DateField()
    location = models.CharField(max_length=150)
    specification = models.CharField(max_length=255, blank=True, null=True)
    maintenance_personnel = models.CharField(max_length=150)
    
    # Store all 7 checklist items inside this JSON structure
    checkpoints = models.JSONField(default=list, blank=True)
    
    prepared_by = models.CharField(max_length=150)
    checked_by = models.CharField(max_length=150)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "hydraulic_pm_checksheet"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.machine_name} - {self.machine_no} - {self.date}"


class PartMaster(models.Model):
    customer_name = models.CharField(max_length=255)
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=255, null=True, blank=True)
    # Python me 'model' keyword hota hai, isliye column ka naam 'part_model' rakha hai
    part_model = models.CharField(max_length=255, null=True, blank=True) 
    inspection_data = models.JSONField()

    class Meta:
        # Yahan aap apna custom table name set kar sakte hain
        db_table = 'master_data_incoming_material_inspection'

    def __str__(self):
        return f"{self.customer_name} - {self.part_name}"
    
class FourMDisplay(models.Model):
    # s_no can be helpful to keep track of the row order submitted from frontend
    s_no = models.IntegerField(blank=True, null=True) 
    machine_no = models.CharField(max_length=100, blank=True, null=True)
    operator_name = models.CharField(max_length=100, blank=True, null=True)
    
    # 4M Details
    man = models.CharField(max_length=255, blank=True, null=True)
    machine = models.CharField(max_length=255, blank=True, null=True)
    material = models.CharField(max_length=255, blank=True, null=True)
    method = models.CharField(max_length=255, blank=True, null=True)
    
    # Tracking Dates (As requested)
    date_filled = models.DateField(auto_now_add=True)      # Only the date (e.g., 2026-06-06)
    created_at = models.DateTimeField(auto_now_add=True)   # Date AND exact time

    class Meta:
        db_table = 'four_m_display_board'

    def __str__(self):
        return f"{self.machine_no} - {self.operator_name} ({self.date_filled})"
    
class FourMSummary(models.Model):
    # Header Info (Prepared & Approved By)
    
    
    # Basic Row Details
    s_no = models.IntegerField(blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    part_name_no = models.CharField(max_length=255, blank=True, null=True)
    type_of_change = models.CharField(max_length=255, blank=True, null=True)
    change_detail = models.TextField(blank=True, null=True)
    
    # Retroactive Inspection Status
    retro_total_qty = models.IntegerField(blank=True, null=True)
    retro_ok_qty = models.IntegerField(blank=True, null=True)
    retro_rej_qty = models.IntegerField(blank=True, null=True)
    
    # Actions, Customers & Signatures
    status_after_final = models.CharField(max_length=255, blank=True, null=True)
    action_for_ng = models.CharField(max_length=255, blank=True, null=True)
    customer = models.CharField(max_length=255, blank=True, null=True)
    sup_signature = models.CharField(max_length=255, blank=True, null=True)
    sign_prod_head = models.CharField(max_length=255, blank=True, null=True)
    sign_qa_head = models.CharField(max_length=255, blank=True, null=True)
    remarks = models.TextField(blank=True, null=True)
    # Header Info (Prepared & Approved By)
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)
    # Tracking Dates
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'four_m_summary_sheet'

    def __str__(self):
        return f"{self.part_name_no} - {self.date}"
    
    
class FixtureMaintenanceRecord(models.Model):
    
    # 1. Basic Top Level Fields
    part_name = models.CharField(max_length=255, help_text="Assembly / Part Name")
    part_no = models.CharField(max_length=100, blank=True, null=True, help_text="Part Number")
    done_on_date = models.DateField()
    fixture_no = models.CharField(max_length=100)
    operation_name = models.CharField(max_length=255)

    # 2. Checklist Data (JSON Format)
     
    checklist_data = models.JSONField(default=list, help_text="Stores the 8 checklist points data")

    # 3. Technical Chart Data (JSON Format)
    
    pin_chart_data = models.JSONField(default=list, help_text="Stores 12 rows of Pin data")
    bush_chart_data = models.JSONField(default=list, help_text="Stores 12 rows of Bush data")

    # 4. Inspected By (Alag se)
    inspected_by = models.CharField(max_length=255, help_text="Engineer Sign / Name")

    # Audit Trail (Kab create hua)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.fixture_no} - {self.part_name} on {self.done_on_date}"

    class Meta:
        db_table = 'fixture_maintenance_records'
        verbose_name = 'Fixture Maintenance Record'
        verbose_name_plural = 'Fixture Maintenance Records'
        
class IncomingMaterialInspection(models.Model):
    # --- Header Information ---
    supplier = models.CharField(max_length=255, default="ATOMONE TECHNOLOGIES PVT.LTD")
    customer = models.CharField(max_length=255)
    part_name = models.CharField(max_length=255)
    part_no = models.CharField(max_length=100, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    
    # --- Material Details ---
    grade = models.CharField(max_length=100, blank=True, null=True)
    mtc = models.CharField(max_length=10, blank=True, null=True)     # Options: YES, NO, N/A
    ga_nga = models.CharField(max_length=10, blank=True, null=True)  # Options: GA, NGA, N/A
    coil_no = models.CharField(max_length=100, blank=True, null=True)
    invoice_no = models.CharField(max_length=100, blank=True, null=True)
    qty = models.CharField(max_length=50, blank=True, null=True)     # CharField in case of unit inclusion (e.g. '50 kgs')

    # --- Inspection Parameters (Table Rows in JSON) ---
    inspection_data = models.JSONField(
        default=list, 
        help_text="Stores array of objects containing parameter, specification, inspMethod, observations array, and remark"
    )

    # --- Authorization ---
    prepared_by = models.CharField(max_length=100, blank=True, null=True)
    checked_by = models.CharField(max_length=100, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)

    # --- Timestamps ---
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "incoming_material_inspection_report"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.supplier} | {self.part_name} ({self.part_no}) - {self.date}"
    
    
    
    
# from django.db import models
# from django.contrib.auth.models import User

# # 1. User Profile: Kaunsa user kis Department/Plant ka hai
# class UserProfile(models.Model):
#     user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    
#     # Department / Plant ke options
#     DEPARTMENT_CHOICES = (
#         ('Plant 1', 'Plant 1'),
#         ('Plant 2', 'Plant 2'),
#         ('QA Hub', 'QA Hub'),
#         ('Production Hub', 'Production Hub'),
#         ('Maintenance Hub', 'Maintenance Hub'),
#     )
#     department_name = models.CharField(max_length=50, choices=DEPARTMENT_CHOICES, default='Plant 1')

#     class Meta:
#         # 🔥 Puraani default table ki jagah ye naam DB mein aayega
#         db_table = 'user_department_profiles' 

#     def __str__(self):
#         return f"{self.user.username} - {self.department_name}"

from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver

# ==============================================================================
# 🏭 ENTERPRISE USER PROFILE MASTER
# ==============================================================================
class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    
    # 📌 Nayi Fields Jo Aapne Maangi Thi
    employee_id = models.CharField(max_length=50, unique=True, null=True, blank=True, help_text="Unique Employee Code")
    mobile_no = models.CharField(max_length=15, null=True, blank=True)
    contact_email = models.EmailField(null=True, blank=True, help_text="Alternate/Direct Email")
    
    # 📌 Location (Konsa Plant Hai)
    LOCATION_CHOICES = (
        ('Plant 1', 'Plant 1'),
        ('Plant 2', 'Plant 2'),
        ('HQ', 'Headquarters'),
    )
    location = models.CharField(max_length=50, choices=LOCATION_CHOICES, default='Plant 1')

    # 📌 Department
    DEPARTMENT_CHOICES = (
        ('QA', 'Quality Assurance (QA)'),
        ('Production', 'Production'),
        ('Maintenance', 'Maintenance'),
        ('Management', 'Management'),
    )
    department = models.CharField(max_length=50, choices=DEPARTMENT_CHOICES, default='QA')

    class Meta:
        db_table = 'user_department_profiles' 

    def __str__(self):
        emp_code = self.employee_id if self.employee_id else "NO-ID"
        return f"{self.user.username} | ID: {emp_code} | {self.location} - {self.department}"
    

from django.db import models
from django.contrib.auth.models import User


class ReportActivityLog(models.Model):
    username = models.CharField(max_length=255)
    department_name = models.CharField(max_length=100) 
    report_name = models.CharField(max_length=255)
    record_id = models.IntegerField(null=True, blank=True)
    form_key = models.CharField(max_length=100, blank=True, null=True)
    hub = models.CharField(max_length=100, blank=True, null=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=255, default="In Progress") 

    class Meta:
        db_table = 'user_report_activity_logs'

    def __str__(self):
        return f"{self.username} | {self.report_name} | Status: {self.status}"


# ==========================================
# 3. 🔥 NAYA MODEL: SIRF QA HUB NOTIFICATIONS KE LIYE
# ==========================================
class QANotification(models.Model):
    # Jisko notification bhejna hai (e.g., rajeshdhiman)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='qa_notifications')
    
    # Kis report ke baare mein hai ye notification
    report_log = models.ForeignKey(ReportActivityLog, on_delete=models.CASCADE)
    
    message = models.TextField()
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'qa_hub_notifications' # 🔥 Table ka naam clear rakha hai
        ordering = ['-created_at']

    def __str__(self):
        return f"QA Alert for {self.user.username}: {self.report_log.report_name}"


from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver

# ==============================================================================
# 🏭 ENTERPRISE USER PROFILE MASTER
# ==============================================================================
class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    
    # 📌 Nayi Fields Jo Aapne Maangi Thi
    employee_id = models.CharField(max_length=50, unique=True, null=True, blank=True, help_text="Unique Employee Code")
    mobile_no = models.CharField(max_length=15, null=True, blank=True)
    contact_email = models.EmailField(null=True, blank=True, help_text="Alternate/Direct Email")
    full_name = models.CharField(max_length=100, null=True, blank=True, help_text="Full Name of the User")
    designation = models.CharField(max_length=100, null=True, blank=True, help_text="Designation of the User")
    profile_image = models.ImageField(upload_to="profile_images/", blank=True, null=True)
    # 📌 Location (Konsa Plant Hai)
    LOCATION_CHOICES = (
        ('Plant 1', 'Plant 1'),
        ('Plant 2', 'Plant 2'),
        ('HQ', 'Headquarters'),
    )
    location = models.CharField(max_length=50, choices=LOCATION_CHOICES, default='Plant 1')

    # 📌 Department
    DEPARTMENT_CHOICES = (
        ('QA', 'Quality Assurance (QA)'),
        ('Production', 'Production'),
        ('Maintenance', 'Maintenance'),
        ('Management', 'Management'),
    )
    department = models.CharField(max_length=50, choices=DEPARTMENT_CHOICES, default='QA')

    class Meta:
        db_table = "user_department_profiles"

    def __str__(self):
        emp_code = self.employee_id if self.employee_id else "NO-ID"
        return f"{self.user.username} | ID: {emp_code} | {self.location} - {self.department}"
    

class FourMInformationSheet(models.Model):

    # Tracking row order from frontend

    s_no = models.IntegerField(blank=True, null=True)

    # Specifically added for the Information Sheet form

    time = models.TimeField(blank=True, null=True) 

    # Basic Machine & Operator Details

    machine_no = models.CharField(max_length=100, blank=True, null=True)
    operator_name = models.CharField(max_length=100, blank=True, null=True)


    # 4M Details (Expects "Change" or "No Change")

    man = models.CharField(max_length=50, blank=True, null=True)
    machine = models.CharField(max_length=50, blank=True, null=True)
    material = models.CharField(max_length=50, blank=True, null=True)
    method = models.CharField(max_length=50, blank=True, null=True)

    # Description of the change

    change_description = models.TextField(blank=True, null=True)

    # Person who prepared/submitted the report

    prepared_by = models.CharField(max_length=100, blank=True, null=True)

    # Tracking Dates

    date_filled = models.DateField(auto_now_add=True)      # Only the date

    created_at = models.DateTimeField(auto_now_add=True)   # Date AND exact time

    class Meta:

        db_table = 'four_m_information_sheet'

    def __str__(self):

        return f"{self.machine_no} - {self.operator_name} ({self.date_filled})"
    
