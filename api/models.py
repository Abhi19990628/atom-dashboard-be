# models.py - Complete updated code
from django.db import models
from django.utils import timezone


class Operator(models.Model):
    """
    Operator Master Table - NEW MODEL
    """
    PLANT_CHOICES = [
        ('plant_1', 'Plant 1'),
        ('plant_2', 'Plant 2'),
    ]
    
    name = models.CharField(max_length=100)
    plant = models.CharField(max_length=20, choices=PLANT_CHOICES)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)  # IST time automatic
    
    class Meta:
        db_table = 'operators'
        ordering = ['name']
        unique_together = ['name', 'plant']
    
    def __str__(self):
        return f"{self.name} - {self.plant}"


class OperatorAssignment(models.Model):
    """
    Operator Assignment Table - UPDATED
    """
    SHIFT_CHOICES = [
        ('A', 'Shift A'),
        ('B', 'Shift B'), 
    ]
    
    PLANT_CHOICES = [
        ('plant_1', 'Plant 1'),
        ('plant_2', 'Plant 2'),
    ]
    
    id = models.AutoField(primary_key=True)
    plant = models.CharField(max_length=20, choices=PLANT_CHOICES, default='plant_2')
    machine_no = models.CharField(max_length=10)
    operator_name = models.CharField(max_length=100)
    shift = models.CharField(max_length=1, choices=SHIFT_CHOICES)
    start_time = models.DateTimeField(default=timezone.now) 
    created_at = models.DateTimeField(default=timezone.now)
    
    def __str__(self):
        return f"{self.plant}: {self.operator_name} -> Machine {self.machine_no}"
    
    class Meta:
        db_table = 'operator_assignments'


class IdleReport(models.Model):
    """
    Idle Case Report Table - UPDATED
    """
    IDLE_REASON_CHOICES = [
        ('TOOL_BD', 'Tool Breakdown'),
        ('MC_BD', 'Machine Breakdown'),
        ('MAINT', 'Scheduled Maintenance'),
        ('CHANGEOVER', 'Changeover'),
        ('NO_MATERIAL', 'Material Shortage'),
        ('QUALITY_ISSUE', 'Quality Issue'),
        ('OTHER', 'Other'),
    ]
    
    PLANT_CHOICES = [
        ('plant_1', 'Plant 1'),
        ('plant_2', 'Plant 2'),
    ]
    
    id = models.AutoField(primary_key=True)
    plant = models.CharField(max_length=20, choices=PLANT_CHOICES, default='plant_2')
    machine_no = models.CharField(max_length=10)
    operator_name = models.CharField(max_length=100)
    tool_id = models.CharField(max_length=100)
    reason = models.CharField(max_length=20, choices=IDLE_REASON_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.plant} - Idle: Machine {self.machine_no} - {self.reason}"
    
    class Meta:
        db_table = 'idle_reports'


# ========================================
# ✅ NEW MODEL - HOURLY IDLE TIME TABLE (NO AUTO ID)
# ========================================

class Plant2HourlyIdletime(models.Model):
    """
    Hourly idle time tracking for each machine
    ORDER: timestamp, tool_id, machine_no, idle_time, shut_height, shift
    NO AUTO-INCREMENT ID
    """
    timestamp = models.DateTimeField()  # ✅ FIRST
    tool_id = models.CharField(max_length=50)  # ✅ SECOND (NOT NULL)
    machine_no = models.CharField(max_length=10)  # ✅ THIRD
    idle_time = models.IntegerField(default=0)  # ✅ FOURTH (renamed from idle_minutes)
    shut_height = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # ✅ FIFTH
    shift = models.CharField(max_length=5)  # ✅ SIXTH
    
    class Meta:
        db_table = 'Plant2_hourly_idle'
        unique_together = [('timestamp', 'machine_no')]  # ✅ Composite primary key
        indexes = [
            models.Index(fields=['timestamp', 'machine_no']),
            models.Index(fields=['machine_no', 'timestamp']),
        ]
        ordering = ['-timestamp', 'machine_no']
    
    def __str__(self):
        return f"M{self.machine_no} - {self.timestamp.strftime('%Y-%m-%d %H:%M')} - {self.idle_time}min"
