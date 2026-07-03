def normalize_report_name(value):
    return str(value or "").strip().lower()


REPORT_ROUTE_MAP = {
    # =========================
    # PRODUCTION HUB
    # =========================
    normalize_report_name("Daily Prod Form"): {
        "form_key": "daily-prod-plan",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Daily Production plan"): {
        "form_key": "daily-prod-plan",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Bin Trolley Form"): {
        "form_key": "bin-trolley",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Machine Checksheet"): {
        "form_key": "machine-checksheet",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Tip Change Monitor Form"): {
        "form_key": "tip-change",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Rework Report"): {
        "form_key": "rework",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("5S Checksheet"): {
        "form_key": "five-s",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("4M Change Inspection"): {
        "form_key": "four-m-inspection",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("4M Tracking Record"): {
        "form_key": "four-m-record",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("4M Display Board"): {
        "form_key": "four-m-display",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("4M Summary Sheet"): {
        "form_key": "four-m-summary",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("4M Information Sheet"): {
        "form_key": "four-m-information",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Monthly Prod Plan"): {
        "form_key": "monthly-prod-plan",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Operator Observance Checklist"): {
        "form_key": "operator-observance-checklist",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Operator Observance Plan"): {
        "form_key": "operator-observance-plan",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("PM Checklist MHE"): {
        "form_key": "pm-checklist-mhe",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Projection Welder"): {
        "form_key": "projection-welder",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Spot Welder"): {
        "form_key": "spot-welder",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("TIG/MIG Welder"): {
        "form_key": "tig-mig-welder",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    normalize_report_name("Process Validation"): {
        "form_key": "process-validation",
        "hub": "production-hub",
        "target_group": "Production_Approvers",
    },
    # =========================
    # QA HUB
    # =========================
    normalize_report_name("Deviation Approval Form"): {
        "form_key": "deviation",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Deviation Report"): {
        "form_key": "deviation",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Redbin Approval Form"): {
        "form_key": "redbin",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Red Bin Attendance"): {
        "form_key": "redbin-attendance",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Incoming Inspection"): {
        "form_key": "incoming",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Incoming Material Inspection"): {
        "form_key": "incoming",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Inspection Report"): {
        "form_key": "inspection",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Inspection"): {
        "form_key": "inspection",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Scrap Note"): {
        "form_key": "scrap",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Poka Yoke"): {
        "form_key": "poka-yoke",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("PDI"): {
        "form_key": "pdi",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("QA Rework Report"): {
        "form_key": "rework",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Good Receipt"): {
        "form_key": "good-receipt",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Material Requisition Slip"): {
        "form_key": "good-receipt",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Process Audit"): {
        "form_key": "process-audit",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Coherence"): {
        "form_key": "coherence",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Coherence Checklist"): {
        "form_key": "coherence",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Layout Inspection"): {
        "form_key": "layout-inspection",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Product Audit"): {
        "form_key": "product-audit-plan",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Product Audit Plan"): {
        "form_key": "product-audit-plan",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Customer Complaint"): {
        "form_key": "customer-complaint",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Customer Satisfaction"): {
        "form_key": "customer-satisfaction",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Warranty Claim"): {
        "form_key": "warranty-claim",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("MOM"): {
        "form_key": "mom",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Meeting"): {
        "form_key": "mom",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    # =========================
    # MAINTENANCE HUB
    # =========================
    normalize_report_name("Machine Breakdown Form"): {
        "form_key": "machine-breakdown",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Machine Preventive Maintenance"): {
        "form_key": "preventive-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Tool Breakdown Form"): {
        "form_key": "tool-breakdown",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Tool Preventive Maintenance"): {
        "form_key": "tool-preventive-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Machine History Card"): {
        "form_key": "machine-history",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Machine History Form"): {
        "form_key": "machine-history",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Daily Power Press Checksheet"): {
        "form_key": "power-press-checksheet",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Power Press Checksheet"): {
        "form_key": "power-press-checksheet",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Machine Breakdown Slip"): {
        "form_key": "machine-breakdown",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Poka Yoke Monitoring"): {
        "form_key": "poka-yoke",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Vertical Milling Machine Check Sheet"): {
        "form_key": "vmm",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("VMM Maintenance Form"): {
        "form_key": "vmm",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Machine Preventive Maintenance"): {
        "form_key": "preventive-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Projection Welding PM Check Sheet"): {
        "form_key": "projection-welding",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Projection Welding Maintenance"): {
        "form_key": "projection-welding",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("CNC Maintenance Report"): {
        "form_key": "cnc",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("CNC Preventive Maintenance"): {
        "form_key": "cnc",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Power Press PM Check Sheet"): {
        "form_key": "power-press-pm",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Power Press Preventive Maintenance"): {
        "form_key": "power-press-pm",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Hydraulic PM Check Sheet"): {
        "form_key": "hydraulic-pm",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Hydraulic Preventive Maintenance"): {
        "form_key": "hydraulic-pm",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("TIG Welding Maintenance"): {
        "form_key": "tig-welding-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("TIG Maintenance Form"): {
        "form_key": "tig-welding-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Spot Welding Maintenance"): {
        "form_key": "spot-welding-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Spot Welding Maintenance Form"): {
        "form_key": "spot-welding-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Compressor Maintenance"): {
        "form_key": "compressor-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Compressor Maintenance Form"): {
        "form_key": "compressor-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Lathe Machine Maintenance"): {
        "form_key": "lathe-machine-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Lathe Maintenance Form"): {
        "form_key": "lathe-machine-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Vertical Drill Machine Maintenance"): {
        "form_key": "vertical-drill-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Drill Machine Maintenance"): {
        "form_key": "vertical-drill-maintenance",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Surface Grinder Maintenance"): {
        "form_key": "surface-grinder",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Surface Grinder Maintenance Form"): {
        "form_key": "surface-grinder",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Belt Grinder Maintenance"): {
        "form_key": "belt-grinder",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Belt Grinder Maintenance Form"): {
        "form_key": "belt-grinder",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Base Grinder Maintenance"): {
        "form_key": "base-grinder",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Base Grinder Maintenance Form"): {
        "form_key": "base-grinder",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Pipe Cutting Maintenance"): {
        "form_key": "pipe-cutting",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Pipe Cutting Maintenance Form"): {
        "form_key": "pipe-cutting",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Vibra Maintenance"): {
        "form_key": "vibra",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Vibra Maintenance Form"): {
        "form_key": "vibra",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Dip Molding Maintenance"): {
        "form_key": "dip-molding",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Dip Molding Maintenance Form"): {
        "form_key": "dip-molding",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Servo Press Maintenance"): {
        "form_key": "servo-press",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Servo Press Maintenance Form"): {
        "form_key": "servo-press",
        "hub": "maintenance-hub",
        "target_group": "Maintenance_Approvers",
    },
    normalize_report_name("Process Audit"): {
        "form_key": "process-audit",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
    normalize_report_name("Process Audit Checksheet"): {
        "form_key": "process-audit",
        "hub": "qa-hub",
        "target_group": "Quality_Approvers",
    },
}


DEFAULT_ROUTE_CONFIG = {
    "form_key": "daily-prod-plan",
    "hub": "production-hub",
    "target_group": "Production_Approvers",
}


def get_route_config(report_name):
    return REPORT_ROUTE_MAP.get(
        normalize_report_name(report_name),
        DEFAULT_ROUTE_CONFIG,
    )
