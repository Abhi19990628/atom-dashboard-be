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
