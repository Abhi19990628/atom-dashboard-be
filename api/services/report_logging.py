import traceback

from django.contrib.auth.models import Group, User
from django.utils import timezone

from api.models import QANotification, ReportActivityLog
from api.services.report_registry import get_route_config

# //Aman is a good boy

GROUP_ALIASES = {
    "Quality_Approvers": ["Quality_Approvers", "QA_Approvers"],
    "QA_Approvers": ["QA_Approvers", "Quality_Approvers"],
    "Production_Approvers": ["Production_Approvers"],
    "Maintenance_Approvers": ["Maintenance_Approvers"],
}


def get_profile(user):
    return getattr(user, "userprofile", getattr(user, "profile", None))


def resolve_existing_group_name(group_name):
    possible_names = GROUP_ALIASES.get(group_name, [group_name])

    for name in possible_names:
        if Group.objects.filter(name=name).exists():
            return name

    return group_name


def auto_log_report(
    username,
    report_name,
    record_id,
    department_name=None,
    target_group=None,
):
    """
    Common activity-log and notification creator.

    Important:
    - If department_name is passed from form, it will NOT be overwritten by old user profile.
    - Notification goes to target_group approvers.
    - Plant/location filter is still applied.
    """

    if not username:
        username = "Unknown User"

    def location_code(value):
        text = str(value or "").strip()

        # Example: "Plant 1 (Maintenance)" -> "Plant 1"
        if "(" in text:
            text = text.split("(", 1)[0].strip()

        text = text.replace("_", " ")
        return text.replace(" ", "").lower()

    try:
        user_obj = User.objects.filter(username=username).first()

        explicit_department_given = bool(str(department_name or "").strip())
        dept_name = str(department_name or "").strip()
        submitter_location_code = ""

        profile = get_profile(user_obj) if user_obj else None

        profile_loc = ""
        profile_dept = ""

        if profile:
            profile_loc = str(getattr(profile, "location", "") or "").strip()
            profile_dept = str(getattr(profile, "department", "") or "").strip()

        # ✅ IMPORTANT FIX:
        # If form passes department_name, use it. Do not overwrite with old profile department.
        if explicit_department_given:
            dept_name = str(department_name).strip()

            if "plant" in dept_name.lower():
                submitter_location_code = location_code(dept_name)
            elif profile_loc:
                submitter_location_code = location_code(profile_loc)

        else:
            if profile_loc or profile_dept:
                dept_name = f"{profile_loc} ({profile_dept})".strip()
                submitter_location_code = location_code(profile_loc)

        if not dept_name:
            dept_name = "Unknown Department"

        existing_log = None

        if record_id:
            existing_log = (
                ReportActivityLog.objects.filter(
                    username=username,
                    report_name=report_name,
                    record_id=record_id,
                )
                .order_by("-id")
                .first()
            )

        if existing_log:
            log = existing_log
        else:
            log = ReportActivityLog.objects.create(
                username=username,
                department_name=dept_name,
                report_name=report_name,
                record_id=record_id,
            )

        try:
            route_config = get_route_config(report_name)
        except Exception:
            route_config = {}

        final_target_group = (
            target_group or route_config.get("target_group") or "Maintenance_Approvers"
        )

        final_target_group = resolve_existing_group_name(final_target_group)

        approvers = User.objects.filter(groups__name=final_target_group).distinct()

        local_now = timezone.localtime(timezone.now())
        date_str = local_now.strftime("%d-%b-%Y")
        time_str = local_now.strftime("%I:%M %p")
        msg = f"{username} submitted {report_name} on {date_str} at {time_str}."

        created_count = 0
        skipped_location_count = 0

        for approver in approvers:
            approver_profile = get_profile(approver)

            if (
                submitter_location_code
                and approver_profile
                and getattr(approver_profile, "location", None)
            ):
                approver_location_code = location_code(approver_profile.location)

                if submitter_location_code != approver_location_code:
                    skipped_location_count += 1
                    continue

            notification, created = QANotification.objects.get_or_create(
                user=approver,
                report_log=log,
                defaults={
                    "message": msg,
                    "is_read": False,
                },
            )

            if not created:
                notification.message = msg
                notification.is_read = False
                notification.save(update_fields=["message", "is_read"])

            if created:
                created_count += 1

        print(
            f"✅ Auto log done | log_id={log.id} | report={report_name} | "
            f"department={dept_name} | group={final_target_group} | "
            f"approvers={approvers.count()} | notifications_created={created_count} | "
            f"skipped_location={skipped_location_count}"
        )

        return log

    except Exception as e:
        print(f"🔥 Auto Log Failed for {report_name}: {str(e)}")
        traceback.print_exc()
        return None
