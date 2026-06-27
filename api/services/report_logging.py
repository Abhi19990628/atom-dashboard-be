import traceback

from django.contrib.auth.models import Group, User
from django.utils import timezone

from api.models import QANotification, ReportActivityLog
from api.services.report_registry import get_route_config


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
    form_key=None,
    hub=None,
    target_group=None,
):
    """
    Common activity-log and notification creator.

    Safe behavior:
    - Creates ReportActivityLog.
    - Sends notifications to same plant/location approvers.
    - Avoids duplicate logs for same username/report/record/form/hub.
    - Avoids duplicate notifications for same approver + log.
    """

    if not username:
        username = "Unknown User"

    try:
        route_config = get_route_config(report_name)

        final_form_key = form_key or route_config["form_key"]
        final_hub = hub or route_config["hub"]
        final_target_group = target_group or route_config["target_group"]
        final_target_group = resolve_existing_group_name(final_target_group)

        user_obj = User.objects.filter(username=username).first()

        dept_name = final_hub
        submitter_location_code = ""

        if user_obj:
            profile = get_profile(user_obj)

            if profile:
                loc = str(getattr(profile, "location", "") or "").strip()
                dept = str(getattr(profile, "department", "") or "").strip()

                if loc or dept:
                    dept_name = f"{loc} ({dept})".strip()

                submitter_location_code = loc.replace(" ", "").lower()

        existing_log = None

        if record_id:
            existing_log = ReportActivityLog.objects.filter(
                username=username,
                report_name=report_name,
                record_id=record_id,
                form_key=final_form_key,
                hub=final_hub,
            ).order_by("-id").first()

        if existing_log:
            log = existing_log
        else:
            log = ReportActivityLog.objects.create(
                username=username,
                department_name=dept_name,
                report_name=report_name,
                record_id=record_id,
                form_key=final_form_key,
                hub=final_hub,
            )

        approvers = User.objects.filter(groups__name=final_target_group).distinct()

        date_str = timezone.localtime(log.timestamp).strftime("%d-%b-%Y")
        time_str = timezone.localtime(log.timestamp).strftime("%I:%M %p")
        msg = f"{username} submitted {report_name} on {date_str} at {time_str}."

        for approver in approvers:
            approver_profile = get_profile(approver)

            if submitter_location_code and approver_profile and getattr(approver_profile, "location", None):
                approver_location_code = str(
                    approver_profile.location
                ).strip().replace(" ", "").lower()

                if submitter_location_code != approver_location_code:
                    continue

            QANotification.objects.get_or_create(
                user=approver,
                report_log=log,
                defaults={"message": msg},
            )

        return log

    except Exception as e:
        print(f"🔥 Auto Log Failed for {report_name}: {str(e)}")
        traceback.print_exc()
        return None
