import pytz
from django.utils.timezone import now


def apply_date_filter(request, queryset, date_field):
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")

    if start_date == "all":
        return queryset

    if not start_date and not end_date:
        ist_tz = pytz.timezone("Asia/Kolkata")
        today_str = now().astimezone(ist_tz).strftime("%Y-%m-%d")

        if date_field == "created_at":
            return queryset.filter(**{f"{date_field}__date": today_str})

        return queryset.filter(**{date_field: today_str})

    if start_date and end_date:
        if date_field == "created_at":
            return queryset.filter(**{f"{date_field}__date__range": [start_date, end_date]})

        return queryset.filter(**{f"{date_field}__range": [start_date, end_date]})

    if start_date:
        if date_field == "created_at":
            return queryset.filter(**{f"{date_field}__date__gte": start_date})

        return queryset.filter(**{f"{date_field}__gte": start_date})

    if end_date:
        if date_field == "created_at":
            return queryset.filter(**{f"{date_field}__date__lte": end_date})

        return queryset.filter(**{f"{date_field}__lte": end_date})

    return queryset
