from django.contrib import admin

# Register your models here.
from django.contrib import admin
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin
from .models import UserProfile, ReportActivityLog, Notification


# User form ke andar hi Department select karne ka dropdown dikhane ke liye
class UserProfileInline(admin.StackedInline):
    model = UserProfile
    can_delete = False


# Existing UserAdmin ko modify kar rahe hain
class CustomUserAdmin(UserAdmin):
    inlines = (UserProfileInline,)

    @admin.display(description="Plant")
    def plant_location(self, obj):
        profile = getattr(obj, "profile", None)

        if profile:
            return profile.location

        return "-"

    list_display = (
        "username",
        "email",
        "first_name",
        "last_name",
        "plant_location",
        "is_staff",
        "is_active",
    )


# Pehle se registered User admin ko hata kar apna custom admin lagayenge
admin.site.unregister(User)
admin.site.register(User, CustomUserAdmin)


# Logs table ko admin panel mein professional tarike se dekhne ke liye
@admin.register(ReportActivityLog)
class ReportActivityLogAdmin(admin.ModelAdmin):
    list_display = (
        "username",
        "department_name",
        "report_name",
        "timestamp",
    )  # Columns jo admin mein dikhenge
    list_filter = ("department_name", "timestamp")  # Right side mein filters
    search_fields = ("username", "report_name")  # Search bar search karne ke liye


@admin.register(Notification)
class NotificationAdmin(admin.ModelAdmin):

    list_display = (
        "notification_id",
        "plant_location",
        "machine_no",
        "ideal_mode",
        "report_status",
        "user",
        "is_read",
        "created_at",
    )

    list_filter = (
        "is_read",
        "ideal_event__plant_location",
        "ideal_event__ideal_mode",
        "ideal_event__report_status",
    )

    search_fields = (
        "ideal_event__machine_no",
        "message",
        "user__username",
    )

    readonly_fields = (
        "notification_id",
        "ideal_event",
        "created_at",
    )

    ordering = (
        "-ideal_event__ideal_start_at",
    )

    @admin.display(
        description="ID",
        ordering="ideal_event",
    )
    def notification_id(self, obj):
        return obj.pk

    @admin.display(
        description="Plant",
        ordering="ideal_event__plant_location",
    )
    def plant_location(self, obj):
        return obj.ideal_event.plant_location

    @admin.display(
        description="Machine",
        ordering="ideal_event__machine_no",
    )
    def machine_no(self, obj):
        return obj.ideal_event.machine_no

    @admin.display(
        description="Ideal Mode",
        ordering="ideal_event__ideal_mode",
    )
    def ideal_mode(self, obj):
        return obj.ideal_event.ideal_mode

    @admin.display(
        description="Status",
        ordering="ideal_event__report_status",
    )
    def report_status(self, obj):
        return obj.ideal_event.report_status