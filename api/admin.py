from django.contrib import admin

# Register your models here.
from django.contrib import admin
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin
from .models import UserProfile, ReportActivityLog

# User form ke andar hi Department select karne ka dropdown dikhane ke liye
class UserProfileInline(admin.StackedInline):
    model = UserProfile
    can_delete = False

# Existing UserAdmin ko modify kar rahe hain
class CustomUserAdmin(UserAdmin):
    inlines = (UserProfileInline, )

# Pehle se registered User admin ko hata kar apna custom admin lagayenge
admin.site.unregister(User)
admin.site.register(User, CustomUserAdmin)

# Logs table ko admin panel mein professional tarike se dekhne ke liye
@admin.register(ReportActivityLog)
class ReportActivityLogAdmin(admin.ModelAdmin):
    list_display = ('username', 'department_name', 'report_name', 'timestamp') # Columns jo admin mein dikhenge
    list_filter = ('department_name', 'timestamp') # Right side mein filters
    search_fields = ('username', 'report_name') # Search bar search karne ke liye