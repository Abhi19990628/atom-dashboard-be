from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("api", "0068_alter_notification_user"),
    ]

    operations = [

        # ==========================================================
        # OLD NOTIFICATION TABLE / MODEL REMOVE
        #
        # Old rows are intentionally disposable.
        # Django will DROP api_notification.
        # ==========================================================
        migrations.DeleteModel(
            name="Notification",
        ),

        # ==========================================================
        # FINAL NOTIFICATION MODEL
        #
        # Physical PostgreSQL columns:
        #
        # id
        # message
        # is_read
        # created_at
        # user_id
        #
        # id is both:
        #   PRIMARY KEY
        #   FOREIGN KEY -> IdealTimeSegmentReason.id
        # ==========================================================
        migrations.CreateModel(
            name="Notification",
            fields=[
                (
                    "ideal_event",
                    models.OneToOneField(
                        db_column="id",
                        on_delete=django.db.models.deletion.CASCADE,
                        primary_key=True,
                        related_name="notification",
                        serialize=False,
                        to="api.idealtimesegmentreason",
                    ),
                ),
                (
                    "message",
                    models.TextField(),
                ),
                (
                    "is_read",
                    models.BooleanField(
                        default=False,
                    ),
                ),
                (
                    "created_at",
                    models.DateTimeField(
                        blank=True,
                        null=True,
                    ),
                ),
                (
                    "user",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="submitted_idle_notifications",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "db_table": "api_notification",
            },
        ),
    ]