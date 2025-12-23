# =============== Built-in Modules ===============
import os
# ================================================

# =============== Third-Party Modules ===============
import django
from celery import Celery
# ===================================================

# =============== Local Modules ===============
from Reporting_Server.settings import (
    BROKER_URL,
    BROKER_RESULT_BACKEND,
    BROKER_TASK_SOFT_TIME_LIMIT,
    BROKER_TASK_EXPIRES,
    BROKER_TASK_TIME_LIMIT,
)
# =============================================


# Set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Reporting_Server.settings")

app = Celery("Reporting_Server")

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

app.conf.update(
    {
        "broker_url": BROKER_URL,
        "result_backend": BROKER_RESULT_BACKEND,
        "task_soft_time_limit": BROKER_TASK_SOFT_TIME_LIMIT,
        "task_time_limit": BROKER_TASK_TIME_LIMIT,
        "imports": (
            # "admin_panel.views",
            # "data_discovery.views",
            # "data_analysis.views",
            # "reports.views",
        ),
        "task_routes": {
            "core.tasks.scan_files": {"queue": "cloud_scanner"},
        },
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
    }
)

