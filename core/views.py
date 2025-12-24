# ==================== Built-in Modules ====================
import os
import threading
import time
import json
from datetime import datetime as datetime_module, timedelta
from pprint import pprint
import traceback
# ==========================================================

# ==================== Third-party Modules ====================
from django.utils import timezone
from django.conf import settings
from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
# =============================================================

# =============== DataForesight Modules ===============
# =====================================================

# =============== Local Modules ===============
from Reporting_Server.settings import CONNECTOR_TYPE
# =============================================


# Create your views here.

@csrf_exempt
def check_status(request):
    response = {
        "connector_type": CONNECTOR_TYPE,
        "status": "Alive",
    }
    return JsonResponse(response, safe=False)
