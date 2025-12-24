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
from common.common_utils import (
    collect_customer_dbs,
    get_customer_db_from_user,
    get_connector_obj,
    filter_scan_breakdown_objs,
)
from common.custom_utils import log_info, log_traceback
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



@csrf_exempt
def generate_report(request):
    response = {
        "status": "success",
        "message": "Report generation initiated.",
    }

    if request.method == "POST":
        post_req = request.POST
        report_ids = post_req.getlist("report_ids")
        report_type = post_req.get("report_type")
        current_user = post_req.get("current_user")
        customer_db = get_customer_db_from_user(current_user)

        if not customer_db:
            response["status"] = "error"
            response["message"] = f"Customer database not found for user ({current_user})."
            return JsonResponse(response, status=400)

        elif report_type in ("scan_report", "scan_status_report" ,"data_report", "db_report", "table_report", "column_report", "scan_unit_report", "scan_details_report", "host_details_report", "audit_log_report", "bucket_name_report", "found_data_type_report", "host_user_detail_report", "data_store_report", "asset_inventory_report", "scan_breakdown_report"):
            for report_id in report_ids:
                threading.Thread(
                    target=gen_report,
                    args=(customer_db, report_id),
                    daemon=True,
                    name=f"{report_type}_generation_{report_id}",
                ).start()
                log_info(f"Initiated report ({report_type}) generation: Report ID ({report_id})")

        elif report_type == 'detailed_compliance_report':
            for report_id in report_ids:
                threading.Thread(
                    target=gen_detailed_compliance_report,
                    args=(current_user, report_id),
                    daemon=True,
                    name=f"{report_type}_generation_{report_id}",
                ).start()
                log_info(f"Initiated report ({report_type}) generation: Report ID ({report_id})")

        else:
            response["status"] = "error"
            response["message"] = "Invalid report type specified."
            print(f"Invalid report type: {report_type}")
            return JsonResponse(response, status=400)

    return JsonResponse(response, safe=False)
