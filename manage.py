# =============== Built-in Modules ===============
import os
import sys
import time
import threading
import multiprocessing
from multiprocessing import Process
import traceback
from getpass import getpass
# ================================================

# =============== Third-Party Modules ===============
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Reporting_Server.settings")
django.setup()

from django.core.cache import cache
from waitress import serve
# ===================================================

# =============== Local Modules ===============
from Reporting_Server.settings import (
    BASE_DIR,
    DATABASES,
    PRIVATE_IP,
    CONNECTOR_TYPE,
    SYS_CORE_COUNT,
    TOTAL_WORKER_COUNT,
    WORKER_LOG_LEVEL,
    WORKER_POOL_TYPE,
    WAITRESS_CONNECTION_LIMIT,
    WAITRESS_MAX_REQUEST_BODY_SIZE,
    WAITRESS_MAX_THREAD,
)
from Reporting_Server.wsgi import application
from Reporting_Server.local_celery import app as celery_app
# =============================================

# =============== DataForesight Modules ===============
from common.config_utils import add_db_config
from common.custom_utils import log_traceback
from common.common_utils import (
    collect_customer_dbs, start_worker, update_connector_info
)
# =====================================================


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Reporting_Server.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    
    if "add_db" not in sys.argv and "shell" not in sys.argv and "run_workers" not in sys.argv:
        # Updaing connector object's info.
        for customer_db in DATABASES.keys():
            try:
                kwargs = {
                    "customer_db": customer_db,
                    "connector_type": CONNECTOR_TYPE,
                    "connector_host": PRIVATE_IP,
                    "total_worker_count": TOTAL_WORKER_COUNT,
                }
                thread = threading.Thread(
                    target=update_connector_info,
                    kwargs=kwargs,
                    daemon=True
                )
                thread.start()
            except Exception:
                log_traceback(customer_db=customer_db)


    if "add_db" in sys.argv:
        add_db_config(db_config_file=f"{BASE_DIR}/configs/db_config.env")


    elif "run_prod" in sys.argv:
        host_port = sys.argv[2].split(":")
        host = host_port[0]
        port = host_port[1]
        print("Host:", host)
        print("Port:", port)
        print("Running server...")
        # Serving the application using waitress.
        serve(
            app=application,
            host=host,
            port=port,
            threads=WAITRESS_MAX_THREAD,
            max_request_body_size=WAITRESS_MAX_REQUEST_BODY_SIZE,
            connection_limit=WAITRESS_CONNECTION_LIMIT,
        )
        print("Server exited.")
    

    elif "run_workers" in sys.argv:
        num_workers = TOTAL_WORKER_COUNT
        print(f"Starting {num_workers} workers...")

        processes = []
        for i in range(num_workers):
            worker_id = i + 1
            kwargs = {
                "celery_app": celery_app,
                "hostname": "DataForesight",
                "worker_id": worker_id,
                "queue_names": ["cloud_scanner"],
                "log_level": WORKER_LOG_LEVEL,
                "pool_type": WORKER_POOL_TYPE,
            }
            process = Process(
                target=start_worker, 
                kwargs=kwargs
            )
            processes.append(process)
            print(f"Started Worker: {worker_id}")
            process.start()
            time.sleep(1)

        for process in processes:
            try:
                process.join()
            except KeyboardInterrupt:
                print("Main process interrupted. Terminating workers...")
                for p in processes:
                    try:
                        p.terminate()
                    except Exception:
                        pass
                sys.exit(0)
            

    elif "shell" in sys.argv:
        entry = getpass(': ')
        if entry == 'Whitehats@2025':
            execute_from_command_line(sys.argv)
        else:
            print('Access Denied!!!')
            sys.exit(0)


    else:
        execute_from_command_line(sys.argv)


if __name__ == '__main__':
    multiprocessing.freeze_support()

    # Collecting customer DBs.
    try:
        databases = collect_customer_dbs(databases=DATABASES)
    except Exception:
        print(traceback.format_exc())

    # Clearing old cache if any.
    try:
        print("Clearing old cache...")
        cache.clear()
        print("Old cache cleared.")
    except Exception:
        print(traceback.format_exc())
    
    # Running the main function.
    main()

