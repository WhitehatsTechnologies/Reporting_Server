# =============== Built-in Modules ===============
import os
import sys
from pathlib import Path
import multiprocessing
import traceback
from pprint import pprint
# ================================================

# ================ Third-Party Modules ===============
from dotenv import load_dotenv
# ====================================================

SYS_CORE_COUNT = multiprocessing.cpu_count()

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# General configuration file path.
gen_config_file = os.path.join(BASE_DIR, "configs/gen_config.env")

# Loading the environment variables from the .env file.
load_dotenv(gen_config_file)

DATAFORESIGHT_PATH = os.getenv("DATAFORESIGHT_PATH")
sys.path.append(DATAFORESIGHT_PATH)
print(f"DATAFORESIGHT_PATH: {DATAFORESIGHT_PATH}")

# ================ DataForesight Modules ===============
from common.config_utils import read_db_config
from common.custom_utils import get_private_ip
# ======================================================


SECRET_KEY = os.getenv("SECRET_KEY", "pRmgMa8T0INjEAfksaq2aafzoZXEuwKI7wDe4c1F8AY=")

DEBUG = os.getenv("DEBUG", "False") == "True"
print(f"DEBUG: {DEBUG}")

ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "*").split(",")
print(f"ALLOWED_HOSTS: {ALLOWED_HOSTS}")

# Application definition
INSTALLED_APPS = [
    # Django Apps
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # DataForesight Apps
    'accounts',
    'admin_panel',
    'data_discovery',
    'data_analysis',
    'reports',
    # Reporting Server Apps
    'core',
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "Reporting_Server.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "Reporting_Server.wsgi.application"


# Database
CONN_MAX_AGE = os.getenv("CONN_MAX_AGE")  # in seconds
if CONN_MAX_AGE is not None:
    CONN_MAX_AGE = int(CONN_MAX_AGE)

CONN_HEALTH_CHECKS = os.getenv("CONN_HEALTH_CHECKS", "True") == "True"

###### Database Settings #####
try:
    db_config_file = os.path.join(BASE_DIR, "configs/db_config.env")
    db_config = read_db_config(db_config_file=db_config_file)

    DB_ENGINE = db_config["DB_Engine"]

    if DB_ENGINE.lower() == "mysql":
        DB_ENGINE = "django.db.backends.mysql"
        DB_DRIVER = None
    elif DB_ENGINE.lower() == "mssql":
        DB_ENGINE = "mssql"
        DB_DRIVER = db_config["DB_Driver"]
    elif DB_ENGINE.lower() == "pgsql":
        DB_ENGINE = "django.db.backends.postgresql"
        DB_DRIVER = None
        
    DB_NAME = db_config["DB_Name"]
    DB_USER = db_config["DB_User"]
    DB_PASSWORD = db_config["DB_Password"]
    DB_HOST = db_config["DB_Host"]
    DB_PORT = db_config["DB_Port"]
    print(
        f"DB_ENGINE: {DB_ENGINE}, DB_NAME: {DB_NAME}, DB_USER: {DB_USER}, DB_HOST: {DB_HOST}, DB_PORT: {DB_PORT}"
    )

except Exception:
    # print(traceback.format_exc())
    DB_ENGINE = ""
    DB_NAME = ""
    DB_USER = ""
    DB_PASSWORD = ""
    DB_HOST = ""
    DB_PORT = ""

try:
    DATABASES = {
        "default": {
            "ENGINE": DB_ENGINE,
            "NAME": DB_NAME,
            "USER": DB_USER,
            "PASSWORD": DB_PASSWORD,
            "HOST": DB_HOST,
            "PORT": DB_PORT,
            "CONN_MAX_AGE": CONN_MAX_AGE,
            'CONN_HEALTH_CHECKS': CONN_HEALTH_CHECKS,
        }
    }

    if DB_ENGINE.lower() == "mssql":
        DATABASES["default"] |= {"OPTIONS": {"driver": DB_DRIVER}}

    # print('========== Databases ==========')
    # pprint(DATABASES)
    # print('===============================')

except Exception:
    print(traceback.format_exc())




# Password validation
# https://docs.djangoproject.com/en/5.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.0/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.0/howto/static-files/

STATIC_URL = "static/"

MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / "media"

# Default primary key field type
# https://docs.djangoproject.com/en/5.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


PRIVATE_IP = os.getenv("PRIVATE_IP")
if not PRIVATE_IP:
    if DEBUG:
        PRIVATE_IP = "127.0.0.1"
    else:
        PRIVATE_IP = get_private_ip()
print(f"PRIVATE_IP: {PRIVATE_IP}")

CONNECTOR_TYPE = "Reporting_Server"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

BROKER_URL = os.getenv("BROKER_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")
BROKER_RESULT_BACKEND = os.getenv("BROKER_RESULT_BACKEND", f"redis://{REDIS_HOST}:{REDIS_PORT}/1")
BROKER_TASK_SOFT_TIME_LIMIT = int(os.getenv("BROKER_TASK_SOFT_TIME_LIMIT", 86400))  # in seconds
BROKER_TASK_TIME_LIMIT = int(os.getenv("BROKER_TASK_TIME_LIMIT", 86400))  # in seconds
BROKER_TASK_EXPIRES = int(os.getenv("BROKER_TASK_EXPIRES", 86400))  # in seconds

WORKER_LOG_LEVEL = os.getenv("WORKER_LOG_LEVEL", "info")
WORKER_POOL_TYPE = os.getenv("WORKER_POOL_TYPE", "threads")

TOTAL_WORKER_COUNT = int(os.getenv("TOTAL_WORKER_COUNT", SYS_CORE_COUNT))

WAITRESS_CONNECTION_LIMIT = int(os.getenv("WAITRESS_CONNECTION_LIMIT", 1000))
WAITRESS_MAX_REQUEST_BODY_SIZE = int(os.getenv("WAITRESS_MAX_REQUEST_BODY_SIZE", 1024 * 1024 * 1024 * 10))  # in bytes
WAITRESS_MAX_THREAD = int(os.getenv("WAITRESS_MAX_THREAD", 100))
