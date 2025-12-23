import pymysql
pymysql.install_as_MySQLdb()

from .local_celery import app as celery_app

_all_ = ("celery_app",)