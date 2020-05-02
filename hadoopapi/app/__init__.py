"""
base init file
"""
# pylint:disable=wrong-import-position
from flask import Flask

app = Flask(__name__)
from app.views import hadoop_apis
from . import apis