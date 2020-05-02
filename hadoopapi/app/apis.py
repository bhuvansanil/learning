"""
This is the default service for heartbeat check for this app
"""
from flask import make_response, request
from app import app
@app.route("/heartbeat/", methods=['GET'])
def heartbeat():
    """
    This is the default service for heartbeat check for this app
    DO NOT DELETE THIS
    :return:
    """
    return make_response("I am up from Hadoop service", 200)
