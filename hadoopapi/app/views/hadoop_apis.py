from flask import request, make_response, jsonify, abort
from app import app
from app.modles.hadoop_fs import  Hadoop


@app.route("/hadoop/list", methods=['GET', 'POST'])
def project_method(**kwargs):
    hadoop = Hadoop()
    hadoop.list_files()





