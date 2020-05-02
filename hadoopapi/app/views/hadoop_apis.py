from flask import request, make_response, jsonify, abort
from app import app
from app.modles.hadoop_fs import Hadoop
import http


@app.route('/hadoop/hdfs', methods=['GET'])
def hdfs_get(**kwargs):
    try:
        hadoop = Hadoop()
        response = make_response(jsonify(hadoop.list_files()))
    except Exception as e:
        response = make_response(jsonify({"ERROR": str(e)}), http.HTTPStatus.INTERNAL_SERVER_ERROR)
    return response


@app.route('/hadoop/hdfs', methods=['DELETE'])
def hdfs_delete(**kwargs):
    try:
        hadoop = Hadoop()
        rep = hadoop.delete_files()
        response = make_response(jsonify(rep))
    except Exception as e:
        response = make_response(jsonify({"ERROR File Cannot be deleted": str(e)}), http.HTTPStatus.INTERNAL_SERVER_ERROR)
    return response

@app.route('/hadoop/hdfs/<dir_name>', methods=['PUT'])
def hdfs_put(dir_name,**kwargs):
    try:
        hadoop = Hadoop()
        rep = hadoop.put_files(dir_name)
        response = make_response(jsonify(rep))
    except Exception as e:
        response = make_response(jsonify({"ERROR File Cannot be Uploaded": str(e)}), http.HTTPStatus.INTERNAL_SERVER_ERROR)
    return response


@app.route('/local/<dir_name>', methods=['GET'])
def local_get(dir_name,**kwargs):
    try:
        hadoop = Hadoop()
        rep = hadoop.get_local_files(dir_name)
        response = make_response(jsonify(rep))
    except Exception as e:
        response = make_response(jsonify({"ERROR While getting Local Dir": str(e)}), http.HTTPStatus.INTERNAL_SERVER_ERROR)
    return response










