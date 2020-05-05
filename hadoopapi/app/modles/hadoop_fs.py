from flask import json, Response, abort, make_response, jsonify
import os
import pyarrow as pa
os.environ["JAVA_HOME"] = "C://Program Files//Java//jdk1.8.0_151//"
os.environ["HADOOP_HOME"] = "C://Users//Bhuvan//hadoop//hadoop-2.6.0//"
os.environ["CLASSPATH"] = "$JAVA_HOME/bin:$HADOOP_HOME/bin"

class Hadoop:
    def __init__(self):
        self.workdir = '/frv/frv_trans_l0'

    def list_files(self):

        hdfs = pa.hdfs.connect(host='192.168.101.67', port=8020,user='impala')
        file_list = hdfs.ls(self.workdir, detail=True)
        print(file_list)
