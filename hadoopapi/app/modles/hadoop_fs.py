from flask import json, Response, abort, make_response, jsonify
import os
import pyarrow as pa
import subprocess
from app.exception.custom_exception import DeleteError
from app.exception.custom_exception import UploadError
from app.exception.custom_exception import InvalidDirError
import datetime
import set_env
import configparser


def run_cmd(args_list):
    """
    run linux commands
    """
    # import subprocess
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err



class Hadoop:
    def __init__(self):

        abs_path = os.path.abspath(os.path.dirname(__file__))
        config_path = os.path.join(abs_path, '..//..//config//config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)
        self.configs = config
        set_env.set_env()

    def list_files(self):
        hdfs = pa.hdfs.connect(host='192.168.101.67', port=8020, user='impala')
        hdfs_dir = self.configs['hdfs']['dir_name']
        file_list = hdfs.ls(hdfs_dir, detail=True)
        print(file_list)
        return  file_list


    def delete_files(self):
        hdfs_dir = self.configs['hdfs']['dir_name']
        delete_files = f'hdfs://192.168.101.67{hdfs_dir}*'
        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-rm','-skipTrash', delete_files])
        if ret != 0:
            raise DeleteError(f'RC: {ret} Stdout: {out} Error: {err}')
        return {'message':'Files Deleted'}

    def put_files(self,dir_name):
        local_dir = f'/home/python/FRV/{dir_name}'
        date_1 = datetime.datetime.now()
        (ret, out, err) = run_cmd(['sh', '/home/python/FRV/api_load.sh', local_dir])
        if ret != 0:
            raise UploadError(f'Stdout: {out} Error: {err}')
        date_2 = datetime.datetime.now()
        difference = date_2 - date_1
        message = f'Time taken to uplaod file ${difference}'
        return {'message':message}

    def get_local_files(self,dir_name):
        if dir_name not in ['one','multi']:
            raise InvalidDirError("Directory must be one or multi")
        output = []
        work_dir = self.configs['local']['base_dir']
        abs_path = f'{work_dir}{dir_name}'
        with os.scandir(abs_path) as entries:
            for entry in entries:
                size = os.path.getsize(abs_path +'/' +entry.name)
                x = {'name': entry.name, 'size': size}
                output.append(x)
        return output
