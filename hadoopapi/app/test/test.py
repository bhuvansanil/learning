import pyarrow as pa
import  subprocess
import os
import  json

#classpath = subprocess.Popen(["/home/python/hadoop-2.6.0/bin/hdfs", "classpath", "--glob"], stdout=subprocess.PIPE).communicate()[0]

# CONFIGURE ENVIRONMENT VARIABLES
os.environ["HADOOP_HOME"] = "/home/python/hadoop-2.6.0"
os.environ["JAVA_HOME"] = "/usr/lib/java/jdk1.8.0_131/"
#os.environ["CLASSPATH"] = classpath.decode("utf-8")
os.environ["ARROW_LIBHDFS_DIR"] = "/home/python/hadoop-2.6.0/lib/native"

hdfs = pa.hdfs.connect(host='192.168.101.67', port=8020, user='hdfs')
file_list = hdfs.ls('/frv/frv_trans_l0', detail=True)





hdfs = pa.hdfs.connect(host='192.168.101.67', port=8020, user='impala')
workdir = '/frv/apitesting'
delete_files = f'hdfs://192.168.101.67:8020{workdir}/*'


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
#return {'message':'SuccessFully deleted all Files in Directory','stdout':stdout}

(ret, out, err)= run_cmd(['hdfs', 'dfs', '-ls', 'hdfs://192.168.101.67:8020/frv/apitesting/*'])
print(out)
