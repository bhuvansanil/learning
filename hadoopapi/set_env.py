import  os
def set_env():
    os.environ["HADOOP_HOME"] = "/home/python/hadoop-2.6.0"
    os.environ["JAVA_HOME"] = "/usr/lib/java/jdk1.8.0_131/"
    os.environ["ARROW_LIBHDFS_DIR"] = "/home/python/hadoop-2.6.0/lib/native"
