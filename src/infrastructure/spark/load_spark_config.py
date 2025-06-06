import yaml
from pyspark import SparkConf

def load_spark_config(path: str ="src/config/spark_config.yaml") -> SparkConf:
    with open(path, 'r') as file:
        spark_config = yaml.safe_load(file)
    
    spark_attributes = spark_config['spark']
    conf = SparkConf()
    conf.setAppName(spark_attributes["app_name"]).setMaster(spark_attributes["master"])
    return conf