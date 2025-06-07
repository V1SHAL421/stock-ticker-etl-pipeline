import yaml
from pyspark import SparkConf

from utils.safe_run import safe_run


@safe_run()
def load_spark_config(path: str = "src/config/spark_config.yaml") -> SparkConf:
    """Loads the Spark configuration using fields from the Spark configuration YAML file

        Args:
            - path: The file path of the Spark configuration YAML file
        """
    with open(path, "r") as file:
        spark_config = yaml.safe_load(file)

    spark_attributes = spark_config["spark"]
    conf = SparkConf()
    conf.setAppName(spark_attributes["app_name"]).setMaster(spark_attributes["master"])
    return conf
