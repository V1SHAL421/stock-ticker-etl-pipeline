import boto3
import yaml
from pyspark import SparkConf

from utils.safe_run import safe_run


@safe_run()
def inject_boto3_sso_credentials(conf, profile_name="vishal-sso"):
    session = boto3.Session(profile_name=profile_name)
    creds = session.get_credentials().get_frozen_credentials()

    conf.set("fs.s3a.access.key", creds.access_key)
    conf.set("fs.s3a.secret.key", creds.secret_key)
    conf.set("fs.s3a.session.token", creds.token)
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )


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

    conf.set("spark.jars.packages", spark_attributes["jars_packages"])
    for key, value in spark_attributes["hadoop_aws_configs"].items():
        conf.set(key, value)

    # conf.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    # conf.set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    # conf.set("fs.s3a.session.token", os.environ["AWS_SESSION_TOKEN"])
    # conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

    inject_boto3_sso_credentials(conf, profile_name="vishal-sso")

    return conf
