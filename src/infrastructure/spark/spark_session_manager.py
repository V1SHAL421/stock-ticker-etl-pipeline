"""Setup of Spark configurations"""

from pyspark.sql import SparkSession

from utils.load_spark_config import load_spark_config
from utils.safe_run import safe_run
import os


class SparkSessionManager:
    """Represents the Spark Session Manager

    Attributes:
        - spark_session (SparkSession or None): Contains the singleton instance of the SparkSession"""

    spark_session = None

    def __init__(self, logger):
        self.logger = logger

    @safe_run()
    def get_spark_session(self) -> SparkSession:
        """Gets the current SparkSession or builds a new one

        Returns:
            - spark_session (SparkSession): The current or new SparkSession
        """
        if not SparkSessionManager.spark_session:
            self.logger.info("Creating Spark Session")
            SparkSessionManager.spark_session = SparkSession.builder.config(
                conf=load_spark_config()
            ).getOrCreate()

        os.environ["AWS_PROFILE"] = "vishal-sso"  # Sets AWS profile

        # Sets S3A credentials provider to use the profile credentials
        SparkSessionManager.spark_session.sparkContext._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider",
        )
        """
        The AWS_PROFILE environment variable is set to use AWS SSO profile so that Spark uses the temporary credentials
        The S3A connector for Spark is configured to use the ProfileCredentialsProvider class, which looks up credentials
        from the AWS CLI profile.
        """

        self.logger.info("Spark session has been initialised")

        # for k, v in SparkSessionManager.spark_session.sparkContext.getConf().getAll():
        #     print(f"{k} = {v}")

        # for k, v in SparkSessionManager.spark_session.sparkContext._conf.getAll():
        #     if isinstance(v, str) and "24h" in v:
        #         print(f"{k} = {v}")

        # hadoop_conf = SparkSessionManager.spark_session.sparkContext._jsc.hadoopConfiguration()
        # for item in hadoop_conf.iterator():
        #     key = item.getKey()
        #     value = item.getValue()
        #     if "24h" in value.lower():
        #         print(f"{key} = {value}")

        return SparkSessionManager.spark_session

    @safe_run()
    def stop_spark_session(self):
        """Stops the current SparkSession

        Raises:
            - Exception: If there is no current SparkSession
        """
        if SparkSessionManager.spark_session is None:
            raise Exception("Spark Session has not been initialised")

        SparkSessionManager.spark_session.stop()
        SparkSessionManager.spark_session = None
