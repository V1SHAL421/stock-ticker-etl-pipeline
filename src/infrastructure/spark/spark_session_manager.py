"""Setup of Spark configurations"""

from pyspark.sql import SparkSession

from utils.load_spark_config import load_spark_config
from utils.safe_run import safe_run


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

        self.logger.info("Spark session has been initialised")
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
