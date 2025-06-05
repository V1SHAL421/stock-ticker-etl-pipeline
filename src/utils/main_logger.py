import logging

class MainLogger:
    """
    Represents the main logger

    Attributes:
        _instance (MainLogger or None): Instance of the MainLogger or None
    """
    _instance = None

    def __new__(cls):
        """Returns the MainLogger instance if initialised before, else initialises a new MainLogger instance"""
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._initialise()
        return cls._instance

    def _initialise(
        self, format: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    ):
        """Initialises the logger

        Args:
            format (str, optional): The format of the log. Defaults to %(asctime)s - %(levelname)s - %(name)s - %(message)s"
        """
        self.logger = logging.getLogger("Logger")  # Name the logger as "Logger"
        self.logger.setLevel(logging.INFO)  # Set the logging level to INFO
        handler = logging.StreamHandler()  # Initialise handler
        formatter = logging.Formatter(
            format
        )  # Initialise formatter to the format described in the parameter
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    #
    def get_logger(self):
        """Initialises the logger

        Returns:
            logger (Logger): The logger from the logging module
        """
        return self.logger
