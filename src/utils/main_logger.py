import logging

class MainLogger:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._initialise()
        return cls._instance
    
    def _initialise(self, format: str ="%(asctime)s - %(levelname)s - %(name)s - %(message)s"):
        self.logger = logging.getLogger("Logger")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(format)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def get_logger(self):
        return self.logger
