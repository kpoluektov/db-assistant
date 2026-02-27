import logging
from logging.handlers import RotatingFileHandler

class yLogger:
    def __init__(self):
        self.logger = logging.getLogger("openai.agents")
        self.logger.setLevel(logging.DEBUG)

    def initFile(self, fileName):
        file_handler = RotatingFileHandler(
            fileName
        )
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(file_handler)
