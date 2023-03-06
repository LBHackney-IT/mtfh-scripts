import os
from pathlib import Path


class Logger:
    def __init__(self, log_file_name: str = None, log_file_dir: str = None, ):
        self.log_file_name = "logs" if log_file_name is None else log_file_name
        self.log_file_dir = "logs" if log_file_dir is None else log_file_dir
        self.log_file = f"{self.log_file_dir}/{self.log_file_name}.txt"

    def log(self, message: str, end="\n"):
        """
        :param message: Message to log
        :param end: String to end the log message with
        """
        # Create the log file if it doesn't exist

        os.makedirs(Path(self.log_file_dir), exist_ok=True)

        with open(self.log_file, "a") as outfile:
            print(message, file=outfile, end=end)
        print(message, end=end)
