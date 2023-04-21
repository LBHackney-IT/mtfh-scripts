import os
from datetime import datetime
from pathlib import Path


class Logger:
    def __init__(self, log_file_name: str = None, log_file_dir: str = None, ):
        self.log_file_name = "logs" if log_file_name is None else log_file_name
        self.log_file_dir = "logs" if log_file_dir is None else log_file_dir
        self.log_file = f"{self.log_file_dir}/{self.log_file_name}.txt"
        self.log(f'\n== Logging started: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")} ==')

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

    @staticmethod
    def log_call(logger):
        def wrapper(func):
            def wrapped(*args, **kwds):
                result = func(*args, **kwds)
                formatted_kwargs = ", ".join([f"{k}={v}" for k, v in kwds.items()])
                logger.log(f"Called {func.__name__}{*args, formatted_kwargs} -> {result}")
                return result

            return wrapped

        return wrapper


if __name__ == "__main__":
    @Logger.log_call(Logger())
    def add_two(a, b):
        return a + b


    _ = add_two(10, b=20)
