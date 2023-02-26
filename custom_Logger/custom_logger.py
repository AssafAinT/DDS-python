import logging
import datetime


class MyLogger:
    @staticmethod
    def Init(logger_name, log_file_path):
        current_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        log_file_path_with_time = f"{log_file_path}_{current_time}.log"
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %('
                                          'levelname)s - %(message)s',
                            handlers=[logging.FileHandler(log_file_path_with_time),
                             logging.StreamHandler()])
