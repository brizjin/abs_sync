import logging
import os


def get_logger(logger_name):
    format_string = '%(asctime)s [{0}] [%(levelname)s] [%(processName)s,%(threadName)s] %(funcName)s - %(message)s'
    format_string = format_string.format(logger_name)

    logger = logging.getLogger(logger_name)
    if len(logger.handlers) == 0:
        logger.setLevel(logging.DEBUG)

        file_handler = False
        if file_handler:
            logger_file_name = os.path.join('../logs/', logger_name + '.log')
            if not os.path.exists(os.path.dirname(logger_file_name)):
                os.makedirs(os.path.dirname(logger_file_name))
            file_handler = logging.FileHandler(logger_file_name)

        handlers = [logging.StreamHandler()] + ([file_handler] if file_handler else [])

        formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
        for h in handlers:
            h.setFormatter(formatter)
            h.setLevel(logging.DEBUG)
            logger.addHandler(h)
    return logger
