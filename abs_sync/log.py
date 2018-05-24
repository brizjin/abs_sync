import logging

import os


def log_init(logger_name):
    # logging.basicConfig(level=logging.INFO,
    #                     format='%(asctime)-15s PID %(process)5s %(threadName)10s %(name)18s: %(message)s')
    # log_parse_html = logging.getLogger('logger')
    # log_parse_html.info("beg")

    # create logger with 'spam_application'
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages

    logs_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    fh = logging.FileHandler(os.path.join(logs_dir, logger_name + '.log'))
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter(
        '%(asctime)-15s %(levelname)s PID %(process)5s %(threadName)10s %(name)18s: %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
