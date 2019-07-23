import logging
import os

import pandas as pd
from database_client.yml_config import load_logging_config, load_yaml_file_as_dict, DotDict

logger = logging.getLogger(__name__)


def load_config(config_file_name):
    config_dict = load_yaml_file_as_dict(config_file_name)
    globals().update(config_dict)
    return DotDict(config_dict)


def load_oracle_config(dict_config):
    os.environ["ORACLE_HOME"] = dict_config.get('ORACLE_HOME') or os.environ.get("ORACLE_HOME")
    os.environ['NLS_LANG'] = '.AL32UTF8'


def load_pandas_config(pandas_config_dict: dict):
    if pandas_config_dict:
        for key, value in pandas_config_dict.items():
            logger.debug(f"key={key},value={value}")
            pd.set_option(key, value)


# Переменные которые заполняться из config.yml
DEFAULT_TNS = None
TNS = {}
GIT_URL = None
# путь до папки где лежит проект с текстами, куда будут сохраняться тексты при ручном скачивании комиттов
WORKING_GIT_FOLDER = None

# кол-во дней от текущей даты на базе
# которое учитывается чтобы обновить объект
# например по событию компиляции
# объекты старше сохраняться по компиляции не будут
DAYS_WHEN_OBJECT_MODIFIED_UPDATE_FROM_ACTION = 30

# кол-во дней, за которое грузятся обновленные объекты
# при перезапуске джоба
DAYS_UPDATE_ON_START = 7
USERS_TO_SAVE_OBJECTS_IN_GIT = []
#

TESTS_FOLDER = os.path.dirname(__file__)
PROJECT_ROOT_FOLDER = os.path.dirname(os.path.realpath(os.path.join(__file__, '..')))
CONFIG_FILE_NAME = os.path.join(PROJECT_ROOT_FOLDER, "config.yml")
LOG_FOLDER = os.path.join(PROJECT_ROOT_FOLDER, "logs")
FOLDER_FOR_DB_GIT_FOLDERS = os.path.join(PROJECT_ROOT_FOLDER, 'dbs')

config = load_config(CONFIG_FILE_NAME)
load_logging_config(config.LOGGING, LOG_FOLDER)
load_oracle_config(config)
load_pandas_config(config.get('pandas'))

# вычисляемые параметры после загрузки config.yml
DEFAULT_CONNECTION_STRING = TNS[DEFAULT_TNS]

# список пользователей
# операции которых сохраняются в гит
USERS_TO_SAVE_OBJECTS_IN_GIT = list(map(str.upper, USERS_TO_SAVE_OBJECTS_IN_GIT))
