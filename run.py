import logging
import re
import time

import schedule

from abs_sync import git_funcs
from abs_sync.config import dbs

logger = logging.getLogger(__name__)


def do_schedule(connection_string):
    schedule.every(1).hours.do(git_funcs.update, connection_string)
    git_funcs.update(connection_string)


def schedule_all_db():
    for connection_string in dbs.values():
        do_schedule(connection_string)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    schedule_all_db()
