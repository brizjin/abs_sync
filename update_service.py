import logging
import os
import re

import cx_Oracle
from git import Repo

import config
import dirs
from selects import create_tune_date_update, select_objects_in_folder_or_date_modified

connection_string = "ibs/HtuRhtl@day"

logger = logging.getLogger('root')







def create_jobs():
    os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
    os.environ['NLS_LANG'] = '.AL32UTF8'
    dbs = ["ibs/HtuRhtl@day"]
    for db_cnn_str in dbs:
        db_name = re.match(r"(?P<user>.+)/(?P<pass>.+)@(?P<dbname>.+)", db_cnn_str).group('dbname')
        # os.makedirs(os.path.join('dbs', db_name))
        git_url = "http://git.brc.local:3000/ivan.bryzzhin/abs.git"
        repo_dir = os.path.join('dbs', db_name)

        if not os.path.isdir(os.path.join(repo_dir, '.git')):
            logger.info("clone repo for %s" % db_name)
            Repo.clone_from(git_url, repo_dir)
        repo = Repo(repo_dir)
        logger.info("open repo %s is_dirty: %s" % (db_name, repo.is_dirty()))

        cnn = cx_Oracle.connect(db_cnn_str)
        if not select_tune_date_update(cnn):
            logger.info("База была обновлена. Запишим новую настройку об обновлении")
            create_tune_date_update(cnn)
            date_updated = select_tune_date_update(cnn)
            branch_name = '%s_%s' % (db_name, date_updated.strftime('%Y%m%d_%H%M'))  # %Y%m%d_%H%M%S
            logger.info("checkout new branch %s" % branch_name)
            past_branch = repo.create_head(branch_name, 'master')
            repo.head.reference = past_branch
            repo.head.reset(index=True, working_tree=True)
            # repo.head.reference = past_branch
            # assert not repo.head.is_detached
            # reset the index and working tree to match the pointed-to commit
            # repo.head.reset(index=True, working_tree=True)


# create_jobs()
def job():
    os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
    os.environ['NLS_LANG'] = '.AL32UTF8'
    cnn = cx_Oracle.connect("ibs/HtuRhtl@day")
    logger.info("cnn update_service")
    print(select(cnn, """select m.class_id, m.short_name, m.package_name, m.modified, m.user_modified, s.type, s.line, s.text a--, m.user_driven, m.*
from sources s
inner join METHODS m on m.id = s.name
where m.class_id = 'BRK_MSG' and m.short_name = 'L'
order by m.class_id, m.short_name, s.type, s.line""").groupby(
        ['CLASS_ID', 'SHORT_NAME', 'PACKAGE_NAME', 'MODIFIED', 'USER_MODIFIED', 'TYPE']))


job()
# schedule.every(2).seconds.do(job)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)
