import os
import unittest

import cx_Oracle
from git import Repo

import config
import log
from selects import *

git_url = "http://git.brc.local:3000/ivan.bryzzhin/abs.git"
os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
os.environ['NLS_LANG'] = '.AL32UTF8'
db_cnn_str = "ibs/HtuRhtl@midabs"
cnn = cx_Oracle.connect(db_cnn_str)
db_name = cnn.dsn

logger = log.log_init("root")


def clone_or_open_repo(folder_path):
    logger.info("check repo " + folder_path)
    if not os.path.isdir(os.path.join(folder_path, '.git')):
        logger.info("clone repo for %s" % db_name)
        repo = Repo.clone_from(git_url, folder_path)
    else:
        repo = Repo(folder_path)
    logger.info("open repo %s is_dirty: %s" % (db_name, repo.is_dirty()))
    return repo


class GitNewDatabaseTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        delete_tune_date_update(cnn)

    # @classmethod
    # def tearDownClass(cls):
    #     delete_tune_date_update(cnn)
    #     pass

    def test_check_if_database_is_updated(self):
        if not select_tune_date_update(cnn):
            create_tune_date_update(cnn)
            tune_date_update = select_tune_date_update(cnn)
            print(tune_date_update)

    def test_master_commit_on_updated_database(self):
        pass

    def test_clone_repo(self):
        repos_dir = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script"
        repo_dir = os.path.join(repos_dir, 'dbs', db_name)
        clone_or_open_repo(repo_dir)

    def test_checkout_master(self):
        if not select_tune_date_update(cnn):
            date_updated = create_tune_date_update(cnn)

            git_dir = os.path.join(config.git_folder, cnn.dsn)

            branch_name = '%s_%s' % (db_name, date_updated.strftime('%d.%m.%Y_%H.%M'))  # %Y%m%d_%H%M%S
            repo = clone_or_open_repo(git_dir)

            past_branch = repo.create_head(branch_name, 'master')
            repo.head.reference = past_branch
            repo.head.reset(index=True, working_tree=True)
            logger.info("checkout new branch %s" % branch_name)

            # Сохраним тексты
            df = select_objects_in_folder_or_date_modified(cnn, git_dir, 1, 'd')
            dirs.write_object_from_df(df, git_dir)

            # Закомитим
            # repo.git.add(update=True)
            repo.git.add(A=True)
            index = repo.index

            from git import Actor
            author = Actor("An author", "author@example.com")
            committer = Actor("A committer", "committer@example.com")

            index.commit("my commit message", author=author, committer=committer)