import os
import unittest

import cx_Oracle
from git import Actor

import config
from dirs import write_row
from git_funcs import clone_or_open_repo, init_git_db
from selects import *

git_url = "http://git.brc.local:3000/ivan.bryzzhin/abs.git"
os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
os.environ['NLS_LANG'] = '.AL32UTF8'
db_cnn_str = "ibs/HtuRhtl@midabs"
cnn = cx_Oracle.connect(db_cnn_str)
db_name = cnn.dsn

# logger = log.log_init("root")
# logger.info("cnn get_test")
logger = logging.getLogger('root')


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

    def test_inti_git(self):
        init_git_db(cnn)

    def test_update_git(self):
        git_dir = os.path.join(config.git_folder, cnn.dsn)
        repo = clone_or_open_repo(git_dir)

        def commit(repo, msg, author_name, author_mail, commiter_name, commiter_mail):
            repo.git.add(A=True)
            index = repo.index
            author = Actor(author_name, author_mail)
            committer = Actor(commiter_name, commiter_mail)
            index.commit(msg, author=author, committer=committer)

        last_date_update = '27.09.2017 18:30:30'
        df = select_all_by_date_modified(cnn, last_date_update)
        df = df.sort_values('MODIFIED', ascending=False)
        prev_user = None
        i = 1
        msg_template = "autosave"
        msg = msg_template
        for ids, row in df.iterrows():
            current_user = row['USER_MODIFIED']
            logger.debug("prev_user=%s, current_user=%s" % (prev_user, current_user))
            if i == 1:
                i += 1
            elif prev_user != current_user and repo.is_dirty():
                logger.debug("commit")
                commit(repo, msg.rstrip(','), prev_user, (prev_user or '') + '@mail', 'sources_sync_job', '@mail')
                msg = msg_template
            msg += ' %s.%s,' % (row['CLASS_ID'], row['SHORT_NAME'])
            write_row(os.path.join(config.git_folder, cnn.dsn), row)
            prev_user = current_user
        commit(repo, msg.rstrip(','), prev_user, (prev_user or '') + '@mail', 'sources_sync_job', '@mail')

        print(df)

    def test_update_git2(self):
        git_dir = os.path.join(config.git_folder, cnn.dsn)
        repo = clone_or_open_repo(git_dir)

        def commit(repo, msg, author_name, author_mail, commiter_name, commiter_mail):
            repo.git.add(A=True)
            index = repo.index
            author = Actor(author_name, author_mail)
            committer = Actor(commiter_name, commiter_mail)
            index.commit(msg, author=author, committer=committer)

        last_date_update = '27.09.2017 18:30:30'
        df = select_all_by_date_modified(cnn, last_date_update)
        df = df.sort_values('MODIFIED', ascending=False)
        prev_user = None
        i = 1
        msg_template = "autosave "
        msg = msg_template

        group = []
        for ids, row in df.iterrows():
            current_user = row['USER_MODIFIED']
            logger.debug("prev_user=%s, current_user=%s" % (prev_user, current_user))
            if i == 1:
                i += 1
            elif prev_user != current_user:

                group = pd.DataFrame(group)
                #print(group)
                msg = msg_template
                for class_id, class_rows in group.groupby(['CLASS_ID']):
                    #print(class_id)
                    msg += class_id + " ["
                    for (object_class_id, short_name), object_rows in class_rows.groupby(['CLASS_ID', 'SHORT_NAME']):
                        msg += short_name + ", "
                    msg = msg.strip(", ") + "]"
                print(msg)
                        #print(object_key, object_rows)
                        # for class_name, class_objects in pd.DataFrame(group_rows).groupby('CLASS_ID'):
                        #     print(class_name)
                group = []
                if repo.is_dirty():
                    logger.debug("commit")
                    commit(repo, msg, prev_user, (prev_user or '') + '@mail', 'sources_sync_job', '@mail')
                # msg = msg_template
            # msg += ' %s.%s,' % (row['CLASS_ID'], row['SHORT_NAME'])
            write_row(os.path.join(config.git_folder, cnn.dsn), row)
            prev_user = current_user
            # group = pd.concat(group, row) if group is not None else row
            # group = group.append(row) if group is not None else row
            group.append(row)
        commit(repo, msg, prev_user, (prev_user or '') + '@mail', 'sources_sync_job', '@mail')

        print(df)

    def test_None(self):
        print(None != None)
