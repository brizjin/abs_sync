# coding=utf-8
import datetime
import re
import time
import unittest

import cx_Oracle
import schedule
from git import Actor

import git_funcs
import log
from dirs import write_object_from_row
from git_funcs import clone_or_open_repo
from selects import *

git_url = "http://git.brc.local:3000/ivan.bryzzhin/abs.git"
os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
os.environ['NLS_LANG'] = '.AL32UTF8'
# db_cnn_str = "ibs/HtuRhtl@mideveryday"
# cnn = cx_Oracle.connect(db_cnn_str)
# db_name = cnn.dsn

# logger = log.log_init("root")
# logger.info("cnn get_test")
logger = logging.getLogger('root')

# last_date_update = datetime.datetime.now() - datetime.timedelta(days=1)
last_date_update = None


class GitNewDatabaseTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass
        # delete_tune_date_update(cnn)

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

    def test_update_git(self):
        git_dir = os.path.join(config.git_folder, cnn.dsn)
        repo = clone_or_open_repo(git_dir)
        # checkout_branch(repo, )

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
                git_funcs.commit(repo, msg.rstrip(','), prev_user, (prev_user or '') + '@mail', 'sources_sync_job',
                                 '@mail')
                msg = msg_template
            msg += ' %s.%s,' % (row['CLASS_ID'], row['SHORT_NAME'])
            write_object_from_row(os.path.join(config.git_folder, cnn.dsn), row)
            prev_user = current_user
        git_funcs.commit(repo, msg.rstrip(','), prev_user, (prev_user or '') + '@mail', 'sources_sync_job', '@mail')

        print(df)

    def test_update_git2(self):
        git_dir = os.path.join(config.git_folder, cnn.dsn)
        repo = clone_or_open_repo(git_dir)
        if not select_tune_date_update(cnn):
            create_tune_date_update(cnn)

        # git_funcs.checkout_branch(repo, git_funcs.tune_branch_name(cnn))
        repo.heads[git_funcs.tune_branch_name(cnn)].checkout()

        last_date_update = '27.09.2017 18:30:30'
        df = select_all_by_date_modified(cnn, last_date_update)
        # df = df.sort_values('MODIFIED', ascending=True)
        prev_user = None
        i = 1

        def commit_group(repo, group):
            dfg = pd.DataFrame(group)
            msg = "autosave "
            msg += ", ".join(
                "%s [%s]" % (class_id, ", ".join(name for name, rows in class_rows.groupby('SHORT_NAME')))
                for class_id, class_rows in dfg.groupby('CLASS_ID'))
            if repo.is_dirty() or repo.untracked_files:
                repo.git.add(A=True)
                author = Actor(prev_user or 'Неизвестный автор', (prev_user or 'Неизвестный автор') + '@mail')
                committer = Actor('sources_sync_job', '@mail')
                repo.index.commit(msg, author=author, committer=committer)
                # git_funcs.commit(repo, msg, prev_user, (prev_user or '') + '@mail', 'sources_sync_job', '@mail')
            return msg

        group = []
        for ids, row in df.iterrows():
            current_user = row['USER_MODIFIED']
            logger.debug("prev_user=%s, current_user=%s" % (prev_user, current_user))
            if i == 1:
                i += 1
            elif prev_user != current_user:
                logger.debug("commit")
                commit_group(repo, group)
                group = []
            write_object_from_row(os.path.join(config.git_folder, cnn.dsn), row)
            prev_user = current_user
            group.append(row)
        commit_group(repo, group)
        print(df)

    def test_None(self):
        print(None != None)

    def test_comit_by_date(self):
        git_dir = os.path.join(config.git_folder, cnn.dsn)
        repo = clone_or_open_repo(git_dir)
        if not select_tune_date_update(cnn):
            create_tune_date_update(cnn)

        # git_funcs.checkout_branch(repo, git_funcs.tune_branch_name(cnn))
        repo.heads[git_funcs.tune_branch_name(cnn)].checkout()

        last_date_update = '27.09.2017 18:30:30'
        df = select_all_by_date_modified(cnn, last_date_update)
        git_funcs.commit_by_dataframe(repo, df, cnn.dsn)
        print(df)

    def test_comit_by_job(self):
        logger.debug("start")

        def update():
            global last_date_update
            df = None
            if not last_date_update:
                last_date_update = select_max_object_date_modified(cnn)["MODIFIED"][0] - datetime.timedelta(days=7)

            repo = clone_or_open_repo(os.path.join(config.git_folder, cnn.dsn))

            if not select_tune_date_update(cnn):
                logger.debug("update init_git_db")
                create_tune_date_update(cnn)
                df = select_objects_in_folder_or_date_modified(cnn, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
            else:
                logger.debug("select object on date %s" % last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
                df = select_all_by_date_modified(cnn, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
            if len(df) > 0:
                logger.debug("update by_time")
                branch_name = git_funcs.tune_branch_name(cnn)
                if branch_name in repo.branches:
                    repo.heads[branch_name].checkout()
                else:
                    repo.create_head(branch_name, 'master').checkout()
                git_funcs.commit_by_dataframe(repo, df, cnn.dsn)
                last_date_update = select_max_object_date_modified(cnn)["MODIFIED"][0]
            else:
                logger.debug("up-to-date")

        schedule.every(5).seconds.do(update)

        while True:
            schedule.run_pending()
            time.sleep(1)

    def test_time_now(self):
        # date_updated.strftime('%d.%m.%Y_%H.%M')
        print((datetime.datetime.now() - datetime.timedelta(minutes=1)).strftime('%d.%m.%Y %H:%M:%S'))
        # print(time.time())

    def test_max_date(self):
        print(select_max_object_date_modified(cnn))
        # print(datetime.datetime.strptime(select_max_object_date_modified(cnn)["MODIFIED"][0], '%Y-%m-%d %H:%M:%S') - datetime.timedelta(seconds=5))
        print(select_max_object_date_modified(cnn)["MODIFIED"][0] - datetime.timedelta(seconds=5))

    def test_git_update(self):
        # schedule.every(5).seconds.do(git_funcs.update, cx_Oracle.connect("ibs/HtuRhtl@mideveryday"))
        # def run_threaded(connection_string):
        #     cnn = cx_Oracle.connect(connection_string)
        #     log.log_init(cnn.dsn)
        #     job_thread = threading.Thread(target=lambda: git_funcs.update(cnn))
        #     job_thread.start()
        def do_schedule(connection_string):
            cnn = cx_Oracle.connect(connection_string)
            log.log_init(cnn.dsn)
            schedule.every(5).seconds.do(git_funcs.update, cnn)

        # schedule.every(5).seconds.do(run_threaded, "ibs/HtuRhtl@lw-abs-abs")
        # schedule.every(5).seconds.do(run_threaded, "ibs/HtuRhtl@mideveryday")
        do_schedule("ibs/HtuRhtl@lw-abs-abs")
        do_schedule("ibs/HtuRhtl@mideveryday")

        while True:
            schedule.run_pending()
            time.sleep(1)
            # git_funcs.update(cnn)

    def test_commit_authorized_time(self):
        # repos_dir = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script"
        # repo_dir = os.path.join(repos_dir, 'dbs', 'lw-abs-abs')
        # repo = clone_or_open_repo(repo_dir)
        # repo.git.add(A=True)
        # author = Actor('sources_sync_job', '@mail')
        # committer = Actor('sources_sync_job', '@mail')
        # time_str = "2012-07-24T23:14:29-07:00"
        # time_str = "2012-07-24T23:14:29"
        # repo.index.commit('test', author=author, committer=committer, author_date=time_str)

        last_date_update = '28.09.2017 18:30:30'
        df = select_all_by_date_modified(cnn, last_date_update)
        df = df.sort_values('MODIFIED', ascending=False)
        # localtz = pytz.timezone('Europe/Moscow')
        # time_str = localtz.localize(df.iloc[0]["MODIFIED"]).strftime('%d.%m.%YT%H:%M:%S%z')
        time_str = (df.iloc[0]["MODIFIED"] + datetime.timedelta(hours=-3)).strftime('%d.%m.%YT%H:%M:%S')
        print(time_str)
        repos_dir = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script"
        # repo_dir = os.path.join(repos_dir, 'dbs', 'lw-abs-abs')
        repo_dir = os.path.join(repos_dir, 'dbs', 'mideveryday')
        repo = clone_or_open_repo(repo_dir)
        repo.git.add(A=True)
        author = Actor('sources_sync_job', '@mail')
        committer = Actor('sources_sync_job', '@mail')
        # time_str = "2012-07-24T23:14:29-07:00"
        # time_str = "2012-07-24T23:14:29"
        repo.index.commit('test', author=author, committer=committer, author_date=time_str)
        # #print(pytz.utc)

    def test_push(self):
        repos_dir = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script"
        # repo_dir = os.path.join(repos_dir, 'dbs', 'lw-abs-abs')
        repo_dir = os.path.join(repos_dir, 'dbs', 'mideveryday')
        repo = clone_or_open_repo(repo_dir)
        print(repo.remotes)
        local_branch = 'mideveryday_03.10.2017_15.26'
        remote_branch = local_branch
        repo.remotes['origin'].push(refspec='{}:{}'.format(local_branch, remote_branch))

    def test_pull(self):
        repos_dir = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script"
        repo_dir = os.path.join(repos_dir, 'dbs', 'lw-abs-abs')
        # repo_dir = os.path.join(repos_dir, 'dbs', 'mideveryday')
        repo = clone_or_open_repo(repo_dir)
        # print(repo.remotes)
        local_branch = 'lw-abs-abs_26.09.2017_13.17'
        remote_branch = 'origin/%s' % local_branch

        # print(repo.refs['origin/%s' % local_branch])
        # print(repo.branches)
        try:
            repo.refs[remote_branch]
        except IndexError:
            pass
            # repo.head.reset(commit=repo.refs[remote_branch], index=True, working_tree=True)

    def test_job(self):
        def do_schedule(connection_string):
            # cnn_object = cx_Oracle.connect(connection_string)
            m = re.match(r"(?P<user>.+)/(?P<pass>.+)@(?P<dbname>.+)", connection_string)
            db_name = m.group('dbname')
            log.log_init(db_name)
            # schedule.every(5).seconds.do(git_funcs.update, connection_string)
            schedule.every(1).hours.do(git_funcs.update, connection_string)
            git_funcs.update(connection_string)

        do_schedule("ibs/HtuRhtl@day")
        do_schedule("ibs/HtuRhtl@mideveryday")
        do_schedule("ibs/HtuRhtl@msb")
        do_schedule("ibs/HtuRhtl@lw-ass-abs")
        do_schedule("ibs/HtuRhtl@lw-abs-abs")
        do_schedule("ibs/HtuRhtl@lw-p2-abs")
        do_schedule("ibs/HtuRhtl@midabs")
        while True:
            schedule.run_pending()
            time.sleep(1)

    def test_fetch_master(self):
        repos_dir = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script"
        repo_dir = os.path.join(repos_dir, 'dbs', 'mideveryday')
        repo = clone_or_open_repo(repo_dir)
        # try:
        repo.heads['master'].checkout()
        repo.remotes['origin'].fetch(refspec='{}'.format('master'))
        repo.head.reset(commit=repo.refs['origin/master'], index=True, working_tree=True)
        #
        # repo.create_head(branch_name, 'master').checkout()

    def test_discard_changes(self):
        repos_dir = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script"
        repo_dir = os.path.join(repos_dir, 'dbs', 'midabs')
        repo = clone_or_open_repo(repo_dir)
        repo.head.reset(commit='HEAD', index=True, working_tree=True)
        # repo.git.clean('-xdf')
        # try:
        # repo.index.checkout([filename]. force=True)
        #
        # repo.create_head(branch_name, 'master').checkout()
