import datetime
import re

from git import Repo, Actor

from abs_sync.selects import *

logger = logging.getLogger('root')


def clone_or_open_repo(git_dir, db_name='withoutdb'):
    # folder_path = os.path.join(config.git_folder, db_name)
    db_logger = logging.getLogger(db_name)
    if not os.path.isdir(os.path.join(git_dir, '.git')):
        db_logger.debug("clone repo %s to %s" % (config.git_url, git_dir))
        repo = Repo.clone_from(config.git_url, git_dir)
    else:
        repo = Repo(git_dir)
        db_logger.debug("open repo %s is_dirty: %s" % (git_dir, repo.is_dirty()))
    return repo


# def init_git_db(cnn):
#     date_updated = select_tune_date_update(cnn)
#     if not date_updated:
#         date_updated = create_tune_date_update(cnn)
#
#     git_dir = os.path.join(config.git_folder, cnn.dsn)
#
#     repo = clone_or_open_repo(git_dir)
#
#     # past_branch = repo.create_head(branch_name, 'master')
#     # past_branch.checkout()
#     #branch_name = tune_branch_name(cnn)
#     branch_name = '%s_%s' % (cnn.dsn, date_updated.strftime('%d.%m.%Y_%H.%M'))  # %Y%m%d_%H%M%S
#     if branch_name in repo.branches:
#         repo.heads[branch_name].checkout()
#     else:
#         repo.create_head(branch_name, 'master').checkout()
#     logger.info("checkout new branch %s" % branch_name)
#
#     # Сохраним тексты
#     df = select_objects_in_folder_or_date_modified(cnn, 1, 'd')
#     commit_by_dataframe(repo, df, cnn.dsn)

# def checkout_branch(repo, branch_name):
#     past_branch = repo.create_head(branch_name, 'master')
#     repo.head.reference = past_branch
#     repo.head.reset(index=True, working_tree=True)
#     logger.info("checkout new branch %s" % branch_name)


def commit(repo, msg, author_name, author_mail, commiter_name, commiter_mail):
    repo.git.add(A=True)
    index = repo.index
    author = Actor(author_name, author_mail)
    committer = Actor(commiter_name, commiter_mail)
    index.commit(msg, author=author, committer=committer)


def tune_branch_name(cnn):
    date_updated = select_tune_date_update(cnn)
    # return '%s_%s' % (cnn.dsn, date_updated.strftime('%d.%m.%Y_%H.%M'))
    return '%s_%s' % (cnn.dsn, date_updated.strftime('%Y.%m.%d_%H.%M'))


def commit_group(repo, group, db_name, days_delta):
    db_logger = logging.getLogger(db_name)
    if repo.is_dirty() or repo.untracked_files:
        dfg = pd.DataFrame(group)
        db_logger.debug("commit_group\n%s" % dfg)
        msg = "autosave "
        msg += ", ".join(
            "%s [%s]" % (class_id, ", ".join(name for name, rows in class_rows.groupby('SHORT_NAME')))
            for class_id, class_rows in dfg.groupby('CLASS_ID'))
        repo.git.add(A=True)
        author_name = dfg.iloc[0]["USER_MODIFIED"]
        author = Actor(author_name or 'Неизвестный автор', (author_name or 'Неизвестный автор') + '@mail')
        committer = Actor('sources_sync_job', '@mail')
        db_logger.debug("dfg.iloc[0]['MODIFIED']=%s, days_delta=%s" % (dfg.iloc[0]["MODIFIED"], days_delta))
        date_str = (dfg.iloc[0]["MODIFIED"] + datetime.timedelta(hours=-3) + days_delta).strftime('%d.%m.%YT%H:%M:%S')

        repo.index.commit(msg, author=author, committer=committer, author_date=date_str)


def commit_by_dataframe(repo, df, db_name, days_delta):
    db_logger = logging.getLogger(db_name)
    if len(df) > 0:
        df = df.sort_values('MODIFIED', ascending=True)
        db_logger.debug("commit_by_dataframe\n%s" % df)

        # prev_group_user = None  # Нужен, когда по текущему нет изменений, а следующий совпадает с предыдущим
        # groups = []
        # prev_group = None
        # if prev_group:
        #     if prev_group[0]["USER_MODIFIED"] == group[0]["USER_MODIFIED"]:
        #         prev_group = prev_group + group
        #     else
        #         commit_group(repo, group, db_name, days_delta)
        rows = []
        prev_len = 0
        for ids, row in df.iterrows():
            dirs.write_object_from_row(os.path.join(config.git_folder, db_name), row)
            current_len = len(repo.index.diff(None)) + len(repo.untracked_files)
            if prev_len != current_len:
                prev_len = current_len
                rows.append(row)
        # test = ""
        # repo.head.reset(index=True, working_tree=True)
        # branch_name = git_funcs.tune_branch_name(cnn)
        # repo.heads["master"].checkout(force=True)
        # repo.heads[branch_name].checkout(force=True)
        repo.git.clean('-xdf')  # очищает все новые файлы в рабочем каталоге
        repo.head.reset(commit='HEAD', index=True, working_tree=True)  # очищает модифицированные файлы
        group = []
        i = 1
        prev_user = None
        df = pd.DataFrame(rows)
        db_logger.debug("commit_by_dataframe\nchanged rows\n%s" % df)
        for ids, row in df.iterrows():
            current_user = row['USER_MODIFIED']
            if i == 1:
                i += 1
            elif prev_user != current_user:
                commit_group(repo, group, db_name, days_delta)
                group = []
            dirs.write_object_from_row(os.path.join(config.git_folder, db_name), row)
            prev_user = current_user
            # current_len = len(repo.index.diff(None)) + len(repo.untracked_files)
            # if prev_len != current_len:
            #     prev_len = current_len
            group.append(row)

        commit_group(repo, group, db_name, days_delta)
        # def make_groups(rf):
        #     group = []
        #     i = 1
        #     prev_user = None
        #     prev_len = 0
        #     groups = []
        #     for ids, row in rf.iterrows():
        #         current_user = row['USER_MODIFIED']
        #         # logger.debug("prev_user=%s, current_user=%s" % (prev_user, current_user))
        #         if i == 1:
        #             i += 1
        #         elif prev_user != current_user and len(group) > 0:
        #             # db_logger.debug("prev_user=%s, current_user=%s" % (prev_user, current_user))
        #             # commit_group(repo, group, db_name, days_delta)
        #             groups.append(group)
        #             group = []
        #             prev_len = 0
        #         dirs.write_object_from_row(os.path.join(config.git_folder, db_name), row)
        #         prev_user = current_user
        #         current_len = len(repo.index.diff(None)) + len(repo.untracked_files)
        #         if prev_len != current_len:
        #             prev_len = current_len
        #             group.append(row)
        #     groups.append(group)
        #     return groups
        # groups = make_groups(df)
        # branch_name = git_funcs.tune_branch_name(cnn)
        # repo.heads[branch_name].checkout(force=True)
        # for group in make_groups(pd.DataFrame([r for g in groups for r in g])):
        #     for ids, row in pd.DataFrame(group).iterrows():
        #         dirs.write_object_from_row(os.path.join(config.git_folder, db_name), row)
        #     commit_group(repo, group, db_name, days_delta)

        # groups.append(group)
        # merged_groups = []
        # for g in groups:


last_dates_update = {}
dbs = {}


def update(connection_string):
    # global last_date_update
    # df = None
    m = re.match(r"(?P<user>.+)/(?P<pass>.+)@(?P<dbname>.+)", connection_string)
    db_name = m.group('dbname')

    db_logger = logging.getLogger(db_name)
    db_logger.debug("update %s" % db_name)

    cnn = dbs.get(connection_string)
    if not cnn:
        try:
            cnn = cx_Oracle.connect(connection_string)
            dbs[connection_string] = cnn
        except cx_Oracle.DatabaseError as e:
            error = e.args[0]
            # print "Code:", error.code
            # print "Message:", error.message
            dbs[connection_string] = None
            db_logger.exception(
                "update exception on connection to database code=%s, message=%s" % (error.code, error.message))
            return

    try:
        last_date_update = last_dates_update.get(db_name)
        sysdate = select_sysdate(cnn)
        # кол-во дней на которые на сервере передвинули время
        # корректируем дату коммитов на эту дельту
        days_delta = datetime.timedelta(days=(
            datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - sysdate.replace(hour=0,
                                                                                                         minute=0,
                                                                                                         second=0,
                                                                                                         microsecond=0)).days)
        db_logger.debug("delta_days=%s, datetime.datetime.now()=%s, sysdate=%s" % (
            days_delta, datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
            sysdate.replace(hour=0, minute=0, second=0, microsecond=0)))

        if not last_date_update:
            last_date_update = sysdate - datetime.timedelta(days=config.days_update_on_start)
            db_logger.debug("last_date_update is null set it to %s" % last_date_update)
            # залогируем список пользователей за последний месяц правивших методы
            # чтобы можно было найти тех по кому мы не сохраняем изменения
            # db_logger.debug(",\n".join(["'%s'" % a for a in select_users(cnn)['USER_MODIFIED']]))

        repo = clone_or_open_repo(os.path.join(config.git_folder, db_name), db_name)

        if not select_tune_date_update(cnn):
            db_logger.debug("update init_git_db")
            create_tune_date_update(cnn)
            df = select_objects_in_folder_or_date_modified(cnn, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
        else:
            db_logger.debug("select object on date %s" % last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
            df = select_all_by_date_modified(cnn, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
        if len(df) > 0:

            branch_name = tune_branch_name(cnn)
            if branch_name in repo.branches:
                # repo.heads[branch_name].checkout()
                try:
                    remote_branch_name = 'origin/%s' % branch_name
                    repo.remotes['origin'].fetch(refspec='{}'.format(branch_name))
                    repo.head.reset(commit=repo.refs[remote_branch_name], index=True, working_tree=True)
                except Exception:
                    db_logger.exception("exceprion checkout and reset %s %s" % (branch_name, traceback.format_exc()))
            else:
                try:
                    repo.heads['master'].checkout()

                    # with repo.remotes['origin'].config_writer as cw:
                    #     cw.set("push", "refs/heads/*:refs/heads/qa/*")
                    repo.remotes['origin'].fetch(refspec='{}'.format('master'))
                    repo.head.reset(commit=repo.refs['origin/master'], index=True, working_tree=True)
                except Exception:
                    db_logger.exception("exceprion checkout and reset master %s" % traceback.format_exc())
                repo.create_head(branch_name, 'master').checkout()

            # try:
            #     if repo.refs[remote_branch_name].tracking_branch():
            #         repo.remotes['origin'].fetch(refspec='{}'.format(branch_name))
            #         # repo.remotes['origin'].fetch(refspec='{}'.format(branch_name))
            #         repo.head.reset(commit=repo.refs[remote_branch_name], index=True, working_tree=True)
            # except IndexError:
            #     pass

            git_funcs.commit_by_dataframe(repo, df, db_name, days_delta)
            # repo.remotes['origin'].push(refspec='{}:refs/heads/autosave/{}'.format(branch_name, branch_name))
            repo.remotes['origin'].push(refspec='{}:{}'.format(branch_name, branch_name))
        else:
            db_logger.debug("up-to-date")

        last_dates_update[db_name] = sysdate
        db_logger.info("updated finish")

    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        # ORA-01033: ORACLE initialization or shutdown in progress
        # ORA-03114: not connected to ORACLE
        if error.code in [1033, 3114]:
            dbs[connection_string] = None
            db_logger.exception(
                "update db not init: %s, %s" % (error.code, error.message))
        else:
            db_logger.exception(
                "update db exception code=%s, message=%s" % (error.code, error.message))
    except Exception:
        db_logger.exception("update exception %s" % traceback.format_exc())
