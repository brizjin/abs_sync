import datetime

from git import Repo, Actor

import git_funcs
from selects import *

logger = logging.getLogger('root')


def clone_or_open_repo(git_dir):
    # folder_path = os.path.join(config.git_folder, db_name)
    if not os.path.isdir(os.path.join(git_dir, '.git')):
        logger.info("clone repo %s to %s" % (config.git_url, git_dir))
        repo = Repo.clone_from(config.git_url, git_dir)
    else:
        repo = Repo(git_dir)
        logger.info("open repo %s is_dirty: %s" % (git_dir, repo.is_dirty()))
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
    return '%s_%s' % (cnn.dsn, date_updated.strftime('%d.%m.%Y_%H.%M'))


def commit_group(repo, group):
    if repo.is_dirty() or repo.untracked_files:
        dfg = pd.DataFrame(group)
        # logger.debug("commit_group\n%s" % dfg)
        msg = "autosave "
        msg += ", ".join(
            "%s [%s]" % (class_id, ", ".join(name for name, rows in class_rows.groupby('SHORT_NAME')))
            for class_id, class_rows in dfg.groupby('CLASS_ID'))
        repo.git.add(A=True)
        author_name = dfg.iloc[0]["USER_MODIFIED"]
        author = Actor(author_name or 'Неизвестный автор', (author_name or 'Неизвестный автор') + '@mail')
        committer = Actor('sources_sync_job', '@mail')
        date_str = (dfg.iloc[0]["MODIFIED"] + datetime.timedelta(hours=-3)).strftime('%d.%m.%YT%H:%M:%S')
        repo.index.commit(msg, author=author, committer=committer, author_date=date_str)


def commit_by_dataframe(repo, df, db_name):
    db_logger = logging.getLogger(db_name)
    if len(df) > 0:
        df = df.sort_values('MODIFIED', ascending=True)
        db_logger.debug("commit_by_dataframe\n%s" % df)
        group = []
        i = 1
        prev_user = None
        for ids, row in df.iterrows():
            current_user = row['USER_MODIFIED']
            # logger.debug("prev_user=%s, current_user=%s" % (prev_user, current_user))
            if i == 1:
                i += 1
            elif prev_user != current_user:
                commit_group(repo, group)
                group = []
            dirs.write_object_from_row(os.path.join(config.git_folder, db_name), row)
            prev_user = current_user
            group.append(row)
        commit_group(repo, group)


last_dates_update = {}


def update(cnn):
    # global last_date_update
    # df = None

    db_logger = logging.getLogger(cnn.dsn)
    db_logger.debug("update %s" % cnn.dsn)

    last_date_update = last_dates_update.get(cnn.dsn)

    if not last_date_update:
        last_date_update = select_max_object_date_modified(cnn).iloc[0]["MODIFIED"] - datetime.timedelta(days=7)

    repo = clone_or_open_repo(os.path.join(config.git_folder, cnn.dsn))

    if not select_tune_date_update(cnn):
        db_logger.debug("update init_git_db")
        create_tune_date_update(cnn)
        df = select_objects_in_folder_or_date_modified(cnn, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
    else:
        db_logger.debug("select object on date %s" % last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
        df = select_all_by_date_modified(cnn, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
    if len(df) > 0:
        branch_name = git_funcs.tune_branch_name(cnn)
        if branch_name in repo.branches:
            repo.heads[branch_name].checkout()
        else:
            repo.create_head(branch_name, 'master').checkout()

        remote_branch_name = 'origin/%s' % branch_name
        try:
            # noinspection PyStatementEffect
            repo.refs[remote_branch_name]
            repo.remotes['origin'].fetch(refspec='{}'.format(branch_name))
            repo.head.reset(commit=repo.refs[remote_branch_name], index=True, working_tree=True)
        except IndexError:
            pass


        git_funcs.commit_by_dataframe(repo, df, cnn.dsn)
        repo.remotes['origin'].push(refspec='{}:{}'.format(branch_name, branch_name))
    else:
        db_logger.debug("up-to-date")

    last_dates_update[cnn.dsn] = select_max_object_date_modified(cnn)["MODIFIED"][0]
