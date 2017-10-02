import logging
import os

import pandas as pd
from git import Repo, Actor

import config
import dirs
from selects import create_tune_date_update, select_objects_in_folder_or_date_modified, select_tune_date_update

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


def init_git_db(cnn):
    date_updated = select_tune_date_update(cnn)
    if not date_updated:
        date_updated = create_tune_date_update(cnn)

    git_dir = os.path.join(config.git_folder, cnn.dsn)

    repo = clone_or_open_repo(git_dir)

    # past_branch = repo.create_head(branch_name, 'master')
    # past_branch.checkout()
    #branch_name = tune_branch_name(cnn)
    branch_name = '%s_%s' % (cnn.dsn, date_updated.strftime('%d.%m.%Y_%H.%M'))  # %Y%m%d_%H%M%S
    if branch_name in repo.branches:
        repo.heads[branch_name].checkout()
    else:
        repo.create_head(branch_name, 'master').checkout()
    logger.info("checkout new branch %s" % branch_name)

    # Сохраним тексты
    df = select_objects_in_folder_or_date_modified(cnn, 1, 'd')
    commit_by_dataframe(repo, df, cnn.dsn)

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


def commit_group(repo, group, commit_author):
    if repo.is_dirty() or repo.untracked_files:
        dfg = pd.DataFrame(group)
        msg = "autosave "
        msg += ", ".join(
            "%s [%s]" % (class_id, ", ".join(name for name, rows in class_rows.groupby('SHORT_NAME')))
            for class_id, class_rows in dfg.groupby('CLASS_ID'))
        repo.git.add(A=True)
        author = commit_author
        committer = Actor('sources_sync_job', '@mail')
        repo.index.commit(msg, author=author, committer=committer)
        # git_funcs.commit(repo, msg, prev_user, (prev_user or '') + '@mail', 'sources_sync_job', '@mail')
        # return msg


def commit_by_dataframe(repo, df, db_name):
    prev_user = None
    i = 1
    if len(df) > 0:
        df = df.sort_values('MODIFIED', ascending=True)
    group = []
    for ids, row in df.iterrows():
        current_user = row['USER_MODIFIED']
        logger.debug("prev_user=%s, current_user=%s" % (prev_user, current_user))
        if i == 1:
            i += 1
        elif prev_user != current_user:
            logger.debug("commit")
            author = Actor(prev_user or 'Неизвестный автор', (prev_user or 'Неизвестный автор') + '@mail')
            commit_group(repo, group, author)
            group = []
        dirs.write_object_from_row(os.path.join(config.git_folder, db_name), row)
        prev_user = current_user
        group.append(row)
    author = Actor(prev_user or 'Неизвестный автор', (prev_user or 'Неизвестный автор') + '@mail')
    commit_group(repo, group, author)
