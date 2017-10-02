import logging
import os

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
    date_updated = create_tune_date_update(cnn)

    git_dir = os.path.join(config.git_folder, cnn.dsn)

    branch_name = '%s_%s' % (cnn.dsn, date_updated.strftime('%d.%m.%Y_%H.%M'))  # %Y%m%d_%H%M%S
    repo = clone_or_open_repo(git_dir)

    checkout_branch(repo, branch_name)

    # Сохраним тексты
    df = select_objects_in_folder_or_date_modified(cnn, git_dir, 1, 'd')
    dirs.write_object_from_df(df, git_dir)

    commit(repo, "my commit message", "An author", "author@example.com", "A committer", "committer@example.com")

    # Закомитим
    # repo.git.add(update=True)
    # repo.git.add(A=True)
    # index = repo.index
    #
    # from git import Actor
    # author = Actor("An author", "author@example.com")
    # committer = Actor("A committer", "committer@example.com")
    # index.commit("my commit message", author=author, committer=committer)


def checkout_branch(repo, branch_name):
    past_branch = repo.create_head(branch_name, 'master')
    repo.head.reference = past_branch
    repo.head.reset(index=True, working_tree=True)
    logger.info("checkout new branch %s" % branch_name)


def commit(repo, msg, author_name, author_mail, commiter_name, commiter_mail):
    repo.git.add(A=True)
    index = repo.index
    author = Actor(author_name, author_mail)
    committer = Actor(commiter_name, commiter_mail)
    index.commit(msg, author=author, committer=committer)


def tune_branch_name(cnn):
    date_updated = select_tune_date_update(cnn)
    return '%s_%s' % (cnn.dsn, date_updated.strftime('%d.%m.%Y_%H.%M'))
