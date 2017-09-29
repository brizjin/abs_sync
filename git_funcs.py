import logging
import os

from git import Repo

import config
import dirs
from selects import create_tune_date_update, select_objects_in_folder_or_date_modified

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
