import logging

import pytest
from git import Repo
from hamcrest import has_length, greater_than
from hamcrest.core import assert_that

from abs_sync import config

log = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def repo(request, tmpdir_factory):
    repo_dir = request.config.cache.get('repo_temp_dir', None)
    try:
        logging.debug(f'open repo from folder: {repo_dir}')
        repo_object = Repo(repo_dir)
    except Exception:
        repo_dir = tmpdir_factory.mktemp("repo")
        logging.debug(f'cloning repo {config.git_url} to {repo_dir}')
        repo_object = Repo.clone_from(config.git_url, repo_dir)
        request.config.cache.set("repo_temp_dir", str(repo_dir))

    return repo_object


def test_branches(repo):
    """
    Пока тест не работает
    При получении списка веток выдается ошибка
    UnicodeDecodeError: 'charmap' codec can't decode byte 0x98 in position 436:
    """
    # locale.setlocale(locale.LC_ALL, ('ru_RU', 'utf-8'))
    # import _locale
    # _locale._getdefaultlocale = (lambda *args: ['ru_RU', 'utf8'])

    branches = list(repo.branches)
    assert_that(branches, has_length(greater_than(0)))


def test_fetch(repo):

    local_branch_name = 'test'
    remote_origin = 'origin'
    # repo.remotes[remote_origin].fetch(refspec=f'{local_branch_name}')
    repo.remotes[remote_origin].fetch()


def test_checkout(repo):
    local_branch_name = 'test'
    remote_name = f'origin/{local_branch_name}'
    repo.git.checkout(local_branch_name)

    # repo.git.fetch
