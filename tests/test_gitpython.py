import pytest
from git import Repo
from hamcrest import has_length, greater_than
from hamcrest.core import assert_that

from abs_sync import config


@pytest.fixture(scope='session')
def repo(request, tmpdir_factory):
    value = request.config.cache.get('repo_temp_dir', None)
    try:
        repo_object = Repo(value)
    except Exception:
        repo_dir = tmpdir_factory.mktemp("repo")
        repo_object = Repo.clone_from(config.git_url, repo_dir)
        request.config.cache.set("repo_temp_dir", str(repo_dir))

    return repo_object



def test_clone(repo):
    """
    Пока тест не работает
    При получении списка веток выдается ошибка
    UnicodeDecodeError: 'charmap' codec can't decode byte 0x98 in position 436:
    """
    branches = repo.branches
    assert_that(branches, has_length(greater_than(0)))


def test_fetch(repo):
    local_branch_name = 'test'
    remote_origin = 'origin'
    # repo.remotes[remote_origin].fetch(refspec=f'{local_branch_name}')
    repo.remotes[remote_origin].fetch(stdout_as_string=False)


def test_checkout(repo):
    local_branch_name = 'test'
    remote_name = f'origin/{local_branch_name}'
    repo.git.checkout(local_branch_name)

    # repo.git.fetch
