from git import Repo
from hamcrest import has_length, greater_than
from hamcrest.core import assert_that

from abs_sync import config


def test_tmpdir(tmpdir):
    repo = Repo.clone_from(config.git_url, tmpdir)
    branches = repo.branches
    assert_that(branches, has_length(greater_than(0)))
