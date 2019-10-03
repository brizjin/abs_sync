from git import Repo

from abs_sync import config


def test_clone():
    url = "git@gitlab.moduldev.ru:abs/plplus.git"
    folder = "/tmp/dbs"
    repo = Repo.clone_from(url, folder)