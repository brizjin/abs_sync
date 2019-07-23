from datetime import datetime, timedelta

from hamcrest import has_length, greater_than, assert_that

from abs_sync.selects import select_by_date_modified, select_all_by_date_modified, create_tune_date_update, \
    select_objects_in_folder_or_date_modified


def test_select_by_date_modified(oracle_client):
    assert_that(select_by_date_modified(oracle_client, 'METHOD', datetime.today() - timedelta(days=1)),
                has_length(greater_than(0)))


def test_select_all_by_date_modified(oracle_client):
    assert_that(select_all_by_date_modified(oracle_client, datetime.today() - timedelta(days=7)),
                has_length(greater_than(0)))


def test_select_objects_in_folder_or_date_modified(oracle_client):
    assert_that(select_objects_in_folder_or_date_modified(oracle_client, datetime.today() - timedelta(days=1)),
                has_length(greater_than(0)))


def test_create_tune(oracle_client):
    create_tune_date_update(oracle_client)
