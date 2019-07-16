from datetime import datetime, timedelta

from hamcrest import has_length, greater_than, assert_that

from abs_sync.selects import select_by_date_modified, select_all_by_date_modified


def test_select_by_date_modified(oracle_client):
    assert_that(select_by_date_modified(oracle_client, 'METHOD', datetime.today() - timedelta(days=1)),
                has_length(greater_than(0)))


def test_select_all_by_date_modified(oracle_client):
    assert_that(select_all_by_date_modified(oracle_client, datetime.today() - timedelta(days=1)),
                has_length(greater_than(0)))
