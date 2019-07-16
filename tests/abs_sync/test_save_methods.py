from hamcrest import assert_that, has_length, greater_than
from pkg_resources import resource_string

from abs_sync import sql


def test_file_as_resource():
    assert_that(resource_string('abs_sync.sql', 'save_method_sources.tst'), has_length(greater_than(0)))


def test_get_sql_resources():
    assert_that(sql.save_method_sources, has_length(greater_than(0)))
