from hamcrest import has_entry, equal_to
from hamcrest.core import assert_that


def test_database_client_installed(oracle_client):
    assert_that(oracle_client.select("select * from dual").iloc[0], has_entry('DUMMY', equal_to('X')))
