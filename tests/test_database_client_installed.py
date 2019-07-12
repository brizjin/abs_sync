from database_client.oracle import OracleClient
from hamcrest import has_entry, equal_to
from hamcrest.core import assert_that

from abs_sync import config


def test_database_client_installed():
    default_tns = config.dbs['ass']
    client = OracleClient()
    client.connect(default_tns)
    assert_that(client.select("select * from dual").iloc[0], has_entry('DUMMY', equal_to('X')))
