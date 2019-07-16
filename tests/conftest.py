import pytest
from database_client.oracle import OracleClient

from abs_sync.config import DEFAULT_CONNECTION_STRING


@pytest.fixture
def oracle_client():
    client = OracleClient()
    client.connect(DEFAULT_CONNECTION_STRING)
    yield client
