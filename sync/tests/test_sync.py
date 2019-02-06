import unittest

from oracle_client.oracle_client import OracleClient

from abs_sync.log import get_logger
from sync import log, config
from sync.abs import DirSchema, Element


class GitNewDatabaseTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_sync(self):
        # self.client = OracleClient("ibs/HtuRhtl@lw-ass-abs.brc.local:1521/assabs")
        logger = log.get_logger('test')
        logger.debug("testing")

        ds = DirSchema(config.texts_working_dir)
        print(ds.elements)
        # print(Element.__subclasses__())
