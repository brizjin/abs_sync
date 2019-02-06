import unittest

from abs_sync import log


class LogTest(unittest.TestCase):
    def setUp(self):
        self.logger = log.get_logger("root")

    def test_log(self):
        self.logger.info("test")
