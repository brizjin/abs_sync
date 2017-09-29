import unittest

import log


class LogTest(unittest.TestCase):
    def setUp(self):
        self.logger = log.log_init("root")

    def test_check_if_database_is_updated(self):
        self.logger.info("test")
