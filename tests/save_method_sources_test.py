import unittest

import os

import save_methods


class SaveMethodTest(unittest.TestCase):
    def test_tst_read_func(self):
        self.assertIn('Ошибка сохранения методов', save_methods.save_method_sources_tst)

    def test_read_method(self):
        db = save_methods.Db("ibs/HtuRhtl@day")
        m = save_methods.Method(db, 'BRK_MSG', 'EDIT_AUTO')
        texts = m.read_from_disk()
        m.write_to_db(**texts)

    def test_user_domain(self):
        domain = os.environ['userdomain']
        print(domain)
        print(os.getlogin())