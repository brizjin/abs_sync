import unittest

import dirs


class DirsTest(unittest.TestCase):
    def test_dual(self):
        df = dirs.get_project_objects(r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day")
        self.assertEqual(len(df[df.isnull().any(axis=1)]),0)
        #print(df[df.isnull().any(axis=1)])
