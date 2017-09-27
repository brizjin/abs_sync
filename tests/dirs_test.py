import unittest

import dirs


class DirsTest(unittest.TestCase):
    def test_dual(self):
        print(dirs.get_project_objects(r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"))

    def test_iterate_over_methods_in_folder(self):
        df = dirs.get_project_objects(r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day")
        df = df[df["TYPE"] == 'METHOD']
        df.SHORT_NAME = df.SHORT_NAME.apply(lambda a: "'%s'" % a.upper())
        group = df.groupby(["CLASS_ID"])
        condition = "CLASS_ID = '%s' and SHORT_NAME in (%s)"
        where_clause = "\n or ".join(condition % (class_id, ",".join(rows["SHORT_NAME"])) for class_id, rows in group)
        print(where_clause)

    def test_iterate_over_views_in_folder(self):
        def where_clause_by_name(objs):
            # objs.loc[:, "SHORT_NAME"].apply(lambda a: "'%s'" % a.upper())
            objs.SHORT_NAME = objs.SHORT_NAME.apply(lambda a: "'%s'" % a.upper())
            group = objs.groupby(["CLASS_ID"])
            condition = "CLASS_ID = '%s' and SHORT_NAME in (%s)"
            where_clause = "\n or ".join(
                condition % (class_id, ",".join(rows["SHORT_NAME"])) for class_id, rows in group)
            return where_clause

        objs = dirs.get_project_objects(r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day")
        views = objs[objs["TYPE"] == 'VIEW']
        print(where_clause_by_name(objs[objs["TYPE"] == 'VIEW']))
        print(where_clause_by_name(objs[objs["TYPE"] == 'TRIGGER']))
