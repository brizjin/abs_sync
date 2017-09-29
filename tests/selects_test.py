import os
import unittest

import cx_Oracle

import log
from selects import *

os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
os.environ['NLS_LANG'] = '.AL32UTF8'
db_cnn_str = "ibs/HtuRhtl@midabs"
cnn = cx_Oracle.connect(db_cnn_str)

logger = log.log_init("root")


class SelectTest(unittest.TestCase):
    def test_dual(self):
        self.assertEqual(len(select(cnn, "SELECT * FROM dual")), 1)

    def test_nested_cursor(self):
        print(select(cnn, "SELECT d.*, cursor(select * from dual) c FROM dual d"))

    def test_nested_sourses(self):
        print(select(cnn, methods_sql))

    def test_select_triggers(self):
        df = select(cnn, triggers_sql)
        df["TEXT"] = df["HEADER"] + df["TEXT"]
        print(df["TEXT"][0])

    def test_select_method_by_name(self):
        self.assertEqual(len(select_methods_by_name(cnn, 'BRK_MSG', 'L')), 5)

    def test_select_methods_in_folder(self):
        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        print(select_all_methods_in_folder(cnn, folder_path))

    def test_select_methods_in_folder_or_date_modified(self):
        def select_methods_in_folder_or_date_modified(cnn, folder_path, num, interval_name):
            interval = {'d': 'day', 'h': 'hour', 'm': 'minute', 's': 'second'}

            df = dirs.objects_in_folder(folder_path)
            where_by_name = where_clause_by_name(df[df["TYPE"] == 'METHOD'])
            sql = methods_sql + """\n where (%s) or
                    modified > sysdate - interval '%s' %s
                    order by modified nulls last""" % (where_by_name, num, interval[interval_name])
            print(sql)
            return select(cnn, sql, read_value_cursor)

        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        print(select_methods_in_folder_or_date_modified(cnn, folder_path, 1, 'd'))

    def test_select_methods_in_folder_or_date_modified2(self):
        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        folder_objects_df = dirs.objects_in_folder(folder_path)
        df = select_types_in_folder_or_date_modified(cnn, 'METHOD', folder_objects_df, 1, 'd')
        self.assertGreater(len(df), 0)
        print(df)
        # print(df[df["TEXT"].map(lambda a: a.strip() != '')])

    def test_select_view_in_folder_or_date_modified(self):
        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        folder_objects_df = dirs.objects_in_folder(folder_path)
        print(select_types_in_folder_or_date_modified(cnn, 'VIEW', folder_objects_df, 1, 'd'))

    def test_select_trigger_in_folder_or_date_modified(self):
        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        folder_objects_df = dirs.objects_in_folder(folder_path)
        print(select_types_in_folder_or_date_modified(cnn, 'TRIGGER', folder_objects_df, 1, 'd'))

    def test_select_objects_in_folder_or_date_modified(self):
        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        nstr = lambda a: a or ''
        folder_objects_df = dirs.objects_in_folder(folder_path)
        method_df = select_types_in_folder_or_date_modified(cnn, 'METHOD', folder_objects_df, 1, 'd')
        view_df = select_types_in_folder_or_date_modified(cnn, 'VIEW', folder_objects_df, 1, 'd')
        view_df["TEXT"] = view_df["CONDITION"].map(nstr) + view_df["ORDER_BY"].map(nstr) + view_df["GROUP_BY"].map(nstr)
        view_df = view_df.drop(["CONDITION", "ORDER_BY", "GROUP_BY"], axis=1)
        trigger_df = select_types_in_folder_or_date_modified(cnn, 'TRIGGER', folder_objects_df, 1, 'd')
        trigger_df["TEXT"] = trigger_df["HEADER"].map(nstr) + trigger_df["TEXT"].map(nstr)
        del trigger_df["HEADER"]
        df = pd.concat([method_df, view_df, trigger_df])
        df = df[df["TEXT"].map(lambda a: a.strip() != '')]  # Удалим строки с пустыми TEXT

        print(df)

    def test_select_objects_in_folder_or_date_modified_func(self):
        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        df = select_objects_in_folder_or_date_modified(cnn, folder_path, 1, 'd')
        self.assertGreater(len(df), 0)
        print(df)

    def test_tune_date_updated(self):
        print(select_tune_date_update(cnn))

    def test_create_tune(self):
        print(create_tune_date_update(cnn))

    def test_delete_tune(self):
        print(delete_tune_date_update(cnn))



