import os
import unittest

import cx_Oracle
import pandas as pd

import dirs
from selects import select, select_methods_by_name, select_all_methods_in_folder, methods_sql, where_clause_by_name, \
    read_value_cursor, select_types_in_folder_or_date_modified

os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
os.environ['NLS_LANG'] = '.AL32UTF8'
db_cnn_str = "ibs/HtuRhtl@midabs"
cnn = cx_Oracle.connect(db_cnn_str)


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

            df = dirs.get_project_objects(folder_path)
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
        df = select_types_in_folder_or_date_modified(cnn, 'METHOD', folder_path, 1, 'd')
        self.assertGreater(len(df), 0)
        #print(df[df["TEXT"].map(lambda a: a.strip() != '')])

    def test_select_view_in_folder_or_date_modified(self):
        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        print(select_types_in_folder_or_date_modified(cnn, 'VIEW', folder_path, 1, 'd'))

    def test_select_trigger_in_folder_or_date_modified(self):
        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        print(select_types_in_folder_or_date_modified(cnn, 'TRIGGER', folder_path, 1, 'd'))

    def test_select_objects_in_folder_or_date_modified(self):
        folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        method_df = select_types_in_folder_or_date_modified(cnn, 'METHOD', folder_path, 1, 'd')
        view_df = select_types_in_folder_or_date_modified(cnn, 'VIEW', folder_path, 1, 'd')
        view_df["TEXT"] = view_df.CONDITION.map(str) + view_df.ORDER_BY.map(str) + view_df.GROUP_BY.map(str)
        view_df = view_df.drop(["CONDITION", "ORDER_BY", "GROUP_BY"], axis=1)
        trigger_df = select_types_in_folder_or_date_modified(cnn, 'TRIGGER', folder_path, 1, 'd')
        trigger_df["TEXT"] = trigger_df["HEADER"].map(str) + trigger_df["TEXT"]
        del trigger_df["HEADER"]
        df = pd.concat([method_df, view_df, trigger_df])
        df = df[df["TEXT"].map(lambda a: a.strip() != '')]  # Удалим строки с пустыми TEXT

        print(df)
