import os
import re
import unittest

import cx_Oracle as cx_Oracle
import pandas as pd

from abs_sync import config
import dirs
from abs_sync.selects import select_types_in_folder_or_date_modified, select_objects_in_folder_or_date_modified

pd.options.display.width = 300


class DirsTest(unittest.TestCase):
    def test_dual(self):
        print(dirs.objects_in_folder(r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"))

    def test_iterate_over_methods_in_folder(self):
        df = dirs.objects_in_folder(r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day")
        df = df[df["TYPE"] == 'METHOD']
        df.SHORT_NAME = df.SHORT_NAME.apply(lambda a: "'%s'" % a.upper())
        group = df.groupby(["CLASS_ID"])
        condition = "CLASS_ID = '%s' and SHORT_NAME in (%s)"
        where_clause = "\n or ".join(condition % (class_id, ",".join(rows["SHORT_NAME"])) for class_id, rows in group)
        print(where_clause)

    def test_iterate_over_views_in_folder(self):
        def where_clause_by_name(objs):
            objs["SHORT_NAME"] = objs["SHORT_NAME"].map(lambda a: "'%s'" % a.upper())
            group = objs.groupby(["CLASS_ID"])
            condition = "CLASS_ID = '%s' and SHORT_NAME in (%s)"
            where_clause = "\n or ".join(
                condition % (class_id, ",".join(rows["SHORT_NAME"])) for class_id, rows in group)
            return where_clause

        objs = dirs.objects_in_folder(r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day")
        # views = objs[objs["TYPE"] == 'VIEW']
        # print(where_clause_by_name(objs[objs["TYPE"] == 'VIEW'].copy()))
        print(where_clause_by_name(objs[objs["TYPE"] == 'VIEW'].copy()))
        # print(objs)
        # print(where_clause_by_name(objs[objs["TYPE"] == 'TRIGGER']))

    def test_writing_views(self):
        def write_object(project_folder, class_id, short_name, extention, text):
            if text:
                dirname = os.path.join(project_folder, class_id)
                extention = ('.' + (extention or '')).rstrip('.')
                filename = os.path.join(dirname, short_name + extention + '.sql')
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                with open(filename, "wb+") as f:
                    part = re.sub(r'[ \t\r\f\v]*\n', '\n', text, flags=re.M)
                    f.write(part.replace('\n', '\r\n').encode())

        os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
        os.environ['NLS_LANG'] = '.AL32UTF8'
        db_cnn_str = "ibs/HtuRhtl@midabs"
        cnn = cx_Oracle.connect(db_cnn_str)

        project_folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        folder_objects_df = dirs.objects_in_folder(project_folder_path)
        view_df = select_types_in_folder_or_date_modified(cnn, 'VIEW', folder_objects_df, 1, 'd')
        nstr = lambda a: a or ''
        view_df["TEXT"] = view_df.CONDITION.map(nstr) + view_df.ORDER_BY.map(nstr) + view_df.GROUP_BY.map(nstr)
        view_df = view_df.drop(["CONDITION", "ORDER_BY", "GROUP_BY"], axis=1)

        # row = view_df[view_df["SHORT_NAME"] == 'VW_CRIT_RKO_CUR_EXT'].iloc[0]
        # write_object(project_folder_path, row["CLASS_ID"], row["SHORT_NAME"], row["EXTENTION"], row["TEXT"])
        # write_object(project_folder_path, row["CLASS_ID"], row["SHORT_NAME"], 'locals', row["TEXT"])

        for idx, row in view_df.iterrows():
            write_object(project_folder_path, row["CLASS_ID"], row["SHORT_NAME"], row["EXTENTION"], row["TEXT"])
            # row = row.map(str)
            # print(row)

    def test_writing_methods(self):
        def write_object(project_folder, class_id, short_name, extention, text):
            if text:
                dirname = os.path.join(project_folder, class_id)
                extention = ('.' + (extention or '')).rstrip('.')
                filename = os.path.join(dirname, short_name + extention + '.sql')
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                with open(filename, "wb+") as f:
                    part = re.sub(r'[ \t\r\f\v]*\n', '\n', text, flags=re.M)
                    f.write(part.replace('\n', '\r\n').encode())

        os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
        os.environ['NLS_LANG'] = '.AL32UTF8'
        db_cnn_str = "ibs/HtuRhtl@midabs"
        cnn = cx_Oracle.connect(db_cnn_str)

        project_folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        folder_objects_df = dirs.objects_in_folder(project_folder_path)
        method_df = select_types_in_folder_or_date_modified(cnn, 'METHOD', folder_objects_df, 1, 'd')
        method_df = method_df[method_df["TEXT"].map(lambda a: a.strip() != '')]  # Удалим строки с пустыми TEXT
        i = 0
        for idx, row in method_df.iterrows():
            print(row.tolist())
            write_object(project_folder_path, row["CLASS_ID"], row["SHORT_NAME"], row["EXTENTION"], row["TEXT"])
            # if i > 10:
            #    break
            # i += 1
            # row = row.map(str)
            # print(row)

    def test_writing_objects(self):
        def write_object(project_folder, class_id, short_name, extention, text):
            if text:
                dirname = os.path.join(project_folder, class_id)
                extention = ('.' + (extention or '')).rstrip('.')
                filename = os.path.join(dirname, short_name + extention + '.sql')
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                with open(filename, "wb+") as f:
                    part = re.sub(r'[ \t\r\f\v]*\n', '\n', text, flags=re.M)
                    # part = part.replace('\n', '\r\n').encode()
                    part = part.encode()
                    f.write(part)

        os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
        os.environ['NLS_LANG'] = '.AL32UTF8'
        db_cnn_str = "ibs/HtuRhtl@midabs"
        cnn = cx_Oracle.connect(db_cnn_str)

        project_folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        # folder_objects_df = dirs.objects_in_folder(project_folder_path)
        # method_df = select_types_in_folder_or_date_modified(cnn, 'METHOD', folder_objects_df, 1, 'd')
        # method_df = method_df[method_df["TEXT"].map(lambda a: a.strip() != '')]  # Удалим строки с пустыми TEXT
        # i = 0
        df = select_objects_in_folder_or_date_modified(cnn, project_folder_path, 1, 'd')
        for idx, row in df.iterrows():
            print(row.tolist())
            write_object(project_folder_path, row["CLASS_ID"], row["SHORT_NAME"], row["EXTENTION"], row["TEXT"])

            # if not os.path.exists(dirname):
            #     os.makedirs(dirname)
            # with open(name, "wb+") as f:
            #     part = re.sub(r'[ \t\r\f\v]*\n', '\n', part, flags=re.M)
            #     f.write(part.replace('\n', '\r\n').encode())

    def test_writing_objects_by_df(self):
        os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
        os.environ['NLS_LANG'] = '.AL32UTF8'
        db_cnn_str = "ibs/HtuRhtl@midabs"
        cnn = cx_Oracle.connect(db_cnn_str)

        # project_folder_path = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs\day"
        project_folder_path = os.path.join(config.git_folder, cnn.dsn)
        df = select_objects_in_folder_or_date_modified(cnn, project_folder_path, 1, 'd')
        dirs.write_object_from_df(df, project_folder_path)

    def test_clear_folder(self):
        # dirs.clear_folder(r"C:\Users\BryzzhinIS\Documents\Хранилища\test")
        dirs.clear_folder(r"C:\Users\BryzzhinIS\Documents\Хранилища\pack_texts")
