import unittest

import os

import pandas as pd

from abs_sync import config, save_methods
from abs_sync.scripts.abs2 import Db


class SaveMethodTest(unittest.TestCase):
    def test_tst_read_func(self):
        self.assertIn('Ошибка сохранения методов', save_methods.save_method_sources_tst)

    def test_read_method(self):
        db = save_methods.Db("ibs/HtuRhtl@lw-abs-abs")
        m = save_methods.Method(db, 'DOC_CARD_INDEX', 'BRK_DOC_NULL')
        texts = m.read_from_disk()
        m.write_to_db(**texts)

    def test_user_domain(self):
        domain = os.environ['userdomain']
        print(domain)
        print(os.getlogin())


class CftSchemaTest(unittest.TestCase):
    def test_read_from_disk(self):
        schema = save_methods.CftSchema.read_disk_schema()
        # print(schema.elements)
        print(schema.as_df().sort_values(['TYPE']))
        # print(schema.as_cls())
        # for s in schema.as_cls():
        #     print(s)

    def test_read_from_db(self):
        cnn = Db(config.dbs['p2'])
        schema = save_methods.CftSchema.read_db_schema(cnn, save_methods.CftSchema.read_disk_schema().elements)
        print(schema.as_df())

    def test_remove_unknowns(self):
        save_methods.CftSchema.remove_unknown_files_on_disk()

    def test_pull_all(self):
        db_name = 'p2'
        save_methods.CftSchema.remove_unknown_files_on_disk()
        disk_schema = save_methods.CftSchema.read_disk_schema()
        cnn = Db(config.dbs[db_name])
        db_schema = save_methods.CftSchema.read_db_schema(cnn, disk_schema.elements)

        df1 = disk_schema.as_df()
        df2 = db_schema.as_df()
        # df3 = df1.set_index(['TYPE', 'CLASS', 'NAME']).subtract(df2.set_index(['TYPE', 'CLASS', 'NAME']), fill_value=0)
        df4 = pd.merge(df1, df2, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
        for m in save_methods.CftSchema(df4).as_cls():
            m.remove_from_disk()
        # print(df4)

        for element in db_schema.as_cls():
            if type(element) not in [save_methods.Package]:
                element.read_from_db(cnn)
                element.write_to_disk()

    def test_read_method(self):
        cnn = Db(config.dbs['mid'])
        # schema = save_methods.CftSchema.read_db(cnn, save_methods.CftSchema.read_disk().elements)
        # print(schema.as_df())
        m = save_methods.Method('BRK_MSG', 'EDIT_AUTO')
        m.read_from_db(cnn)
        print(m.texts)
        m.write_to_disk()
        print(m.read_from_disk().texts)

    def test_read_view_from_db(self):
        cnn = Db(config.dbs['p2'])
        # schema = save_methods.CftSchema.read_db(cnn, save_methods.CftSchema.read_disk().elements)
        # print(schema.as_df())
        m = save_methods.View('BRK_MSG', 'VW_RPT_BRK_MSG_SLA_GROUP')
        m.read_from_db(cnn)
        print(m.texts)
        m.write_to_disk()
        # print(m.read_from_disk().texts)

    def test_read_trigger(self):
        cnn = Db(config.dbs['p2'])
        # schema = save_methods.CftSchema.read_db(cnn, save_methods.CftSchema.read_disk().elements)
        # print(schema.as_df())
        m = save_methods.Trigger('AC_FIN', 'BKB#AC_FIN')
        m.read_from_db(cnn)
        print(m.texts)
        # m.write_to_disk()
        # print(m.read_from_disk().texts)
