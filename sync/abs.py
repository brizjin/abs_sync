import os

from pathlib import Path

from oracle_client.oracle_client import OracleClient

from sync import config


class Element:
    exts = []

    def __init__(self, class_name, name, modified=None, last_user_modified=None):
        self.class_name = class_name
        self.name = name
        self.modified = modified
        self.last_user_modified = last_user_modified

    def __str__(self):
        return "%s ::[%s].[%s]" % (self.__class__.__name__, self.class_name, self.name)

    def __repr__(self):
        return "<%s ::[%s].[%s]>" % (self.__class__.__name__, self.class_name, self.name)


class Method(Element):
    parts = ['body', 'validate', 'globals', 'locals', 'script']
    exts = [".%s.sql" % part for part in parts]


class View(Element):
    exts = ['.sql']


class Trigger(Element):
    exts = ['.trg.sql']


class Package(Element):
    exts = ['.bdy', '.spc']


class Unknown(Element):
    pass


class DirSchema:
    def __init__(self, path):
        self.path = path

    @property
    def elements(self):
        folders = [f for f in os.listdir(self.path) if not os.path.isfile(os.path.join(self.path, f))]
        folders = [f for f in folders if f not in ['.git', '.idea', '.sync', 'PLSQL', 'TESTS']]

        def element_type(file_name):
            ext = "".join(Path(file_name).suffixes)
            for sub in Element.__subclasses__():
                if ext in sub.exts:
                    return sub
            return Unknown

        elements = []
        for folder in folders:
            for file_name in os.listdir(os.path.join(self.path, folder)):
                element = element_type(file_name)(class_name=folder, name=file_name.split(".")[0])
                elements.append(element)
        return elements


class DbSchema:
    db_obj_sql_text = """
                      select CLASS_ID, SHORT_NAME, MODIFIED, 'METHOD' TYPE from methods 
            union all select CLASS_ID, SHORT_NAME, MODIFIED, 'VIEW' from criteria
            union all select substr(t.TABLE_NAME,3), object_name, to_date(timestamp,'yyyy-mm-dd hh24:mi:ss'),object_type
                      from user_objects u
                      left join all_triggers t on u.OBJECT_NAME = t.TRIGGER_NAME
                      where object_type = 'TRIGGER'
            union all select 'PACKAGES' CLASS, OBJECT_NAME NAME, LAST_DDL_TIME MODIFIED, 'PACKAGE' TYPE
                      from SYS.ALL_OBJECTS obj where UPPER(OBJECT_TYPE) = 'PACKAGE' and OBJECT_NAME='ISIMPLE2CIT'
            """

    @staticmethod
    def read_tst(file_name):
        with open(file_name, "r", encoding='1251') as f:
            text = f.read()
            text_lines = text.split("\n")
            return "\n".join(text_lines[2:int(text_lines[1]) + 2])

    save_method_sources_tst = read_tst(os.path.join(config.project_root, "sql", "save_method_sources.tst"))
    read_method_sources_tst = read_tst(os.path.join(config.project_root, "sql", "method_sources.tst"))

    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.cnn = OracleClient(self.connection_string)
