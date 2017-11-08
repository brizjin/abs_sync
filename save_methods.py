import os

import cx_Oracle

import config


def read_tst(file_name):
    with open(file_name, "r", encoding='1251') as f:
        text = f.read()
        text_lines = text.split("\n")
        return "\n".join(text_lines[2:int(text_lines[1]) + 2])


def read(file_name):
    with open(file_name, "r", encoding='utf8') as f:
        return f.read()


def read_if_exists(file_name):
    if os.path.exists(file_name):
        return read(file_name)


this_dir = os.path.dirname(os.path.realpath(__file__))
save_method_sources_tst = read_tst(os.path.join(this_dir, "sql", "save_method_sources.tst"))


class Db:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.cnn = cx_Oracle.connect(connection_string)

    # def working_dir(self):
    #     return os.path.join(config.git_folder, self.cnn.dsn)


method_parts = ['body', 'validate', 'globals', 'locals', 'script']


class Method:
    def __init__(self, db, class_name, name):
        self.db = db
        self.name = name
        self.class_name = class_name
        self.texts = None

    def read_from_disk(self):
        file_name = os.path.join(config.texts_working_dir, self.class_name, self.name + '.%s.sql')
        self.texts = dict((part, read_if_exists(file_name % part)) for part in method_parts)
        return self.texts

    def write_to_db(self, body, validate, globals, locals, script):
        cursor = self.db.cnn.cursor()
        cursor.setinputsizes(
            b=cx_Oracle.CLOB,
            v=cx_Oracle.CLOB,
            g=cx_Oracle.CLOB,
            l=cx_Oracle.CLOB,
            s=cx_Oracle.CLOB
        )
        err_clob, out_others, err_num = cursor.var(cx_Oracle.CLOB), cursor.var(cx_Oracle.CLOB), cursor.var(
            cx_Oracle.NUMBER)

        cursor.execute(
            save_method_sources_tst,
            class_name=self.class_name.upper(),
            method_name=self.name.upper(),

            user_name=os.getlogin(),

            b=body,
            v=validate,
            g=globals,
            l=locals,
            s=script,

            out=err_clob,
            out_count=err_num,
            out_others=out_others
        )
        err_num = int(err_num.getvalue())
        print(err_num)

        if out_others.getvalue():
            print(out_others.getvalue().read(), '1251')

        self.db.cnn.commit()
