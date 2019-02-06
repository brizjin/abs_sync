import datetime
import logging
import os
import re
import shutil
import traceback
from pathlib import Path

import cx_Oracle
import pandas as pd

from abs_sync import config
from abs_sync.git_funcs import clone_or_open_repo, select_tune_date_update, create_tune_date_update, tune_branch_name, \
    commit_by_dataframe
from abs_sync.selects import select_sysdate
from cx_Oracle import Connection


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


def pull_all_objects(cnn, from_date=None):
    CftSchema.remove_unknown_files_on_disk()
    disk_schema = CftSchema.read_disk_schema()
    db_schema = CftSchema.read_db_schema(cnn, disk_schema.elements)

    df1 = disk_schema.as_df()
    df2 = db_schema.as_df()
    df4 = pd.merge(df1, df2, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
    if from_date:
        time_schema = CftSchema.read_db_schema_from_time(from_date)
        df4 = pd.merge(df4, time_schema, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge',
                                                                                                        axis=1)
    for m in CftSchema(df4).as_cls():
        m.remove_from_disk()

    db_schema.write_to_disk_from_db(cnn)
    return df2


def pull_last_objects(cnn, count, unit):
    db_schema = CftSchema.read_db_schema_by_time(cnn, count, unit)
    db_schema.write_to_disk_from_db(cnn)
    return db_schema.as_df()


def pull_time_objects(cnn, from_date):
    db_schema = CftSchema.read_db_schema_from_time(cnn, from_date)
    db_schema.write_to_disk_from_db(cnn)
    return db_schema.as_df()


class Db:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.username = str(connection_string[:connection_string.find('/')])
        self.password = str(connection_string[connection_string.find('/') + 1:connection_string.find('@')])
        self.dsn = str(connection_string[connection_string.find('@') + 1:])
        self.cnn = Connection(user=self.username, password=self.password, dsn=self.dsn)

    def __str__(self):
        return "%s.%s[%s]" % (self.__module__, self.__class__.__name__, self.connection_string)

    def select(self, sql_text, **kwargs):
        cursor = self.cnn.cursor()
        try:
            cursor.execute(sql_text, **kwargs)
            desc = [d[0] for d in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=desc)
            # df = df.fillna(value=nan)
            df = df.fillna('')
            return df
        finally:
            cursor.close()

    def execute_plsql(self, pl_sql_text, **kwargs):
        cursor_vars = {}

        cursor = self.cnn.cursor()
        try:
            for k, v in kwargs.items():
                if v in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.NCHAR, cx_Oracle.NUMBER):
                    cursor_vars[k] = cursor.var(v)
                else:
                    cursor_vars[k] = v

            cursor.execute(pl_sql_text, **cursor_vars)

            for k, v in kwargs.items():
                if v in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.NCHAR, cx_Oracle.NUMBER):
                    value = cursor_vars[k].getvalue()
                    if value and v in (cx_Oracle.CLOB, cx_Oracle.BLOB):
                        value = value.read()
                    cursor_vars[k] = value
            return cursor_vars
        finally:
            cursor.close()

    def select_sid(self):
        return self.select("select sys_context('userenv','instance_name') sid from dual")["SID"][0]


save_method_sources_tst = read_tst(os.path.join(config.project_root, "sql", "save_method_sources.tst"))
read_method_sources_tst = read_tst(os.path.join(config.project_root, "sql", "method_sources.tst"))
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

method_parts = ['body', 'validate', 'globals', 'locals', 'script']
exts = dict(PACKAGE=['.bdy', '.spc'], VIEW=['.sql'], TRIGGER=['.trg.sql'],
            METHOD=[".%s.sql" % part for part in method_parts])


class CftElement:
    def __init__(self, class_name, name):
        self.name = name
        self.class_name = class_name
        self.texts = None

    def __str__(self):
        return "%s ::[%s].[%s]" % (self.__class__.__name__, self.class_name, self.name)

    def __repr__(self):
        return "<%s ::[%s].[%s]>" % (self.__class__.__name__, self.class_name, self.name)

    def remove_from_disk(self, project_directory=config.texts_working_dir):
        for ext in exts[self.__class__.__name__.upper()]:
            filename = os.path.join(project_directory, self.class_name, self.name + ext)
            # print("remove", filename)
            try:
                os.remove(filename)
            except OSError:
                pass


def write_text(text, filename):
    if text:
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(filename, "wb+") as f:
            # text = re.sub(r'[ \t\r\f\v]*\n', '\n', text, flags=re.M)
            # f.write(text.replace('\n', '\r\n').encode())
            f.write(text.encode())
    else:
        try:
            os.remove(filename)
        except OSError:
            pass


class Method(CftElement):
    def read_from_disk(self, project_directory=config.texts_working_dir):
        file_name = os.path.join(project_directory, self.class_name, self.name + '.%s.sql')
        r = dict((key, read_if_exists(file_name % key)) for key in method_parts)
        self.texts = r
        return self

    def read_from_db(self, cnn):
        output_vars = dict((key, cx_Oracle.CLOB) for key in method_parts)
        output_vars.update(class_name=self.class_name, method_name=self.name)
        r = cnn.execute_plsql(read_method_sources_tst, **output_vars)
        self.texts = {key: r[key] for key in method_parts if r[key]}
        return self

    def write_to_disk(self, project_directory=config.texts_working_dir):
        file_name = os.path.join(project_directory, self.class_name, self.name)
        for part in method_parts:
            write_text(self.texts.get(part), file_name + ".%s.sql" % part)

        # write_text(self.texts['body'], file_name + ".body.sql")
        # write_text(self.texts["validate"], file_name + ".validate.sql")
        # write_text(self.texts["globals"], file_name + ".globals.sql")
        # write_text(self.texts["locals"], file_name + ".locals.sql")
        # write_text(self.texts["script"], file_name + ".script.sql")

    def write_to_db(self, db):
        cursor = db.cnn.cursor()
        # cursor_vars = ['b', 'v', 'g', 'l', 's'] # **dict((key, cx_Oracle.CLOB) for key in cursor_vars)
        cursor.setinputsizes(
            body=cx_Oracle.CLOB,
            validate=cx_Oracle.CLOB,
            globals=cx_Oracle.CLOB,
            locals=cx_Oracle.CLOB,
            script=cx_Oracle.CLOB
        )
        err_clob, out_others, err_num = cursor.var(cx_Oracle.CLOB), cursor.var(cx_Oracle.STRING), cursor.var(
            cx_Oracle.NUMBER)

        cursor.execute(
            save_method_sources_tst,
            class_name=self.class_name.upper(),
            method_name=self.name.upper(),

            user_name=os.getlogin(),

            body=self.texts.get('body'),
            validate=self.texts.get('validate'),
            globals=self.texts.get('globals'),
            locals=self.texts.get('locals'),
            script=self.texts.get('script'),

            out=err_clob,
            out_count=err_num,
            out_others=out_others,

            # **dict((key, self.texts[key]) for key in cursor_vars)
        )
        err_num = int(err_num.getvalue())
        print("Всего сообщений: ", err_num)
        print(err_clob.getvalue())
        # print("OTHERS=", out_others.getvalue())

        # if others:
        #     print(others.read(), '1251')

        db.cnn.commit()


class View(CftElement):
    def read_from_db(self, cnn):
        r = cnn.select("""
            select condition, order_by, group_by
            from criteria cr
            where cr.class_id = :class_id
              and cr.short_name = :short_name""", **dict(class_id=self.class_name, short_name=self.name))
        if r.size > 0:
            self.texts = r.to_dict('index')[0]
        else:
            self.texts = None
        return self

    def write_to_disk(self, project_directory=config.texts_working_dir):
        file_name = os.path.join(project_directory, self.class_name, self.name + ".sql")
        # if self.texts:
        write_text(self.texts["CONDITION"] + self.texts["ORDER_BY"] + self.texts["GROUP_BY"], file_name)


class Trigger(CftElement):
    def read_from_db(self, cnn):
        r = cnn.execute_plsql("""
                declare 
                  c long;
                  d varchar2(32000);
                begin
                  select trigger_body, description, table_name
                  into c, d, :table_name
                  from all_triggers where trigger_name = :trigger_name;
                  :trigger_body := 'CREATE OR REPLACE TRIGGER ' || d || c;
                end;""", **dict(trigger_name=self.name, trigger_body=cx_Oracle.CLOB, table_name=cx_Oracle.NCHAR))
        class_name = r["table_name"].split('#')[1]
        self.texts = r
        return self

    def write_to_disk(self, project_directory=config.texts_working_dir):
        file_name = os.path.join(project_directory, self.class_name, self.name + ".trg.sql")
        write_text(self.texts["trigger_body"], file_name)


class Package(CftElement):
    pass


class Unknown(CftElement):
    pass


class CftSchema:
    def __init__(self, elements):
        if type(elements) == pd.DataFrame:
            # print(elements)
            self.elements = [tuple(row) for i, row in elements.iterrows()]
        else:
            self.elements = list(set(elements))

    def write_to_disk_from_db(self, cnn):
        for element in self.as_cls():
            if type(element) not in [Package]:
                element.read_from_db(cnn)
                element.write_to_disk()

    @staticmethod
    def read_files_list(project_directory=config.texts_working_dir):
        folders = [f for f in os.listdir(project_directory) if not os.path.isfile(os.path.join(project_directory, f))]
        folders = [f for f in folders if f not in ['.git', '.idea', '.sync', 'PLSQL', 'TESTS']]

        def get_element_type(file):
            ext = "".join(Path(file).suffixes)
            # print(file,ext)

            for key, value in exts.items():
                if ext in value:
                    return key
            return 'UNKNOWN'
            # sp = file.split(".")
            # if sp[-1].lower() in ['bdy', 'spc']:
            #     return 'PACKAGE'
            # elif len(sp) <= 2:
            #     return 'VIEW'
            # elif sp[-2].lower() in ['body', 'validate', 'script', 'globals', 'locals']:
            #     return 'METHOD'
            # elif sp[-2].lower() == 'trg':
            #     return 'TRIGGER'
            # else:
            #     return 'UNKNOWN'

        files = [(folder, file, get_element_type(file)) for folder in folders for file in
                 os.listdir(os.path.join(project_directory, folder))]
        return files

    @staticmethod
    def read_disk_schema(project_directory=config.texts_working_dir):
        files = CftSchema.read_files_list(project_directory)
        return CftSchema([(elem_type, folder, file.split('.')[0]) for folder, file, elem_type in files])

    @staticmethod
    def remove_unknown_files_on_disk(project_directory=config.texts_working_dir):
        files = CftSchema.read_files_list(project_directory)
        for folder, filename, element_type in files:
            if element_type == 'UNKNOWN':
                try:
                    remove_filename = os.path.join(project_directory, folder, filename)
                    # print("remove unknown", remove_filename)
                    os.remove(remove_filename)
                except OSError:
                    pass

    @staticmethod
    def clear_folder(folder):
        for the_file in os.listdir(folder):
            if the_file[0] == '.':
                continue

            file_path = os.path.join(folder, the_file)

            if the_file in ['requirements.txt']:
                continue
            if the_file in ['PLSQL', 'TESTS', 'PACKAGES'] and os.path.isdir(file_path):
                continue

            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(e)

    @staticmethod
    def read_db_schema(cnn, elements_list):
        where = " or ".join(["TYPE = '%s' and CLASS_ID = '%s' and SHORT_NAME = '%s'" % e for e in elements_list])
        df = cnn.select("""select TYPE, CLASS_ID, SHORT_NAME from (%s) where %s""" % (db_obj_sql_text, where))
        return CftSchema(df)

    @staticmethod
    def read_db_schema_by_time(cnn, num, interval_name):
        time_interval = {'d': 'day', 'h': 'hour', 'm': 'minute'}
        df = cnn.select("""
                    select TYPE, CLASS_ID, SHORT_NAME from (%s)
                    where modified > sysdate - interval '%s' %s
                    order by modified desc nulls last""" % (db_obj_sql_text, num, time_interval[interval_name]))
        return CftSchema(df)

    @staticmethod
    def read_db_schema_from_time(cnn, from_date):
        df = cnn.select("""
                    select TYPE, CLASS_ID, SHORT_NAME from (%s)
                    where modified between to_date('%s','dd.mm.yyyy hh24:mi:ss') and sysdate
                    order by modified desc nulls last""" % (db_obj_sql_text, from_date))
        return CftSchema(df)

    def as_df(self):
        return pd.DataFrame(self.elements, columns=['TYPE', 'CLASS', 'NAME'])

    def as_cls(self):
        element_classes = {'METHOD': Method, 'VIEW': View, 'PACKAGE': Package, 'TRIGGER': Trigger, 'UNKNOWN': Unknown}
        return [element_classes[row[0]](row[1], row[2]) for row in self.elements]


last_dates_update = {}
dbs = {}


# def update(connection_string):
#     # global last_date_update
#     # df = None
#     m = re.match(r"(?P<user>.+)/(?P<pass>.+)@(?P<dbname>.+)", connection_string)
#     db_name = m.group('dbname')
#
#     db_logger = logging.getLogger(db_name)
#     db_logger.debug("update %s" % db_name)
#
#     db = dbs.get(connection_string)
#     if not db:
#         try:
#             # cnn = cx_Oracle.connect(connection_string)
#             db = Db(connection_string)
#             dbs[connection_string] = db
#         except cx_Oracle.DatabaseError as e:
#             error = e.args[0]
#             # print "Code:", error.code
#             # print "Message:", error.message
#             dbs[connection_string] = None
#             db_logger.exception(
#                 "update exception on connection to database code=%s, message=%s" % (error.code, error.message))
#             return
#
#     try:
#         last_date_update = last_dates_update.get(db_name)
#         sysdate = select_sysdate(db.cnn)
#         # кол-во дней на которые на сервере передвинули время
#         # корректируем дату коммитов на эту дельту
#         days_delta = datetime.timedelta(days=(
#                 datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - sysdate.replace(hour=0,
#                                                                                                              minute=0,
#                                                                                                              second=0,
#                                                                                                              microsecond=0)).days)
#         db_logger.debug("delta_days=%s, datetime.datetime.now()=%s, sysdate=%s" % (
#             days_delta, datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
#             sysdate.replace(hour=0, minute=0, second=0, microsecond=0)))
#
#         if not last_date_update:
#             last_date_update = sysdate - datetime.timedelta(days=config.days_update_on_start)
#             db_logger.debug("last_date_update is null set it to %s" % last_date_update)
#             # залогируем список пользователей за последний месяц правивших методы
#             # чтобы можно было найти тех по кому мы не сохраняем изменения
#             # db_logger.debug(",\n".join(["'%s'" % a for a in select_users(cnn)['USER_MODIFIED']]))
#
#         repo = clone_or_open_repo(os.path.join(config.git_folder, db_name), db_name)
#
#         if not select_tune_date_update(db.cnn):
#             db_logger.debug("update init_git_db")
#             create_tune_date_update(db.cnn)
#             # df = select_objects_in_folder_or_date_modified(cnn, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
#             df = pull_all_objects(db, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
#         else:
#             db_logger.debug("select object on date %s" % last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
#             # df = select_all_by_date_modified(cnn, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
#             df = pull_time_objects(db, last_date_update.strftime('%d.%m.%Y %H:%M:%S'))
#         if len(df) > 0:
#
#             branch_name = tune_branch_name(db.cnn)
#             if branch_name in repo.branches:
#                 # repo.heads[branch_name].checkout()
#                 try:
#                     remote_branch_name = 'origin/%s' % branch_name
#                     repo.remotes['origin'].fetch(refspec='{}'.format(branch_name))
#                     repo.head.reset(commit=repo.refs[remote_branch_name], index=True, working_tree=True)
#                 except Exception:
#                     db_logger.exception("exceprion checkout and reset %s %s" % (branch_name, traceback.format_exc()))
#             else:
#                 try:
#                     repo.heads['master'].checkout()
#
#                     # with repo.remotes['origin'].config_writer as cw:
#                     #     cw.set("push", "refs/heads/*:refs/heads/qa/*")
#                     repo.remotes['origin'].fetch(refspec='{}'.format('master'))
#                     repo.head.reset(commit=repo.refs['origin/master'], index=True, working_tree=True)
#                 except Exception:
#                     db_logger.exception("exceprion checkout and reset master %s" % traceback.format_exc())
#                 repo.create_head(branch_name, 'master').checkout()
#
#             # try:
#             #     if repo.refs[remote_branch_name].tracking_branch():
#             #         repo.remotes['origin'].fetch(refspec='{}'.format(branch_name))
#             #         # repo.remotes['origin'].fetch(refspec='{}'.format(branch_name))
#             #         repo.head.reset(commit=repo.refs[remote_branch_name], index=True, working_tree=True)
#             # except IndexError:
#             #     pass
#
#             commit_by_dataframe(repo, df, db_name, days_delta)
#             # repo.remotes['origin'].push(refspec='{}:refs/heads/autosave/{}'.format(branch_name, branch_name))
#             repo.remotes['origin'].push(refspec='{}:{}'.format(branch_name, branch_name))
#         else:
#             db_logger.debug("up-to-date")
#
#         last_dates_update[db_name] = sysdate
#         db_logger.info("updated finish")
#
#     except cx_Oracle.DatabaseError as e:
#         error = e.args[0]
#         # ORA-01033: ORACLE initialization or shutdown in progress
#         # ORA-03114: not connected to ORACLE
#         if error.code in [1033, 3114]:
#             dbs[connection_string] = None
#             db_logger.exception(
#                 "update db not init: %s, %s" % (error.code, error.message))
#         else:
#             db_logger.exception(
#                 "update db exception code=%s, message=%s" % (error.code, error.message))
#     except Exception:
#         db_logger.exception("update exception %s" % traceback.format_exc())
