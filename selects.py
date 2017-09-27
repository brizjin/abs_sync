import cx_Oracle
import pandas as pd

import dirs


def read_value_default(v):
    if v and type(v) in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.LOB):
        return v.read()
    return v


def read_value_cursor(v):
    if v and type(v) in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.LOB):
        return v.read()
    elif v and type(v) == cx_Oracle.Cursor:
        return "\n".join(filter(None, [a[0] for a in v]))
    return v


def select(cnn, sql_text, read_value=read_value_default):
    def read():
        cursor = cnn.cursor()
        cursor.execute(sql_text)
        rows = [[read_value(v) for v in row] for row in cursor]
        names = [x[0] for x in cursor.description]
        cursor.close()
        return pd.DataFrame(rows, columns=names)

    df = read()
    return df


select_sql = "select * from (%s)"

methods_sql = """
            with types as (select 'EXECUTE' type, 'body' EXT from dual
               union select 'VALIDATE', 'validate' from dual
               union select 'VBSCRIPT', 'script' from dual
               union select 'PUBLIC', 'globals' from dual
               union select 'PRIVATE', 'locals' from dual)
            select m.class_id
              , m.short_name
              , m.modified
              , m.user_modified
              ,cursor(select s.text from sources s where s.name = m.id and s.type = t.type order by s.line) TEXT
              ,t.EXT EXTENTION
            from methods m
            cross join types t
            --inner join METHODS m on m.id = s.name
            --where m.class_id = 'BRK_MSG' and m.short_name = 'L'
            --order by m.class_id, m.short_name"""

triggers_sql = """select * from (
            select substr(tr.table_name,instr(tr.table_name,'#')+1) CLASS_ID
              , tr.TRIGGER_NAME SHORT_NAME
              , to_date(obj.TIMESTAMP, 'yyyy-mm-dd:hh24:mi:ss') MODIFIED
              , '' USER_MODIFIED
              , 'CREATE OR REPLACE TRIGGER ' || tr.description HEADER
              , tr.trigger_body TEXT
              , 'trg' EXTENTION
            from all_triggers tr
            inner join user_objects obj on obj.OBJECT_NAME = tr.TRIGGER_NAME
            )
            --where tr.trigger_name = 'BKB_DEL_MAIN_DOCUM'
            """

creteria_sql = """select cr.class_id
  , cr.short_name
  , cr.MODIFIED
  , '' USER_MODIFIED
  , cr.condition
  , cr.order_by
  , cr.group_by
  , '' EXTENTION
from criteria cr
--where cr.class_id = 'MAIN_DOCUM'
--and cr.short_name = 'VW_CRIT_BRK_BP_DOCS_BP'"""

texts_sql = {'METHOD': methods_sql, 'VIEW': creteria_sql, 'TRIGGER': triggers_sql}


def select_methods_by_name(cnn, class_id, short_name):
    sql = methods_sql + "\nwhere class_id = '%s' and short_name = '%s'" % (class_id, short_name)
    return select(cnn, sql, read_value_cursor)


def where_clause_by_name(objs):
    objs.SHORT_NAME = objs.SHORT_NAME.apply(lambda a: "'%s'" % a.upper())
    group = objs.groupby(["CLASS_ID"])
    condition = "CLASS_ID = '%s' and SHORT_NAME in (%s)"
    where_clause = "\n or ".join(
        condition % (class_id, ",".join(rows["SHORT_NAME"])) for class_id, rows in group)
    return where_clause


def select_all_methods_in_folder(cnn, folder_name):
    df = dirs.get_project_objects(folder_name)
    where_clause = where_clause_by_name(df[df["TYPE"] == 'METHOD'])
    sql = methods_sql + "\nwhere " + where_clause
    return select(cnn, sql, read_value_cursor)


interval = {'d': 'day', 'h': 'hour', 'm': 'minute', 's': 'second'}


def select_types_in_folder_or_date_modified(cnn, type, folder_path, num, interval_name):
    df = dirs.get_project_objects(folder_path)
    where_by_name = where_clause_by_name(df[df["TYPE"] == type])
    sql = texts_sql[type] + """\n where (%s) or
                    modified > sysdate - interval '%s' %s
                    order by modified nulls last""" % (where_by_name, num, interval[interval_name])
    #print(sql)
    df = select(cnn, sql, read_value_cursor)
    return df


def select_methods_by_names_list(cnn, names):
    pass
