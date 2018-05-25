import logging
import os
import traceback

import cx_Oracle
import pandas as pd

from abs_sync import config, dirs
from strings import nstr

logger = logging.getLogger('root')
pd.options.display.width = 300


def read_value_default(v):
    if v and type(v) in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.LOB):
        return v.read()
    return v


def read_value_cursor(v):
    if v and type(v) in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.LOB):
        return v.read()
    elif v and type(v) == cx_Oracle.Cursor:
        # return "\n".join(filter(None, [a[0] for a in v]))
        # return "\n".join([a[0] or '' for a in v])
        s = ""
        for a in v:
            s += (a[0] or '') + "\n"
        return s
        # return "\n".join([a[0] or '' for a in v])
    return v


def select(cnn, sql_text, read_value=read_value_default):
    db_logger = logging.getLogger(cnn.dsn)

    try:
        def read():
            cursor = cnn.cursor()
            cursor.execute(sql_text)
            select_result_df = None
            if cursor.description:
                rows = [[read_value(v) for v in row] for row in cursor]
                names = [x[0] for x in cursor.description]
                select_result_df = pd.DataFrame(rows, columns=names)
            cursor.close()
            return select_result_df

        df = read()
        return df
    except Exception as e:
        db_logger.debug("\n***************\n%s\n*************" % sql_text)
        db_logger.exception("%s" % traceback.format_exc())
        raise e


abs_users_sql = """
    select distinct regexp_replace(USER_ID,'.*\.(.*)','\1') from (
              select user_id from aud.ibs_diary1
        union select user_id from aud.ibs_diary2
        union select user_id from aud.ibs_diary3
        union select user_id from aud.ibs_diary4
        --union select user_id from aud.ibs_diary5
        union select user_id from aud.ibs_diary6
        union select user_id from aud.ibs_diary7
        )
    order by regexp_replace(USER_ID,'.*\.(.*)','\1')"""

# select_sql = "SELECT * FROM (%s)"

methods_sql = u"""
            WITH types AS (SELECT 'EXECUTE' type, 'body' EXT FROM dual
               UNION SELECT 'VALIDATE', 'validate' FROM dual
               UNION SELECT 'VBSCRIPT', 'script' FROM dual
               UNION SELECT 'PUBLIC', 'globals' FROM dual
               UNION SELECT 'PRIVATE', 'locals' FROM dual)
            SELECT m.class_id
              , m.short_name
              , m.modified
              , m.user_modified
              ,CURSOR(SELECT s.text FROM sources s WHERE s.name = m.id AND s.type = t.type ORDER BY s.line) TEXT
              ,t.EXT EXTENTION
            FROM methods m
            CROSS JOIN types t
            WHERE greatest(m.modified, coalesce((SELECT max(dm.TIME) FROM DIARY_METHODS dm WHERE dm.method_id = m.id),m.modified)) BETWEEN to_date('%s','dd.mm.yyyy hh24:mi:ss') and sysdate
              and m.modified >= sysdate - {0}
              and upper(m.user_modified) in ({1})
            --inner join METHODS m on m.id = s.name
            --where m.class_id = 'BRK_MSG' and m.short_name = 'L'
            --order by m.class_id, m.short_name""".format(config.days_when_object_modified_update_from_action, config.users_to_save_objects_in_git_str)

triggers_sql = u"""
  select * from (
            SELECT substr(tr.table_name,instr(tr.table_name,'#')+1) CLASS_ID
              , tr.TRIGGER_NAME SHORT_NAME
              , to_date(obj.TIMESTAMP, 'yyyy-mm-dd:hh24:mi:ss') MODIFIED
              , '' USER_MODIFIED
              , 'CREATE OR REPLACE TRIGGER ' || tr.description HEADER
              , tr.trigger_body TEXT
              , 'trg' EXTENTION
            FROM all_triggers tr
            INNER JOIN user_objects obj ON obj.OBJECT_NAME = tr.TRIGGER_NAME
            WHERE to_date(obj.TIMESTAMP, 'yyyy-mm-dd:hh24:mi:ss') BETWEEN to_date('%s','dd.mm.yyyy hh24:mi:ss') AND sysdate
            --where tr.trigger_name = 'BKB_DEL_MAIN_DOCUM'
            ) where 1=1
            """

creteria_sql = r"""
select * from (
    SELECT cr.class_id
      , cr.short_name
      , cr.MODIFIED
      , coalesce(    (SELECT max(d.TIME) FROM DIARY_CRITERIA d WHERE d.criteria_id = cr.id),cr.MODIFIED) ACTION_DATE
      , regexp_replace((SELECT d.USER_ID FROM DIARY_CRITERIA d WHERE d.criteria_id = cr.id AND d.ACTION IN ('INSERTED','UPDATED') AND d.TIME = (SELECT max(d.TIME) FROM DIARY_CRITERIA d WHERE d.criteria_id = cr.id AND d.ACTION IN ('INSERTED','UPDATED'))),'.*\.(.*)','\1') USER_MODIFIED
      , cr.condition
      , cr.order_by
      , cr.group_by
      , '' EXTENTION
    FROM criteria cr
    WHERE cr.modified >= sysdate - {0}
) cr
where greatest(cr.MODIFIED, cr.ACTION_DATE) BETWEEN to_date('%s','dd.mm.yyyy hh24:mi:ss') and sysdate
  and upper(cr.user_modified) in ({1})""".format(config.days_when_object_modified_update_from_action, config.users_to_save_objects_in_git_str)

texts_sql = {'METHOD': methods_sql, 'VIEW': creteria_sql, 'TRIGGER': triggers_sql}
interval = {'d': 'day', 'h': 'hour', 'm': 'minute', 's': 'second'}


def norm_object(df, type):
    if type == 'VIEW':
        df["TEXT"] = df["CONDITION"].map(nstr) + df["ORDER_BY"].map(nstr) + df["GROUP_BY"].map(nstr)
        df = df.drop(["CONDITION", "ORDER_BY", "GROUP_BY"], axis=1)
    elif type == 'TRIGGER':
        df["TEXT"] = df["HEADER"].map(nstr) + df["TEXT"]
        del df["HEADER"]
    df = df[df["TEXT"].map(lambda a: a.strip() != '')]  # Удалим строки с пустыми TEXT
    # df['USER_MODIFIED'] = df['USER_MODIFIED'].map(lambda a: a or '')
    return df


def select_by_date_modified(cnn, object_type, last_date_update):
    # sql = """select * from (%s\n) where
    #         ACTION_DATE between to_date('%s','dd.mm.yyyy hh24:mi:ss') and sysdate
    #         order by modified nulls last
    #         """ % (texts_sql[object_type], last_date_update)
    sql = texts_sql[object_type] % last_date_update
    # print(sql)
    return norm_object(select(cnn, sql, read_value_cursor), object_type)


def select_all_by_date_modified(cnn, last_date_update):
    df = pd.concat([select_by_date_modified(cnn, t, last_date_update) for t in texts_sql.keys()])
    # if len(df) > 0:
    #     df = df.sort_values('MODIFIED', ascending=True)
    return df


def select_types_in_folder_or_date_modified(cnn, object_type, folder_objects, update_from_date):
    objs = folder_objects[folder_objects["TYPE"] == object_type].copy()
    objs.SHORT_NAME = objs.SHORT_NAME.apply(lambda a: "'%s'" % a.upper())
    group = objs.groupby(["CLASS_ID"])
    condition = "CLASS_ID = '%s' and SHORT_NAME in (%s)"
    where_by_name = "\n or ".join(condition % (class_id, ",".join(rows["SHORT_NAME"])) for class_id, rows in group)
    # sql = texts_sql[object_type] + """\n where (%s) or
    #                 action_date between to_date('%s','dd.mm.yyyy hh24:mi:ss') and sysdate
    #                 """ % (where_by_name, update_from_date)
    sql = texts_sql[object_type] % update_from_date + "\n or %s" % where_by_name
    df = norm_object(select(cnn, sql, read_value_cursor), object_type)
    return df


def select_objects_in_folder_or_date_modified(cnn, update_from_date):
    git_dir = os.path.join(config.git_folder, cnn.dsn)
    folder_objects_df = dirs.objects_in_folder(git_dir)
    s = select_types_in_folder_or_date_modified
    df = pd.concat([s(cnn, t, folder_objects_df, update_from_date) for t in texts_sql.keys()])
    # df = df[df["TEXT"].map(lambda a: a.strip() != '')]  # Удалим строки с пустыми TEXT
    return df


def select_tune_date_update(cnn):
    sql = """SELECT to_date(Z$FP_TUNE_LIB.get_str_value('BRK_DB_UPDATE_DATE',throw_error=>'0'),'dd/mm/yyyy hh24:mi:ss') date_update
             FROM dual"""
    tune = select(cnn, sql)["DATE_UPDATE"][0]
    # logger.info("Tune selected: %s" % tune)
    return tune


def create_tune_date_update(cnn):
    logger.debug("Create tune")
    sql = u"""
        declare
          i integer := rtl.open;
        begin
        
            declare
                plp$ID_1  number;
              V_CODE  varchar2(200) := 'BRK_DB_UPDATE_DATE';
              V_TUNE  number;
              V_VALUE varchar2(200) := TO_CHAR(SYSDATE,'dd/mm/yyyy hh24:mi:ss');
            begin
              begin
                declare
                  cursor c_obj is
                    select  a1.id
                    from Z#FP_TUNE a1
                    where a1.C_CODE = V_CODE;
                begin
                  plp$ID_1 := NULL;
                  for plp$c_obj in c_obj loop
                    plp$ID_1 := plp$c_obj.id; exit;
                  end loop;
                  if plp$ID_1 is NULL then raise rtl.NO_DATA_FOUND; end if;
                end;
                V_TUNE := plp$ID_1;
              exception
              when RTL.NO_DATA_FOUND then
                V_TUNE := Z$FP_TUNE_NEW#AUTO.NEW#AUTO_EXECUTE(NULL,'FP_TUNE',V_CODE,'БРК. Дата обновления тестовой базы с боевой','BRK','STRING',null,'True - обрабочкики включены',false,null,false);
                plp$ID_1 := Z$FP_TUNE_LIB.SET_VALUE(V_CODE,V_VALUE);
                :date_value := to_date(V_VALUE,'dd/mm/yyyy hh24:mi:ss');
              end;
              
            end;
            commit;
            rtl.close(i);
        end;"""
    cursor = cnn.cursor()
    date_value = cursor.var(cx_Oracle.DATETIME)  # http://cx-oracle.readthedocs.io/en/latest/module.html
    cursor.execute(sql, date_value=date_value)
    date_value = date_value.getvalue()
    cursor.close()
    logger.debug("Tune created with value=%s" % date_value)
    return date_value


def delete_tune_date_update(cnn):
    sql = """declare
         i integer := rtl.open;
        begin
         delete from Z#FP_TUNE where C_CODE = 'BRK_DB_UPDATE_DATE';
         commit;
         rtl.close(i);
        end;"""
    select(cnn, sql)


def select_max_object_date_modified(cnn):
    sql = """SELECT max(t.MODIFIED) MODIFIED FROM (
            SELECT m.modified FROM methods m
        UNION ALL SELECT  to_date(obj.TIMESTAMP, 'yyyy-mm-dd:hh24:mi:ss') MODIFIED FROM all_triggers tr
        INNER JOIN user_objects obj ON obj.OBJECT_NAME = tr.TRIGGER_NAME
        UNION ALL SELECT c.modified
        FROM criteria c) t --where t.modified <= sysdate
        """
    return select(cnn, sql)


def select_sysdate(cnn):
    return select(cnn, "SELECT sysdate FROM dual").iloc[0]['SYSDATE']


def select_users(cnn):
    sql = """SELECT DISTINCT m.user_modified FROM methods m
WHERE m.modified >= sysdate - 30
ORDER BY m.user_modified"""
    return select(cnn, sql)
