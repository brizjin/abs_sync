import logging
import os

import cx_Oracle
import database_client
import pandas as pd
from database_client.jinja import render
from database_client.oracle import OracleClient

from abs_sync import dirs
from abs_sync.config import FOLDER_FOR_DB_GIT_FOLDERS, DAYS_WHEN_OBJECT_MODIFIED_UPDATE_FROM_ACTION, \
    USERS_TO_SAVE_OBJECTS_IN_GIT
from abs_sync.strings import nstr

logger = logging.getLogger(__name__)

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


def methods_sql(on_date):
    text = """
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
        WHERE greatest(m.modified, coalesce(
            (SELECT max(dm.TIME)
             FROM DIARY_METHODS dm
             WHERE dm.method_id = m.id), m.modified)) BETWEEN {{on_date|sqlvar}} and sysdate
          and m.modified >= sysdate - {{days_modified|sqlvar}}
          and upper(m.user_modified) in {{users| sqlvar}}"""
    users_list = USERS_TO_SAVE_OBJECTS_IN_GIT
    days_count = DAYS_WHEN_OBJECT_MODIFIED_UPDATE_FROM_ACTION
    return render(text, on_date=on_date, users=users_list, days_modified=days_count)


def triggers_sql(on_date):
    text = """
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
            WHERE to_date(obj.TIMESTAMP, 'yyyy-mm-dd:hh24:mi:ss') BETWEEN {{on_date|sqlvar}} AND sysdate
            --where tr.trigger_name = 'BKB_DEL_MAIN_DOCUM'
              AND obj.OBJECT_TYPE = 'TRIGGER'
        ) where 1=1
        """
    users_list = USERS_TO_SAVE_OBJECTS_IN_GIT
    days_count = DAYS_WHEN_OBJECT_MODIFIED_UPDATE_FROM_ACTION
    return render(text, on_date=on_date, users=users_list, days_modified=days_count)


def creteria_sql(on_date):
    text = """
        select * from (
            SELECT cr.class_id
                , cr.short_name
                , cr.MODIFIED
                , coalesce(    (SELECT max(d.TIME) FROM DIARY_CRITERIA d WHERE d.criteria_id = cr.id),cr.MODIFIED) ACTION_DATE
                , regexp_replace((SELECT d.USER_ID FROM DIARY_CRITERIA d
                                  WHERE d.criteria_id = cr.id AND d.ACTION IN ('INSERTED','UPDATED')
                                    AND d.TIME = (SELECT max(d.TIME) FROM DIARY_CRITERIA d
                                                  WHERE d.criteria_id = cr.id
                                                    AND d.ACTION IN ('INSERTED','UPDATED'))),'.*\.(.*)','\1') USER_MODIFIED
                , cr.condition
                , cr.order_by
                , cr.group_by
                , '' EXTENTION
            FROM criteria cr
            WHERE cr.modified >= sysdate - {{days_modified|sqlvar}}
        ) cr
        where greatest(cr.MODIFIED, cr.ACTION_DATE) BETWEEN {{on_date|sqlvar}} and sysdate
          and upper(cr.user_modified) in {{users|sqlvar}}"""
    users_list = USERS_TO_SAVE_OBJECTS_IN_GIT
    days_count = DAYS_WHEN_OBJECT_MODIFIED_UPDATE_FROM_ACTION
    return render(text, on_date=on_date, users=users_list, days_modified=days_count)


texts_sql = {'METHOD': methods_sql, 'VIEW': creteria_sql, 'TRIGGER': triggers_sql}
interval = {'d': 'day', 'h': 'hour', 'm': 'minute', 's': 'second'}


def cursor_reader(cursor):
    return "\n".join(value[0] or '' for value in cursor)


def norm_object(df, type):
    # logger.debug(f"type={type}")
    if type == 'VIEW':
        df["TEXT"] = df["CONDITION"].map(nstr) + df["ORDER_BY"].map(nstr) + df["GROUP_BY"].map(nstr)
        df = df.drop(["CONDITION", "ORDER_BY", "GROUP_BY"], axis=1)
    elif type == 'TRIGGER':
        df["TEXT"] = df["HEADER"].map(nstr) + df["TEXT"]
        del df["HEADER"]
    # df = df[df["TEXT"].map(lambda a: a.strip() != '')]  # Удалим строки с пустыми TEXT
    if type in ['METHOD', ]:
        df["TEXT"] = df["TEXT"].map(cursor_reader)
    return df


def select_by_date_modified(cnn, object_type, last_date_update):
    text = texts_sql[object_type](on_date=last_date_update)
    logger.debug("before select")
    df_list = []
    for row_df in cnn.select_gn(text):
        df_list.append(norm_object(row_df, object_type))
    # df = norm_object(df, object_type)
    # logger.debug(f"df_list={df_list}")
    if len(df_list) > 0:
        df_list = pd.concat(df_list)

    df = pd.DataFrame(df_list)
    # logger.debug(f"after select:{df}")
    return df


def select_all_by_date_modified(cnn, last_date_update):
    return pd.concat([select_by_date_modified(cnn, t, last_date_update) for t in texts_sql.keys()], sort=True)


def select_types_in_folder_or_date_modified(cnn, object_type, folder_objects, update_from_date):
    objs = folder_objects[folder_objects["TYPE"] == object_type].copy()
    objs.SHORT_NAME = objs.SHORT_NAME.apply(lambda a: "'%s'" % a.upper())
    group = objs.groupby(["CLASS_ID"])
    condition = "CLASS_ID = '%s' and SHORT_NAME in (%s)"
    where_by_name = "\n or ".join(condition % (class_id, ",".join(rows["SHORT_NAME"])) for class_id, rows in group)
    sql = texts_sql[object_type](on_date=update_from_date) + "\n or %s" % where_by_name
    df = norm_object(cnn.select(sql), object_type)
    return df


def select_objects_in_folder_or_date_modified(cnn: OracleClient, update_from_date):
    git_dir = os.path.join(FOLDER_FOR_DB_GIT_FOLDERS, cnn.connection.dsn)
    folder_objects_df = dirs.objects_in_folder(git_dir)
    s = select_types_in_folder_or_date_modified
    df = pd.concat([s(cnn, t, folder_objects_df, update_from_date) for t in texts_sql.keys()])
    return df


def select_tune_date_update(cnn):
    sql = """
        SELECT to_date(Z$FP_TUNE_LIB.get_str_value('BRK_DB_UPDATE_DATE',throw_error=>'0'),'dd/mm/yyyy hh24:mi:ss') date_update
        FROM dual"""
    tune = cnn.select(sql)["DATE_UPDATE"][0]
    logger.info("Tune selected: %s" % tune)
    return tune


def create_tune_date_update(cnn: database_client.oracle.OracleClient):
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
    date_value = cnn.execute_plsql(sql, date_value=cx_Oracle.TIMESTAMP).get('date_value')

    logger.debug("Tune created with value=%s" % date_value)
    return date_value


def delete_tune_date_update(cnn: OracleClient):
    sql = """
        declare
            i integer := rtl.open;
        begin
            delete from Z#FP_TUNE where C_CODE = 'BRK_DB_UPDATE_DATE';
            commit;
            rtl.close(i);
        end;"""
    cnn.execute_plsql(sql)


def select_max_object_date_modified(cnn):
    sql = """
        SELECT max(t.MODIFIED) MODIFIED FROM (
            SELECT m.modified FROM methods m
        UNION ALL SELECT  to_date(obj.TIMESTAMP, 'yyyy-mm-dd:hh24:mi:ss') MODIFIED FROM all_triggers tr
        INNER JOIN user_objects obj ON obj.OBJECT_NAME = tr.TRIGGER_NAME
        UNION ALL SELECT c.modified
        FROM criteria c) t --where t.modified <= sysdate
        """
    return cnn.select(sql)


def select_sysdate(cnn):
    return cnn.select("SELECT sysdate FROM dual").iloc[0]['SYSDATE']


def select_users(cnn):
    sql = """
        SELECT DISTINCT m.user_modified FROM methods m
        WHERE m.modified >= sysdate - 30
        ORDER BY m.user_modified"""
    return cnn.select(sql)
