import logging
import os
import re

import cx_Oracle
import pandas as pd
from git import Repo

connection_string = "ibs/HtuRhtl@day"

logger = logging.getLogger('root')


def select(cnn, sql_text):
    # conn = pool.acquire()

    def read_value(v):
        if v and type(v) in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.LOB):
            return v.read()
        return v

    def read():
        cursor = cnn.cursor()
        cursor.execute(sql_text)
        rows = [[read_value(v) for v in row] for row in cursor]
        # print(cursor.description)
        names = [x[0] for x in cursor.description]
        cursor.close()
        return pd.DataFrame(rows, columns=names)

    df = read()
    # pool.release(conn)
    return df


# def execute_plsql(cnn, pl_sql_text):
#     # conn = pool.acquire()
#     cursor = cnn.cursor()
#     cursor.execute(pl_sql_text)
#     cursor.close()
#     # pool.release(conn)
#
#
# def select_tune_date_update(cnn):
#     sql = """select to_date(Z$FP_TUNE_LIB.get_str_value('BRK_DB_UPDATE_DATE',throw_error=>'0'),'dd/mm/yyyy hh24:mi:ss') date_update
#              from dual"""
#     tune = select(cnn, sql)["DATE_UPDATE"][0]
#     logger.info("Tune selected: %s" % tune)
#     return tune
#
#
# def create_tune_date_update(cnn):
#     logger.info("Create tune")
#     sql = """
#         declare
#           i integer := rtl.open;
#         begin
#
#             declare
#                 plp$ID_1  number;
#               V_CODE  varchar2(200) := 'BRK_DB_UPDATE_DATE';
#               V_TUNE  number;
#               V_VALUE varchar2(200) := TO_CHAR(SYSDATE,'dd/mm/yyyy hh24:mi:ss');
#             begin
#               begin
#                 declare
#                   cursor c_obj is
#                     select  a1.id
#                     from Z#FP_TUNE a1
#                     where a1.C_CODE = V_CODE;
#                 begin
#                   plp$ID_1 := NULL;
#                   for plp$c_obj in c_obj loop
#                     plp$ID_1 := plp$c_obj.id; exit;
#                   end loop;
#                   if plp$ID_1 is NULL then raise rtl.NO_DATA_FOUND; end if;
#                 end;
#                 V_TUNE := plp$ID_1;
#               exception
#               when RTL.NO_DATA_FOUND then
#                 V_TUNE := Z$FP_TUNE_NEW#AUTO.NEW#AUTO_EXECUTE(NULL,'FP_TUNE',V_CODE,'БРК. Дата обновления тестовой базы с боевой','BRK','STRING',null,'True - обрабочкики включены',false,null,false);
#               end;
#               plp$ID_1 := Z$FP_TUNE_LIB.SET_VALUE(V_CODE,V_VALUE);
#             end;
#             commit;
#             rtl.close(i);
#         end;"""
#     execute_plsql(cnn, sql)
#     logger.info("Tune created")


# def job():
#     if not select_tune_date_update():
#         create_tune_date_update()


def create_jobs():
    os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
    os.environ['NLS_LANG'] = '.AL32UTF8'
    dbs = ["ibs/HtuRhtl@day"]
    for db_cnn_str in dbs:
        db_name = re.match(r"(?P<user>.+)/(?P<pass>.+)@(?P<dbname>.+)", db_cnn_str).group('dbname')
        # os.makedirs(os.path.join('dbs', db_name))
        git_url = "http://git.brc.local:3000/ivan.bryzzhin/abs.git"
        repo_dir = os.path.join('dbs', db_name)

        if not os.path.isdir(os.path.join(repo_dir, '.git')):
            logger.info("clone repo for %s" % db_name)
            Repo.clone_from(git_url, repo_dir)
        repo = Repo(repo_dir)
        logger.info("open repo %s is_dirty: %s" % (db_name, repo.is_dirty()))

        cnn = cx_Oracle.connect(db_cnn_str)
        if not select_tune_date_update(cnn):
            logger.info("База была обновлена. Запишим новую настройку об обновлении")
            create_tune_date_update(cnn)
            date_updated = select_tune_date_update(cnn)
            branch_name = '%s_%s' % (db_name, date_updated.strftime('%Y%m%d_%H%M'))  # %Y%m%d_%H%M%S
            logger.info("checkout new branch %s" % branch_name)
            past_branch = repo.create_head(branch_name, 'master')
            repo.head.reference = past_branch
            repo.head.reset(index=True, working_tree=True)
            # repo.head.reference = past_branch
            # assert not repo.head.is_detached
            # reset the index and working tree to match the pointed-to commit
            # repo.head.reset(index=True, working_tree=True)


# create_jobs()
def job():
    os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
    os.environ['NLS_LANG'] = '.AL32UTF8'
    cnn = cx_Oracle.connect("ibs/HtuRhtl@day")
    print(select(cnn, """select m.class_id, m.short_name, m.package_name, m.modified, m.user_modified, s.type, s.line, s.text a--, m.user_driven, m.*
from sources s
inner join METHODS m on m.id = s.name
where m.class_id = 'BRK_MSG' and m.short_name = 'L'
order by m.class_id, m.short_name, s.type, s.line""").groupby(
        ['CLASS_ID', 'SHORT_NAME', 'PACKAGE_NAME', 'MODIFIED', 'USER_MODIFIED', 'TYPE']))


job()
# schedule.every(2).seconds.do(job)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)
