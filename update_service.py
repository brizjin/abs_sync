import logging
import os
import re

import cx_Oracle
import pandas as pd
from git import Repo

connection_string = "ibs/HtuRhtl@day"

# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)-15s PID %(process)5s %(threadName)10s %(name)18s: %(message)s')
# log_parse_html = logging.getLogger('logger')
# log_parse_html.info("beg")

# create logger with 'spam_application'
logger = logging.getLogger('update_service')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('update_service.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
formatter = logging.Formatter('%(asctime)-15s %(levelname)s PID %(process)5s %(threadName)10s %(name)18s: %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


# def init_pool(conn_str):
#     global cnn
#
#     # instant_client_path = r'C:\Users\BryzzhinIS\Downloads\instantclient_12_2'
#     # if instant_client_path not in sys.path:
#     #     sys.path.insert(0,instant_client_path)
#
#     m = re.match(r"(?P<user>.+)/(?P<pass>.+)@(?P<dbname>.+)", conn_str)
#     os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
#     os.environ['NLS_LANG'] = '.AL32UTF8'
#     # pool = cx_Oracle.SessionPool(user=m.group('user'), password=m.group('pass'), dsn=m.group('dbname'), min=1, max=2,
#     #                              increment=1, threaded=True)
#     cnn = cx_Oracle.connect(conn_str)


# init_pool(connection_string)


def select(sql_text):
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


def execute_plsql(pl_sql_text):
    # conn = pool.acquire()
    cursor = cnn.cursor()
    cursor.execute(pl_sql_text)
    cursor.close()
    # pool.release(conn)


def select_tune_date_update():
    sql = """select to_date(Z$FP_TUNE_LIB.get_str_value('BRK_DB_UPDATE_DATE',throw_error=>'0'),'dd/mm/yyyy hh24:mi:ss') date_update
             from dual"""
    tune = select(sql)["DATE_UPDATE"][0]
    logger.info("Tune selected: %s" % tune)
    return tune


def create_tune_date_update():
    logger.info("Create tune")
    sql = """
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
              end;
              plp$ID_1 := Z$FP_TUNE_LIB.SET_VALUE(V_CODE,V_VALUE);
            end;
            commit;
            rtl.close(i);
        end;"""
    execute_plsql(sql)
    logger.info("Tune created")


def job():
    if not select_tune_date_update():
        create_tune_date_update()


def create_jobs():
    os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
    os.environ['NLS_LANG'] = '.AL32UTF8'
    dbs = ["ibs/HtuRhtl@day"]
    for db_cnn in dbs:
        db_name = re.match(r"(?P<user>.+)/(?P<pass>.+)@(?P<dbname>.+)", db_cnn).group('dbname')
        #os.makedirs(os.path.join('dbs', m.group('dbname')))
        git_url = "http://git.brc.local:3000/ivan.bryzzhin/abs.git"
        repo_dir = os.path.join('dbs', db_name)
        Repo.clone_from(git_url, repo_dir)

        # cnn = cx_Oracle.connect(conn_str)
create_jobs()
# schedule.every(2).seconds.do(job)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)
