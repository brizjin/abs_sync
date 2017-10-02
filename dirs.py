import os
import re

import pandas as pd

import log

logger = log.log_init("root")

def objects_in_folder(dir_path):
    folders = [f for f in os.listdir(dir_path) if not os.path.isfile(os.path.join(dir_path, f))]
    folders = [f for f in folders if f not in ['.git', '.idea', '.sync', 'PLSQL', 'TESTS']]
    files = [(f, file) for f in folders for file in os.listdir(os.path.join(dir_path, f))]

    def f(s):
        sp = s.split(".")
        if len(sp) <= 2:
            return 'VIEW'
        elif sp[-2].lower() in ['body', 'validate', 'script', 'globals', 'locals']:
            return 'METHOD'
        elif sp[-2].lower() in ['package', 'package body']:
            return 'PACKAGE'
        elif sp[-2].lower() == 'trg':
            return 'TRIGGER'

    df = pd.DataFrame([(file[0], file[1].split('.')[0], f(file[1])) for file in files],
                      columns=['CLASS_ID', 'SHORT_NAME', 'TYPE']).drop_duplicates()
    return df  # [df["TYPE"] == 'METHOD']


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


def write_object_from_row(project_folder, row):
    # logger.info("write_row to folder %s" % project_folder)
    write_object(project_folder, row["CLASS_ID"], row["SHORT_NAME"], row["EXTENTION"], row["TEXT"])

def write_object_from_df(df, project_folder_path):
    for idx, row in df.iterrows():
        write_object(project_folder_path, row["CLASS_ID"], row["SHORT_NAME"], row["EXTENTION"], row["TEXT"])
