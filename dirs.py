import os

import pandas as pd


def get_project_objects(dir_path):
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
