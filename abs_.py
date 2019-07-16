import re
import sys

from database_client.oracle import OracleClient

from abs_sync import save_methods
from abs_sync.config import DEFAULT_CONNECTION_STRING
from refresh_methods import update_from_dir_list, update_for_time, update_from_list, update


def f():
    # print("sys=%s" % sys.argv)
    print("start")
    params = sys.argv[1:]

    operation_name = params[0]

    if len(params) < 2:
        print('Необходимо указать базу куда отправляем операцию')
        print('abs [push/pull] [имя базы] [имя класса].[имя операции]')
        return

    db_name = params[1]
    cnn_str = DEFAULT_CONNECTION_STRING
    db = save_methods.Db(cnn_str)

    if operation_name == 'push':

        if len(params) < 3:
            print('Необходимо указать какую операцию отправляем')
            print('abs push [имя базы] [имя класса].[имя операции]')
            return

        object_name = params[2]

        abs_obj_match = re.match(r"(?P<class_name>.+)\.(?P<method_name>.+)", object_name)

        class_name = abs_obj_match.group('class_name')
        method_name = abs_obj_match.group('method_name')

        m = save_methods.Method(db, class_name, method_name)
        texts = m.read_from_disk()
        m.write_to_db(**texts)
        # abs push BKB_TUNE_TARIF.LIB2
    elif operation_name == 'pull':
        print('pull ', params)
        params = params[1:]
        cnn = OracleClient()
        cnn.connect(cnn_str)
        objs = []
        if len(params) == 1:
            print("Обновим все файлы.")
            objs = update_from_dir_list(cnn)
        elif len(params) == 2:
            s = params[1]
            time_match = re.match(r"(?P<num>\d+)(?P<interval_name>\w+)", s)
            list_match = re.match(r"\w+.\w+(,\w+.\w+)*", s)
            if time_match:
                print("Сохраним все изменения за %s." % s)
                objs = update_for_time(cnn, time_match.group('num'), time_match.group('interval_name'))
            elif list_match:
                print("Сохраним по списку операций %s" % s)
                objs = update_from_list(cnn, s)

        for row in objs:
            print("%s ::[%s].[%s]" % (row["type"], row["class_id"], row["short_name"]))
            update(cnn, row["type"], row["class_id"], row["short_name"])


f()
