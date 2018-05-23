import os
import re
# import importlib.util
#
import sys

import cx_Oracle
from git import Repo

import config
import oracle_connection

os.environ["ORACLE_HOME"] = "C:/app/BryzzhinIS/product/11.2.0/client_1/"
os.environ['NLS_LANG'] = '.AL32UTF8'

# prj_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sync_script_dir = os.path.dirname(os.path.realpath(__file__))



# os.chdir(prj_dir)
#
# spec = importlib.util.spec_from_file_location("module.name", "/path/to/file.py")
# foo = importlib.util.module_from_spec(spec)
# spec.loader.exec_module(foo)


def read_tst(file_name):
    with open(file_name, "r") as f:
        text = f.read()
        text_lines = text.split("\n")
        return "\n".join(text_lines[2:int(text_lines[1]) + 2])


def write_part(part, name):
    if part:
        dirname = os.path.dirname(name)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(name, "wb+") as f:
            part = re.sub(r'[ \t\r\f\v]*\n', '\n', part, flags=re.M)
            f.write(part.replace('\n', '\r\n').encode())
            # f.write(part.encode())


def write_method_text(props):
    file_name = os.path.join(config.texts_working_dir, props["class_name"], props["method_name"])
    write_part(props["e"], file_name + ".body.sql")
    write_part(props["v"], file_name + ".validate.sql")
    write_part(props["g"], file_name + ".globals.sql")
    write_part(props["l"], file_name + ".locals.sql")
    write_part(props["s"], file_name + ".script.sql")


method_sources_tst = read_tst(os.path.join(sync_script_dir, "sql", "method_sources.tst"))


def update_method(cnn, class_name, method_name):
    r = cnn.execute_plsql(method_sources_tst, **dict(class_name=class_name, method_name=method_name, e=cx_Oracle.CLOB,
                                                     v=cx_Oracle.CLOB, g=cx_Oracle.CLOB,
                                                     l=cx_Oracle.CLOB, s=cx_Oracle.CLOB))
    write_method_text(r)


def update_class(cnn, class_name):
    brk_msg_dir = os.path.join(config.texts_working_dir, class_name)
    methods = set([f.split(".")[0] for f in os.listdir(brk_msg_dir) if os.path.isfile(os.path.join(brk_msg_dir, f))])
    for m in methods:
        update_method(cnn, class_name, m)


def update_creteria(cnn, class_name, creteria_name):
    s = cnn.select("""
        select condition, order_by, group_by
        from criteria cr
        where cr.class_id = :class_id
          and cr.short_name = :short_name""", **dict(class_id=class_name, short_name=creteria_name))
    # print(s)
    # print(s["CONDITION"][0])
    # print(s["ORDER_BY"][0])
    # print(s["GROUP_BY"][0])
    file_name = os.path.join(config.texts_working_dir, class_name, creteria_name)
    write_part(s["CONDITION"][0] + s["ORDER_BY"][0] + s["GROUP_BY"][0], file_name + ".sql")


def update_trigger(cnn, trigger_name):
    r = cnn.execute_plsql("""
                declare 
                  c long;
                  d varchar2(32000);
                begin
                  select trigger_body, description, table_name
                  into c, d, :table_name
                  from all_triggers where trigger_name = :trigger_name;
                  :trigger_body := 'CREATE OR REPLACE TRIGGER ' || d || c;
                end;""", **dict(trigger_name=trigger_name, trigger_body=cx_Oracle.CLOB, table_name=cx_Oracle.NCHAR))
    class_name = r["table_name"].split('#')[1]
    file_name = os.path.join(config.texts_working_dir, class_name, trigger_name)
    write_part(r["trigger_body"], file_name + ".trg.sql")


def update(cnn, object_type, class_id, short_name):
    if object_type == 'METHOD':
        update_method(cnn, class_id, short_name)
    elif object_type == 'VIEW':
        update_creteria(cnn, class_id, short_name)
    elif object_type == 'TRIGGER':
        update_trigger(cnn, short_name)


def git_staged_files(repo):
    modified_files = ["m " + m.a_path for m in repo.index.diff(None)]
    staged_files = ["s " + s.a_path for s in repo.index.diff("HEAD")]
    untrack_files = ["u " + u for u in repo.untracked_files]
    return sorted(modified_files + staged_files + untrack_files)


def git_checkout(repo, branch_name):
    s = git_staged_files(repo)
    if len(s) > 0:
        print('Имеются незакомиченные изменения:\n' + "\n".join(s) + "\nОперация прервана.")
        # repo.git.stash('save')
        # repo.git.stash('pop')
        return s
    repo.git.checkout(branch_name)
    print("Текущая ветка %s." % repo.active_branch.name)
    return


def git_commit(repo):
    # repo.git.add(update=True)
    repo.git.add(all=True)
    # modified_files = [os.path.join(config.texts_working_dir, m.a_path) for m in repo.index.diff(None)]
    staged_files = [os.path.join(config.texts_working_dir, s.a_path) for s in repo.index.diff("HEAD")]
    # untrack_files = [os.path.join(config.texts_working_dir, u) for u in repo.untracked_files]
    # s = sorted(modified_files + staged_files + untrack_files)
    s = sorted(staged_files)
    # print("Current branch: %s" % repo.active_branch.name)
    if len(s) > 0:
        print("\n".join(["commit " + f for f in s]))
        # repo.index.add(s)
        repo.index.commit("commited by autosync script")  # .type
        # print(repo.index.entries.items())
    else:
        print("Все изменения уже пременены, нечего коммитить")


db_obj_sql_text = """
              select CLASS_ID, SHORT_NAME, MODIFIED, 'METHOD' TYPE from methods 
    union all select CLASS_ID, SHORT_NAME, MODIFIED, 'VIEW' from criteria
    union all select substr(t.TABLE_NAME,3), object_name, to_date(timestamp,'yyyy-mm-dd hh24:mi:ss'),object_type
    from user_objects u left join all_triggers t on u.OBJECT_NAME = t.TRIGGER_NAME where object_type = 'TRIGGER'
    """


def update_for_time(cnn, num, interval_name):
    if interval_name == 'd':
        interval_name = 'day'
    elif interval_name == 'h':
        interval_name = 'hour'
    elif interval_name == 'm':
        interval_name = 'minute'
    # print(num, interval_name)

    return cnn.select("""
            select * from (%s)
            where modified > sysdate - interval '%s' %s
            order by modified desc nulls last""" % (db_obj_sql_text, num, interval_name))
    # for row in s:
    #     update(cnn, row["type"], row["class_id"], row["short_name"], p)


def update_from_dir_list(cnn, dir_path=config.texts_working_dir):
    folders = [f for f in os.listdir(dir_path) if not os.path.isfile(os.path.join(dir_path, f))]
    folders = [f for f in folders if f not in ['.git', '.idea', '.sync', 'PLSQL', 'TESTS']]
    files = [(f, set([file.split(".")[0] for file in os.listdir(os.path.join(dir_path, f))
                      if os.path.isfile(os.path.join(dir_path, f, file))])) for f in folders]
    files = [(folder, s) for folder, s in files if len(s) > 0]
    files_where = " or ".join(
        ["CLASS_ID = '%s' and SHORT_NAME in (%s)" % (folder[0], ','.join(["'%s'" % f for f in folder[1]])) for
         folder in files])

    return cnn.select("""select CLASS_ID, SHORT_NAME, TYPE from (%s) where %s""" % (db_obj_sql_text, files_where))

    # for row in s:
    #     update(cnn, row["type"], row["class_id"], row["short_name"], p)


def update_from_list(cnn, o):
    objs = [[part for part in obj.split('.')] for obj in o.split(',')]
    files_where = " or ".join("CLASS_ID = '%s' and SHORT_NAME = '%s'" % (obj[0], obj[1]) for obj in objs)
    print(files_where)
    return cnn.select("""select CLASS_ID, SHORT_NAME, TYPE from (%s) where %s""" % (db_obj_sql_text, files_where))

    # for class_name, method_or_view in [obj.split('.') for obj in o.split(',')]:
    # for row in s:
    #     print('::[%s].[%s]' % (row["class_id"], row["short_name"]))
    #     update(cnn, row["type"], row["class_id"], row["short_name"], p)
    # update_method(cnn, class_name, method_or_view)
    # print(objs_list)


# @click.command()
# @click.argument('db')
# @click.option('-o', help='Список объектов, например так: BRK_MSG.L,BRK_MSG.EVENTS2')
# @click.option('-t', help="""Время за которое сохраняем, цифра и вид интервала.
#     d-день
#     h-час
#     m-минута
# Например
#     за 36 часов: -t 36h
#     за последние 3 дня: -t 3d""")
# @click.option('-p/-no-p', help='Только вывод на экран. Не сохранять методы', default=False)
# @click.option('-b/-no-b', help='Переключить ветку', default=False)
# @click.option('-u/-no-u', help='Обновить все что уже скаченно', default=False)
# @click.option('--name', prompt='Your name', help='The person to greet.')
def updater(db, t, p, u, o, b):
    # db = 'day'
    cnn = oracle_connection.Db().connect("ibs/HtuRhtl@%s" % db)
    repo = None
    prev_branch = None
    if b and not p:
        repo = Repo(config.texts_working_dir)
        prev_branch = repo.active_branch.name
        if git_checkout(repo, db):
            print("Чтобы использовать флаг -b сначала закомментируйте изменения")
            return
    if o:
        update_from_list(cnn, o)
    if t:
        update_for_time(cnn, t, p)
    if u:
        update_from_dir_list(cnn)
    if b and not p:
        git_commit(repo)
        git_checkout(repo, prev_branch)


if __name__ == '__main__':
    def updater_argv():
        print("sys=%s" % sys.argv)
        params = [p for p in sys.argv[1:] if not re.match("-", p)]
        flags = [p for p in sys.argv[1:] if re.match("-", p)]

        if len(params) == 0:
            raise Exception("Необходимо указать имя базы данных")
        db_name = params[0]
        cnn = oracle_connection.Db().connect("ibs/HtuRhtl@%s" % db_name)
        c = "-c" in flags  # коммитить
        p = "-p" in flags  # только вывод на экран
        objs = None
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

        prev_branch = None
        repo = None
        if not p:
            repo = Repo(config.texts_working_dir)
            if git_staged_files(repo):
                print("В рабочем каталоге имеются не зафиксированные изменения")
                return
            prev_branch = repo.active_branch.name
            if c:
                git_checkout(repo, db_name)
        for row in objs:
            print("%s ::[%s].[%s]" % (row["type"], row["class_id"], row["short_name"]))
            if not p:
                update(cnn, row["type"], row["class_id"], row["short_name"])
        if c:
            git_commit(repo)
            git_checkout(repo, prev_branch)


    updater_argv()
    # db_name = sys.argv[1]
    # updater()

    # update_class("BRK_BP")
    # update_creteria('BRK_MSG', 'VW_RPT_BRK_MSG_GET_RECORDS')
    # update_method("BRK_MSG", "L")
    # update_method("BRK_MSG", "EVENTS2")
    # update_class("BRK_MSG")
    # update_method("BRK_MSG", "NEW_AUTO")

# r = cnn.execute_plsql(sql_text, **dict(class_name="BRK_MSG", method_name="NEW_AUTO", e=cx_Oracle.CLOB,
#                                        v=cx_Oracle.CLOB, g=cx_Oracle.CLOB,
#                                        l=cx_Oracle.CLOB, s=cx_Oracle.CLOB))
#
# write_method_text(r)
# print(r)
