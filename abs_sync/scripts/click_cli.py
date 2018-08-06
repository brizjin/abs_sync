import re
import time
from cx_Oracle import Connection

import click
import cx_Oracle
import pandas as pd
import schedule

from abs_sync import config, save_methods, git_funcs, log
# from refresh_methods import update
from abs_sync.config import read_parameters, write_parameters, default_parameters
from abs_sync.save_methods import pull_all_objects, pull_last_objects, Db


@click.group()
# @click.option('--test', is_flag=True, help="Тестирование. Ни изменяет данные, только вывод на экран")
# @click.argument('db')
@click.pass_context
def cli(ctx):
    """Скрипт для автоматизации работы с исходниками

    \b
    Примеры использования:
    1.  Посмотреть настройки:               abs env print
    2.  Установить текущий каталог проекта: abs env set PROJECT_DIRECTORY=C:\\Users\\BryzzhinIS\\Documents\\Хранилища\\pack_texts
    3.  Установить текущую базу для работы: abs env set DB=p2
    4.1 Получить все изменившиеся исходники за 2 минуты:            abs pull time -m 2
    4.2 Получить все изменившиеся исходники за 2 часа:              abs pull time -h 2
    4.3 Получить все изменившиеся исходники за 2 дня:               abs pull time -d 2
    4.4 Получить все изменившиеся исходники за 2 дня с базы mid:    abs pull time -d 2 --db mid
    5.1 Получить последнюю версию всех операций из каталога проета: abs pull all
    5.2 ... по базе мид:                                            abs pull all --db mid
    6.  Отправить исходники и перекомпилировать операцию: abs push list BRK_MSG.EDIT_AUTO,BRK_MSG.NEW_AUTO
    """

    pass
    # ctx.obj['test'] = test
    # ctx.obj['db'] = db
    # return ctx


@cli.command(name="tns", help="Вывести на экран TNS по-умолчанию")
def tns():
    print_cfg(config.dbs)


@cli.command(name="sync", help='!!Не использовать')
@click.option('-s', 's', flag_value='m', default=False)
def sync(s):
    if s:
        def do_schedule(connection_string):
            # cnn_object = cx_Oracle.connect(connection_string)
            m = re.match(r"(?P<user>.+)/(?P<pass>.+)@(?P<dbname>.+)", connection_string)
            db_name = m.group('dbname')
            log.log_init(db_name)
            # schedule.every(5).seconds.do(git_funcs.update, connection_string)
            schedule.every(1).hours.do(git_funcs.update, connection_string)
            git_funcs.update(connection_string)

        dbs = ["day", "mideveryday", "msb", "lw-ass-abs", "lw-abs-abs", "lw-p2-abs", "midabs", "mid-abs-ssd"]
        # do_schedule("ibs/HtuRhtl@day")
        # do_schedule("ibs/HtuRhtl@mideveryday")
        # do_schedule("ibs/HtuRhtl@msb")
        # do_schedule("ibs/HtuRhtl@lw-ass-abs")
        # do_schedule("ibs/HtuRhtl@lw-abs-abs")
        # do_schedule("ibs/HtuRhtl@lw-p2-abs")
        # do_schedule("ibs/HtuRhtl@midabs")
        # do_schedule("ibs/HtuRhtl@ssd")
        for db_name in dbs:
            do_schedule("ibs/HtuRhtl@%s" % db_name)
        while True:
            schedule.run_pending()
            time.sleep(1)


@cli.group()
def env():
    """Параметры выполнения комманд"""


def print_cfg(cfg):
    ml = len(max(cfg.keys(), key=len))

    for k, v in sorted(cfg.items()):
        click.echo("%-{0}s = %s".format(ml) % (k.upper(), v))


@env.command(name='print')
def env_print():
    """Вывести на экран текущие настройки"""
    print_cfg(read_parameters())


@env.command(name='set')
@click.argument('params', nargs=-1)
def env_set(params):
    """Установить параметры"""
    cfg = read_parameters()
    for p in params:
        key, value = p.split('=')
        cfg[key.lower()] = value
    write_parameters(cfg)
    print_cfg(read_parameters())


@env.command(name='reset')
def env_reset():
    """Сбросить параметры значениями по-умолчанию"""
    write_parameters(default_parameters())
    print_cfg(read_parameters())


# @cli.command()
# @click.option('--a', 'mode', flag_value='all', default=True, help="""Получить все элементы по списку файлов с диска""")
# @click.option('-t', 'mode', flag_value='time', default=True, help="""Получить недавно обновленные элементы.
#     Например:
#         pull -t 1d""")
# @click.option('--l', 'mode', flag_value='list', default=True)
# @click.argument('params', nargs=-1)
# def pull(mode, params):
#     """Получить последние изменения исходников с базы"""
#     print("mode=%s, params=%s" % (mode, params))
#     click.echo(click.style('Hello World!', fg='green'))
#     click.echo('Initialized the database %s,%s' % (mode, params))


@cli.group()
# @click.argument('db')
# @click.option('-p', is_flag=True, help="Только вывести список операций которые будут загуржены")
@click.pass_context
def pull(ctx):
    """Получить последнии изменения"""
    pass


def print_objects(objs):
    click.echo(click.style("Список сохраненных операций:", fg='green'))
    click.echo(click.style(str(objs.sort_values(['CLASS_ID', 'SHORT_NAME']))))


# def update_objects(cnn, ctx, objs):
#     for i, row in objs.iterrows():
#         update(cnn, row["TYPE"], row["CLASS_ID"], row["SHORT_NAME"])


@pull.command(name="all")
@click.option('--db', help="База данных для подключения")
@click.pass_context
def pull_all(ctx, db):
    """Получает все изменения по всем операциям/вьюхам/тригирам, что лежат в гите"""
    cfg = read_parameters()
    db = db if db else cfg['db']
    cnn = Db(config.dbs[db])
    click.echo('Подключаемся к базе %s' % cnn.select_sid())

    df2 = pull_all_objects(cnn)

    click.echo(str(df2.sort_values(['CLASS', 'NAME'])))


@pull.command(name="time", short_help="Получить последние изменения исходников за время", epilog="Пока!")
@click.option('-m', 'unit', flag_value='m', default=True, help="Минуты")
@click.option('-h', 'unit', flag_value='h', default=False, help="Часы")
@click.option('-d', 'unit', flag_value='d', default=False, help="Дни")
@click.argument('count', required=False)  # , metavar="КОЛ-ВО"
@click.option('--db', help="База данных для подключения")
@click.pass_context
def pull_time(ctx, count, unit, db):
    """Получить последние изменения исходников за время."""

    cfg = read_parameters()
    db = db if db else cfg['db']
    cnn = Db(config.dbs[db])
    click.echo('Подключаемся к базе %s' % cnn.select_sid())

    if count is None:
        # click.echo(click.style('Необходимо указать единицу измерения времени', fg='red'))
        click.echo(click.style('Необходимо указать кол-во времени', fg='yellow'))
        click.echo(click.style('Пример вызова: pull time -d 2', fg='yellow'))
        click.echo(ctx.get_help())
        return

    df = pull_last_objects(cnn, count, unit)
    click.echo(str(df.sort_values(['CLASS', 'NAME'])))


@pull.command(name="list")
def pull_list():
    """Получить операции по списку"""
    pass
    # click.echo("pull list")


# @pull.command(name="t")
# def pull_by_time():
#     click.echo('pull_by_time')


# class PullParams:
#     def __init__(self):
#
# def pull_callback(ctx, param, value):
#     print(ctx, param, value)
#     return value
#
#
# @cli.command(help="test")
# # @click.option('--count', default=1, help='number of greetings')
# @click.argument('name', callback=pull_callback, nargs=-1)
# def pull(name):
#     print("name", name)
#     click.echo('Dropped the database %s' % name)

@cli.group()
def push():
    """Отправить операции"""
    pass
    # click.echo('Initialized the database')


@push.command(name="list")
@click.option('--db', help="База данных для подключения")
@click.argument('list', required=False)
def list(db, list):
    cfg = read_parameters()
    db = db if db else cfg['db']
    cnn = Db(config.dbs[db])
    click.echo('Подключаемся к базе %s' % cnn.select_sid())

    click.echo(list)
    elements = list.split(',')
    where = " or ".join(["CLASS == '%s' and NAME == '%s'" % tuple(element.split('.')) for element in elements])
    disk_schema = save_methods.CftSchema.read_disk_schema()
    for element in save_methods.CftSchema(disk_schema.as_df().query(where)).as_cls():
        element.read_from_disk()
        element.write_to_db(cnn)


if __name__ == '__main__':
    cli(obj={})
