import cx_Oracle
import json
import os
import pandas as pd
import shutil
from pathlib import Path
import click
from cx_Oracle import Connection

from numpy import nan

import config
import dirs
import oracle_connection
import save_methods
from refresh_methods import update_for_time, update, update_from_dir_list


class Db:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.username = str(connection_string[:connection_string.find('/')])
        self.password = str(connection_string[connection_string.find('/') + 1:connection_string.find('@')])
        self.dsn = str(connection_string[connection_string.find('@') + 1:])
        self.cnn = Connection(user=self.username, password=self.password, dsn=self.dsn)

    def __str__(self):
        return "%s.%s[%s]" % (self.__module__, self.__class__.__name__, self.connection_string)

    def select(self, sql_text, **kwargs):
        cursor = self.cnn.cursor()
        try:
            cursor.execute(sql_text, **kwargs)
            desc = [d[0] for d in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=desc)
            # df = df.fillna(value=nan)
            df = df.fillna('')
            return df
        finally:
            cursor.close()

    def execute_plsql(self, pl_sql_text, **kwargs):
        cursor_vars = {}

        cursor = self.cnn.cursor()
        try:
            for k, v in kwargs.items():
                if v in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.NCHAR, cx_Oracle.NUMBER):
                    cursor_vars[k] = cursor.var(v)
                else:
                    cursor_vars[k] = v

            cursor.execute(pl_sql_text, **cursor_vars)

            for k, v in kwargs.items():
                if v in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.NCHAR, cx_Oracle.NUMBER):
                    value = cursor_vars[k].getvalue()
                    if value and v in (cx_Oracle.CLOB, cx_Oracle.BLOB):
                        value = value.read()
                    cursor_vars[k] = value
            return cursor_vars
        finally:
            cursor.close()

    def select_sid(self):
        return self.select("select sys_context('userenv','instance_name') sid from dual")["SID"][0]


@click.group()
# @click.option('--test', is_flag=True, help="Тестирование. Ни изменяет данные, только вывод на экран")
# @click.argument('db')
@click.pass_context
def cli(ctx):
    """Скрипт для автоматизации работы с исходниками

    \b
    Примеры использования:
    abs2 pull p2
    """

    pass
    # ctx.obj['test'] = test
    # ctx.obj['db'] = db
    # return ctx





@cli.group()
def env():
    """Параметры выполнения комманд"""


def default_parameters():
    return dict(db_user_name='ibs', db='p2', project_directory=config.texts_working_dir,
                oracle_home=os.environ["ORACLE_HOME"])


def write_parameters(cfg):
    if not os.path.exists(os.path.dirname(config.user_config_file_name)):
        os.makedirs(os.path.dirname(config.user_config_file_name))
    with open(config.user_config_file_name, 'w+') as f:
        f.write(json.dumps(cfg))


def read_parameters():
    if os.path.isfile(config.user_config_file_name):
        with open(config.user_config_file_name, 'r') as f:
            cfg = json.load(f)

    else:
        cfg = default_parameters()
        write_parameters(cfg)
    return cfg


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
        cfg[key] = value
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


def update_objects(cnn, ctx, objs):
    for i, row in objs.iterrows():
        update(cnn, row["TYPE"], row["CLASS_ID"], row["SHORT_NAME"])


@pull.command(name="all")
@click.option('--db', help="База данных для подключения")
@click.pass_context
def pull_all(ctx, db):
    """Получает все изменения по всем операциям/вьюхам/тригирам, что лежат в гите"""
    cfg = read_parameters()
    db = db if db else cfg['db']
    cnn = Db(config.dbs[db])
    click.echo('Подключаемся к базе %s' % cnn.select_sid())

    save_methods.CftSchema.remove_unknown_files_on_disk()
    disk_schema = save_methods.CftSchema.read_disk_schema()
    db_schema = save_methods.CftSchema.read_db_schema(cnn, disk_schema.elements)

    df1 = disk_schema.as_df()
    df2 = db_schema.as_df()
    df4 = pd.merge(df1, df2, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
    for m in save_methods.CftSchema(df4).as_cls():
        m.remove_from_disk()

    db_schema.write_to_disk_from_db(cnn)
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

    db_schema = save_methods.CftSchema.read_db_schema_by_time(cnn, count, unit)
    print(db_schema.as_df())
    db_schema.write_to_disk_from_db(cnn)


@pull.command(name="list")
def pull_list():
    click.echo("pull list")


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


    click.echo('Initialized the database')

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
