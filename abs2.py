import os
import shutil

import click

import config
import dirs
import oracle_connection
import save_methods
from refresh_methods import update_for_time, update, update_from_dir_list


@click.group()
@click.option('--test', is_flag=True, help="Тестирование. Ни изменяет данные, только вывод на экран")
@click.argument('db')
@click.pass_context
def cli(ctx, db, test):
    pass
    ctx.obj['test'] = test
    ctx.obj['db'] = db
    # return ctx


@cli.group()
def push():
    click.echo('Initialized the database')


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
def pull():
    # return ctx
    pass


def print_objects(objs):
    for row in objs:
        click.echo(click.style("%s ::[%s].[%s]" % (row["type"], row["class_id"], row["short_name"]), fg='green'))


def update_objects(cnn, ctx, objs):
    for row in objs:
        # click.echo(click.style("1. %s ::[%s].[%s]" % (row["type"], row["class_id"], row["short_name"]), fg='green'))
        is_test = ctx.obj['test']
        if is_test is None or is_test is False:
            # click.echo(click.style("2. %s ::[%s].[%s]" % (row["type"], row["class_id"], row["short_name"]), fg='green'))
            update(cnn, row["type"], row["class_id"], row["short_name"])


@pull.command(name="all")
@click.pass_context
def pull_all(ctx):
    click.echo("pull all")
    cnn_str = config.dbs[ctx.obj['db']]
    cnn = oracle_connection.Db().connect(cnn_str)
    objs = []
    objs = update_from_dir_list(cnn)

    def clear_folder(folder):
        for the_file in os.listdir(folder):
            if the_file[0] == '.':
                continue

            file_path = os.path.join(folder, the_file)

            if the_file in ['requirements.txt']:
                continue
            if the_file in ['PLSQL', 'TESTS', 'PACKAGES'] and os.path.isdir(file_path):
                continue

            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(e)

    clear_folder(config.texts_working_dir)
    update_objects(cnn, ctx, objs)
    print_objects(objs)


@pull.command(name="time", short_help="Получить последние изменения исходников за время")
@click.option('-m', 'unit', flag_value='m', default=False, help="Минуты")
@click.option('-h', 'unit', flag_value='h', default=False, help="Часы")
@click.option('-d', 'unit', flag_value='d', default=False, help="Дни")
@click.argument('count', required=False)  # , metavar="КОЛ-ВО"
@click.pass_context
def pull_time(ctx, count, unit):
    """Получить последние изменения исходников за время."""

    if unit is None:
        click.echo(click.style('Необходимо указать единицу измерения времени', fg='red'))
        click.echo(click.style('Пример вызова: pull time -d 1', fg='yellow'))
    # if count is None:
    #     ctx.invoke(pull_time, "--help")

    click.echo("pull time %s,%s context=%s" % (count, unit, ctx.obj))
    cnn_str = config.dbs[ctx.obj['db']]
    cnn = oracle_connection.Db().connect(cnn_str)

    objs = update_for_time(cnn, count, unit)
    update_objects(cnn, ctx, objs)
    print_objects(objs)
    # return ctx


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


if __name__ == '__main__':
    cli(obj={})
