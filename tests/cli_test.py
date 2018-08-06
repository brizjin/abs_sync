import unittest

from abs_sync.scripts import click_cli
from click.testing import CliRunner


def call_cli(params=list()):
    print("###begin with params", params)
    runner = CliRunner()
    result = runner.invoke(click_cli.cli, params, obj={}, catch_exceptions=False)
    print(result.output)
    print("###end")


class CliTest(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def tearDown(self):
        pass
        # print("########")

    def test_pull(self):
        call_cli()

    def test_push(self):
        call_cli(['pull'])
        call_cli(['pull', '--help'])
        call_cli(['pull', 'time', '--help'])

    def test_push_p1(self):
        call_cli(['pull', '--time', '1d'])

    def test_push_all(self):
        call_cli(['midday', 'pull', 'all'])

    def test_pull2(self):
        call_cli(['pull', '-p', 'time', '-d', '1'])


    def test_pull3(self):
        call_cli(['pull', 'time'])

    def test_pull_all(self):
        # call_cli(['pull', 'all', '--db', 'day'])
        call_cli(['sync', '-s'])
