import unittest

from click.testing import CliRunner

from abs2 import cli


def call_cli(params=list()):
    print("###begin with params", params)
    runner = CliRunner()
    result = runner.invoke(cli, params, obj={})
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
        call_cli(['pull', '--time','1d'])

    def test_push_all(self):
        call_cli(['day', 'pull', 'all'])
