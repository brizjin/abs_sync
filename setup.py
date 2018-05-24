from setuptools import setup

setup(
    name='abs2',
    version='0.0.8',
    # py_modules=['abs2', 'config', 'git_funcs', 'log', 'save_methods', 'selects'],
    # eager_resources=['sql/method_sources.tst', 'sql/save_method_sources.tst'],
    include_package_data=True,
    install_requires=[
        'Click',
        'pandas',
        'cx_Oracle',
        'gitpython',
        'schedule',
    ],
    entry_points='''
        [console_scripts]
        abs=abs_sync.scripts.click_cli:cli
    ''',
)
