from setuptools import setup, find_packages

setup(
    name='abs',
    version='0.0.17',
    # py_modules=['abs2', 'config', 'git_funcs', 'log', 'save_methods', 'selects'],
    eager_resources=['sql/method_sources.tst', 'sql/save_method_sources.tst'],
    packages=find_packages(),
    # include_package_data=True,
    # zip_safe=False,
    package_data={
        # If any package contains *.txt or *.rst files, include them:
        # '': ['*.txt', '*.rst'],
        '': ['sql/*.tst'],
        # And include any *.msg files found in the 'hello' package, too:
        # 'hello': ['*.msg'],
    },
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
