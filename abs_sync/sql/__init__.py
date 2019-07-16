from pkg_resources import resource_string


def get_plsql_from_tst_file_text(text):
    text_lines = text.split("\n")
    return "\n".join(text_lines[2:int(text_lines[1]) + 2])


def read_resource(file_name, encoding='utf-8'):
    return resource_string('abs_sync.sql', file_name).decode(encoding)


def read_tst_resource(file_name):
    return get_plsql_from_tst_file_text(read_resource(file_name, 'windows-1251'))


read_method_sources = read_tst_resource("read_method_sources.tst")
save_method_sources = read_tst_resource("save_method_sources.tst")
