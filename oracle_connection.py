import traceback

import cx_Oracle


# from __builtin__ import unicode


class Db:
    def __init__(self):
        self.pool = None

    def connect(self, connection_string):
        try:
            user_ = str(connection_string[:connection_string.find('/')])
            pass_ = str(connection_string[connection_string.find('/') + 1:connection_string.find('@')])
            dsn_ = str(connection_string[connection_string.find('@') + 1:])

            self.pool = cx_Oracle.SessionPool(user=user_, password=pass_, dsn=dsn_, min=1, max=5, increment=1,
                                              threaded=True)
            return self
        except Exception as e:
            print("*** Ошибка выполнения connect:%s,%s" % (traceback.format_exception(), traceback.format_exc()))

    def request(self, xml_in):
        try:
            conn = self.pool.acquire()
            cursor = conn.cursor()
            xml_out = cursor.var(cx_Oracle.CLOB)

            sql = "begin :xml_out := ibs.Z$BRK_MSG_L.REQUEST(:xml_in); end;"
            cursor.execute(sql, (xml_out, xml_in))

            result = xml_out.getvalue().read()
            self.pool.release(conn)
            return result
        except Exception as e:
            print("*** Ошибка выполнения request:%s,%s" % (traceback.format_exception(), traceback.format_exc()))

    def execute_plsql(self, pl_sql_text, **kwargs):
        try:
            with self.pool.acquire() as conn:
                cursor = conn.cursor()

                cursor_vars = {}

                for k, v in kwargs.items():
                    if v in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.NCHAR):
                        cursor_vars[k] = cursor.var(v)
                    else:
                        cursor_vars[k] = v

                cursor.execute(pl_sql_text, **cursor_vars)

                for k, v in kwargs.items():
                    if v in (cx_Oracle.CLOB, cx_Oracle.BLOB, cx_Oracle.NCHAR):
                        value = cursor_vars[k].getvalue()
                        if value and v in (cx_Oracle.CLOB, cx_Oracle.BLOB):
                            value = value.read()
                        cursor_vars[k] = value
                        # cursor_vars[k] = cursor_vars[k].read()

                return cursor_vars
        except Exception as e:
            print("*** Ошибка выполнения execute_plsql:%s" % (traceback.format_exc()))

    def select(self, sql_text, **kwargs):
        try:
            # conn = self.pool.acquire()
            with self.pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute(sql_text, **kwargs)
                desc = [d[0].lower() for d in cursor.description]
                table = [[(lambda: t if t.__class__ == str else t)() for t in row] for row in
                         cursor]  # Конвертнем все строковые значения в юникод
                table = [[(lambda: "" if not t else t)() for t in row] for row in
                         table]  # Заменяем все значение None на пустую строку

                return [dict(zip(desc, row)) for row in table]
        except Exception:
            print("*** Ошибка выполнения select:%s" % (traceback.format_exc()))

    def select_value(self, sql_text, **kwargs):
        try:
            s = self.select(sql_text, **kwargs)
            for row in s:
                for k, v in row.items():
                    return v
            return
        except Exception:
            print("*** Ошибка выполнения select_value:%s" % (traceback.format_exc()))
