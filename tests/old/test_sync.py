import unittest

from oracle_client.oracle_client import OracleClient

from abs_sync.log import get_logger


class GitNewDatabaseTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_check_if_database_is_updated(self):
        self.client = OracleClient("ibs/HtuRhtl@lw-ass-abs.brc.local:1521/assabs")


class GitNewDatabaseTestTNS(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_check_if_database_is_updated(self):
        logger = get_logger('test_update')
        self.client = OracleClient("ibs/HtuRhtl@lw-ass-abs")
        # self.client.select("select * from dual")

        try:
            last_date_update = last_dates_update.get(db_name)
            sysdate = select_sysdate(cnn)
            # кол-во дней на которые на сервере передвинули время
            # корректируем дату коммитов на эту дельту
            days_delta = datetime.timedelta(days=(
                    datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - sysdate.replace(hour=0,
                                                                                                                 minute=0,
                                                                                                                 second=0,
                                                                                                                 microsecond=0)).days)
            db_logger.debug("delta_days=%s, datetime.datetime.now()=%s, sysdate=%s" % (
                days_delta, datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                sysdate.replace(hour=0, minute=0, second=0, microsecond=0)))

            if not last_date_update:
                last_date_update = sysdate - datetime.timedelta(days=config.days_update_on_start)
                db_logger.debug("last_date_update is null set it to %s" % last_date_update)
                # залогируем список пользователей за последний месяц правивших методы
                # чтобы можно было найти тех по кому мы не сохраняем изменения
                # db_logger.debug(",\n".join(["'%s'" % a for a in select_users(cnn)['USER_MODIFIED']]))

            repo = clone_or_open_repo(os.path.join(config.git_folder, db_name), db_name)
        except Exception:
            db_logger.exception("update exception %s" % traceback.format_exc())