# coding: utf-8

import lglass.nic

import lglass_sql.database


class SQLDatabase(lglass_sql.database.SQLDatabase, lglass.nic.NicDatabaseMixin):
    def __init__(self, *args, **kwargs):
        lglass_sql.database.SQLDatabase.__init__(self, *args, **kwargs)
        lglass.nic.NicDatabaseMixin.__init__(self)
