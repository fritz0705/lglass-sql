# coding: utf-8

import lglass.whois.server
from lglass.whois.engine import *

import lglass_sql.nic

if __name__ == '__main__':
    lglass.whois.server.main(database_cls=lglass_sql.nic.SQLDatabase)
