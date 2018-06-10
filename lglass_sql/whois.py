# coding: utf-8

import lglass.whois.server
from lglass.whois.engine import *

import lglass_sql.pgsql


class PooledWhoisServer(lglass.whois.server.SimpleWhoisServer):
    def __init__(self, engine, pool):
        super().__init__(engine)
        self.connection_pool = pool

    def perform_query(self, writer, query_args, query_kwargs, inverse_fields):
        db = self.engine.database.session(self.connection_pool.getconn())
        try:
            for term in query_args.terms or []:
                if inverse_fields is not None:
                    results = self.engine.query((inverse_fields, (term,)),
                            database=db,
                            **query_kwargs)
                else:
                    results = self.engine.query(term, database=db, **query_kwargs)
                writer.write(self.format_results(
                    results,
                    primary_keys=query_args.primary_keys,
                    pretty_print_options={
                        "min_padding": 16,
                        "add_padding": 0},
                    database=db).encode())
        finally:
            self.connection_pool.putconn(db.conn)


if __name__ == '__main__':
    import psycopg2 as pg
    import psycopg2.pool
    lglass.whois.server.main(
        database_cls=lglass_sql.pgsql.NicDatabase,
        server_cls=lambda eng: PooledWhoisServer(
            eng, pg.pool.SimpleConnectionPool(2, 10, eng.database._dsn)))
