# coding: utf-8

import psycopg2 as pg
import psycopg2.extras
import psycopg2.extensions
import psycopg2.pool

import lglass.database


class Database(lglass.database.Database):
    pool = None
    dsn = None

    def __init__(self, dsn_or_pool, connect_options={}):
        if isinstance(dsn_or_pool, pg.pool.AbstractConnectionPool):
            self.pool = dsn_or_pool
        else:
            self.dsn = dsn_or_pool
        self._connect_options = connect_options

    def save(self, obj, **options):
        with self.session() as sess:
            sess.save(obj, **options)
            sess.commit()

    def delete(self, obj):
        with self.session() as sess:
            sess.delete(obj)
            sess.commit()

    def fetch(self, class_, key):
        with self.session() as sess:
            return sess.fetch(class_, key)

    def lookup(self, classes=None, keys=None):
        with self.session() as sess:
            return list(sess.lookup(classes=classes, keys=keys))

    def search(self, query={}, classes=None, keys=None):
        with self.session() as sess:
            return list(sess.search(query={}, classes=None, keys=None))

    def find(self, filter=None, classes=None, keys=None):
        with self.session() as sess:
            return list(sess.find(filter=filter, classes=classes, keys=keys))

    def session(self, conn=None):
        if conn is not None:
            return Session(self, conn)
        if self.pool is not None:
            return Session(self, self.pool.getconn(), pool=self.pool)
        return Session(self, pg.connect(self.dsn, **self._connect_options))


class Session(lglass.database.ProxyDatabase):
    def __init__(self, backend, conn, pool=None):
        super().__init__(backend)
        self.conn = conn
        self.pool = pool

    def save(self, obj, **options):
        primary_class, primary_key = self.primary_spec(obj)
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO object (class, key) "
                "VALUES (lower(%s), lower(%s)) "
                "ON CONFLICT (lower(class), lower(key)) DO NOTHING "
                "RETURNING id", (primary_class, primary_key))
            obj_id = cur.fetchone()[0]
            cur.execute(
                "DELETE FROM object_field WHERE object_id = %s", (obj_id,))
            pg.extras.execute_values(
                cur,
                "INSERT INTO object_field (key, value, "
                "object_id, position) VALUES %s",
                [(line[0], line[1], obj_id, offset) for offset, line
                 in enumerate(obj.data)])
        return obj_id

    def delete(self, obj):
        primary_class, primary_key = self.primary_spec(obj)
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM object WHERE lower(class) = lower(%s) "
                "AND lower(key) = lower(%s)",
                (primary_class, primary_key))
            if not cur.rowcount:
                raise KeyError(repr((primary_class, primary_key)))

    def fetch(self, class_, key):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM object "
                "WHERE lower(class) = lower(%s) "
                "AND lower(key) = lower(%s)", (class_, key))
            if not cur.rowcount:
                raise KeyError(repr((class_, key)))
            obj_id = cur.fetchone()[0]
            cur.execute(
                "SELECT key, value FROM object_field "
                "WHERE object_id = %s "
                "ORDER BY position", (obj_id,))
            return lglass.object.Object(cur.fetchall())
    
    def fetch_by_id(self, object_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT key, value FROM object_field "
                    "WHERE object_id = %s "
                    "ORDER BY position", (object_id,))
            if not cur.rowcount:
                raise KeyError(repr((class_, key)))
            return lglass.object.Object(cur.fetchall())

    def all_ids(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM object")
            yield from cur

    def lookup(self, classes=None, keys=None):
        if not classes:
            classes = self.object_classes
        classes = tuple(classes)

        if keys is None:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT class, key FROM object "
                    "WHERE lower(class) IN %s", (classes,))
                yield from cur
        elif callable(keys):
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT class, key FROM object "
                    "WHERE lower(class) IN %s ", (classes,))
                yield from filter(lambda x: keys(x[1]), cur)
        else:
            keys = tuple(map(str.lower, keys))
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT class, key FROM object "
                    "WHERE lower(class) IN %s "
                    "AND lower(key) IN %s", (classes, keys))
                yield from cur

    def search(self, query={}, classes=None, keys=None):
        return lglass.database.Database.search(
            self,
            query=query, classes=classes, keys=keys)

    def find(self, filter=None, classes=None, keys=None):
        return lglass.database.Database.find(
            self,
            filter=filter, classes=classes, keys=keys)

    def close(self):
        if self.pool is not None:
            self.pool.putconn(self.conn)
        else:
            self.conn.close()

    def commit(self):
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

