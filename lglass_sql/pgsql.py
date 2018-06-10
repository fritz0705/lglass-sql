# coding: utf-8

import psycopg2 as pg
import psycopg2.extras

import lglass.database
import lglass.nic

import lglass_sql.database


class Database(lglass.database.Database):
    def __init__(self, dsn, connect_options={}):
        self._dsn = dsn
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
            return sess.lookup(classes=classes, keys=keys)

    def search(self, query={}, classes=None, keys=None):
        with self.session() as sess:
            return sess.search(query={}, classes=None, keys=None)

    def find(self, filter=None, classes=None, keys=None):
        with self.session() as sess:
            return list(sess.find(filter=filter, classes=classes, keys=keys))

    def session(self):
        return Session(self, pg.connect(self._dsn, **self._connect_options))


class Session(lglass.database.ProxyDatabase):
    def __init__(self, backend, conn):
        super().__init__(backend)
        self.conn = conn

    def save(self, obj, **options):
        primary_class, primary_key = self.primary_spec(obj)
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO object (class, key) "
                        "VALUES (lower(%s), lower(%s)) "
                        "ON CONFLICT (lower(class), lower(key)) DO NOTHING "
                        "RETURNING id", (primary_class, primary_key))
            obj_id = cur.fetchone()[0]
            cur.execute(
                "DELETE FROM object_field WHERE object_id = %s", (obj_id,))
            pg.extras.execute_values(cur,
                "INSERT INTO object_field (key, value, "
                "object_id, position) VALUES %s",
                [(line[0], line[1], obj_id, offset) for offset, line
                 in enumerate(obj.data)])
        return obj_id

    def delete(self, obj):
        primary_class, primary_key = self.primary_spec(obj)
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM object WHERE lower(class) = lower(%s) "
                        "AND lower(key) = lower(%s)",
                        (primary_class, primary_key))
            if not cur.rowcount:
                raise KeyError(repr((primary_class, primary_key)))

    def fetch(self, class_, key):
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM object "
                    "WHERE lower(class) = lower(%s) "
                    "AND lower(key) = lower(%s)", (class_, key))
            if not cur.rowcount:
                raise KeyError(repr((class_, key)))
            obj_id = cur.fetchone()[0]
            cur.execute("SELECT key, value FROM object_field "
                    "WHERE object_id = %s "
                    "ORDER BY position", (obj_id,))
            return lglass.object.Object(cur.fetchall())

    def lookup(self, classes=None, keys=None):
        if not classes:
            classes = self.object_classes
        classes = tuple(classes)

        if keys is None:
            with self.conn.cursor() as cur:
                cur.execute("SELECT class, key FROM object "
                            "WHERE lower(class) IN %s", (classes,))
                return cur.fetchall()
        elif callable(keys):
            with self.conn.cursor() as cur:
                cur.execute("SELECT class, key FROM object "
                        "WHERE lower(class) IN %s ", (classes,))
                return list(filter(lambda x: keys(x[1]), cur.fetchall()))
        else:
            keys = tuple(map(str.lower, keys))
            with self.conn.cursor() as cur:
                cur.execute("SELECT class, key FROM object "
                            "WHERE lower(class) IN %s "
                            "AND lower(key) IN %s", (classes, keys))
                return cur.fetchall()

    def search(self, query={}, classes=None, keys=None):
        return lglass.database.Database.search(
            self,
            query=query, classes=classes, keys=keys)

    def find(self, filter=None, classes=None, keys=None):
        return lglass.database.Database.find(
            self,
            filter=filter, classes=classes, keys=keys)

    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class NicDatabase(Database, lglass.nic.NicDatabaseMixin):
    inverse_keys = {"abuse-mailbox", "admin-c", "author", "auth",
                    "fingerprint", "person", "irt-nfy", "local-as", "mnt-irt",
                    "mbrs-by-ref", "member-of", "mnt-by", "mnt-domains",
                    "mnt-lower", "mnt-nfy", "mnt-routes", "mnt-ref", "notify",
                    "nserver", "origin", "org", "ref-nfy", "tech-c", "upd-to",
                    "zone-c"}

    def __init__(self, *args, database_name="dn42-gen2", **kwargs):
        Database.__init__(self, *args, **kwargs)
        lglass.nic.NicDatabaseMixin.__init__(self)
        self.inverse_keys = set(self.inverse_keys)
        self._manifest = None
        self._database_name = database_name

    def session(self):
        return NicSession(self, pg.connect(self._dsn, **self._connect_options))

    def search_route(self, address):
        with self.session() as sess:
            return sess.search_route(address)

    def search_inetnum(self, address):
        with self.session() as sess:
            return sess.search_inetnum(address)

    def search_as_block(self, asn):
        with self.session() as sess:
            return sess.search_as_block(asn)

    @property
    def manifest(self):
        if self._manifest is None:
            self._manifest = self.fetch("database", self._database_name)
        return self._manifest


class NicSession(Session):
    @property
    def inverse_keys(self):
        return self.backend.inverse_keys

    def create_object(self, *args, **kwargs):
        return self.backend.create_object(*args, **kwargs)

    def lookup_route(self, address, limit=None):
        address = str(address)
        with self.conn.cursor() as cur:
            cur.execute("SELECT object.class, object.key FROM route "
                        "LEFT JOIN object ON object.id = object_id "
                        "WHERE address >> %s "
                        "ORDER BY masklen(address) DESC "
                        "LIMIT %s", (address,limit,))
            return cur.fetchall()

    def lookup_inetnum(self, address, limit=None):
        address = str(address)
        objs = []
        with self.conn.cursor() as cur:
            cur.execute("SELECT object.class, object.key FROM inetnum "
                        "LEFT JOIN object ON object.id = object_id "
                        "WHERE address >> %s "
                        "ORDER BY masklen(address) DESC "
                        "LIMIT %s", (address,limit,))
            return cur.fetchall()

    def search_as_block(self, asn):
        with self.conn.cursor() as cur:
            cur.execute("SELECT object.class, object.key FROM as_block "
                        "LEFT JOIN object ON object.id = object_id "
                        "WHERE range @> (%s::int8) ", (asn,))
            return cur.fetchall()

    def fetch(self, class_, key):
        return self.create_object(super().fetch(class_, key))

    def save(self, obj, **options):
        obj = self.create_object(obj)
        with self.conn.cursor() as cur:
            obj_id = self._save_raw_object(obj, cur)
            self._save_aux(obj, obj_id, cur)
            self._save_inverse(obj, obj_id, cur)
        return obj_id

    def _save_raw_object(self, obj, cur):
        primary_class, primary_key = self.primary_spec(obj)
        cur.execute(
            "INSERT INTO object (class, key, source, created, "
            "last_modified) VALUES (lower(%(class)s), lower(%(key)s), "
            "%(source)s, %(created)s, %(last_modified)s) ON CONFLICT "
            "(lower(class), lower(key)) DO UPDATE SET "
            "source = %(source)s, created = %(created)s, "
            "last_modified = %(last_modified)s RETURNING id",
            {"class": primary_class, "key": primary_key,
             "source": obj.source, "created": obj.created,
             "last_modified": obj.last_modified})
        obj_id = cur.fetchone()[0]
        cur.execute(
            "DELETE FROM inverse_field WHERE object_id = %s", (obj_id,))
        cur.execute(
            "DELETE FROM object_field WHERE object_id = %s", (obj_id,))
        pg.extras.execute_values(
            cur, "INSERT INTO object_field "
            "(key, value, object_id, position) VALUES %s",
            [(line[0], line[1], obj_id, offset)
             for offset, line in enumerate(obj.data)])
        return obj_id

    def _save_aux(self, obj, obj_id, cur):
        if obj.object_class in {"inetnum", "inet6num"}:
            cur.execute(
                "INSERT INTO inetnum (object_id, address) VALUES "
                "(%(obj_id)s, %(addr)s) ON CONFLICT (address) "
                "DO UPDATE SET object_id = %(obj_id)s",
                {"obj_id": obj_id, "addr": str(obj.ip_network)})
        elif obj.object_class in {"route", "route6"}:
            cur.execute(
                "INSERT INTO route (object_id, address, asn) "
                "VALUES (%(obj_id)s, %(addr)s, %(asn)s) ON CONFLICT "
                "(address, asn) DO UPDATE SET object_id = %(obj_id)s",
                {"obj_id": obj_id, "addr": str(obj.ip_network),
                 "asn": obj.origin[2:]})
        elif obj.object_class == "as-block" and False:
            cur.execute(
                "INSERT INTO as_block (object_id, range) VALUES "
                "(%(obj_id)s, %(range)s) ON CONFLICT (range) "
                "DO UPDATE SET object_id = %(obj_id)s",
                {"obj_id": obj_id, "range": pg.extras.Range(obj.start,
                    obj.end, '[]')})
        elif obj.object_class == "database":
            cur.execute(
                "INSERT INTO source (name, serial, object_id) "
                "VALUES (%(name)s, %(serial)s, %(obj_id)s) "
                "ON CONFLICT (lower(name)) DO UPDATE SET "
                "serial = %(serial)s",
                {"obj_id": obj_id, "name": obj.primary_key,
                 "serial": obj.get("serial") or 0})

    def _save_inverse(self, obj, obj_id, cur):
        pg.extras.execute_values(
            cur,
            "INSERT INTO inverse_field (object_id, key, value) VALUES %s "
            "ON CONFLICT DO NOTHING",
            [(obj_id, key, value.split()[0].lower()) for key, value in obj.data
                if key in self.backend.inverse_keys])
