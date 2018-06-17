# coding: utf-8

import psycopg2 as pg
import psycopg2.extras
import psycopg2.extensions
import psycopg2.pool

import lglass.nic

import lglass_sql.base


class NicDatabase(lglass_sql.base.Database, lglass.nic.NicDatabaseMixin):
    def __init__(self, dsn_or_pool, *args, database_name=None, **kwargs):
        lglass_sql.base.Database.__init__(self, dsn_or_pool, *args, **kwargs)
        lglass.nic.NicDatabaseMixin.__init__(self)
        self._manifest = None
        if database_name is None:
            database_name = self._get_database_name()
        self._database_name = database_name

    def session(self, conn=None):
        if conn is not None:
            return NicSession(self, conn)
        if self.pool is not None:
            return NicSession(self, self.pool.getconn(), pool=self.pool)
        return NicSession(self, pg.connect(self.dsn, **self._connect_options))

    def search_route(self, address):
        with self.session() as sess:
            return list(sess.search_route(address))

    def search_inetnum(self, address):
        with self.session() as sess:
            return list(sess.search_inetnum(address))

    def search_as_block(self, asn):
        with self.session() as sess:
            return list(sess.search_as_block(asn))

    def _get_database_name(self):
        try:
            with self.session() as sess:
                with sess.conn.cursor() as cur:
                    cur.execute("SHOW lglass.dbname")
                    return cur.fetchone()[0]
        except pg.ProgrammingError:
            pass
        if self.dsn is not None:
            return pg.extensions.parse_dsn(self.dsn)["dbname"]

    @property
    def manifest(self):
        if self._manifest is None:
            try:
                self._manifest = self.fetch("database", self._database_name)
            except KeyError:
                self._manifest = self.create_object(
                    [("database", self._database_name)])
        return self._manifest

    def save_manifest(self):
        self.save(self.manifest)


class NicSession(lglass_sql.base.Session):
    @property
    def inverse_keys(self):
        return self.backend.inverse_keys

    def create_object(self, *args, **kwargs):
        return self.backend.create_object(*args, **kwargs)

    def search_inverse(self, inverse_keys, inverse_values,
                       classes=None, keys=None):
        if classes is None:
            classes = self.object_classes
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT object.class, object.key FROM inverse_field "
                "LEFT JOIN object ON object.id = object_id "
                "WHERE inverse_field.key IN %(keys)s "
                "AND inverse_field.value IN %(values)s "
                "ORDER BY inverse_field.value",
                {"keys": tuple(inverse_keys),
                 "values": tuple(map(str.lower, inverse_values))})
            for class_, key in cur:
                if class_ in classes:
                    yield self.fetch(class_, key)

    def lookup_route(self, address, limit=None):
        address = str(address)
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT object.class, object.key FROM route "
                "LEFT JOIN object ON object.id = object_id "
                "WHERE address >> %(addr)s OR address = %(addr)s "
                "ORDER BY masklen(address) DESC "
                "LIMIT %(limit)s", {"addr": address,
                                    "limit": limit})
            yield from cur

    def lookup_inetnum(self, address, relation='>>=', limit=None,
                       order='DESC'):
        address = str(address)
        objs = []
        if order not in {'ASC', 'DESC'}:
            raise ValueError("{!r} is not a valid order, must be one "
                             "of 'ASC' or 'DESC'".format(order))
        if relation not in {'>>', '<<', '>>=', '<<='}:
            raise ValueError("{!r} is not a valid relation, must be one "
                             "of '>>', '>>=', '<<' or '<<='".format(relation))
        query = "SELECT object.class, object.key FROM inetnum, object " \
                "WHERE object.id = inetnum.object_id " \
                "AND address {relation} %(addr)s " \
                "ORDER BY masklen(address) {order}, address " \
                "LIMIT %(limit)s".format(
                    relation=relation,
                    order=order)
        with self.conn.cursor() as cur:
            cur.execute(query, {"addr": str(address), "limit": limit})
            yield from cur

    def lookup_as_block(self, asn):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT object.class, object.key FROM as_block "
                "LEFT JOIN object ON object.id = object_id "
                "WHERE range @> (%s::int8) "
                "ORDER BY (upper(range) - lower(range)) DESC", (asn,))
            yield from cur

    def lookup_domain(self, domain):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT object.class, object.key FROM domain "
                "LEFT JOIN object ON object.id = object_id "
                "WHERE reverse(name) LIKE %s "
                "ORDER BY name", (domain[::-1] + '%',))
            yield from cur

    def fetch(self, class_, key):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT object.id, object.source, object.last_modified, "
                "object.created, object_field.key, object_field.value "
                "FROM object, object_field "
                "WHERE object.id = object_field.object_id "
                "AND lower(object.class) = lower(%s) "
                "AND lower(object.key) = lower(%s) "
                "ORDER BY object_field.position",
                (class_, key))
            if not cur.rowcount:
                raise KeyError(repr((class_, key)))
            obj_id, source, last_modified, created, fkey, fval = cur.fetchone()
            obj = self.create_object([(fkey, fval)])
            obj.sql_id = obj_id
            obj.extend((l[4], l[5]) for l in cur)
        if "last-modified" not in obj and last_modified:
            obj.last_modified = last_modified
        if "created" not in obj and created:
            obj.created = created
        if "source" not in obj and source:
            obj.source = source
        return obj

    def fetch_by_id(self, object_id):
        return self.create_object(super().fetch_by_id(object_id))

    def fetch_id(self, obj):
        if hasattr(obj, "sql_id"):
            return obj.sql_id
        spec = self.primary_spec(obj)
        return self.fetch(spec).sql_id
        pass
    
    def reindex(self, obj):
        obj_id = self.fetch_id(obj)
        with self.conn.cursor() as cur:
            self._save_inverse(obj, obj_id, cur)

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
            "DELETE FROM object_field WHERE object_id = %s", (obj_id,))
        pg.extras.execute_values(
            cur, "INSERT INTO object_field "
            "(key, value, object_id, position) VALUES %s "
            "RETURNING id",
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
                 "asn": obj.origin[2:].split()[0]})
        elif obj.object_class == "as-block":
            cur.execute(
                "INSERT INTO as_block (object_id, range) VALUES "
                "(%(obj_id)s, %(range)s) ON CONFLICT (range) "
                "DO UPDATE SET object_id = %(obj_id)s",
                {"obj_id": obj_id, "range": pg.extras.NumericRange(
                    obj.start,
                    obj.end, '[]')})
        elif obj.object_class == "domain":
            cur.execute(
                "INSERT INTO domain (object_id, name) "
                "VALUES (%(obj_id)s, lower(%(name)s)) "
                "ON CONFLICT (name) DO UPDATE SET "
                "object_id = %(obj_id)s",
                {"obj_id": obj_id, "name": obj.primary_key})

    def _save_inverse(self, obj, obj_id, cur):
        cur.execute(
            "DELETE FROM inverse_field WHERE object_id = %s", (obj_id,))
        inverse_fields = [(obj_id, key, value.lower()) for key, value
                          in obj.inverse_fields()]
        pg.extras.execute_values(
            cur,
            "INSERT INTO inverse_field (object_id, key, value) VALUES %s "
            "ON CONFLICT DO NOTHING", inverse_fields)
