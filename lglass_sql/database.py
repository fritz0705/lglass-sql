# coding: utf-8

import datetime

from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, \
    create_engine, or_, and_, func
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import lglass.database

Base = declarative_base()


class Object(Base):
    __tablename__ = 'object'
    id = Column('id', Integer, primary_key=True)
    class_ = Column('object_class', String, nullable=False)
    key = Column('object_key', String, nullable=False)
    source = Column('source', String)
    last_modified = Column('last_modified', DateTime)
    fields = relationship("ObjectField", backref="object")


class ObjectField(Base):
    __tablename__ = 'object_field'
    id = Column('id', Integer, primary_key=True)
    object_id = Column('object_id', ForeignKey('object.id'), nullable=False)
    position = Column('position', Integer, nullable=False)
    key = Column('field_key', String, nullable=False)
    value = Column('field_value', String, nullable=False)


class SQLDatabase(lglass.database.Database):
    def __init__(self, database_url):
        self._manifest = None
        self._database_url = database_url
        self._engine = create_engine(database_url)
        self._Session = sessionmaker(bind=self._engine)

    def fetch(self, class_, key):
        with self.session() as sess:
            return sess.fetch(class_, key)

    def save(self, obj, **kwargs):
        with self.session() as sess:
            sess.save(obj, **kwargs)
            sess.commit()

    def delete(self, obj):
        with self.session() as sess:
            sess.delete(obj)
            sess.commit()

    def search(self, query={}, classes=None, keys=None):
        with self.session() as sess:
            return sess.search(query=query, classes=classes, keys=keys)

    def lookup(self, classes=None, keys=None):
        with self.session() as sess:
            return sess.lookup(classes=classes, keys=keys)

    def session(self):
        return Session(self, self._Session())

    @property
    def manifest(self):
        if self._manifest is None:
            self._manifest = self.fetch("database", "self")
        return self._manifest

    def save_manifest(self):
        mf = self.manifest
        self.save(mf, local_manifest=True)

    def create_tables(self):
        Base.metadata.create_all(self._engine)


class Session(object):
    def __init__(self, database, session):
        self.session = session
        self.database = database

        self.commit = self.session.commit
        self.close = self.session.close

    def lookup(self, classes=None, keys=None):
        if classes is None:
            classes = self.database.object_classes
        else:
            classes = set(map(self.database.primary_class, classes)
                          ) & self.database.object_classes
        query = self.session.query(Object).filter(Object.class_.in_(classes))
        if keys is not None and not callable(keys):
            keys = list(map(str.lower, keys))
            query = query.filter(Object.key.in_(keys))
        for obj in query.all():
            if not callable(keys) or keys(obj.key):
                yield (obj.class_, obj.key)

    def fetch(self, class_, key):
        sqlobj = self.session.query(Object).filter(
            Object.class_ == self.database.primary_class(class_), Object.key == key.lower()).first()
        if sqlobj is None:
            raise KeyError
        fields = self.session.query(ObjectField).filter(
            ObjectField.object == sqlobj).order_by(ObjectField.position).all()
        obj_data = []
        for field in self.session.query(ObjectField).filter(ObjectField.object == sqlobj).order_by(ObjectField.position).all():
            obj_data.append((field.key, field.value))
        obj = self.database.object_class_type(class_)(obj_data)
        if obj.source is None:
            obj.source = sqlobj.source
        if obj.last_modified is None:
            obj.last_modified = sqlobj.last_modified
        return obj

    def save(self, obj, local_manifest=False, **kwargs):
        primary_class, primary_key = self.database.primary_spec(obj)
        if local_manifest:
            primary_key = "self"
        sqlobj = self.session.query(Object).filter(Object.class_ == primary_class,
                                                   Object.key == primary_key.lower()).first()
        if sqlobj is None:
            sqlobj = Object(class_=primary_class, key=primary_key.lower())
        save_obj = lglass.nic.NicObject(obj)
        sqlobj.source = self.database.database_name\
                if save_obj.source is None\
                else save_obj.source
        if self.database.database_name in save_obj.get("source") or \
                not save_obj.get("source"):
            sqlobj.last_modified = datetime.datetime.utcnow()
            save_obj.remove("last-modified")
        elif sqlobj.last_modified is not None:
            sqlobj.last_modified = sqlobj.last_modified
        self.session.add(sqlobj)
        self.session.query(ObjectField).filter(
            ObjectField.object == sqlobj).delete()
        if save_obj.source == self.database.database_name:
            save_obj.remove("source")
        for pos, line in enumerate(save_obj):
            field = ObjectField(object=sqlobj, position=pos,
                                key=line[0], value=line[1])
            self.session.add(field)

    def delete(self, obj):
        primary_class, primary_key = self.database.primary_spec(obj)
        sqlobj = self.session.query(Object).filter(Object.class_ == primary_class,
                                                   Object.key == primary_key.lower()).first()
        if sqlobj is None:
            raise KeyError
        self.session.query(ObjectField).filter(
            ObjectField.object == sqlobj).delete()
        self.session.delete(sqlobj)

    def search(self, query={}, classes=None, keys=None):
        if classes is None:
            classes = self.object_classes
        else:
            classes = set(map(self.database.primary_class, classes)
                          ) & self.database.object_classes
        filters = []
        for k, v in query.items():
            if isinstance(v, str):
                v = (v,)
            filters.append(and_(ObjectField.key == k,
                                func.lower(ObjectField.value).in_(v)))
        q = self.session.query(ObjectField).filter(or_(*filters))
        for fieldobj in q.all():
            sqlobj = fieldobj.object
            spec = (sqlobj.class_, sqlobj.key)
            if spec[0] in classes and (keys is None or spec[1] in keys):
                yield self.fetch(*spec)

    def __enter__(self, *args):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
