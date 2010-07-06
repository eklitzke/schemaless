from collections import defaultdict
from index import IndexCollection
from schemaless.index import reduce_args
from schemaless.log import ClassLogger
from schemaless import c

DEFAULT_NONCE = ()

class Column(object):

    def __init__(self, name, default=DEFAULT_NONCE, nullable=False, convert=None):
        self.name = name
        self.default = default
        self.nullable = nullable
        self.convert = convert

def make_base(session, meta_base=type, base_cls=object):

    tags = set()

    class metacls(meta_base):

        def __new__(mcs, name, bases, cls_dict):

            if '_tag' in cls_dict:
                if cls_dict['_tag'] in tags:
                    raise TypeError('Tag %r has already been defined' % (cls_dict['_tag'],))
                tags.add(cls_dict['_tag'])

            s = set()
            for b in bases:
                s |= set(getattr(b, '_columns', set()))
            s |= set(cls_dict.get('_columns', set()))
            for x in s:
                if not isinstance(x, Column):
                    raise TypeError('Got unexpected %r instead of Column' % (x,))

            cls_dict['_columns'] = s
            cls_dict['_column_map'] = dict((c.name, c) for c in s)
            cls_dict['_column_names'] = frozenset(c.name for c in s)
            cls_dict['_required_columns'] = frozenset(c.name for c in s if not c.nullable)

            if not '_abstract' in cls_dict:
                cls_dict.setdefault('_indexes', [])
                cls_dict['_schemaless_index_collection'] = IndexCollection(cls_dict['_indexes'])
                for idx in cls_dict['_indexes']:
                    idx.declare(session.datastore)

            cls_dict['_session'] = session
            return meta_base.__new__(mcs, name, bases, cls_dict)

    class Document(base_cls):

        __metaclass__ = metacls

        _abstract = True
        _columns = [Column('_tag')]
        _indexes = []
        _id_field = None

        log = ClassLogger()

        def __init__(self, from_dict=None, is_dirty=True, **kwargs):

            if base_cls is not object:
                super(Document, self).__init__()

            if from_dict is None:
                from_dict = kwargs

            if hasattr(self, '_tag') and '_tag' in from_dict:
                if getattr(self, '_tag') != from_dict['_tag']:
                    raise TypeError('Inconsistent tag')
            
            # FIXME: ought to grab other attributes off the class dict as well
            self.__dict__['_schemaless_collected_fields'] = set(['_tag'])
            self.__dict__['_schemaless_id'] = from_dict.get('id', None)

            for k, v in from_dict.iteritems():
                if k in self._column_names:
                    self.__dict__[k] = v
                    self._schemaless_collected_fields.add(k)

            # Add default values
            dict_keys = from_dict.keys()
            for c in self._columns:
                if c.default != DEFAULT_NONCE and c.name not in dict_keys:
                    if callable(c.default):
                        v = c.default()
                    else:
                        v = c.default
                    self.__dict__[c.name] = v
                    self._schemaless_collected_fields.add(c.name)

            self._schemaless_dirty = is_dirty
            if self._schemaless_dirty and self._saveable():
                self._session.dirty_documents.add(self)

        def _saveable(self):
            return self._schemaless_collected_fields >= self._required_columns

        def __setattr__(self, k, v):
            if k in self._column_names:
                self._schemaless_collected_fields.add(k)
                self._schemaless_dirty = True
                if self not in self._session.dirty_documents and self._saveable():
                    self._session.dirty_documents.add(self)
            super(Document, self).__setattr__(k, v)

        def __delattr__(self, k):
            try:
                self._schemaless_collected_fields.remove(k)
            except KeyError:
                pass
            super(Document, self).__delattr__(k)

        @property
        def is_dirty(self):
            return self._schemaless_dirty

        @classmethod
        def from_datastore(cls, d):
            missing = cls._required_columns - set(d.keys())
            if missing:
                raise ValueError('Missing from %s the following keys: %s' % (d, ', '.join(k for k in sorted(missing))))
            for k, v in d.iteritems():
                c = cls._column_map.get(k)
                if c and c.convert:
                    d[k] = c.convert.from_db(v)

            obj = cls(d, is_dirty=False)
            obj.updated = d['updated']
            return obj

        def to_dict(self):
            d = {'id': self.id}
            for f in self._column_names:
                if f in self._required_columns:
                    val = getattr(self, f)
                elif hasattr(self, f):
                    val = getattr(self, f)
                else:
                    continue
                if self._column_map[f].convert:
                    val = self._column_map[f].convert.to_db(val)
                d[f] = val
            return d

        @property
        def id(self):
            return getattr(self, '_schemaless_id', None)

        def save(self, clear_session=True):
            if not self._saveable():
                missing = self._required_columns - self._schemaless_collected_fields
                raise ValueError('This object is not yet saveable, missing: %s' % (', '.join(str(k) for k in missing),))
            if self._schemaless_dirty:
                obj = self._session.datastore.put(self.to_dict())
                self.updated = obj['updated']
                self._schemaless_id = obj['id']
                self._schemaless_dirty = False
                if clear_session and self in self._session.dirty_documents:
                    self._session.dirty_documents.remove(self)
            return self

        def delete(self, clear_session=True):
            if not self._saveable():
                raise ValueError('This object is not yet saveable')
            if not hasattr(self, '_schemaless_id'):
                raise ValueError('This object has no entity id (or has not been persisted)')
            self._session.datastore.delete(id=self._schemaless_id)
            if clear_session and self in self._session.dirty_documents:
                self._session.dirty_documents.remove(self)

        @classmethod
        def _query(cls, *exprs, **kwargs):
            exprs, order_by, limit = reduce_args(*exprs, **kwargs)
            columns = set(e.name for e in exprs)
            idx = cls._schemaless_index_collection.best_index(columns)
            using = idx.field_set & columns

            query_exprs = [e for e in exprs if e.name in using]
            result = idx.underlying._do_query(query_exprs, order_by, limit)
            retained_result = []
            for x in result:
                if all(e.check(x) for e in exprs):
                    retained_result.append(cls.from_datastore(x))
            return retained_result

        @classmethod
        def get(cls, *exprs, **kwargs):
            kwargs['limit'] = 1
            result = cls._query(*exprs, **kwargs)
            if len(result) == 0:
                return None
            elif len(result) == 1:
                return result[0]
            else:
                raise ValueError('Got more than one result')

        @classmethod
        def query(cls, *exprs, **kwargs):
            return cls._query(*exprs, **kwargs)

        @classmethod
        def all(cls):
            return cls._query(c.entity_id != None)

        @classmethod
        def by_id(cls, id):
            entity = cls._session.datastore.by_id(id)
            if not entity:
                return entity
            if entity._tag != cls._tag:
                raise ValueError('Entity had tag %r, our class has tag %r' % (entity._tag, cls._tag))
            return cls.from_datastore(entity)

    return Document
