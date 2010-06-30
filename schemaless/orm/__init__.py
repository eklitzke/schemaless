class Session(object):

    def __init__(self, datastore):
        self.datastore = datastore
        self.dirty_documents = set()

    def save(self):
        for d in self.dirty_documents:
            d.save(clear_session=False)
        self.dirty_documents.clear()

def make_base(session, meta_base=type, base_cls=object):

    tags = set()

    class metacls(meta_base):

        def __new__(mcs, name, bases, cls_dict):

            if '_tag' in cls_dict:
                if cls_dict['_tag'] in tags:
                    raise TypeError('Tag %r has already been defined' % (cls_dict['_tag'],))
                tags.add(cls_dict['_tag'])

            persist = set()
            for b in bases:
                persist |= set(getattr(b, '_persist', set()))
            persist |= set(cls_dict.get('_persist', set()))
            cls_dict['_persist'] = persist
            cls_dict['_session'] = session
            return meta_base.__new__(mcs, name, bases, cls_dict)

    class Document(base_cls):

        __metaclass__ = metacls

        _persist = ['_tag']
        _id_field = None

        def __init__(self, from_dict=None, is_dirty=True):

            if base_cls is not object:
                super(Document, self).__init__()


            if from_dict is None:
                from_dict = {}

            if hasattr(self, '_tag') and '_tag' in from_dict:
                if getattr(self, '_tag') != from_dict['_tag']:
                    raise TypeError('Inconsistent tag')
            
            # FIXME: ought to grab other attributes off the class dict as well
            self.__dict__['_schemaless_collected_fields'] = set(['_tag'])
            self.__dict__['_schemaless_id'] = from_dict.get('id', None)

            for k, v in from_dict.iteritems():
                if k in self._persist:
                    self.__dict__[k] = v
                    self._schemaless_collected_fields.add(k)

            self._schemaless_dirty = is_dirty
            if self._schemaless_dirty and self._saveable():
                self._session.dirty_documents.add(self)

        def _saveable(self):
            return self._schemaless_collected_fields >= self._persist

        def __setattr__(self, k, v):
            if k in self._persist:
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
            missing = self._persist - set(d.keys())
            if missing:
                raise ValueError('Missing the following keys: ' + ', '.join(k for k in sorted(missing)))
            return cls(d, is_dirty=False)

        def to_dict(self):
            d = {}
            for f in self._persist:
                d[f] = getattr(self, f)
            return d

        def save(self, clear_session=True):
            if not self._saveable():
                raise ValueError('This object is not yet saveable')
            if self._schemaless_dirty:
                obj = self._session.datastore.put(self.to_dict())
                self._schemaless_id = obj['id']
                self._schemaless_dirty = False
                if clear_session and self in self._session.dirty_documents:
                    self._session.dirty_documents.remove(self)

        def delete(self, clear_session=True):
            if not self._saveable():
                raise ValueError('This object is not yet saveable')
            if not hasattr(self, '_schemaless_id'):
                raise ValueError('This object has no entity id (or has not been persisted)')
            self._session.datastore.delete(id=self._schemaless_id)
            if clear_session and self in self._session.dirty_documents:
                self._session.dirty_documents.remove(self)



    return Document
