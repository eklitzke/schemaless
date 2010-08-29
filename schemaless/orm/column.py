import schemaless.orm.converters

DEFAULT_NONCE = Ellipsis

class Column(object):

    def __init__(self, name, default=DEFAULT_NONCE, nullable=False, convert=None):
        self.name = name
        self.default = default
        self.nullable = nullable
        self.convert = convert

    def to_string(self):
        return 'COLUMN'

    def __str__(self):
        s = '`%s` %s' % (self.name, self.to_string())
        if not self.nullable:
            s += ' NOT NULL'
        return s

class CHAR(Column):

    def __init__(self, name, length, **kwargs):
        super(CHAR, self).__init__(name, **kwargs)
        self.length = length

    def to_string(self):
        return 'CHAR(%d)' % (self.length,)

class BINARY(Column):

    def __init__(self, name, length, **kwargs):
        super(BINARY, self).__init__(name, **kwargs)
        self.length = length

    def to_string(self):
        return 'BINARY(%d)' % (self.length,)

class VARCHAR(Column):
    def __init__(self, name, length, **kwargs):
        super(VARCHAR, self).__init__(name, **kwargs)
        self.length = length

    def to_string(self):
        return 'VARCHAR(%d)' % (self.length,)

class TEXT(Column):

    def to_string(self):
        return 'TEXT'

class DATETIME(Column):

    def __init__(self, name, **kwargs):
        if not kwargs.get('convert'):
            kwargs['convert'] = schemaless.orm.converters.DateTimeConverter
        super(DATETIME, self).__init__(name, **kwargs)

    def to_string(self):
        return 'INTEGER UNSIGNED'

class GUID(CHAR):

    def __init__(self, name, **kwargs):
        super(GUID, self).__init__(name, 32, **kwargs)
