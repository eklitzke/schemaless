import schemaless.orm.converters

DEFAULT_NONCE = Ellipsis

class Column(object):

    def __init__(self, name, default=DEFAULT_NONCE, required=False, convert=None):
        self.name = name
        self.default = default
        self.required = required
        self.convert = convert

    def to_string(self):
        return 'COLUMN'

    def __str__(self):
        s = '`%s` %s' % (self.name, self.to_string())
        if self.required:
            s += ' NOT NULL'
        return s

class Char(Column):

    def __init__(self, name, length, **kwargs):
        super(Char, self).__init__(name, **kwargs)
        self.length = length

    def to_string(self):
        return 'CHAR(%d)' % (self.length,)

class Binary(Column):

    def __init__(self, name, length, **kwargs):
        super(Binary, self).__init__(name, **kwargs)
        self.length = length

    def to_string(self):
        return 'BINARY(%d)' % (self.length,)

class String(Column):
    def __init__(self, name, length, **kwargs):
        super(String, self).__init__(name, **kwargs)
        self.length = length

    def to_string(self):
        return 'VARCHAR(%d)' % (self.length,)

class Text(Column):

    def to_string(self):
        return 'TEXT'

class DateTime(Column):

    def __init__(self, name, **kwargs):
        if not kwargs.get('convert'):
            kwargs['convert'] = schemaless.orm.converters.DateTimeConverter
        super(DateTime, self).__init__(name, **kwargs)

    def to_string(self):
        return 'INTEGER UNSIGNED'

class Guid(Char):

    def __init__(self, name, **kwargs):
        super(Guid, self).__init__(name, 32, **kwargs)
UUID = GUID = Guid

class Bool(Column):

    def __init__(self, name, **kwargs):
        if not kwargs.get('convert'):
            kwargs['convert'] = schemaless.orm.converters.BooleanConverter
        super(Bool, self).__init__(name, **kwargs)

    def to_string(self):
        return 'BOOL'

Bit = Boolean = Bool
