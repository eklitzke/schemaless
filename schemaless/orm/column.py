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

class Char(Column):

    def __init__(self, column_name, length, **kwargs):
        super(Char, self).__init__(column_name, **kwargs)
        self.length = length

    def to_string(self):
        return 'CHAR(%d)' % (self.length,)

class Binary(Column):

    def __init__(self, column_name, length, **kwargs):
        super(Binary, self).__init__(column_name, **kwargs)
        self.length = length

    def to_string(self):
        return 'BINARY(%d)' % (self.length,)

class Varchar(Column):
    def __init__(self, column_name, length, **kwargs):
        super(Varchar, self).__init__(column_name, **kwargs)
        self.length = length

    def to_string(self):
        return 'VARCHAR(%d)' % (self.length,)
