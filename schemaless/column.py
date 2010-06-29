import simplejson

class Entity(dict):

    @classmethod
    def new(cls):
        return Entity(id=make_guid())

    @classmethod
    def from_row(cls, row):
        d = simplejson.loads(row['body'])
        d['id'] = row['id'].encode('hex')
        d['updated'] = row['updated']
        return cls(d)

    def __hasattr__(self, name):
        return name in self or name in self.__dict__

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
    
    def __setattr__(self, name, val):
        self[name] = val

    def __str__(self):
        return str(dict(self.items()))

class Column(object):

    def __init__(self, name):
        self.name = name
    
    def to_string(self):
        return self.name

    __str__ = to_string

    def __lt__(self, val):
        return ColumnExpression(self.name, ColumnExpression.OP_LT, val)

    def __le__(self, val):
        return ColumnExpression(self.name, ColumnExpression.OP_LE, val)

    def __eq__(self, val):
        return ColumnExpression(self.name, ColumnExpression.OP_EQ, val)

    def __ne__(self, val):
        return ColumnExpression(self.name, ColumnExpression.OP_NE, val)

    def __gt__(self, val):
        return ColumnExpression(self.name, ColumnExpression.OP_GT, val)

    def __ge__(self, val):
        return ColumnExpression(self.name, ColumnExpression.OP_GE, val)

    def in_(self, vals):
        return ColumnExpression(self.name, ColumnExpression.OP_IN, vals)

class ColumnExpression(object):

    OP_LT = 1
    OP_LE = 2
    OP_EQ = 3
    OP_NE = 4
    OP_GT = 5
    OP_GE = 6
    OP_IN = 7

    def __init__(self, name, op, rhs):
        self.name = name
        self.op = op
        self.rhs = rhs

    def build(self):
        if self.op == self.OP_LT:
            return self.name + ' < %s', [self.rhs]
        elif self.op == self.OP_LE:
            return self.name + ' <= %s', [self.rhs]
        elif self.op == self.OP_EQ:
            return (self.name + ' = %s'), [self.rhs]
        elif self.op == self.OP_NE:
            return (self.name + ' != %s'), [self.rhs]
        elif self.op == self.OP_GT:
            return (self.name + ' > %s'), [self.rhs]
        elif self.op == self.OP_GE:
            return (self.name + ' >= %s'), [self.rhs]
        elif self.op == self.OP_IN:
            sql = self.name + ' IN (' + ', '.join('%s' for x in self.rhs) + ')'
            return sql, self.rhs
        else:
            raise ValueError('Unknown operator')

class ColumnBuilder(object):

    def __init__(self):
        self._columns = {}

    def __getattr__(self, name):
        if name not in self._columns:
            self._columns[name] = Column(name)
        return self._columns[name]

c = ColumnBuilder()
