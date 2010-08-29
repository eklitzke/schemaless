from schemaless.column import ColumnExpression, Entity

class Order(object):

    def __init__(self, name, asc=False, desc=False):
        self.name = name
        self.order = 'ASC' if asc else 'DESC'

def reduce_args(*exprs, **kwargs):
    limit = kwargs.pop('limit', None)
    order_by = kwargs.pop('order_by', None)
    asc = kwargs.pop('asc', False)
    desc = kwargs.pop('desc', False)
    if asc and desc:
        raise ValueError('Cannot specify both asc=True and desc=True')
    if order_by:
        if not (asc or desc):
            asc = True
        order_by = Order(order_by, asc=asc, desc=desc)

    exprs = list(exprs)
    for k, v in kwargs.iteritems():
        exprs.append(ColumnExpression(k, ColumnExpression.OP_EQ, v))

    # if it's just an order_by, check for the order_by column not nulll
    #if order_by and not exprs:
    #    exprs.append(ColumnExpression(order_by.name, ColumnExpression.OP_NE, None))

    if not (order_by or exprs):
        raise ValueError('Must provide args/kwargs for a WHERE clause')
    return exprs, order_by, limit

class Index(object):

    def __init__(self, table, properties=[], match_on={}, shard_on=None, connection=None, use_zlib=True):
        if shard_on is not None:
            raise NotImplementedError
        if any(',' in p for p in properties):
            raise ValueError('Bad property name: %r' % (p,))

        self.table = table
        self.properties = frozenset(properties)
        self.match_on = match_on
        self.connection = connection
        self.use_zlib = use_zlib

    def __str__(self):
        return '%s(table=%s, properties=%s, match_on=%s)' % (self.__class__.__name__, self.table, self.properties, self.match_on)
    __repr__ = __str__

    def __cmp__(self, other):
        return cmp(self.table, other.table)

    def matches(self, entity, keys):
        if not (self.properties <= keys):
            return False
        for k, v in self.match_on.iteritems():
            if entity.get(k) != v:
                return False
        return True

    def _query(self, *exprs, **kwargs):
        exprs, order_by, limit = reduce_args(*exprs, **kwargs)
        return self._do_query(exprs, order_by, limit)

    def _do_query(self, exprs, order_by, limit):
        values = []
        where_clause = []
        for e in exprs:
            if e.name not in self.properties:
                raise ValueError('This index has no column named %r' % (e.name,))
            expr_string, vals = e.build()
            where_clause.append(expr_string)
            values.extend(vals)

        if self.table == 'entities':
            # XXX: this is a bit hacky
            q = 'SELECT * FROM entities WHERE ' + ' AND '.join(where_clause)
            if order_by:
                q += ' ORDER BY %s %s' % (order_by.name, order_by.order)
            if limit:
                q += ' LIMIT %d' % (limit,)
            entity_rows = self.connection.query(q, *values)
        else:
            q = 'SELECT entity_id FROM %s' % self.table
            if where_clause:
                q += 'WHERE ' + ' AND '.join(where_clause)
            if order_by:
                q += ' ORDER BY %s %s' % (order_by.name, order_by.order)
            if limit:
                q += ' LIMIT %d' % (limit,)

            rows = self.connection.query(q, *values)
            if rows:
                entity_ids = [r['entity_id'] for r in rows]
                q = 'SELECT * FROM entities WHERE id IN ('
                q += ', '.join('%s' for x in rows)
                q += ')'
                entity_rows = self.connection.query(q, *entity_ids)
            else:
                return []

        if not order_by:
            #sorted_entities = sorted(entity_rows, key=lambda x: x['updated'], reverse=True)
            sorted_entities = sorted(entity_rows, key=lambda x: x['updated'])
        else:
            # XXX: this is O(n^2), bad
            sorted_entities = []
            for row_id in (row['entity_id'] for row in rows):
                for e in entity_rows:
                    if e['id'] == row_id:
                        sorted_entities.append(e)
                        break
                else:
                    assert False

        return [Entity.from_row(row, use_zlib=self.use_zlib) for row in sorted_entities]

    def get(self, *exprs, **kwargs):
        kwargs['limit'] = 1
        rows = self._query(*exprs, **kwargs)
        if len(rows) == 0:
            return None
        elif len(rows) == 1:
            return rows[0]
        else:
            assert False

    def query(self, *exprs, **kwargs):
        return self._query(*exprs, **kwargs)

    def all(self):
        return self._query(c.entity_id != None)
