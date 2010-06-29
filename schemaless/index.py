from schemaless.column import ColumnExpression, Entity

class Index(object):

    def __init__(self, table, properties=[], match_on={}, shard_on=None, connection=None):
        if shard_on is not None:
            raise NotImplementedError
    
        self.table = table
        self.properties = frozenset(properties)
        self.match_on = match_on
        self.connection = connection

    def __cmp__(self, other):
        return cmp(self.table, other.table)

    def matches(self, entity, keys):
        if not self.properties <= keys:
            return False
        for k, v in self.match_on.iteritems():
            if entity.get(k) != v:
                return False
        return True
    
    def _query(self, *exprs, **kwargs):
        limit = kwargs.pop('limit', None)
        order_by = kwargs.pop('order_by', None)

        exprs = list(exprs)
        for k, v in kwargs.iteritems():
            exprs.append(ColumnExpression(k, ColumnExpression.EQ, v))

        if not exprs:
            raise ValueError('Must provide args/kwargs for a WHERE clause')

        values = []
        where_clause = []
        for e in exprs:
            if e.name not in self.properties:
                raise ValueError('This index has no column named %r' % (e.name,))
            expr_string, vals = e.build()
            where_clause.append(expr_string)
            values.extend(vals)

        q = 'SELECT entity_id FROM %s WHERE ' % self.table
        q += ' AND '.join(where_clause)
        if order_by:
            q += ' ORDER BY %s' % (order_by,)
        if limit:
            q += ' LIMIT %d' % (limit,)

        rows = self.connection.query(*([q] + values))

        if rows:
            entity_ids = [r['entity_id'] for r in rows]
            q = 'SELECT * FROM entities WHERE id IN ('
            q += ', '.join('%s' for x in rows)
            q += ')'
            entity_rows = self.connection.query(*([q] + entity_ids))
            entity_rows.sort(key = lambda x: x['added_id'])
            return [Entity.from_row(row) for row in entity_rows]
        else:
            return []

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
