import schemaless.index
from schemaless.log import ClassLogger

class Index(object):

    def __init__(self, table_name, fields):
        self.table_name = table_name
        self.fields = fields
        self.field_set = frozenset(fields)
        self.underlying = None

    def declare(self, datastore, tag=None):
        match_on = {}
        if tag is not None:
            match_on = {'_tag': tag}
        self.underlying = datastore.define_index(self.table_name, self.fields, match_on=match_on)
        return self.underlying

class IndexCollection(object):

    log = ClassLogger()

    def __init__(self, indexes):
        self.indexes = indexes
        self.answer_cache = {}

    def best_index(self, fields):
        """Given some collection of fields (e.g. ['user_id', 'first_name',
        'last_name']) try to determine which index in the collection will match
        the most fields.
        """
        fields = frozenset(fields)
        if fields in self.answer_cache:
            return self.answer_cache[fields]

        # try to find the index that covers the most columns possible, and where
        # the index has the least number of fields possible
        best = (-1, 0, None)
        for idx in self.indexes:
            common = len(fields & idx.field_set)
            val = (common, -len(idx.field_set), idx)
            if val > best:
                best = val

        best = best[-1]
        self.log.debug('chose %s as best index for %s' % (best, fields))
        self.answer_cache[fields] = best
        return best
