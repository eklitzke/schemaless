import hashlib
import schemaless.index
from schemaless.log import ClassLogger

class Index(object):

    log = ClassLogger()

    def __init__(self, table_name, fields):
        self.table_name = table_name
        self.fields = fields
        self.field_set = frozenset(fields)
        self.underlying = None

    @classmethod
    def automatic(cls, tag, fields, datastore, declare=True):
        """This is an "internal" method for declaratively creating
        indexes. Arguments are like this:

        tag -- the tag of the document that this is being created for
        fields -- a list of typed Column objects like [Binary('foo', 16), VarChar('email', 255)]
        datastore -- a handle to the datastore

        A unique table name will be created using the tag and an md5 of the
        field names. The table will be created, if necessary.
        """

        field_string = ', '.join('`%s`' % (f.name,) for f in fields)
        field_hash = hashlib.md5(field_string).hexdigest()
        table_name = 'index_%05d_%s' % (tag, field_hash)

        row = datastore.connection.get('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s', table_name)
        count = row.values()[0]
        if count == 0:
            cls.log.info('Creating %s' % (table_name,))
            sql = ['CREATE TABLE %s (' % (table_name,)]
            for f in fields:
                sql.append('    %s,' % (f,))
            sql.append('    `entity_id` BINARY(16) NOT NULL,')
            sql.append('    KEY (`entity_id`),')
            sql.append('    PRIMARY KEY (%s, `entity_id`)' % (field_string,))
            sql.append(') ENGINE=InnoDB')
            sql = '\n'.join(sql)

            # by this point, sql will contain a query like:
            #
            # CREATE TABLE index_00003_850f22a7c399fd1483275d62703d49de (
            #     `business_id` BINARY(16) NOT NULL,
            #     `entity_id` BINARY(16) NOT NULL,
            #     KEY (`entity_id`),
            #     PRIMARY KEY (`business_id`, `entity_id`)
            # ) ENGINE=InnoDB
            #
            # XXX: no support for unique columns yet

            # create the table
            datastore.connection.execute(sql)

        obj = cls(table_name, [f.name for f in fields])
        if declare:
            obj.declare(datastore, tag=tag)
        return obj

    def declare(self, datastore, tag=None):
        match_on = {}
        if tag is not None:
            match_on = {'tag': tag}
        self.underlying = datastore.define_index(self.table_name, self.fields, match_on=match_on)
        return self.underlying

    def __str__(self):
        if self.underlying is None:
            return '%s(%r, %s)' % (self.__class__.__name__, self.table_name, self.fields)
        else:
            return '%s(%s)' % (self.__class__.__name__, self.underlying)
    __repr__ = __str__

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
        self.log.debug('from %s chose %s as best index for %s' % (self.indexes, best, fields))
        self.answer_cache[fields] = best
        return best
