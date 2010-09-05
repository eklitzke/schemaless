import time
import simplejson
import zlib

import tornado.database

from schemaless.column import Entity
from schemaless.index import Index
from schemaless.guid import raw_guid
from schemaless.log import ClassLogger

class DataStore(object):

    log = ClassLogger()

    def __init__(self, mysql_shards=[], user=None, database=None, password=None, use_zlib=True, indexes=[], create_entities=True):
        if not mysql_shards:
            raise ValueError('Must specify at least one MySQL shard')
        if len(mysql_shards) > 1:
            raise NotImplementedError
        self.use_zlib = use_zlib
        self.indexes = [Index('entities', ['tag'])]
        self.connection = tornado.database.Connection(host=mysql_shards[0], user=user, password=password, database=database)
        if create_entities and not self.check_table_exists('entities'):
            self.create_entities_table()

    @property
    def tag_index(self):
        return self.indexes[0]

    def define_index(self, table, properties=[], match_on={}, shard_on=None):
        idx = Index(table=table, properties=properties, match_on=match_on, shard_on=shard_on, connection=self.connection, use_zlib=self.use_zlib)
        self.indexes.append(idx)
        return idx

    def _find_indexes(self, entity, include_entities=False):
        """Find all of the indexes that may index an entity, based on the keys
        in the entity.
        """
        keys = frozenset(entity.keys())
        for idx in self.indexes:
            if idx.matches(entity, keys):
                if idx.table != 'entities':
                    yield idx
                elif include_entities:
                    yield idx
    
    def put(self, entity, tag=None):
        is_update = False
        entity['updated'] = time.time()
        entity_id = None

        entity_copy = entity.copy()

        # get the entity_id (or create a new one)
        entity_id = entity_copy.pop('id', None)
        if entity_id is None:
            entity_id = raw_guid()
        else:
            is_update = True
            if len(entity_id) != 16:
                entity_id = entity_id.decode('hex')
        body = simplejson.dumps(entity_copy)
        if self.use_zlib:
            body = zlib.compress(body, 1)

        if is_update:
            self._put_update(entity_id, entity_copy, body)
            return entity
        else:
            return self._put_new(entity_id, entity_copy, tag, body)

    def _insert_index(self, index, entity_id, entity):
        pnames = ['entity_id']
        vals = [entity_id]
        for p in index.properties:
            pnames.append(p)
            vals.append(entity[p])

        q = 'INSERT INTO %s (%s) VALUES (' % (index.table, ', '.join(pnames))
        q += ', '.join('%s' for x in pnames)
        q += ')'
        try:
            self.connection.execute(q, *vals)
        except tornado.database.OperationalError:
            self.log.exception('query = %s, vals = %s' % (q, vals))
            raise

    def _update_index(self, index, entity_id, entity):
        row = self.connection.get('SELECT * FROM %s WHERE entity_id = %%s' % (index.table,), entity_id)
        if row:
            vals = []
            q = 'UPDATE %s SET ' % index.table
            qs = []
            for p in index.properties:
                qs.append('%s = %%s' % p)
                vals.append(entity[p])
            q += ', '.join(qs)
            q += ' WHERE entity_id = %s'
            vals.append(entity_id)
            self.connection.execute(q, *vals)
        else:
            self._insert_index(index, entity_id, entity)

    def _put_new(self, entity_id, entity, tag, body):
        pk = self.connection.execute('INSERT INTO entities (id, updated, tag, body) VALUES (%s, FROM_UNIXTIME(%s), %s, %s)', entity_id, int(entity['updated']), tag, body)
        for idx in self._find_indexes(entity):
            self._insert_index(idx, entity_id, entity)
        return self.by_id(entity_id)

    def _put_update(self, entity_id, entity, body):
        self.connection.execute('UPDATE entities SET updated = CURRENT_TIMESTAMP, body = %s WHERE id = %s', body, entity_id)
        for idx in self._find_indexes(entity):
            self._update_index(idx, entity_id, entity)

    def delete(self, entity=None, id=None):
        if entity is None and id is None:
            raise ValueError('Must provide delete with an entity and an id')
        if entity and 'id' not in entity:
            raise ValueError('Cannot provide an entity without an id')
        if not entity:
            entity = self.by_id(id)
            if not entity:
                return 0
        entity_id = entity['id'].decode('hex')

        def _delete(table_name):
            col = 'id' if table_name == 'entities' else 'entity_id'
            return int(bool(self.connection.execute('DELETE FROM %s WHERE %s = %%s' % (table_name, col), entity_id)))

        deleted = 0
        seen_entities = False
        for idx in self._find_indexes(entity):
            if idx.table == 'entities':
                seen_entities = True
            deleted += _delete(idx.table)
        if not seen_entities:
            deleted += _delete('entities')
        return deleted

    def by_id(self, id):
        if len(id) == 32:
            id = id.decode('hex')
        row = self.connection.get('SELECT * FROM entities WHERE id = %s', id)
        return Entity.from_row(row, use_zlib=self.use_zlib) if row else None

    def check_table_exists(self, table_name):
        row = self.connection.get('SELECT COUNT(*) AS tbl_count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s', table_name)
        return bool(row['tbl_count'])

    def create_entities_table(self):
        self.connection.execute("""
            CREATE TABLE entities (
                added_id INTEGER NOT NULL AUTO_INCREMENT,
                id BINARY(16) NOT NULL,
                updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                tag MEDIUMINT,
                body MEDIUMBLOB NOT NULL,
                PRIMARY KEY (added_id),
                UNIQUE KEY (id),
                KEY (updated)
            ) ENGINE=InnoDB""")
