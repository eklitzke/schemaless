import unittest

import schemaless
from schemaless import c

class SchemalessTestCase(unittest.TestCase):

    def setUp(self):
        self.ds = ds = schemaless.DataStore(mysql_shards=['localhost:3306'], user='test', password='test', database='test')
        self.user = ds.define_index('index_user_id', ['user_id'])
        self.user_name = ds.define_index('index_user_name', ['first_name', 'last_name'])
        self.foo = ds.define_index('index_foo', ['bar'], {'m': 'right'})
        for tbl in ['entities', 'index_user_id', 'index_user_name', 'index_foo']:
            ds.connection.execute('DELETE FROM %s' % (tbl,))
        self.entity = ds.put({'user_id': schemaless.guid(), 'first_name': 'evan', 'last_name': 'klitzke'})

    def test_query(self):
        self.assertEqual(1, len(self.user.query(c.user_id == self.entity.user_id)))
        self.assertEqual(1, len(self.user_name.query(c.first_name == 'evan', c.last_name == 'klitzke')))

        new_entity = self.ds.put({'user_id': schemaless.guid(), 'first_name': 'george'})
        self.assertEqual(1, len(self.user.query(c.user_id == new_entity.user_id)))
        self.assertEqual(0, len(self.user_name.query(c.first_name == 'george'))) # didn't have a full index

    def test_delete_by_entity(self):
        self.ds.delete(self.entity)
        self.assertEqual(0, len(self.user.query(c.user_id == self.entity.user_id)))

    def test_delete_by_entity_id(self):
        self.ds.delete(id=self.entity.id)
        self.assertEqual(0, len(self.user.query(c.user_id == self.entity.user_id)))

    def test_match_on(self):
        entity_one = self.ds.put({'foo_id': schemaless.guid(), 'bar': 1, 'm': 'left'})
        entity_two = self.ds.put({'foo_id': schemaless.guid(), 'bar': 1, 'm': 'right'}) # only this should match

        rows = self.foo.query(c.bar == 1)
        self.assertEqual(1, len(rows))
        self.assertEqual(rows[0].foo_id, entity_two.foo_id)

    def test_in_queries(self):
        user_ids = [self.entity.user_id]
        user_ids.append(self.ds.put({'user_id': schemaless.guid()}).user_id)

        rows = self.user.query(c.user_id.in_(user_ids))
        self.assertEqual(2, len(rows))
        self.assertEqual(set(user_ids), set(row['user_id'] for row in rows))

if __name__ == '__main__':
    unittest.main()
