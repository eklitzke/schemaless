import datetime
import logging
import unittest

import schemaless
from schemaless import orm
from schemaless import c

class TestBase(unittest.TestCase):

    def clear_tables(self, datastore):
        for tbl in ['entities', 'index_user_id', 'index_user_name', 'index_foo', 'index_birthdate', 'index_todo_user_id', 'index_todo_user_id_time']:
            datastore.connection.execute('DELETE FROM %s' % (tbl,))

    def assert_equal(self, a, b):
        return self.assertEqual(a, b)

    def assert_len(self, a, b):
        return self.assertEqual(a, len(b))

class SchemalessTestCase(TestBase):

    def setUp(self):
        super(SchemalessTestCase, self).setUp()
        self.ds = schemaless.DataStore(mysql_shards=['localhost:3306'], user='test', password='test', database='test')
        self.user = self.ds.define_index('index_user_id', ['user_id'])
        self.user_name = self.ds.define_index('index_user_name', ['first_name', 'last_name'])
        self.foo = self.ds.define_index('index_foo', ['bar'], {'m': 'right'})
        self.clear_tables(self.ds)

        self.entity = self.ds.put({'user_id': schemaless.guid(), 'first_name': 'evan', 'last_name': 'klitzke'})

    def test_query(self):
        self.assert_len(1, self.user.query(c.user_id == self.entity.user_id))
        self.assert_len(1, self.user_name.query(c.first_name == 'evan', c.last_name == 'klitzke'))

        new_entity = self.ds.put({'user_id': schemaless.guid(), 'first_name': 'george'})
        self.assert_len(1, self.user.query(c.user_id == new_entity.user_id))
        self.assert_len(0, self.user_name.query(c.first_name == 'george')) # didn't have a full index

    def test_delete_by_entity(self):
        self.ds.delete(self.entity)
        self.assert_len(0, self.user.query(c.user_id == self.entity.user_id))

    def test_delete_by_entity_id(self):
        self.ds.delete(id=self.entity.id)
        self.assert_len(0, self.user.query(c.user_id == self.entity.user_id))

    def test_match_on(self):
        entity_one = self.ds.put({'foo_id': schemaless.guid(), 'bar': 1, 'm': 'left'})
        entity_two = self.ds.put({'foo_id': schemaless.guid(), 'bar': 1, 'm': 'right'}) # only this should match

        rows = self.foo.query(c.bar == 1)
        self.assert_len(1, rows)
        self.assert_equal(rows[0].foo_id, entity_two.foo_id)

    def test_in_queries(self):
        user_ids = [self.entity.user_id]
        user_ids.append(self.ds.put({'user_id': schemaless.guid()}).user_id)

        rows = self.user.query(c.user_id.in_(user_ids))
        self.assert_len(2, rows)
        self.assert_equal(set(user_ids), set(row['user_id'] for row in rows))

class ORMTestCase(TestBase):
    def setUp(self):
        datastore = schemaless.DataStore(mysql_shards=['localhost:3306'], user='test', password='test', database='test')
        self.clear_tables(datastore)

        tags_db = {
            'User': 1,
            'ToDo': 2 }

        self.session = orm.Session(datastore)
        self.base_class = orm.make_base(self.session, tags_db=tags_db)

    @property
    def connection(self):
        return self.session.datastore.connection

    def get_index_count(self, index_name):
        row = self.connection.get('SELECT COUNT(*) AS count FROM %s' % (index_name,))
        return row['count']

class SchemalessORMTestCase(ORMTestCase):

    def setUp(self):
        super(SchemalessORMTestCase, self).setUp()

        class User(self.base_class):
            _columns = [orm.Column('user_id'),
                        orm.Column('first_name'),
                        orm.Column('last_name'),
                        orm.Column('birthdate', nullable=True),
                        orm.Column('time_created', default=datetime.datetime.now, convert=schemaless.orm.converters.DateTimeConverter)]
            _indexes = [orm.Index('index_user_id', ['user_id']),
                        orm.Index('index_birthdate', ['birthdate']),
                        orm.Index('index_user_name', ['first_name', 'last_name'])]

        self.User = User

    def test_create_object_save_delete(self):
        # create a new, empty object
        u = self.User()
        assert not u._saveable()
        assert u.is_dirty

        # populate some, but not all of the fields; the object should be dirty,
        # but not saveable
        u.user_id = schemaless.guid()
        u.first_name = 'evan'
        assert not u._saveable()
        assert u.is_dirty
        user = self.User.get(c.user_id == u.user_id)
        assert not user

        # finish populating the fields, check that the object is saveable
        u.last_name = 'klitzke'
        assert u._saveable()
        assert u.is_dirty

        # persist the object, check that it made it to the datastore
        u.save()
        assert u._saveable()
        assert not u.is_dirty
        user = self.User.get(c.user_id == u.user_id)
        assert user

        # delete the object, check that it's deleted from the datastore
        u.delete()
        assert u._saveable()
        assert not u.is_dirty
        user = self.User.get(c.user_id == u.user_id)
        assert not user

    def test_in_query(self):
        user_ids = []
        users = []
        for x in range(5):
            u = self.User(user_id=schemaless.guid(), first_name='foo', last_name='bar')
            user_ids.append(u.user_id)
            users.append(u)
        self.session.save()

        fetched_users = self.User.query(c.user_id.in_(user_ids[:3]))
        self.assert_equal(set(user_ids[:3]), set(u.user_id for u in users[:3]))

    def test_name_query(self):
        u = self.User(user_id=schemaless.guid(), first_name='foo', last_name='bar')
        u.save()
        v = self.User.get(c.first_name == 'foo', c.last_name == 'bar')
        self.assert_equal(u.user_id, v.user_id)

    def test_update(self):
        u = self.User(user_id=schemaless.guid(), first_name='foo', last_name='bar')
        u.save()
        v = self.User.get(c.first_name == 'foo', c.last_name == 'bar')
        self.assert_equal(u.id, v.id)
        self.assert_equal(u.user_id, v.user_id)

        u.first_name = 'baz'
        u.save()
        v = self.User.get(c.first_name == 'foo', c.last_name == 'bar')
        self.assert_equal(None, v)
        v = self.User.get(c.first_name == 'baz', c.last_name == 'bar')
        self.assert_equal(u.id, v.id)
        self.assert_equal(u.user_id, v.user_id)

    def test_double_delete(self):
        u = self.User(user_id=schemaless.guid(), first_name='foo', last_name='bar')
        u.save()
        u.delete()
        u.delete()

    def test_update_preserves_id(self):
        u = self.User(user_id=schemaless.guid(), first_name='foo', last_name='bar')
        u.save()
        orig_id = u.id

        u.first_name = 'baz'
        u.save()
        self.assert_equal(orig_id, u.id)

    def test_converter(self):
        u = self.User(user_id=schemaless.guid(), first_name='foo', last_name='bar')
        u.save()
        self.assert_(isinstance(u.time_created, datetime.datetime))

        v = self.User.get(c.user_id == u.user_id)
        self.assert_(isinstance(v.time_created, datetime.datetime))

    def test_index_update(self):
        u = self.User(user_id=schemaless.guid(), first_name='evan', last_name='klitzke')
        u.save()

        self.assert_equal(self.get_index_count('index_birthdate'), 0)

        u.birthdate = '1986-09-19'
        u.save()
        self.assert_equal(self.get_index_count('index_birthdate'), 1)

class ManyToOneORMTestCase(ORMTestCase):

    def setUp(self):
        super(ManyToOneORMTestCase, self).setUp()

        class ToDo(self.base_class):
            _columns = [orm.Column('user_id'),
                        orm.Column('action'),
                        orm.Column('completion_time', default=None, nullable=True, convert=schemaless.orm.converters.DateTimeConverter)]
            _indexes = [orm.Index('index_todo_user_id', ['user_id'])]
                        #orm.Index('index_todo_user_id_time', ['user_id', 'completion_time'])]

        self.ToDo = ToDo

    @property
    def connection(self):
        return self.session.datastore.connection

    def test_update_multiple(self):
        user_id = schemaless.guid()
        item1 = self.ToDo(user_id=user_id, action='buy groceries').save()
        item2 = self.ToDo(user_id=user_id, action='buy groceries').save()

        self.assert_equal(self.get_index_count('index_todo_user_id'), 2)

        item1.completion_time = datetime.datetime.now()
        item1.save()

        self.assert_equal(self.get_index_count('index_todo_user_id'), 2)

if __name__ == '__main__':
    unittest.main()
