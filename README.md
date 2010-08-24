Introduction
============

Schemaless is a Python module that implements the pattern described by Bret
Taylor in his post
[How FriendFeed uses MySQL to store schema-less data](http://bret.appspot.com/entry/how-friendfeed-uses-mysql). There
are a couple of other Python modules out there that do this already. Here's how
schemaless is different:

 * Only MySQL is supported. That said, I'd love to add SQLite support in the
   future.
 * Sharding isn't yet supported. Should be pretty straightforward to implement,
   though.
 * There's an optional "ORM" (which isn't really relational) implemented as
   `schemaless.orm`. The "ORM" really is optional, and the interface described
   by FriendFeed is all usable and decoupled from the session/object stuff.
 * The ORM is designed to be mostly declarative and easy to use. That means that
   you can say, "I have have a document type `User`, and please can I have an
   index on `(user_id)`, and I'd also like an index on `(first_name, last_name)`
   please." The ORM will then create the necessary index tables and
   automatically update them when you add new users; it will also know how to
   pick the most specific index, given an arbitrary query.

Basic Usage
===========

The code exported under the `schemaless` module exactly mimics the behavior and
interface described by FriendFeed.

Example
-------

Consider the following MySQL database schema:

    CREATE TABLE entities (
        added_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        id BINARY(16) NOT NULL,
        updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        body MEDIUMBLOB,
        UNIQUE KEY (id),
        KEY (updated)
    ) ENGINE=InnoDB;
    
    CREATE TABLE index_user_id (
        entity_id BINARY(16) NOT NULL UNIQUE,
        user_id CHAR(32) NOT NULL,
        PRIMARY KEY (user_id, entity_id)
    ) ENGINE=InnoDB;
    
    CREATE TABLE index_user_name (
        entity_id BINARY(16) NOT NULL UNIQUE,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        PRIMARY KEY (first_name, last_name, entity_id)
    ) ENGINE=InnoDB;
    
    CREATE TABLE index_foo (
        entity_id BINARY(16) NOT NULL UNIQUE,
        bar INTEGER NOT NULL,
        PRIMARY KEY (bar, entity_id)
    ) ENGINE=InnoDB;
    
The meaning of all of these tables should be clear to you if you've read Bret's
blog post. The following code is a simple example of the interface that
Schemaless provides:

    import schemaless
    from schemaless import c
    
    ds = schemaless.DataStore(mysql_shards=['localhost:3306'], user='foo', password='foo', database='foo')
    
    # declare which indexes are available
    user = ds.define_index('index_user_id', ['user_id'])
    user_name = ds.define_index('index_user_name', ['first_name', 'last_name'])
    foo = ds.define_index('index_foo', ['bar'])
    
    # automatically knows that index entries should be created in index_user_id and
    # index_user_name, based on the keys in the row given
    row = ds.put({'first_name': 'evan', 'last_name': 'klitzke', 'user_id': schemaless.guid()})
    
    # query based on user_id, using the index defined by 'index_user_id'
    print user.query(c.user_id == row.user_id)
    
    # query based on first/last name, using the index defined by 'index_user_name'
    print user_name.query(c.first_name == 'evan', c.last_name == 'klitzke')

ORM Layer
=========

There's an optional ORM layer, exported via the module `schemaless.orm`. When
you use the ORM layer you can use indexes declaratively, and Schemaless can
automatically pick the correct index to use based on your query. The ORM layer
also knows how to do queries when a full index isn't available (e.g. if you add
a query restriction that isn't fully covered by an index).

Example
-------

*WARNING: all of the stuff below is old/bad, I'll update the docs soon*

This example assumes the same MySQL tables as in the previous example:

    import schemaless
    import schemaless.orm
    from schemaless import c
    
    datastore = schemaless.DataStore(mysql_shards=['localhost:3306'], user='foo', password='foo', database='foo')
    session = schemaless.orm.Session(datastore)
    Base = schemaless.orm.make_base(self.session)
    
    class User(Base):
        _tag = 1
        _persist = ['user_id', 'first_name', 'last_name']
        _optional = ['age', 'favorite_color']
        _indexes = [schemaless.orm.Index('index_user_id', ['user_id']),
                    schemaless.orm.Index('index_user_name', ['first_name', last_name'])]
    
    user = User(user_id=schemaless.guid(), first_name='evan', last_name='klitzke', age=23)
    user.save()
    
    # knows to just use 'index_user_id'
    queried_user = User.get(c.user_id == user.user_id)
    assert user == queried_user
    
    # internally, this figures out that 'index_user_name' matches on the most
    # columns, and uses that index to fetch rows; after the rows are fetched the
    # expressions are re-run on the fetched rows to validate the accuracy of
    # first_name/last_name, and to do the age filtering
    queried_user = User.get(c.first_name == 'evan', c.last_name == 'klitzke', c.age < 100)
    assert user == queried_user

Adding Indexes
==============

There's a class called `IndexUpdater` exported by the `schemaless` module that
provides a basic template for batches that add/update/prune indexes. It's
probably easiest to understand how it works if you look at the source code for
it, which provides an example of a batch that adds a new index in the module
documentation. Look under `schemaless/batch.py`.
