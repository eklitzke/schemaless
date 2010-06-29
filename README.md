Introduction
============

Schemaless is a Python module that implements the pattern described by Bret
Taylor in his post
[How FriendFeed uses MySQL to store schema-less data](http://bret.appspot.com/entry/how-friendfeed-uses-mysql). There
are a couple of other Python modules out there that do this already. Here's how
schemaless is different:

 * There's no ORM. Schemaless provides just enough support to do simple queries
   and automatically update indexes.
 * Only MySQL is supported. That said, I'd love to add SQLite support in the
   future.
 * Sharding isn't yet supported. Should be pretty straightforward to implement,
   though.

Usage
=====

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
    
    print user.query(c.user_id == row.user_id)
    print '--------'
    rows = user_name.query(c.first_name == 'evan', c.last_name == 'klitzke')
    print '\n'.join(str(r) for r in rows)
