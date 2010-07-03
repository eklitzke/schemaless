"""Module to assist in writing index updating batches.

Here's an example of a really simple batch that adds a user_id index.

----------------------------------------------

import schemaless

class AddUserIdIndex(schemaless.IndexUpdater):

    def initialize(self):
        super(AddUserIdIndex, self).initialize()
        self.datastore = schemaless.DataStore(mysql_shards=['localhost:3306'],
                user='test', password='test', database='test')
        self.conn = self.datastore.connection

    def process_row(self, row, entity):
        if entity.get('user_id'):
            self.conn.execute('INSERT IGNORE INTO index_user_id (entity_id, user_id) VALUES (%s, %s)',
                    schemaless.to_raw(entity.id), entity.user_id)

if __name__ == '__main__':
    AddUserIdIndex().start()
"""
import time
import logging
import optparse

from schemaless.column import Entity
from schemaless.log import ClassLogger

class IndexUpdater(object):
    """Class that implements a simple batch for updating indexes. This is meant
    to be a base class which is subclassed by the user, with the subclass doing
    whatever work is actually required to add the new index. Note that this
    batch is also appropriate for deleting data, or doing any other operation
    which requires iterating over a database table.

    At the very minimum you must implement your own process_row method.
    """

    log = ClassLogger()
    use_zlib = True

    def __init__(self):
        self.parser = optparse.OptionParser()
        self.parser.add_option('--start-added-id', dest='start_added_id', type='int', default=0, help='Which added_id to start at')
        self.parser.add_option('--batch-size', dest='batch_size', type='int', default=100, help='How many rows to process at a time')

    def initialize(self):
        self.rows_processed = 0
        self.start_run = time.time()
        self.last_id_processed = self.opts.start_added_id
        self.configure_logging()

    def configure_logging(self):
        logging.basicConfig(level=logging.DEBUG)

    def start(self):
        self.opts, self.args = self.parser.parse_args()
        self.initialize()
        self.run()

    def row_iterator(self):
        conn = self.datastore.connection
        conn.execute('SET AUTOCOMMIT=1')
        next_row = self.opts.start_added_id
        while True:
            rows = conn.query('SELECT * FROM entities WHERE added_id >= %s ORDER BY added_id ASC LIMIT %s', next_row, self.opts.batch_size)
            if rows:
                for row in rows:
                    yield row
                next_row = row['added_id'] + 1
            else:
                break

    def process_row(self, row, entity):
        """Every subclass must implement this method at a minimum. The function
        takes two arguments, the raw row returned by MySQL, and an entity object
        representing the BLOB data stored in the row.
        """
        raise NotImplementedError

    def run(self):
        rows_processed = 0
        self.log.info('starting run loop')
        try:
            for row in self.row_iterator():
                entity = Entity.from_row(row, use_zlib=self.use_zlib)
                self.process_row(row, entity)
                self.rows_processed += 1
                self.last_id_processed = row['added_id']
        except:
            self.log.exception('exception during run loop!')
        finally:
            elapsed_time = time.time() - self.start_run
            self.log.info('finished run loop, elapsed time = %1.2f seconds, processed %d rows, last added_id was %d' % (elapsed_time, self.rows_processed, self.last_id_processed))

def main(batch_cls):
    batch_instance = batch_cls()
    batch_instance.start()
