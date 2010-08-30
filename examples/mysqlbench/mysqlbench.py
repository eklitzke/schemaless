"""MySQL benchmark tool, for comparing different table schemas.

Some notes:
 * run like: python mysqlbench.py my_config_file.yaml
 * there's a few different options to change things up, invoke with -h to see
   them
 * you should run this with with a total of at least a quarter million rows or
   so (default is one million) to ensure that you see the slowdown from MySQL
   checking uniqueness contraints; i.e., make sure you see a slow down as
   iterations increase for the schemas that create tables with a unique uuid
   column
 * there's some overhead from having to generate uuids (which is done by reading
   16 bytes from /dev/urandom); IME the benchmark is still very much
   MySQL-bound, but if you're concerned you can pre-allocate an array of uuids
   in the bench() function, at the cost of using gobs of memory
 * if you're on a machine without dedicated hardware (e.g. a VPS), you'll
   probably see interesting things with transaction times fluctuating wildly as
   your instance gets access to hardware

An example yaml config file (ignore the lines starting with ---):

--- start yaml file ---
user: test
passwd: test
db: test
--- end yaml file ---

"""

import os
import csv
import math
import time
import yaml
import optparse
import MySQLdb

OVERALL_TIMES = []

def drop_test_entities(conn):
    c = conn.cursor()
    c.execute('SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s', 'test_entities')
    if c.fetchone():
        c.execute('DROP TABLE test_entities')

def increment_worker(c):
    os.urandom(16) # ensure that this has the same overhead as uuid_worker;
                   # comment out if you don't like this fairness
    c.execute('INSERT INTO test_entities (added_id) VALUES (NULL)')

def uuid_worker(c):
    c.execute('INSERT INTO test_entities (id) VALUES (%s)', os.urandom(16))

def bench(name, opts, conn, schema, worker=uuid_worker):
    drop_test_entities(conn)
    if opts.sleep:
        time.sleep(opts.sleep)
    print name
    print '=' * len(name)
    times = []
    c = conn.cursor()
    c.execute(schema)
    for x in xrange(opts.num_iterations):
        if not opts.autocommit:
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        ts = time.time()
        for y in xrange(opts.batch_size):
            worker(c)
        conn.commit()
        elapsed = time.time() - ts
        times.append(elapsed)
        print '% 4d    %f' % (x + 1, elapsed)
    OVERALL_TIMES.append((name, times))
    sorted_times = sorted(times)
    total = sum(times)
    avg = total / len(times)
    if len(times) % 2 == 0:
        idx = len(times) / 2
        med = (sorted_times[idx] + sorted_times[idx + 1]) / 2
    else:
        med = times[len(sorted_times) / 2]
    dev = math.sqrt(sum((x - avg)**2 for x in times) / len(times))
    print
    print 'average = %1.3f' % (avg,)
    print 'median  = %1.3f' % (med,)
    print 'std dev = %1.3f' % (dev,)
    print
    return times

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-a', '--autocommit', action='store_true', default=False, help='Enable auto-commit')
    parser.add_option('-n', '--num-iterations', type='int', default=100, help='How many iterations to run')
    parser.add_option('-b', '--batch-size', type='int', default=10000, help='How many rows to insert per txn')
    parser.add_option('-c', '--csv', default=None, help='Store benchmark output in the specified CSV file')
    parser.add_option('-s', '--sleep', type='int', default=10, help='How long to sleep between tests')
    opts, args = parser.parse_args()
    start = time.time()
    if len(args) != 1:
        parser.error('must pass exactly one argument, the path to the mysql config file')
    cfg = yaml.load(open(args[0]).read())
    conn = MySQLdb.connect(**cfg)

    opsys, host, kernel, dt, arch = os.uname()
    print '%s %s' % (opsys, kernel)
    print 'MySQL ' + conn.get_server_info()
    print
    print 'running %d iterations of %d inserts per txn (%d rows total)' % (opts.num_iterations, opts.batch_size, opts.num_iterations * opts.batch_size)
    if opts.autocommit:
        conn.cursor().execute('SET autocommit = 1')
        print 'autocommit is ON'
    else:
        conn.cursor().execute('SET autocommit = 0')
        print 'autocommit is OFF'
    print

    bench('just auto_increment', opts, conn, """
CREATE TABLE test_entities (
    added_id INTEGER NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (added_id)
) ENGINE=InnoDB
""", increment_worker)

    bench('auto_increment, no key', opts, conn, """
CREATE TABLE test_entities (
    added_id INTEGER NOT NULL AUTO_INCREMENT,
    id BINARY(16) NOT NULL,
    PRIMARY KEY (added_id)
) ENGINE=InnoDB
""")

    bench('auto_increment, key', opts, conn, """
CREATE TABLE test_entities (
    added_id INTEGER NOT NULL AUTO_INCREMENT,
    id BINARY(16) NOT NULL,
    PRIMARY KEY (added_id),
    KEY (id)
) ENGINE=InnoDB
""")

    bench('auto_increment, unique key', opts, conn, """
CREATE TABLE test_entities (
    added_id INTEGER NOT NULL AUTO_INCREMENT,
    id BINARY(16) NOT NULL,
    PRIMARY KEY (added_id),
    UNIQUE KEY (id)
) ENGINE=InnoDB
""")

    bench('w/o auto-increment, key', opts, conn, """
CREATE TABLE test_entities (
    id BINARY(16) NOT NULL,
    KEY (id)
) ENGINE=InnoDB
""")

    bench('w/o auto-increment, unique key', opts, conn, """
CREATE TABLE test_entities (
    id BINARY(16) NOT NULL,
    UNIQUE KEY (id)
) ENGINE=InnoDB
""")

    bench('w/o auto-increment, primary key', opts, conn, """
CREATE TABLE test_entities (
    id BINARY(16) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB""")

    drop_test_entities(conn)
    if opts.csv:
        writer = csv.writer(open(opts.csv, 'w'))
        names = ['cumulative'] + [name for name, _ in OVERALL_TIMES]
        writer.writerow(names)
        writer.writerow([0 for x in xrange(len(OVERALL_TIMES) + 1)])
        for x in xrange(opts.num_iterations):
            tot = (x + 1) * opts.batch_size
            writer.writerow([tot] + [t[x] for _, t in OVERALL_TIMES])
        print 'csv output is in %r' % (opts.csv,)
    print 'total time was %1.3f seconds' % (time.time() - start)
