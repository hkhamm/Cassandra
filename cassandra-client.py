from uuid import UUID
from cassandra.cluster import Cluster
import logging

log = logging.getLogger()
log.setLevel('INFO')


class CassandraClient:
    """
    A command line client for connecting to and modifying a Cassandra database.
    """

    def __init__(self):
        """
        Constructor.
        """
        self.session = None

    def connect(self, nodes):
        """
        Creates a connection with the database.
        :param nodes: is an array of IP address or URLs corresponding to Cassandra nodes
        """
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        log.info('Connected to cluster: ' + metadata.cluster_name)
        for host in metadata.all_hosts():
            log.info('Datacenter: %s; Host: %s; Rack: %s',
                     host.datacenter, host.address, host.rack)

    def close(self):
        """
        Closes the connection to the database.
        """
        self.session.cluster.shutdown()
        log.info('Connection closed.')

    def create_keyspace(self, keyspace, strategy, replication_factor):
        """
        Creates a new keyspace in the database.
        :param keyspace: is the keyspace to create; must be a string.
        :param strategy: is the strategy class, i.e. 'SimpleStrategy'; must be string in quotes.
        :param replication_factor: is the number of nodes in the replication cluster; must be an integer.
        """
        self.session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS {0:s} WITH replication = {{'class':{1:s}, 'replication_factor':{2:s}}};
            """.format(keyspace, strategy, replication_factor)
        )
        log.info('Keyspace %s created.' % keyspace)

    def create_table(self, table, columns):
        """
        Creates a new table in the database.
        :param table: is the new table's name; must be a string.
        :param columns: is a comma separated string of column names.
        """
        self.session.execute('CREATE TABLE IF NOT EXISTS %s (%s);' % (table, columns))
        log.info('Table %s created.' % table)

    def load_data(self, table, columns, values):
        """
        Loads data into a table.
        :param table: is the name of the table to load data into.
        :param columns: is a comma separated string of column names.
        :param values: is a string of values corresponding to the columns.
        """
        self.session.execute('INSERT INTO %s (%s) VALUES (%s);' % (table, columns, values))
        log.info('Data loaded into table %s.' % table)

    def query_table(self, table, *args):
        """
        Query a table for information.
        :param table: is the name of the table to query.
        :param args: is a key value pair with syntax: 'key = value'.
        """
        query = 'SELECT * FROM %s' % table
        if len(args) > 0:
            query += ' WHERE %s' % args[0]  # WHERE key = value
        query += ';'
        results = self.session.execute_async(query)
        results.add_callbacks(self.print_results, self.print_errors)
        log.info('Table %s queried.' % table)

    def update_table(self, table, set_value, condition):
        """
        Updates a table with the provided values, where a condition is met.
        :param table: is the name of the table to update.
        :param set_value: is the value to set.
        :param condition: is the table condition that must be met.
        """
        self.session.execute('UPDATE %s SET %s WHERE %s;' % (table, set_value, condition))
        log.info('Table %s updated.' % table)

    def drop_keyspace(self, keyspace):
        """
        Drops a keyspace from the database.
        :param keyspace: is the keyspace to drop.
        """
        self.session.execute('DROP KEYSPACE IF EXISTS %s;' % keyspace)
        log.info('Keyspace %s dropped.' % keyspace)

    def drop_table(self, table):
        """
        Drops a table from the database.
        :param table: is the name of the table to drop.
        """
        self.session.execute('DROP TABLE IF EXISTS %s;' % table)
        log.info('Table %s dropped.' % table)

    def print_errors(self, errors):
        """
        Prints errors to log.error
        :param errors: are the errors to print.
        """
        log.error(errors)

    def print_results(self, results):
        """
        Prints results to the command line.
        :param results: are the results to print.
        """
        print("%-30s\t%-20s\t%-20s\n%s" %
              ("title", "album", "artist",
               "-------------------------------+-----------------------+--------------------"))
        for row in results:
            print("%-30s\t%-20s\t%-20s" % (row.title, row.album, row.artist))


# A test driver.
logging.basicConfig()
client = CassandraClient()
client.connect(['ec2-54-200-175-168.us-west-2.compute.amazonaws.com'])
client.create_keyspace('simplex', "'SimpleStrategy'", '3')
client.create_table('simplex.songs',
                    """
                    id uuid PRIMARY KEY,
                    title text,
                    album text,
                    artist text,
                    tags set<text>
                    """)
client.load_data('simplex.songs',
                 'id, title, album, artist, tags',
                 """
                 756716f7-2e54-4715-9f00-91dcbea6cf50,
                 'La Petite Tonkinoise',
                 'Bye Bye Blackbird',
                 'Joséphine Baker',
                 {'jazz', '2013'}
                 """)
client.query_table('simplex.songs')
# client.query_table('simplex.songs', 'id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d')
client.drop_keyspace('simplex')
client.close()


# class SimpleClient(CassandraClient):
#     def create_schema(self):
#         self.session.execute(
#             """CREATE KEYSPACE simplex WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};""")
#         self.session.execute("""
#             CREATE TABLE simplex.songs (
#                 id uuid PRIMARY KEY,
#                 title text,
#                 album text,
#                 artist text,
#                 tags set<text>,
#                 data blob
#             );
#         """)
#         self.session.execute("""
#             CREATE TABLE simplex.playlists (
#                 id uuid,
#                 title text,
#                 album text,
#                 artist text,
#                 song_id uuid,
#                 PRIMARY KEY (id, title, album, artist)
#             );
#         """)
#         log.info('Simplex keyspace and schema created.')
#
#     def load_data(self):
#         self.session.execute("""
#             INSERT INTO simplex.songs (id, title, album, artist, tags)
#             VALUES (
#                 756716f7-2e54-4715-9f00-91dcbea6cf50,
#                 'La Petite Tonkinoise',
#                 'Bye Bye Blackbird',
#                 'Joséphine Baker',
#                 {'jazz', '2013'}
#             );
#         """)
#         self.session.execute("""
#             INSERT INTO simplex.playlists (id, song_id, title, album, artist)
#             VALUES (
#                 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,
#                 756716f7-2e54-4715-9f00-91dcbea6cf50,
#                 'La Petite Tonkinoise',
#                 'Bye Bye Blackbird',
#                 'Joséphine Baker'
#             );
#         """)
#         log.info('Data loaded.')
#
#     def query_schema(self):
#         results = self.session.execute("""
#             SELECT * FROM simplex.playlists
#             WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;
#             """)
#         self.print_results(results)
#         log.info('Schema queried.')
#
#
# class BoundStatementsClient(SimpleClient):
#     def prepare_statements(self):
#         self.insert_song_prepared_statement = self.session.prepare("""
#                  INSERT INTO simplex.songs
#                  (id, title, album, artist, tags)
#                  VALUES (?, ?, ?, ?, ?);
#             """)
#         self.insert_playlist_prepared_statement = self.session.prepare("""
#                  INSERT INTO simplex.playlists
#                  (id, song_id, title, album, artist)
#                  VALUES (?, ?, ?, ?, ?);
#             """)
#         log.info('Statements prepared.')
#
#     def load_data(self):
#         tags = {'jazz', '2013'}
#         self.session.execute(self.insert_song_prepared_statement,
#                              [UUID("756716f7-2e54-4715-9f00-91dcbea6cf50"),
#                               "La Petite Tonkinoise",
#                               "Bye Bye Blackbird",
#                               "Joséphine Baker",
#                               tags])
#         self.session.execute(self.insert_playlist_prepared_statement,
#                              [UUID("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
#                               UUID("756716f7-2e54-4715-9f00-91dcbea6cf50"),
#                               "La Petite Tonkinoise",
#                               "Bye Bye Blackbird",
#                               "Joséphine Baker"])
#
#
# class AsynchronousExample(SimpleClient):
#     def query_schema(self):
#         future_results = self.session.execute_async("SELECT * FROM simplex.songs;")
#         future_results.add_callbacks(self.print_results, self.print_errors)
#
#
# def main():
#
#
#     if __name__ == "__main__":
#         main()