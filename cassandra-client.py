from uuid import UUID
from cassandra.cluster import Cluster
import logging
import time

log = logging.getLogger()
log.setLevel('INFO')


class CassandraClient:
    session = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        log.info('Connected to cluster: ' + metadata.cluster_name)
        for host in metadata.all_hosts():
            log.info('Datacenter: %s; Host: %s; Rack: %s',
                     host.datacenter, host.address, host.rack)

    def close(self):
        self.session.cluster.shutdown()
        log.info('Connection closed.')

    def create_keyspace(self, keyspace, strategy, replication_factor):
        self.session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS {0:s} WITH replication = {{'class':{1:s}, 'replication_factor':{2:s}}};
            """.format(keyspace, strategy, replication_factor)
        )
        log.info('Keyspace %s created.' % keyspace)

    def create_table(self, table, columns):
        self.session.execute('CREATE TABLE IF NOT EXISTS %s (%s);' % (table, columns))
        log.info('Table %s created.' % table)

    def load_data(self, table, columns, values):
        self.session.execute('INSERT INTO %s (%s) VALUES (%s);' % (table, columns, values))
        log.info('Data loaded into table %s.' % table)

    def query_table(self, table, *args):
        query = 'SELECT * FROM %s' % table
        if len(args) > 0:
            query += ' WHERE %s' % args[0]  # WHERE key = value
        query += ';'
        results = self.session.execute_async(query)
        results.add_callbacks(self.print_results, self.print_errors)
        log.info('Table %s queried.' % table)

    def update_table(self, table, set_value, where_value):
        self.session.execute('UPDATE %s SET %s WHERE %s;' % (table, set_value, where_value))
        log.info('Table %s updated.' % table)

    def drop_keyspace(self, keyspace):
        self.session.execute('DROP KEYSPACE IF EXISTS %s;' % keyspace)
        log.info('Keyspace %s dropped.' % keyspace)

    def drop_table(self, table):
        self.session.execute('DROP TABLE IF EXISTS %s;' % table)
        log.info('Table %s dropped.' % table)

    def print_errors(self, errors):
        log.error(errors)

    def print_results(self, results):
        print("%-30s\t%-20s\t%-20s\n%s" %
              ("title", "album", "artist",
               "-------------------------------+-----------------------+--------------------"))
        for row in results:
            print("%-30s\t%-20s\t%-20s" % (row.title, row.album, row.artist))


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