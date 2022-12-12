from mock import Mock, patch
from patroni.postgresql.citus import CitusDistNodeHandler

from . import BaseTestPostgresql, MockCursor, psycopg_connect, SleepException
from .test_ha import get_cluster_initialized_with_leader


@patch('patroni.postgresql.citus.Thread', Mock())
@patch('patroni.psycopg.connect', psycopg_connect)
class TestCitus(BaseTestPostgresql):

    def setUp(self):
        super(TestCitus, self).setUp()
        self.c = self.p.citus_handler
        self.c.set_conn_kwargs({'host': 'localhost', 'dbname': 'postgres'})
        self.cluster = get_cluster_initialized_with_leader()
        self.cluster.workers['1'] = self.cluster

    @patch('time.time', Mock(side_effect=[100, 130, 160, 190, 220, 250, 280]))
    @patch('patroni.postgresql.citus.logger.exception', Mock(side_effect=SleepException))
    @patch('patroni.postgresql.citus.logger.warning')
    @patch('patroni.postgresql.citus.Event.wait', Mock())
    @patch('patroni.postgresql.citus.Condition.wait', Mock(side_effect=[Mock(), Mock(), Mock(), SleepException]))
    @patch.object(CitusDistNodeHandler, 'is_alive', Mock(return_value=True))
    def test_run(self, mock_logger_warning):
        # `before_demote` or `before_promote` REST API calls starting a
        # transaction. We want to make sure that it finishes during
        # certain timeout. In case if it is not, we want to roll it back
        # in order to not block other workers that want to update
        # `pg_dist_node`.

        self.c.handle_event(self.cluster, {'type': 'before_demote', 'group': 1, 'leader': 'leader', 'timeout': 30})
        self.c.add_task('after_promote', 2, 'postgres://host3:5432/postgres')
        self.assertRaises(SleepException, self.c.run)
        mock_logger_warning.assert_called_once()
        self.assertTrue(mock_logger_warning.call_args[0][0].startswith('Rolling back transaction'))
        self.assertTrue(repr(mock_logger_warning.call_args[0][1]).startswith('PgDistNode'))

    @patch.object(CitusDistNodeHandler, 'is_alive', Mock(return_value=False))
    @patch.object(CitusDistNodeHandler, 'start', Mock())
    def test_sync_pg_dist_node(self):
        self.c.sync_pg_dist_node(self.cluster)

    def test_handle_event(self):
        self.c.handle_event(self.cluster, {})
        with patch.object(CitusDistNodeHandler, 'is_alive', Mock(return_value=True)):
            self.c.handle_event(self.cluster, {'type': 'after_promote', 'group': 2, 'leader': 'leader', 'timeout': 30})

    def test_add_task(self):
        with patch('patroni.postgresql.citus.logger.error') as mock_logger,\
                patch('patroni.postgresql.citus.urlparse', Mock(side_effect=Exception)):
            self.c.add_task('', 1, None)
            mock_logger.assert_called_once()

        with patch('patroni.postgresql.citus.logger.debug') as mock_logger:
            self.c.add_task('before_demote', 1, 'postgres://host:5432/postgres', 30)
            mock_logger.assert_called_once()
            self.assertTrue(mock_logger.call_args[0][0].startswith('Adding the new task:'))

        with patch('patroni.postgresql.citus.logger.debug') as mock_logger:
            self.c.add_task('before_promote', 1, 'postgres://host:5432/postgres', 30)
            mock_logger.assert_called_once()
            self.assertTrue(mock_logger.call_args[0][0].startswith('Overriding existing task:'))

        # add_task called from sync_pg_dist_node should not override already scheduled or in flight task
        self.assertIsNotNone(self.c.add_task('after_promote', 1, 'postgres://host:5432/postgres', 30))
        self.assertIsNone(self.c.add_task('after_promote', 1, 'postgres://host:5432/postgres'))
        self.c._in_flight = self.c._tasks.pop()
        self.assertIsNone(self.c.add_task('after_promote', 1, 'postgres://host:5432/postgres'))

        # If there is no transaction in progress and cached pg_dist_node matching desired state task should not be added
        self.c._schedule_load_pg_dist_node = False
        self.c._pg_dist_node[self.c._in_flight.group] = self.c._in_flight
        self.c._in_flight = None
        self.assertIsNone(self.c.add_task('after_promote', 1, 'postgres://host:5432/postgres'))

    def test_pick_task(self):
        self.c.add_task('after_promote', 1, 'postgres://host2:5432/postgres')
        with patch.object(CitusDistNodeHandler, 'process_task') as mock_process_task:
            self.c.process_tasks()
            # process_task() shouln't be called because pick_task double checks with _pg_dist_node
            mock_process_task.assert_not_called()

    def test_process_task(self):
        self.c.add_task('after_promote', 0, 'postgres://host2:5432/postgres')
        task = self.c.add_task('before_promote', 1, 'postgres://host4:5432/postgres', 30)
        self.c.process_tasks()
        self.assertTrue(task._event.is_set())

        # the after_promote should result only in COMMIT
        task = self.c.add_task('after_promote', 1, 'postgres://host4:5432/postgres', 30)
        with patch.object(CitusDistNodeHandler, '_query') as mock_query:
            self.c.process_tasks()
            mock_query.assert_called_once()
            self.assertEqual(mock_query.call_args[0][0], 'COMMIT')

    def test_process_tasks(self):
        self.c.add_task('after_promote', 0, 'postgres://host2:5432/postgres')
        self.c.process_tasks()

        self.c.add_task('after_promote', 0, 'postgres://host3:5432/postgres')
        with patch('patroni.postgresql.citus.logger.error') as mock_logger,\
                patch.object(CitusDistNodeHandler, 'query', Mock(side_effect=Exception)):
            self.c.process_tasks()
            mock_logger.assert_called_once()
            self.assertTrue(mock_logger.call_args[0][0].startswith('Exception when working with pg_dist_node: '))

    def test_on_demote(self):
        self.c.on_demote()

    @patch('patroni.postgresql.citus.logger.error')
    @patch.object(MockCursor, 'execute', Mock(side_effect=Exception))
    def test_load_pg_dist_node(self, mock_logger):
        # load_pg_dist_node() triggers, query fails and exception is property handled
        self.c.process_tasks()
        self.assertTrue(self.c._schedule_load_pg_dist_node)
        mock_logger.assert_called_once()
        self.assertTrue(mock_logger.call_args[0][0].startswith('Exception when executing query'))
        self.assertTrue(mock_logger.call_args[0][1].startswith('SELECT nodeid, groupid, '))
