import logging
import time

from six.moves.urllib_parse import urlparse
from threading import Condition, Event, Thread

from .connection import Connection

logger = logging.getLogger(__name__)


class PgDistNode(object):
    """Represents a single row in the `pg_dist_node` table"""

    def __init__(self, group, host, port, event, nodeid=None, timeout=None):
        self.group = group
        self.host = host + ('-demoted' if event == 'before_demote' else '')
        self.port = port
        # Event that is trying to change or changed the given row.
        # Possible values: before_demote, before_promote, after_promote.
        self.event = event
        self.nodeid = nodeid

        # If transaction was started, we need to COMMIT/ROLLBACK before the deadline
        self.timeout = timeout
        self.deadline = 0

        # All changes in the pg_dist_node are serialized on the Patroni
        # side by performing them from a thread. The thread, that is
        # requested a change, sometimes needs to wait for a result.
        # For example, we want to pause client connections before demoting
        # the worker, and once it is done notify the calling thread.
        self._event = Event()

    def wait(self):
        self._event.wait()

    def wakeup(self):
        self._event.set()

    def __eq__(self, other):
        return isinstance(other, PgDistNode) and self.event == other.event\
            and self.host == other.host and self.port == other.port

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return ('PgDistNode(nodeid={0},group={1},host={2},port={3},event={4})'
                .format(self.nodeid, self.group, self.host, self.port, self.event))

    def __repr__(self):
        return str(self)


class CitusDistNodeHandler(Thread):

    def __init__(self, postgresql):
        super(CitusDistNodeHandler, self).__init__()
        self.daemon = True
        self._postgresql = postgresql
        self._connection = Connection()
        self._pg_dist_node = {}  # Cache of pg_dist_node: {groupid: PgDistNode()}
        self._tasks = []  # Requests to change pg_dist_node, every task is a `PgDistNode`
        self._condition = Condition()  # protects _pg_dist_node, _tasks, and _schedule_load_pg_dist_node
        self._in_flight = None  # Reference to the `PgDistNode` if there is a transaction in progress changing it
        self.schedule_cache_rebuild()

    def set_conn_kwargs(self, kwargs):
        self._connection.set_conn_kwargs(kwargs)

    def schedule_cache_rebuild(self):
        with self._condition:
            self._schedule_load_pg_dist_node = True

    def on_demote(self):
        with self._condition:
            self._pg_dist_node.clear()
            self._tasks.clear()
            self._in_flight = None

    def _query(self, sql, *params):
        cursor = None
        try:
            logger.debug('query(%s, %s)', sql, params)
            cursor = self._connection.cursor()
            cursor.execute(sql, params or None)
            return cursor
        except Exception as e:
            logger.error('Exception when executing query "%s", (%s): %r', sql, params, e)
            self._connection.close()
            self._in_flight = None
            self.schedule_cache_rebuild()
            raise e

    def query(self, sql, *args):
        if self._in_flight:  # Don't retry a query if something fails while in transaction
            return self._query(sql, *args)
        return self._postgresql.retry(self._query, sql, *args)

    def load_pg_dist_node(self):
        """Read from the `pg_dist_node` table and put it into the local cache"""

        with self._condition:
            if not self._schedule_load_pg_dist_node:
                return True
            self._schedule_load_pg_dist_node = False

        try:
            cursor = self.query("SELECT nodeid, groupid, nodename, nodeport, noderole"
                                " FROM pg_catalog.pg_dist_node WHERE noderole = 'primary'")
        except Exception:
            return self.schedule_cache_rebuild()

        with self._condition:
            self._pg_dist_node = {r[1]: PgDistNode(r[1], r[2], r[3], 'after_promote', r[0]) for r in cursor}
        return True

    def sync_pg_dist_node(self, cluster):
        """Maintain the `pg_dist_node` from the coordinator leader every heartbeat loop.

        We can't always rely on REST API calls from worker nodes in order
        to maintain `pg_dist_node`, therefore at least once per heartbeat
        loop we make sure that workes registered in `self._pg_dist_node`
        cache are matching the cluster view from DCS by creating tasks
        the same way as it is done from the REST API."""

        try:
            with self._condition:
                if not self.is_alive():
                    self.start()

            self.add_task('after_promote', 0, self._postgresql.connection_string)

            for group, worker in cluster.workers.items():
                group = int(group)
                leader = worker.leader
                if leader and leader.conn_url and leader.data.get('role') == 'master':
                    self.add_task('after_promote', group, leader.conn_url)
        except Exception:
            logger.exception('Exception when scheduling pg_dist_node sync')

    def find_task_by_group(self, group):
        for i, task in enumerate(self._tasks):
            if task.group == group:
                return i

    def pick_task(self):
        """Returns the tuple(i, task), where `i` - is the task index in the self._tasks list

        Tasks are picked by following priorities:
        1. If there is already a transaction in progress, pick a task
           that that will change already affected worker primary.
        2. If the coordinator address should be changed - pick a task
           with group=0 (coordinators are always in group 0).
        3. Pick a task that is the oldest (first from the self._tasks)"""

        with self._condition:
            if self._in_flight:
                i = self.find_task_by_group(self._in_flight.group)
            else:
                while True:
                    i = self.find_task_by_group(0)  # set_coodinator
                    if i is None and self._tasks:
                        i = 0
                    if i is None:
                        break
                    task = self._tasks[i]
                    if task != self._pg_dist_node.get(task.group):
                        break
                    self._tasks.pop(i)
            task = self._tasks[i] if i is not None else None

            # When tasks are added it could happen that self._pg_dist_node
            # wasn't ready (self._schedule_load_pg_dist_node is False)
            # and hence the nodeid wasn't filled.
            if task and task.group in self._pg_dist_node:
                task.nodeid = self._pg_dist_node[task.group].nodeid
            return i, task

    def update_node(self, task):
        if task.group == 0:
            return self.query("SELECT pg_catalog.citus_set_coordinator_host(%s, %s, 'primary', 'default')",
                              task.host, task.port)

        if task.nodeid is None and task.event != 'before_demote':
            task.nodeid = self.query("SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default')",
                                     task.host, task.port, task.group).fetchone()[0]
        elif task.nodeid is not None:
            # A weird way of pausing client connections by adding the `-demoted` suffix to the hostname
            self.query('SELECT pg_catalog.citus_update_node(%s, %s, %s, true, 100)',
                       task.nodeid, task.host, task.port)

    def process_task(self, task):
        """Updates a single row in `pg_dist_node` table, optionally in a transaction.

        The transaction is started if we do a demote of the worker node
        or before promoting the other worker if there is not transaction
        in progress. And, the transaction it is committed when the
        switchover/failover completed.

        This method returns `True` if node was updated (optionally,
        transaction was committed) as an indicator that
        the `self._pg_dist_node` cache should be updated.

        The maximum lifetime of the transaction in progress
        is controlled outside of this method."""

        if task.event == 'after_promote':
            if not self._in_flight or self._in_flight.host != task.host or self._in_flight.port != task.port:
                self.update_node(task)
            if self._in_flight:
                self._query('COMMIT')
                self._in_flight = None
            return True
        else:  # before_demote, before_promote
            if task.timeout:
                task.deadline = time.time() + task.timeout
            if not self._in_flight:
                self.query('BEGIN')
            self.update_node(task)
            self._in_flight = task
        return False

    def process_tasks(self):
        while True:
            if not self._in_flight and not self.load_pg_dist_node():
                break

            i, task = self.pick_task()
            if not task:
                break
            try:
                update_cache = self.process_task(task)
            except Exception as e:
                logger.error('Exception when working with pg_dist_node: %r', e)
            else:
                with self._condition:
                    if self._tasks:
                        if update_cache:
                            self._pg_dist_node[task.group] = task
                        if id(self._tasks[i]) == id(task):
                            self._tasks.pop(i)
            task.wakeup()

    def run(self):
        old_in_flight = None
        while True:
            try:
                with self._condition:
                    if old_in_flight and self._in_flight and id(old_in_flight) == id(self._in_flight):
                        logger.warning('Rolling back transaction. Last known status: %s', self._in_flight)
                        self.query('ROLLBACK')
                        old_in_flight = self._in_flight = None

                    timeout = self._in_flight.deadline - time.time() if self._in_flight else None
                    old_in_flight = self._in_flight
                    if timeout is not None and timeout > 0:
                        self._condition.wait(timeout)
                self.process_tasks()
            except Exception:
                logger.exception('run')

    def _add_task(self, task):
        with self._condition:
            # Override if there is already a task for the same group
            i = self.find_task_by_group(task.group)
            if i is not None:
                if task != self._tasks[i]:
                    logger.debug('Overriding existing task: %s != %s', self._tasks[i], task)
                    self._tasks[i] = task
                    self._condition.notify()
                    return True
            # Add the task to the list if Worker node state is different from the cached `pg_dist_node`
            elif self._schedule_load_pg_dist_node or task != self._pg_dist_node.get(task.group)\
                    or self._in_flight and task.group == self._in_flight.group:
                logger.debug('Adding the new task: %s', task)
                self._tasks.append(task)
                self._condition.notify()
                return True
        return False

    def add_task(self, event, group, conn_url, timeout=None):
        r = urlparse(conn_url)
        host = r.hostname
        port = r.port or 5432
        task = PgDistNode(group, host, port, event, timeout=timeout)
        return task if self._add_task(task) else None

    def handle_event(self, cluster, event):
        if not self.is_alive():
            return

        cluster = cluster.workers[str(event['group'])]
        if not (cluster and cluster.leader and cluster.leader.name == event['leader'] and cluster.leader.conn_url):
            return

        task = self.add_task(event['type'], event['group'], cluster.leader.conn_url, event['timeout'])
        if task and event['type'] == 'before_demote':
            task.wait()
