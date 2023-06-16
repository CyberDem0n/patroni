import logging
import re
import time

from threading import Condition, Event, Thread
from urllib.parse import urlparse
from typing import Any, Collection, Dict, Iterator, List, Optional, Union, Set, Tuple, TYPE_CHECKING

from .connection import Connection
from ..dcs import CITUS_COORDINATOR_GROUP_ID, Cluster
from ..psycopg import connect, quote_ident

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Cursor
    from psycopg2 import cursor
    from . import Postgresql

CITUS_SLOT_NAME_RE = re.compile(r'^citus_shard_(move|split)_slot(_[1-9][0-9]*){2,3}$')
logger = logging.getLogger(__name__)


class PgDistNode:

    def __init__(self, group: int, host: str, port: int, role: str, nodeid: Optional[int] = None) -> None:
        self.group = group
        self.host = host
        self.port = port
        self.role = role
        self.nodeid = nodeid

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PgDistNode) and self.host == other.host and self.port == other.port

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __str__(self) -> str:
        return ('PgDistNode(nodeid={0},group={1},host={2},port={3},role={4})'
                .format(self.nodeid, self.group, self.host, self.port, self.role))

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self):
        return hash((self.host, self.port))

    def is_primary(self) -> bool:
        return self.role in ('primary', 'demoted')


class PgDistGroup(Set[PgDistNode]):

    def __init__(self, group: int, nodes: Optional[Collection[PgDistNode]] = None) -> None:
        self.failover = False
        self.group = group

        if nodes:
            self.update(nodes)

    @staticmethod
    def _node_hash(node: PgDistNode, include_nodeid: bool = False) -> Tuple[str, int, str, Optional[int]]:
        return node.host, node.port, node.role, (node.nodeid if include_nodeid else None)

    def equals(self, other: 'PgDistGroup', check_nodeid: bool = False) -> bool:
        return set(self._node_hash(v, check_nodeid) for v in self)\
            == set(self._node_hash(v, check_nodeid) for v in other)

    def primary(self) -> Optional[PgDistNode]:
        return next(iter(v for v in self if v.is_primary()), None)

    def get(self, value: PgDistNode) -> Optional[PgDistNode]:
        return next(iter(v for v in self if v == value), None)

    def transition(self, old: 'PgDistGroup') -> Iterator[PgDistNode]:
        self.failover = old.failover

        new_primary = self.primary()
        assert new_primary is not None
        old_primary = old.primary()

        gone_nodes = old - self - set([old_primary])
        added_nodes = self - old - set([new_primary])

        if not old_primary:
            yield new_primary
        elif old_primary == new_primary:
            new_primary.nodeid = old_primary.nodeid
            # Controlled switchover with pausing client connections.
            # Achived by updating the primary row and putting hostname = '${host}-demoted' in a transaction.
            if old_primary.role != new_primary.role:
                self.failover = True
                yield new_primary
        elif old_primary != new_primary:
            self.failover = True

            new_primary_old_node = old.get(new_primary)
            old_primary_new_node = self.get(old_primary)

            # The new primary was registered as a secondary before failover
            if new_primary_old_node:
                new_node = None
                # Old primary is gone (failover) and some new secondaries were added.
                # We can use the row of promoted secondary to add the new secondary.
                if not old_primary_new_node and added_nodes:
                    new_node = added_nodes.pop()
                    new_node.nodeid = new_primary_old_node.nodeid
                    yield new_node
                # In opposite case we need to change the primary record to '${host}-demoted:${port}'
                # before we can put its host:port to the row of promoted secondary.
                elif old_primary.role == 'primary':
                    old_primary.role = 'demoted'
                    yield old_primary

                # The old primary is gone and the promoted secondary row wasn't yet used.
                if not old_primary_new_node and not new_node:
                    # We have to "add" the gone primary to the row of promoted secondary because
                    # nodes could not be removed while the metadata isn't synced.
                    old_primary_new_node = PgDistNode(old_primary.group, old_primary.host,
                                                      old_primary.port, new_primary_old_node.role)
                    self.add(old_primary_new_node)

                # put the old primary instead of promoted secondary
                if old_primary_new_node:
                    old_primary_new_node.nodeid = new_primary_old_node.nodeid
                    yield old_primary_new_node

            # update the primary record with the new information
            new_primary.nodeid = old_primary.nodeid
            yield new_primary

            # The new primary was never registered as a standby and there are secondaries that gone away.
            # Since nodes can't be removed while metadate isn't synced we have to temporary "add" the old primary back.
            if not new_primary_old_node and gone_nodes:
                # We were in the middle of controlled switchover while the primary disappeared.
                # If there are any gone nodes that can't be reused for new secondaries we will
                # use one of them to temporary "add" the old primary back as a secondary.
                if not old_primary_new_node and old_primary.role == 'demoted' and len(gone_nodes) > len(added_nodes):
                    old_primary_new_node = PgDistNode(old_primary.group, old_primary.host,
                                                      old_primary.port, 'secondary')
                    self.add(old_primary_new_node)

                # Use one of the gone secondaries to put host:port of the old primary there.
                if old_primary_new_node:
                    old_primary_new_node.nodeid = gone_nodes.pop().nodeid
                    yield old_primary_new_node

        # Fill nodeid for standbys in the new topology from the old ones
        old_replicas = {v: v for v in old if not v.is_primary()}
        for n in self:
            if not n.is_primary() and not n.nodeid and n in old_replicas:
                n.nodeid = old_replicas[n].nodeid

        # Reuse nodeid's of gone standbys to "add" new standbys
        while gone_nodes and added_nodes:
            a = added_nodes.pop()
            a.nodeid = gone_nodes.pop().nodeid
            yield a

        # Remove remaining nodes that are gone, but only in case if metadata is in sync
        for g in gone_nodes:
            if self.failover:  # Otherwise add these nodes to the new topology
                self.add(g)
            else:
                yield PgDistNode(g.group, g.host, g.port, '')

        # Add new nodes to the metadata, but only in case if metadata is in sync
        for a in added_nodes:
            if self.failover:
                self.discard(a)  # Otherwise remove them from the new topology
            else:
                yield a


class PgDistTask(PgDistGroup):

    def __init__(self, group: int, nodes: Optional[Collection[PgDistNode]], event: str,
                 timeout: Optional[float] = None, cooldown: Optional[float] = None) -> None:
        super(PgDistTask, self).__init__(group, nodes)

        # Event that is trying to change or changed the given row.
        # Possible values: before_demote, before_promote, after_promote.
        self.event = event

        # If transaction was started, we need to COMMIT/ROLLBACK before the deadline
        self.timeout = timeout
        self.cooldown = cooldown or 10000  # 10s by default
        self.deadline: float = 0

        # All changes in the pg_dist_node are serialized on the Patroni
        # side by performing them from a thread. The thread, that is
        # requested a change, sometimes needs to wait for a result.
        # For example, we want to pause client connections before demoting
        # the worker, and once it is done notify the calling thread.
        self._event = Event()

    def wait(self) -> None:
        self._event.wait()

    def wakeup(self) -> None:
        self._event.set()

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PgDistTask) and self.event == other.event\
            and super(PgDistTask, self).equals(other)

    def __ne__(self, other: Any) -> bool:
        return not self == other


class CitusHandler(Thread):

    def __init__(self, postgresql: 'Postgresql', config: Optional[Dict[str, Union[str, int]]]) -> None:
        super(CitusHandler, self).__init__()
        self.daemon = True
        self._postgresql = postgresql
        self._config = config
        self._connection = Connection()
        self._pg_dist_group: Dict[int, PgDistTask] = {}  # Cache of pg_dist_node: {groupid: PgDistTask()}
        self._tasks: List[PgDistTask] = []  # Requests to change pg_dist_group, every task is a `PgDistTask`
        self._in_flight: Optional[PgDistTask] = None  # Reference to the `PgDistTask` being changed in a transaction
        self._schedule_load_pg_dist_group = True  # Flag that "pg_dist_group" should be queried from the database
        self._condition = Condition()  # protects _pg_dist_group, _tasks, _in_flight, and _schedule_load_pg_dist_group
        self.schedule_cache_rebuild()

    def is_enabled(self) -> bool:
        return isinstance(self._config, dict)

    def group(self) -> Optional[int]:
        return int(self._config['group']) if isinstance(self._config, dict) else None

    def is_coordinator(self) -> bool:
        return self.is_enabled() and self.group() == CITUS_COORDINATOR_GROUP_ID

    def is_worker(self) -> bool:
        return self.is_enabled() and not self.is_coordinator()

    def set_conn_kwargs(self, kwargs: Dict[str, Any]) -> None:
        if isinstance(self._config, dict):  # self.is_enabled():
            kwargs.update({'dbname': self._config['database'],
                           'options': '-c statement_timeout=0 -c idle_in_transaction_session_timeout=0'})
            self._connection.set_conn_kwargs(kwargs)

    def schedule_cache_rebuild(self) -> None:
        with self._condition:
            self._schedule_load_pg_dist_group = True

    def on_demote(self) -> None:
        with self._condition:
            self._pg_dist_group.clear()
            self._tasks[:] = []
            self._in_flight = None

    def query(self, sql: str, *params: Any) -> Union['Cursor[Any]', 'cursor']:
        try:
            logger.debug('query(%s, %s)', sql, params)
            cursor = self._connection.cursor()
            cursor.execute(sql.encode('utf-8'), params or None)
            return cursor
        except Exception as e:
            logger.error('Exception when executing query "%s", (%s): %r', sql, params, e)
            self._connection.close()
            with self._condition:
                self._in_flight = None
            self.schedule_cache_rebuild()
            raise e

    def load_pg_dist_group(self) -> bool:
        """Read from the `pg_dist_node` table and put it into the local cache"""

        with self._condition:
            if not self._schedule_load_pg_dist_group:
                return True
            self._schedule_load_pg_dist_group = False

        try:
            cursor = self.query('SELECT groupid, nodename, nodeport, noderole, nodeid FROM pg_catalog.pg_dist_node')
        except Exception:
            return False

        pg_dist_group: Dict[int, PgDistTask] = {}

        for row in cursor:
            if row[0] not in pg_dist_group:
                pg_dist_group[row[0]] = PgDistTask(row[0], nodes=set(), event='after_promote')
            pg_dist_group[row[0]].add(PgDistNode(*row))

        with self._condition:
            self._pg_dist_group = pg_dist_group
        return True

    def sync_pg_dist_node(self, cluster: Cluster) -> None:
        """Maintain the `pg_dist_node` from the coordinator leader every heartbeat loop.

        We can't always rely on REST API calls from worker nodes in order
        to maintain `pg_dist_node`, therefore at least once per heartbeat
        loop we make sure that workes registered in `self._pg_dist_group`
        cache are matching the cluster view from DCS by creating tasks
        the same way as it is done from the REST API."""

        if not self.is_coordinator():
            return

        with self._condition:
            if not self.is_alive():
                self.start()

        self.add_task('after_promote', CITUS_COORDINATOR_GROUP_ID, cluster,
                      self._postgresql.name, self._postgresql.connection_string)

        for group, worker in cluster.workers.items():
            leader = worker.leader
            if leader and leader.conn_url\
                    and leader.data.get('role') in ('master', 'primary') and leader.data.get('state') == 'running':
                self.add_task('after_promote', group, worker, leader.name, leader.conn_url)

    def find_task_by_group(self, group: int) -> Optional[int]:
        for i, task in enumerate(self._tasks):
            if task.group == group:
                return i

    def pick_task(self) -> Tuple[Optional[int], Optional[PgDistTask]]:
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
                    i = self.find_task_by_group(CITUS_COORDINATOR_GROUP_ID)  # set_coordinator
                    if i is None and self._tasks:
                        i = 0
                    if i is None:
                        break
                    task = self._tasks[i]
                    if task.group in self._pg_dist_group\
                            and task.event == self._pg_dist_group[task.group].event\
                            and task.equals(self._pg_dist_group[task.group]):
                        self._tasks.pop(i)  # nothing to do because cached version of pg_dist_group already matches
                    else:
                        break
            task = self._tasks[i] if i is not None else None

            return i, task

    def update_node(self, node: PgDistNode, cooldown: float = 10000) -> None:
        if node.role not in ('primary', 'secondary', 'demoted'):
            self.query('SELECT pg_catalog.citus_remove_node(%s, %s)', node.host, node.port)
        elif node.nodeid is not None:
            host = node.host + ('-demoted' if node.role == 'demoted' else '')
            self.query('SELECT pg_catalog.citus_update_node(%s, %s, %s, true, %s)',
                       node.nodeid, host, node.port, cooldown)
        elif node.role != 'demoted':
            row = self.query("SELECT pg_catalog.citus_add_node(%s, %s, %s, %s, 'default')",
                             node.host, node.port, node.group, node.role).fetchone()
            if row is not None:
                node.nodeid = row[0]

    def update_group(self, task: PgDistTask, transaction: bool) -> None:
        current_state = self._in_flight\
            or self._pg_dist_group.get(task.group)\
            or PgDistTask(task.group, set(), 'after_promote')
        transitions = list(task.transition(current_state))
        if transitions:
            if not transaction and len(transitions) > 1:
                self.query('BEGIN')
            for node in transitions:
                self.update_node(node, task.cooldown)
            if not transaction and len(transitions) > 1:
                task.failover = False
                self.query('COMMIT')

    def process_task(self, task: PgDistTask) -> bool:
        """Updates a single row in `pg_dist_group` table, optionally in a transaction.

        The transaction is started if we do a demote of the worker node or before promoting the other worker if
        there is no transaction in progress. And, the transaction is committed when the switchover/failover completed.

        .. note:
            The maximum lifetime of the transaction in progress is controlled outside of this method.

        .. note:
            Read access to `self._in_flight` isn't protected because we know it can't be changed outside of our thread.

        :param task: reference to a :class:`PgDistTask` object that represents a row to be updated/created.
        :returns: `True` if the row was succesfully created/updated or transaction in progress
            was committed as an indicator that the `self._pg_dist_group` cache should be updated,
            or, if the new transaction was opened, this method returns `False`.
        """

        if task.event == 'after_promote':
            self.update_group(task, self._in_flight is not None)
            if self._in_flight:
                self.query('COMMIT')
            task.failover = False
            return True
        else:  # before_demote, before_promote
            if task.timeout:
                task.deadline = time.time() + task.timeout
            if not self._in_flight:
                self.query('BEGIN')
            self.update_group(task, True)
        return False

    def process_tasks(self) -> None:
        while True:
            # Read access to `_in_flight` isn't protected because we know it can't be changed outside of our thread.
            if not self._in_flight and not self.load_pg_dist_group():
                break

            i, task = self.pick_task()
            if not task or i is None:
                break
            try:
                update_cache = self.process_task(task)
            except Exception as e:
                logger.error('Exception when working with pg_dist_node: %r', e)
                update_cache = None
            with self._condition:
                if self._tasks:
                    if update_cache:
                        self._pg_dist_group[task.group] = task

                    if update_cache is False:  # an indicator that process_tasks has started a transaction
                        self._in_flight = task
                    else:
                        self._in_flight = None

                    if id(self._tasks[i]) == id(task):
                        self._tasks.pop(i)
            task.wakeup()

    def run(self) -> None:
        while True:
            try:
                with self._condition:
                    if self._schedule_load_pg_dist_group:
                        timeout = -1
                    elif self._in_flight:
                        timeout = self._in_flight.deadline - time.time() if self._tasks else None
                    else:
                        timeout = -1 if self._tasks else None

                    if timeout is None or timeout > 0:
                        self._condition.wait(timeout)
                    elif self._in_flight:
                        logger.warning('Rolling back transaction. Last known status: %s', self._in_flight)
                        self.query('ROLLBACK')
                        self._in_flight = None
                self.process_tasks()
            except Exception:
                logger.exception('run')

    def _add_task(self, task: PgDistTask) -> bool:
        with self._condition:
            i = self.find_task_by_group(task.group)

            # The `PgDistTask.timeout` == None is an indicator that it was scheduled from the sync_pg_dist_group().
            if task.timeout is None:
                # We don't want to override the already existing task created from REST API.
                if i is not None and self._tasks[i].timeout is not None:
                    return False

                # There is a little race condition with tasks created from REST API - the call made "before" the member
                # key is updated in DCS. Therefore it is possible that :func:`sync_pg_dist_group` will try to create a
                # task based on the outdated values of "state"/"role". To solve it we introduce an artificial timeout.
                # Only when the timeout is reached new tasks could be scheduled from sync_pg_dist_group()
                if self._in_flight and self._in_flight.group == task.group and self._in_flight.timeout is not None\
                        and self._in_flight.deadline > time.time():
                    return False

            # Override already existing task for the same worker group
            if i is not None:
                if task != self._tasks[i]:
                    logger.debug('Overriding existing task: %s != %s', self._tasks[i], task)
                    self._tasks[i] = task
                    self._condition.notify()
                    return True
            # Add the task to the list if Worker node state is different from the cached `pg_dist_group`
            elif self._schedule_load_pg_dist_group\
                    or task.group not in self._pg_dist_group\
                    or task != self._pg_dist_group[task.group]\
                    or self._in_flight and task.group == self._in_flight.group:
                logger.debug('Adding the new task: %s', task)
                self._tasks.append(task)
                self._condition.notify()
                return True
        return False

    @staticmethod
    def _pg_dist_node(group: int, role: str, conn_url: str) -> Optional[PgDistNode]:
        try:
            r = urlparse(conn_url)
            if r.hostname:
                return PgDistNode(group, r.hostname, r.port or 5432, role)
        except Exception as e:
            logger.error('Failed to parse connection url %s: %r', conn_url, e)

    def add_task(self, event: str, group: int, cluster: Cluster, leader_name: str, leader_url: str,
                 timeout: Optional[float] = None, cooldown: Optional[float] = None) -> Optional[PgDistTask]:
        primary = self._pg_dist_node(group, 'demoted' if event == 'before_demote' else 'primary', leader_url)
        if not primary:
            return

        task = PgDistTask(group, {primary}, event=event, timeout=timeout, cooldown=cooldown)
        for member in cluster.members:
            secondary = self._pg_dist_node(group, 'secondary', member.conn_url)\
                if member.name != leader_name and member.is_running and member.conn_url else None
            if secondary:
                task.add(secondary)
        return task if self._add_task(task) else None

    def handle_event(self, cluster: Cluster, event: Dict[str, Any]) -> None:
        if not self.is_alive():
            return

        worker = cluster.workers.get(event['group'])
        if not (worker and worker.leader and worker.leader.name == event['leader'] and worker.leader.conn_url):
            return logger.info('Discarding event %s', event)

        task = self.add_task(event['type'], event['group'], worker,
                             worker.leader.name, worker.leader.conn_url,
                             event['timeout'], event['cooldown'] * 1000)
        if task and event['type'] == 'before_demote':
            task.wait()

    def bootstrap(self) -> None:
        if not isinstance(self._config, dict):  # self.is_enabled()
            return

        conn_kwargs = self._postgresql.config.local_connect_kwargs
        conn_kwargs['options'] = '-c synchronous_commit=local -c statement_timeout=0'
        if self._config['database'] != self._postgresql.database:
            conn = connect(**conn_kwargs)
            try:
                with conn.cursor() as cur:
                    cur.execute('CREATE DATABASE {0}'.format(
                        quote_ident(self._config['database'], conn)).encode('utf-8'))
            finally:
                conn.close()

        conn_kwargs['dbname'] = self._config['database']
        conn = connect(**conn_kwargs)
        try:
            with conn.cursor() as cur:
                cur.execute('CREATE EXTENSION citus')

                superuser = self._postgresql.config.superuser
                params = {k: superuser[k] for k in ('password', 'sslcert', 'sslkey') if k in superuser}
                if params:
                    cur.execute("INSERT INTO pg_catalog.pg_dist_authinfo VALUES"
                                "(0, pg_catalog.current_user(), %s)",
                                (self._postgresql.config.format_dsn(params),))

                if self.is_coordinator():
                    r = urlparse(self._postgresql.connection_string)
                    cur.execute("SELECT pg_catalog.citus_set_coordinator_host(%s, %s, 'primary', 'default')",
                                (r.hostname, r.port or 5432))
        finally:
            conn.close()

    def adjust_postgres_gucs(self, parameters: Dict[str, Any]) -> None:
        if not self.is_enabled():
            return

        # citus extension must be on the first place in shared_preload_libraries
        shared_preload_libraries = list(filter(
            lambda el: el and el != 'citus',
            [p.strip() for p in parameters.get('shared_preload_libraries', '').split(',')]))
        parameters['shared_preload_libraries'] = ','.join(['citus'] + shared_preload_libraries)

        # if not explicitly set Citus overrides max_prepared_transactions to max_connections*2
        if parameters.get('max_prepared_transactions') == 0:
            parameters['max_prepared_transactions'] = parameters['max_connections'] * 2

        # Resharding in Citus implemented using logical replication
        parameters['wal_level'] = 'logical'

    def ignore_replication_slot(self, slot: Dict[str, str]) -> bool:
        if isinstance(self._config, dict) and self._postgresql.is_leader() and\
                slot['type'] == 'logical' and slot['database'] == self._config['database']:
            m = CITUS_SLOT_NAME_RE.match(slot['name'])
            return bool(m and {'move': 'pgoutput', 'split': 'citus'}.get(m.group(1)) == slot['plugin'])
        return False
