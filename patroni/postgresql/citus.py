import logging

from six.moves.urllib_parse import urlparse

logger = logging.getLogger(__name__)


class CitusDistNodeHandler(object):

    def __init__(self, postgresql):
        self._postgresql = postgresql
        self._pg_dist_node = {}
        self.schedule()

    def schedule(self):
        self._schedule_load_pg_dist_node = True

    def _query(self, sql, *params):
        return self._postgresql.query(sql, *params, retry=False)

    def load_pg_dist_node(self):
        cursor = self._query("SELECT nodeid, groupid, nodename, nodeport, noderole"
                             " FROM pg_catalog.pg_dist_node WHERE noderole = 'primary'")
        self._pg_dist_node = {r[1]: {'nodeid': r[0], 'nodename': r[2], 'nodeport': r[3]} for r in cursor}
        self._schedule_load_pg_dist_node = False

    def sync_pg_dist_node(self, cluster):
        try:
            self.load_pg_dist_node()
            coordiator = urlparse(self._postgresql.connection_string)
            host = coordiator.hostname
            port = coordiator.port or 5432
            if 0 not in self._pg_dist_node or\
                    self._pg_dist_node[0]['nodename'] != host or\
                    self._pg_dist_node[0]['nodeport'] != port:
                self._query("SELECT pg_catalog.citus_set_coordinator_host(%s, %s, 'primary', 'default')", host, port)

            for group, worker in sorted(cluster.workers.items()):
                group = int(group)
                leader = worker.leader
                if leader and leader.conn_url and leader.data.get('role') == 'master':
                    leader = urlparse(leader.conn_url)
                    host = leader.hostname
                    port = leader.port or 5432
                    if group in self._pg_dist_node:
                        if self._pg_dist_node[group]['nodename'] != host or\
                                self._pg_dist_node[group]['nodeport'] != port:
                            self._query('SELECT pg_catalog.citus_update_node(%s, %s, %s, true, 100)',
                                        self._pg_dist_node[group]['nodeid'], host, port)
                            self._pg_dist_node[group].update(nodename=host, nodeport=port)
                    else:
                        nodeid = self._query("SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default')",
                                             host, port, group).fetchone()[0]
                        self._pg_dist_node[group] = {'nodeid': nodeid, 'nodename': host, 'nodeport': port}
                else:
                    pass  # TODO: maybe disable worker?
        except Exception as e:
            logger.error('Failed to sync pg_dist_node: %r', e)
            self._schedule_load_pg_dist_node = True
