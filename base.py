import re, os, tempfile, sys, shutil, time, ConfigParser, logging
from ccmlib.cluster import Cluster
from unittest import TestCase

# java
from com.datastax.driver.core import Cluster as JCluster

logging.basicConfig(stream=sys.stderr)

LOG_SAVED_DIR="logs"
LAST_LOG = os.path.join(LOG_SAVED_DIR, "last")

LAST_TEST_DIR='last_test_dir'

DEFAULT_DIR='./'
config = ConfigParser.RawConfigParser()
if len(config.read(os.path.expanduser('~/.cassandra-dtest'))) > 0:
    if config.has_option('main', 'default_dir'):
        DEFAULT_DIR=os.path.expanduser(config.get('main', 'default_dir'))

NO_SKIP = os.environ.get('SKIP', '').lower() in ('no', 'false')
DEBUG = os.environ.get('DEBUG', '').lower() in ('yes', 'true')
TRACE = os.environ.get('TRACE', '').lower() in ('yes', 'true')
KEEP_LOGS = os.environ.get('KEEP_LOGS', '').lower() in ('yes', 'true')
KEEP_TEST_DIR = os.environ.get('KEEP_TEST_DIR', '').lower() in ('yes', 'true')
PRINT_DEBUG = os.environ.get('PRINT_DEBUG', '').lower() in ('yes', 'true')
DISABLE_VNODES = os.environ.get('DISABLE_VNODES', '').lower() in ('yes', 'true')

LOG = logging.getLogger()

def debug(msg):
    if DEBUG:
        LOG.debug(msg)
    if PRINT_DEBUG:
        print msg

class ConnectionProxy(object):
    """
    Wraps a com.datastax.driver.core.Session to
    behave kinda like a dbapi2 connection.
    """
    _session = None
    
    def __init__(self, session):
        self._session = session
    
    def cursor(self):
        return self._session
    
    def close(self):
        self._session.close()


class HybridTester(TestCase):
    """
    Supports testing with python/ccmlib
    and the java driver (via jython).
    """
    def __init__(self, *argv, **kwargs):
        # if False, then scan the log of each node for errors after every test.
        self.allow_log_errors = False
        try:
            self.cluster_options = kwargs['cluster_options']
            del kwargs['cluster_options']
        except KeyError:
            self.cluster_options = None
        super(HybridTester, self).__init__(*argv, **kwargs)
        
    def __get_cluster(self, name='test'):
        self.test_path = tempfile.mkdtemp(prefix='dtest-')
        debug("cluster ccm directory: "+self.test_path)
        try:
            version = os.environ['CASSANDRA_VERSION']
            cluster = Cluster(self.test_path, name, cassandra_version=version)
        except KeyError:
            try:
                cdir = os.environ['CASSANDRA_DIR']
            except KeyError:
                cdir = DEFAULT_DIR
            cluster = Cluster(self.test_path, name, cassandra_dir=cdir)
        if cluster.version() >= "1.2":
            if DISABLE_VNODES:
                cluster.set_configuration_options(values={'num_tokens': None})
            else:
                cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': 256})
        return cluster

    def setUp(self):
        # cleaning up if a previous execution didn't trigger tearDown (which
        # can happen if it is interrupted by KeyboardInterrupt)
        # TODO: move that part to a generic fixture
        if os.path.exists(LAST_TEST_DIR):
            with open(LAST_TEST_DIR) as f:
                self.test_path = f.readline().strip('\n')
                name = f.readline()
                try:
                    self.cluster = Cluster.load(self.test_path, name)
                    # Avoid waiting too long for node to be marked down
                    self.__cleanup_cluster()
                except IOError:
                    # after a restart, /tmp will be emptied so we'll get an IOError when loading the old cluster here
                    pass

        self.cluster = self.__get_cluster()
        # self.__setup_cobertura()
        # the failure detector can be quite slow in such tests with quick start/stop
        self.cluster.set_configuration_options(values={'phi_convict_threshold': 5})

        timeout = 10000
        if self.cluster_options is not None:
            self.cluster.set_configuration_options(values=self.cluster_options)
        elif self.cluster.version() < "1.2":
            self.cluster.set_configuration_options(values={'rpc_timeout_in_ms': timeout})
        else:
            self.cluster.set_configuration_options(values={
                'read_request_timeout_in_ms' : timeout,
                'range_request_timeout_in_ms' : timeout,
                'write_request_timeout_in_ms' : timeout,
                'truncate_request_timeout_in_ms' : timeout,
                'request_timeout_in_ms' : timeout
            })

        with open(LAST_TEST_DIR, 'w') as f:
            f.write(self.test_path + '\n')
            f.write(self.cluster.name)
        if DEBUG:
            self.cluster.set_log_level("DEBUG")
        if TRACE:
            self.cluster.set_log_level("TRACE")
        self.connections = []
        self.runners = []    

    def tearDown(self):
        for con in self.connections:
            con.close()

        for runner in self.runners:
            try:
                runner.stop()
            except:
                pass

        failed = sys.exc_info() != (None, None, None)
        try:
            for node in self.cluster.nodelist():
                if self.allow_log_errors == False:
                    errors = list(self.__filter_errors([ msg for msg, i in node.grep_log("ERROR")]))
                    if len(errors) is not 0:
                        failed = True
                        raise AssertionError('Unexpected error in %s node log: %s' % (node.name, errors))
        finally:
            try:
                if failed or KEEP_LOGS:
                    # means the test failed. Save the logs for inspection.
                    if not os.path.exists(LOG_SAVED_DIR):
                        os.mkdir(LOG_SAVED_DIR)
                    logs = [ (node.name, node.logfilename()) for node in self.cluster.nodes.values() ]
                    if len(logs) is not 0:
                        basedir = str(int(time.time() * 1000))
                        dir = os.path.join(LOG_SAVED_DIR, basedir)
                        os.mkdir(dir)
                        for name, log in logs:
                            shutil.copyfile(log, os.path.join(dir, name + ".log"))
                        if os.path.exists(LAST_LOG):
                            os.unlink(LAST_LOG)
                        os.symlink(basedir, LAST_LOG)
            except Exception as e:
                    print "Error saving log:", str(e)
            finally:
                self.__cleanup_cluster()
    def __cleanup_cluster(self):
        if KEEP_TEST_DIR:
            # Just kill it, leave the files where they are:
            self.cluster.stop(gently=False)
        else:
            # Cleanup everything:
            self.cluster.remove()
            os.rmdir(self.test_path)
        os.remove(LAST_TEST_DIR)
        
    def cql_connection(self, node, keyspace=None, user=None, password=None):
        cluster = JCluster.builder().addContactPoint(node.address()).build()
        session = cluster.connect()
        
        proxy = ConnectionProxy(session)
        self.connections.append(proxy)
        return proxy
    
    def create_ks(self, cursor, name, rf):
        cursor.execute(
            """
            CREATE KEYSPACE {ks_name}
            WITH replication={{'class':'SimpleStrategy', 'replication_factor':{rf_num} }}
            """.format(ks_name=name, rf_num=rf)
            )
        
        cursor.execute("USE {ks_name}".format(ks_name=name))
    
    def __filter_errors(self, errors):
        """Filter errors, removing those that match self.ignore_log_patterns"""
        if not hasattr(self, 'ignore_log_patterns'):
            self.ignore_log_patterns = []
        for e in errors:
            for pattern in self.ignore_log_patterns:
                if re.search(pattern, e):
                    break
            else:
                yield e