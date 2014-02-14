from com.datastax.driver.core import Cluster, SimpleStatement, BoundStatement
import tempfile
import ccmlib.cluster
import time

def connect(node):
    cluster = Cluster.builder().addContactPoint(node).build()
    session = cluster.connect()
    return session

def ccm_create(version="git:cassandra-2.0"):
    test_path = tempfile.mkdtemp(prefix='dtest-')
    print "cluster ccm directory: %s" % test_path
    cluster = ccmlib.cluster.Cluster(test_path, 'test', cassandra_version=version)
    cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': 256})
    cluster.populate(1)
    cluster.start()

if __name__ == "__main__":
    ccm_create()
    # TODO: This needs a more patient connection setup:
    time.sleep(10)
    session = connect("127.0.0.1")

    # WTF... when jython is invoked via ant, it never
    # quits the process unless I am explicit here:
    exit(0)
