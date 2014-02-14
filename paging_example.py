from com.datastax.driver.core import Cluster, SimpleStatement, BoundStatement

def connect(node):
    cluster = Cluster.builder().addContactPoint(node).build()
    session = cluster.connect()
    return session

def insert_data(session):
    session.execute("""create KEYSPACE testks WITH replication = 
                     {'class': 'SimpleStrategy', 'replication_factor': 1}""")
    session.execute("CREATE TABLE testks.testcf ( id int PRIMARY KEY, value text )")

    # Insert 1000 rows:
    stmt = session.prepare('INSERT INTO testks.testcf (id, value) VALUES (?, ?)')
    for i in xrange(1000):
        prepared = BoundStatement(stmt)
        session.execute(prepared.bind(i, 'testing'))
    
    # Query those rows back:
    ids_received = set()
    stmt = SimpleStatement('SELECT id, value FROM testks.testcf')
    stmt.setFetchSize(10)
    results = session.execute(stmt)
    for row in results:
        ids_received.add(row.getInt('id'))
        
    if ids_received == set(xrange(1000)):
        print 'Inserted and retrieved 1000 rows.'
    else:
        print 'Not what I expected.'

if __name__ == "__main__":
    session = connect("127.0.0.1")
    insert_data(session)

    # WTF... when jython is invoked via ant (fork=true), it never
    # quits this process unless I am explicit here:
    exit(0)
