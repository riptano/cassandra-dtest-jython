import time, uuid
import unittest
from base import HybridTester

from datahelp import create_rows, parse_data_into_lists, flatten_into_set, cql_str

#java
from com.datastax.driver.core import SimpleStatement, BoundStatement, exceptions

def wait_for_node_alive(node):
    # for now let's just pause
    time.sleep(10)

class Page(object):
    data = None
    
    def __init__(self):
        self.data = []

    def add_row(self, row, formatters):
        """
        See PageContainer for an explanation of formatters
        """
        if row is None:
            return
        
        values = []
        
        for (colname, methodname, cast_func) in formatters:
            jmethod = getattr(row, methodname)
            # calls java method to get named column
            # analogous to: str(row.getInt('id')) but would differ
            # depending on the provided formatters
            values.append(
                cast_func(jmethod(colname))
                )
        
        self.data.append(values)
                        
class PageFetcher(object):
    """
    Fethches result rows and breaks into pages.
    """
    pages = None
    formatters = None
    results = None
    
    def __init__(self, results, formatters):
        """
        For a given results set, automagically breaks the results into pages.
        
        The formatters value should be provided as a list of tuples, like so:
        [('id', 'getInt', str), ('value', 'getString', str), ...]
        This tells the pager where to get the data, how to get it from the java driver,
        and finally how to cast it for easy comparison.
        """
        self.pages = []
        self.formatters = formatters
        self.results = results

    def get_all_pages(self):
        results = self.results

        while not results.isExhausted():
            self.get_page()
        
        return self.pages
    
    def get_remaining_pages(self):
        # for better intent in tests
        return self.get_all_pages()
    
    def get_page(self):
        """Returns next page"""
        results = self.results
        formatters = self.formatters
        
        if not results.isExhausted():
            page = Page()
            self.pages.append(page)
            
            while results.getAvailableWithoutFetching() > 0:
                page.add_row(results.one(), formatters)
        
            return page
        return None
    
    def pagecount(self):
        return len(self.pages)
    
    def num_results(self, page_num):
        # change page_num to zero-index value
        return len(self.pages[page_num-1].data)
    
    def num_results_all_pages(self):
        return [len(page.data) for page in self.pages]
    
    def all_data(self):
        """
        Returns all retrieved data flattened into a single list
        (instead of separated into Page objects)
        """
        all_pages_combined = []
        for page in self.pages:
            all_pages_combined.extend(page.data[:])
        
        return all_pages_combined

class PageAssertionMixin(object):
    """Can be added to subclasses of unittest.Tester"""
    def assertEqualIgnoreOrder(self, one, two):
        """
        Flattens data into a set and then compare.
        Elements compared should be one of:
        structure returned by parse_data_into_lists (expected data)
        or data from PageFetcher.all_data() (actual data)
        """
        self.assertEqual(
            flatten_into_set(one),
            flatten_into_set(two)
            )
    
    def assertIsSubsetOf(self, subset, superset):
        assert flatten_into_set(subset).issubset(flatten_into_set(superset))

class TestPagingSize(HybridTester, PageAssertionMixin):
    """
    Basic tests relating to page size (relative to results set)
    and validation of page size setting.
    """
    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # CASSANDRA-6964
            r'ByteBuf\.release\(\) was not called before it\'s garbage-collected',
        ]
        HybridTester.__init__(self, *args, **kwargs)
        
    def test_with_no_results(self):
        """
        No errors when a page is requested and query has no results.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        # run a query that has no results and make sure it's exhausted
        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(100)

        results = cursor.execute(stmt)
        self.assertTrue(results.isExhausted())
        
    def test_with_less_results_than_page_size(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, cql_str))

        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(100)

        results = cursor.execute(stmt)
        self.assertTrue(results.isFullyFetched())
        
        actual_data = []
        for row in results:
            row_data = [str(row.getInt('id')), cql_str(row.getString('value'))]
            actual_data.append(row_data)
            self.assertTrue(row_data in expected_data)

        self.assertEqual(len(expected_data), len(actual_data))
        self.assertTrue(results.isExhausted())
    
    def test_with_more_results_than_page_size(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            |6 |testing         |
            |7 |and more testing|
            |8 |and more testing|
            |9 |and more testing|
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, cql_str))

        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(5)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', cql_str)]
            )

        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [5, 4])
        
        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(expected_data, pf.all_data())
    
    def test_with_equal_results_to_page_size(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, cql_str))

        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(5)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', cql_str)]
            )

        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 1)
        self.assertEqual(pf.num_results_all_pages(), [5])
        
        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(expected_data, pf.all_data())
        
    def test_zero_page_size_ignored(self):
        """
        If the page size <= 0 then the default fetch size is used.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id uuid PRIMARY KEY, value text )")

        def random_txt(text):
            return "{random}".format(random=uuid.uuid1())

        data = """
              | id     |value   |
         *5001| [uuid] |testing |
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(random_txt, cql_str))
        time.sleep(5)

        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(0)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getUUID', str), ('value', 'getString', cql_str)]
            )

        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [5000, 1])
        
        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(expected_data, pf.all_data())

class TestPagingWithModifiers(HybridTester, PageAssertionMixin):
    """
    Tests concerned with paging when CQL modifiers (such as order, limit, allow filtering) are used.
    """
    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # CASSANDRA-6964
            r'ByteBuf\.release\(\) was not called before it\'s garbage-collected',
        ]
        HybridTester.__init__(self, *args, **kwargs)
        
    def test_with_order_by(self):
        """"
        Paging over a single partition with ordering should work.
        (Spanning multiple partitions won't though, by design. See CASSANDRA-6722).
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging', 2)
        cursor.execute(
            """
            CREATE TABLE paging_test (
                id int,
                value text,
                PRIMARY KEY (id, value)
            ) WITH CLUSTERING ORDER BY (value ASC)
            """)

        data = """
            |id|value|
            |1 |a    |
            |1 |b    |
            |1 |c    |
            |1 |d    | 
            |1 |e    | 
            |1 |f    | 
            |1 |g    | 
            |1 |h    |
            |1 |i    |
            |1 |j    |
            """
        
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, cql_str))

        stmt = SimpleStatement("select * from paging_test where id = 1 order by value asc")
        stmt.setFetchSize(5)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', cql_str)]
            )

        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [5, 5])
        
        # these should be equal (in the same order)
        self.assertEqual(expected_data, pf.all_data())
        
        # make sure we don't allow paging over multiple partitions with order because that's weird
        with self.assertRaisesRegexp(exceptions.InvalidQueryException, 'Cannot page queries with both ORDER BY and a IN restriction on the partition key'):
            stmt = SimpleStatement("select * from paging_test where id in (1,2) order by value asc")
            cursor.execute(stmt)
    
    def test_with_limit(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id|value           |
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            |6 |testing         |
            |7 |and more testing|
            |8 |and more testing|
            |9 |and more testing|
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, cql_str))

        stmt = SimpleStatement("select * from paging_test limit 5")
        stmt.setFetchSize(9)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', cql_str)]
            )

        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 1)
        self.assertEqual(pf.num_results_all_pages(), [5])
        
        # make sure all the data retrieved is a subset of input data
        self.assertIsSubsetOf(pf.all_data(), expected_data)
        
        # let's do another query with a limit larger than one page
        stmt = SimpleStatement("select * from paging_test limit 8")
        stmt.setFetchSize(5)
        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', cql_str)]
            )
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [5, 3])
        self.assertIsSubsetOf(pf.all_data(), expected_data)
    
    def test_with_allow_filtering(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        data = """
            |id|value           |
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            |6 |testing         |
            |7 |and more testing|
            |8 |and more testing|
            |9 |and more testing|
            """
        create_rows(data, cursor, 'paging_test', format_funcs=(str, cql_str))
        
        stmt = SimpleStatement("select * from paging_test where value = 'and more testing' ALLOW FILTERING")
        stmt.setFetchSize(4)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', cql_str)]
            )
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [4, 3])
        
        # make sure the allow filtering query matches the expected results (ignoring order)
        self.assertEqualIgnoreOrder(pf.all_data(),
            parse_data_into_lists(
            """
            |id|value           |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            |7 |and more testing|
            |8 |and more testing|
            |9 |and more testing|
            """, format_funcs=(str, cql_str)
            ))

class TestPagingData(HybridTester, PageAssertionMixin):
    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # CASSANDRA-6964
            r'ByteBuf\.release\(\) was not called before it\'s garbage-collected',
        ]
        HybridTester.__init__(self, *args, **kwargs)
        
    def test_paging_a_single_wide_row(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())

        data = """
              | id | value                  |
        *10000| 1  | [replaced with random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, random_txt))
        
        stmt = SimpleStatement("select * from paging_test where id = 1")
        stmt.setFetchSize(3000)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', cql_str)]
            )
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all_pages(), [3000, 3000, 3000, 1000])
        
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)
    
    def test_paging_across_multi_wide_rows(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())

        data = """
              | id | value                  |
         *5000| 1  | [replaced with random] |
         *5000| 2  | [replaced with random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, random_txt))
        
        stmt = SimpleStatement("select * from paging_test where id in (1,2)")
        stmt.setFetchSize(3000)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', cql_str)]
            )
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all_pages(), [3000, 3000, 3000, 1000])
        
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)
        
    def test_paging_using_secondary_indexes(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mybool boolean, sometext text, PRIMARY KEY (id, sometext) )")
        cursor.execute("CREATE INDEX ON paging_test(mybool)")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())
        
        data = """
             | id | mybool| sometext |
         *100| 1  | True  | [random] |
         *300| 2  | False | [random] |
         *500| 3  | True  | [random] |
         *400| 4  | False | [random] |
            """
        create_rows(data, cursor, 'paging_test', format_funcs=(str, str, random_txt))
        stmt = SimpleStatement("select * from paging_test where mybool = true")
        stmt.setFetchSize(400)

        results = cursor.execute(stmt)
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('mybool', 'getBool', str)]
            )
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [400, 200])

class TestPagingSizeChange(HybridTester, PageAssertionMixin):
    """
    Tests concerned with paging when the page size is changed between page retrievals.
    """
    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # CASSANDRA-6964
            r'ByteBuf\.release\(\) was not called before it\'s garbage-collected',
        ]
        HybridTester.__init__(self, *args, **kwargs)
        
    def test_page_size_change(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, sometext text, PRIMARY KEY (id, sometext) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())
        
        data = """
              | id | sometext |
         *2000| 1  | [random] |
            """
        create_rows(data, cursor, 'paging_test', format_funcs=(str, random_txt))
        stmt = SimpleStatement("select * from paging_test where id = 1")
        stmt.setFetchSize(1000)

        results = cursor.execute(stmt)
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('sometext', 'getString', str)]
            )
        pf.get_page()
        self.assertEqual(pf.pagecount(), 1)
        self.assertEqual(pf.num_results(1), 1000)
        
        stmt.setFetchSize(500)
        
        pf.get_page()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results(2), 500)
        
        stmt.setFetchSize(100)
        
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 7)
        self.assertEqual(pf.num_results_all_pages(), [1000,500,100,100,100,100,100])
    
    def test_page_size_set_multiple_times_before(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, sometext text, PRIMARY KEY (id, sometext) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())
        
        data = """
              | id | sometext |
         *2000| 1  | [random] |
            """
        create_rows(data, cursor, 'paging_test', format_funcs=(str, random_txt))
        stmt = SimpleStatement("select * from paging_test where id = 1")
        stmt.setFetchSize(1000)
        stmt.setFetchSize(100)
        stmt.setFetchSize(500)

        results = cursor.execute(stmt)
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('sometext', 'getString', str)]
            )

        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all_pages(), [500,500,500,500])
    
    def test_page_size_after_results_all_retrieved(self):
        """
        Confirm that page size change does nothing after results are exhausted.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, sometext text, PRIMARY KEY (id, sometext) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())
        
        data = """
              | id | sometext |
         *2000| 1  | [random] |
            """
        create_rows(data, cursor, 'paging_test', format_funcs=(str, random_txt))
        stmt = SimpleStatement("select * from paging_test where id = 1")
        stmt.setFetchSize(500)

        results = cursor.execute(stmt)
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('sometext', 'getString', str)]
            )

        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all_pages(), [500,500,500,500])
        
        # set statement fetch size and try for more results, should be none
        stmt.setFetchSize(1000)
        self.assertEqual(results.one(), None)

class TestPagingDatasetChanges(HybridTester, PageAssertionMixin):
    """
    Tests concerned with paging when the queried dataset changes while pages are being retrieved.
    """
    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # CASSANDRA-6964
            r'ByteBuf\.release\(\) was not called before it\'s garbage-collected',
        ]
        HybridTester.__init__(self, *args, **kwargs)
        
    def test_data_change_impacting_earlier_page(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())

        data = """
              | id | mytext   |
          *500| 1  | [random] |
          *500| 2  | [random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, random_txt))
        
        stmt = SimpleStatement("select * from paging_test where id in (1,2)")
        # get 501 rows so we have definitely got the 1st row of the second partition
        stmt.setFetchSize(501)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('mytext', 'getString', cql_str)]
            )
        
        pf.get_page()
        
        # we got one page and should be done with the first partition (for id=1)
        # let's add another row for that first partition (id=1) and make sure it won't sneak into results
        cursor.execute(SimpleStatement("insert into paging_test (id, mytext) values (1, 'foo')"))
        
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [501, 499])
        
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)
    
    def test_data_change_impacting_later_page(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())

        data = """
              | id | mytext   |
          *500| 1  | [random] |
          *499| 2  | [random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, random_txt))
        
        stmt = SimpleStatement("select * from paging_test where id in (1,2)")
        stmt.setFetchSize(500)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('mytext', 'getString', cql_str)]
            )
        
        pf.get_page()
        
        # we've already paged the first partition, but adding a row for the second (id=2)
        # should still result in the row being seen on the subsequent pages
        cursor.execute(SimpleStatement("insert into paging_test (id, mytext) values (2, 'foo')"))
        
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [500, 500])
        
        # add the new row to the expected data and then do a compare
        expected_data.append([2, "'foo'"])
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)
    
    def test_data_delete_removing_remainder(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())

        data = """
              | id | mytext   |
          *500| 1  | [random] |
          *500| 2  | [random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, random_txt))
        
        stmt = SimpleStatement("select * from paging_test where id in (1,2)")
        stmt.setFetchSize(500)

        results = cursor.execute(stmt)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('mytext', 'getString', cql_str)]
            )
        
        pf.get_page()
        
        # delete the results that would have shown up on page 2
        cursor.execute(SimpleStatement("delete from paging_test where id = 2"))
        
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 1)
        self.assertEqual(pf.num_results_all_pages(), [500])
    
    def test_row_TTL_expiry_during_paging(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())

        # create rows with TTL (some of which we'll try to get after expiry)
        create_rows("""
              | id | mytext   |
          *300| 1  | [random] |
          *400| 2  | [random] |
            """,
            cursor, 'paging_test', format_funcs=(str, random_txt), postfix = 'USING TTL 10'
            )
        
        # create rows without TTL
        create_rows("""
              | id | mytext   |
          *500| 3  | [random] |
            """,
            cursor, 'paging_test', format_funcs=(str, random_txt)
            )
        
        stmt = SimpleStatement("select * from paging_test where id in (1,2,3)")
        stmt.setFetchSize(300)

        results = cursor.execute(stmt)
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('mytext', 'getString', cql_str)]
            )
        # this page will be partition id=1, it has TTL rows but they are not expired yet
        pf.get_page()
        
        # sleep so that the remaining TTL rows from partition id=2 expire
        time.sleep(15)
        
        pf.get_remaining_pages()
        self.assertEqual(pf.pagecount(), 3)
        self.assertEqual(pf.num_results_all_pages(), [300, 300, 200])
    
    def test_cell_TTL_expiry_during_paging(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("""
            CREATE TABLE paging_test (
                id int,
                mytext text,
                somevalue text,
                anothervalue text,
                PRIMARY KEY (id, mytext) )
            """)

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())

        data = create_rows("""
              | id | mytext   | somevalue | anothervalue |
          *500| 1  | [random] | foo       |  bar         |
          *500| 2  | [random] | foo       |  bar         |
          *500| 3  | [random] | foo       |  bar         |
            """,
            cursor, 'paging_test', format_funcs=(str, random_txt, cql_str, cql_str)
            )
        
        stmt = SimpleStatement("select * from paging_test where id in (1,2,3)")
        stmt.setFetchSize(500)

        results = cursor.execute(stmt)
        pf = PageFetcher(
            results, formatters = [
                ('id', 'getInt', str),
                ('mytext', 'getString', cql_str),
                ('somevalue', 'getString', cql_str),
                ('anothervalue', 'getString', cql_str)
                ]
            )
        # get the first page and check values
        page1 = pf.get_page().data
        self.assertEqualIgnoreOrder(page1, data[:500])
        
        # set some TTLs for data on page 3
        for (_id, mytext, somevalue, anothervalue) in data[1000:1500]:
            stmt = """
                update paging_test using TTL 10
                set somevalue='one'
                where id = {id} and mytext = {mytext}
                """.format(id=_id, mytext=mytext)
            cursor.execute(stmt)
        
        # check page two
        page2 = pf.get_page().data
        self.assertEqualIgnoreOrder(page2, data[500:1000])

        page3expected = []
        for (_id, mytext, somevalue, anothervalue) in data[1000:1500]:
            page3expected.append([_id, mytext, "'None'", "'bar'"])
        
        time.sleep(15)
        
        page3 = pf.get_page().data
        self.assertEqualIgnoreOrder(page3, page3expected)
        
        # TODO: modify test to TTL more than one column after CASSANDRA-6782 is resolved.
    
    def test_node_unavailabe_during_paging(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 1)
        cursor.execute("CREATE TABLE paging_test ( id uuid, mytext text, PRIMARY KEY (id, mytext) )")

        def make_uuid(text):
            return str(uuid.uuid4())

        expected_data = create_rows("""
                | id      | mytext |
          *10000| [uuid]  | foo    |
            """,
            cursor, 'paging_test', format_funcs=(make_uuid, cql_str)
            )
        
        stmt = SimpleStatement("select * from paging_test where mytext = 'foo' allow filtering")
        stmt.setFetchSize(2000)

        results = cursor.execute(stmt)
        pf = PageFetcher(
            results, formatters = [('id', 'getUUID', str), ('mytext', 'getString', cql_str)]
            )

        pf.get_page()
        
        # stop a node and make sure we get an error trying to page the rest
        node1.stop()
        with self.assertRaisesRegexp(exceptions.UnavailableException, 'Not enough replica available for query at consistency ONE'):
            pf.get_remaining_pages()
        
        # TODO: can we resume the node and expect to get more results from the result set or is it done?

class TestPagingQueryIsolation(HybridTester, PageAssertionMixin):
    """
    Tests concerned with isolation of paged queries (queries can't affect each other).
    """
    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # CASSANDRA-6964
            r'ByteBuf\.release\(\) was not called before it\'s garbage-collected',
        ]
        HybridTester.__init__(self, *args, **kwargs)
        
    def test_query_isolation(self):
        """
        Interleave some paged queries and make sure nothing bad happens.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return "'{random}'".format(random=uuid.uuid1())

        data = """
               | id | mytext   |
          *5000| 1  | [random] |
          *5000| 2  | [random] |
          *5000| 3  | [random] |
          *5000| 4  | [random] |
          *5000| 5  | [random] |
          *5000| 6  | [random] |
          *5000| 7  | [random] |
          *5000| 8  | [random] |
          *5000| 9  | [random] |
          *5000| 10 | [random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', format_funcs=(str, random_txt))
        
        stmts = [
            SimpleStatement("select * from paging_test where id in (1)").setFetchSize(500),
            SimpleStatement("select * from paging_test where id in (2)").setFetchSize(600),
            SimpleStatement("select * from paging_test where id in (3)").setFetchSize(700),
            SimpleStatement("select * from paging_test where id in (4)").setFetchSize(800),
            SimpleStatement("select * from paging_test where id in (5)").setFetchSize(900),
            SimpleStatement("select * from paging_test where id in (1)").setFetchSize(1000),
            SimpleStatement("select * from paging_test where id in (2)").setFetchSize(1100),
            SimpleStatement("select * from paging_test where id in (3)").setFetchSize(1200),
            SimpleStatement("select * from paging_test where id in (4)").setFetchSize(1300),
            SimpleStatement("select * from paging_test where id in (5)").setFetchSize(1400),
            SimpleStatement("select * from paging_test where id in (1,2,3,4,5,6,7,8,9,10)").setFetchSize(1500)
        ]

        page_fetchers = []
        
        for stmt in stmts:
            results = cursor.execute(stmt)
            pf = PageFetcher(
                    results, formatters = [('id', 'getInt', str), ('mytext', 'getString', cql_str)]
                    )
            pf.get_page()
            page_fetchers.append(pf)
        
        for pf in page_fetchers:
            pf.get_page()
        
        for pf in page_fetchers:
            pf.get_page()
        
        for pf in page_fetchers:
            pf.get_remaining_pages()
        
        self.assertEqual(page_fetchers[0].pagecount(), 10)
        self.assertEqual(page_fetchers[1].pagecount(), 9)
        self.assertEqual(page_fetchers[2].pagecount(), 8)
        self.assertEqual(page_fetchers[3].pagecount(), 7)
        self.assertEqual(page_fetchers[4].pagecount(), 6)
        self.assertEqual(page_fetchers[5].pagecount(), 5)
        self.assertEqual(page_fetchers[6].pagecount(), 5)
        self.assertEqual(page_fetchers[7].pagecount(), 5)
        self.assertEqual(page_fetchers[8].pagecount(), 4)
        self.assertEqual(page_fetchers[9].pagecount(), 4)
        self.assertEqual(page_fetchers[10].pagecount(), 34)
        
        self.assertEqualIgnoreOrder(page_fetchers[0].all_data(), expected_data[:5000])
        self.assertEqualIgnoreOrder(page_fetchers[1].all_data(), expected_data[5000:10000])
        self.assertEqualIgnoreOrder(page_fetchers[2].all_data(), expected_data[10000:15000])
        self.assertEqualIgnoreOrder(page_fetchers[3].all_data(), expected_data[15000:20000])
        self.assertEqualIgnoreOrder(page_fetchers[4].all_data(), expected_data[20000:25000])
        self.assertEqualIgnoreOrder(page_fetchers[5].all_data(), expected_data[:5000])
        self.assertEqualIgnoreOrder(page_fetchers[6].all_data(), expected_data[5000:10000])
        self.assertEqualIgnoreOrder(page_fetchers[7].all_data(), expected_data[10000:15000])
        self.assertEqualIgnoreOrder(page_fetchers[8].all_data(), expected_data[15000:20000])
        self.assertEqualIgnoreOrder(page_fetchers[9].all_data(), expected_data[20000:25000])
        self.assertEqualIgnoreOrder(page_fetchers[10].all_data(), expected_data[:50000])

if __name__ == '__main__':
    unittest.main()