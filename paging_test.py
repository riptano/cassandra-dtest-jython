import time
import unittest
from base import HybridTester

from datahelp import create_rows, parse_data_into_lists, inner_quotify, flatten_into_set

#java
from com.datastax.driver.core import SimpleStatement, BoundStatement


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
    
    def get_page(self):
        """Returns next page"""
        results = self.results
        formatters = self.formatters
        
        page = Page()
        while not results.isExhausted():
            # last result? let's grab it, then start a new page
            if results.getAvailableWithoutFetching() == 1:
                page.add_row(results.one(), formatters)
                self.pages.append(page)
                return page
            else:
                page.add_row(results.one(), formatters)
    
    def pagecount(self):
        return len(self.pages)
    
    def num_results(self, page_num):
        # change page_num to zero-index value
        return len(self.pages[page_num-1])
    
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
            |id| value            |
            |1 |'testing'         |
            |2 |'and more testing'|
            |3 |'and more testing'|
            |4 |'and more testing'|
            |5 |'and more testing'|
            """
        create_rows(cursor, 'paging_test', data)

        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(100)

        results = cursor.execute(stmt)
        self.assertTrue(results.isFullyFetched())
        
        expected_data = parse_data_into_lists(data)
        
        actual_data = []
        for row in results:
            row_data = [str(row.getInt('id')), inner_quotify(row.getString('value'))]
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
            |id| value            |
            |1 |'testing'         |
            |2 |'and more testing'|
            |3 |'and more testing'|
            |4 |'and more testing'|
            |5 |'and more testing'|
            |6 |'testing'         |
            |7 |'and more testing'|
            |8 |'and more testing'|
            |9 |'and more testing'|
            """
        create_rows(cursor, 'paging_test', data)

        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(5)

        results = cursor.execute(stmt)
        expected_data = parse_data_into_lists(data)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', inner_quotify)]
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
            |id| value            |
            |1 |'testing'         |
            |2 |'and more testing'|
            |3 |'and more testing'|
            |4 |'and more testing'|
            |5 |'and more testing'|
            """
        create_rows(cursor, 'paging_test', data)

        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(5)

        results = cursor.execute(stmt)
        expected_data = parse_data_into_lists(data)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', inner_quotify)]
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
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value            |
            |1 |'testing'         |
            |2 |'and more testing'|
            |3 |'and more testing'|
            |4 |'and more testing'|
            |5 |'and more testing'|
            """
        create_rows(cursor, 'paging_test', data)

        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(0)

        results = cursor.execute(stmt)
        expected_data = parse_data_into_lists(data)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', inner_quotify)]
            )

        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 1)
        self.assertEqual(pf.num_results_all_pages(), [5])
        
        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(expected_data, pf.all_data())

class TestPagingWithModifiers(HybridTester, PageAssertionMixin):
    """
    Tests concerned with paging when CQL modifiers (such as order, limit, allow filtering) are used.
    """
    def test_with_order_by(self):
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
                sometext text,
                PRIMARY KEY (id, sometext)
            ) WITH CLUSTERING ORDER BY (sometext)
            """)

        data = """
            |id| value|
            |1 |'a'   |
            |2 |'b'   |
            |3 |'c'   |
            |4 |'d'   |
            |5 |'e'   |
            |6 |'f'   |
            |7 |'g'   |
            |8 |'h'   |
            |9 |'i'   |
            |10|'j'   |
            """
        
        create_rows(cursor, 'paging_test', data)

        stmt = SimpleStatement("select * from paging_test")
        stmt.setFetchSize(5)

        results = cursor.execute(stmt)
        expected_data = parse_data_into_lists(data)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', inner_quotify)]
            )

        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [5, 5])
        
        # these should be equal (in the same order)
        self.assertEqual(expected_data, pf.all_data())
    
    def test_with_limit(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        wait_for_node_alive(node1)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value            |
            |1 |'testing'         |
            |2 |'and more testing'|
            |3 |'and more testing'|
            |4 |'and more testing'|
            |5 |'and more testing'|
            |6 |'testing'         |
            |7 |'and more testing'|
            |8 |'and more testing'|
            |9 |'and more testing'|
            """
        create_rows(cursor, 'paging_test', data)

        stmt = SimpleStatement("select * from paging_test limit 5")
        stmt.setFetchSize(9)

        results = cursor.execute(stmt)
        expected_data = parse_data_into_lists(data)
        
        pf = PageFetcher(
            results, formatters = [('id', 'getInt', str), ('value', 'getString', inner_quotify)]
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
            results, formatters = [('id', 'getInt', str), ('value', 'getString', inner_quotify)]
            )
        pf.get_all_pages()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all_pages(), [5, 3])
        self.assertIsSubsetOf(pf.all_data(), expected_data)
    
    def test_with_allow_filtering(self):
        pass

class TestPagingData(HybridTester, PageAssertionMixin):
    def test_paging_a_single_wide_row(self):
        pass
    
    def test_paging_across_multi_wide_rows(self):
        pass
    
    def test_paging_using_secondary_indexes(self):
        pass

class TestPagingSizeChange(HybridTester, PageAssertionMixin):
    """
    Tests concerned with paging when the page size is changed between page retrievals.
    """
    def test_page_size_change(self):
        pass
    
    def test_page_size_set_multiple_times_before(self):
        pass
    
    def test_page_size_after_results_all_retrieved(self):
        """
        Confirm that page size change does nothing after results are exhausted.
        """
    
class TestPagingDatasetChanges(HybridTester, PageAssertionMixin):
    """
    Tests concerned with paging when the queried dataset changes while pages are being retrieved.
    """
    def test_data_change_impacting_earlier_page(self):
        pass
    
    def test_data_change_impacting_later_page(self):
        pass
    
    def test_data_delete_removing_remainder(self):
        pass
    
    def test_data_TTL_expiry_during_paging(self):
        pass
    
    def test_node_unavailabe_during_paging(self):
        pass
    
class TestPagingQueryIsolation(HybridTester, PageAssertionMixin):
    """
    Tests concerned with isolation of paged queries (queries can't affect each other).
    """
    pass

if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestPagingWithModifiers("test_with_limit"))
    
    unittest.TextTestRunner(verbosity=2).run(suite)
    
    exit(0)