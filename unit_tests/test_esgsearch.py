import nose.tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from __init__ import TESTDATA, SERVICE

class EsgSearchTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        from malleefowl.esgsearch import ESGSearch
        cls.esgsearch = ESGSearch('http://localhost:8081/esg-search', distrib=False,
                                  latest=True, replica=False)

    @attr('online')
    def test_dataset(self):
        #raise SkipTest
        constraints = []
        constraints.append( ('project', 'CORDEX') )
        constraints.append( ('time_frequency', 'mon' ) )
        constraints.append( ('variable', 'tas') )
        constraints.append( ('experiment', 'historical') )

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='Dataset',
            limit=1,
            offset=0,
            constraints = constraints)

        nose.tools.ok_(len(result) == 1, result)

    @attr('online')
    def test_file(self):
        #raise SkipTest
        constraints = []
        constraints.append( ('project', 'CORDEX') )
        constraints.append( ('time_frequency', 'mon' ) )
        constraints.append( ('variable', 'tas') )
        constraints.append( ('experiment', 'historical') )

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=1,
            offset=0,
            constraints = constraints)

        nose.tools.ok_(len(result) > 1, result)

    @attr('online')
    @attr('slow')
    def test_file_more_than_one(self):
        #raise SkipTest
        constraints = []
        constraints.append( ('project', 'CORDEX') )
        constraints.append( ('time_frequency', 'mon' ) )
        constraints.append( ('variable', 'tas') )
        constraints.append( ('experiment', 'historical') )

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=3,
            offset=0,
            constraints = constraints)

        nose.tools.ok_(len(result) > 1, result)

    @attr('online')
    def test_aggregation(self):
        #raise SkipTest
        constraints = []
        constraints.append( ('project', 'CORDEX') )
        constraints.append( ('time_frequency', 'mon' ) )
        constraints.append( ('variable', 'tas') )
        constraints.append( ('experiment', 'historical') )

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='Aggregation',
            limit=1,
            offset=0,
            constraints = constraints)

        nose.tools.ok_(len(result) == 0, result)

    @attr('online')
    def test_tds_file_cordex(self):
        #raise SkipTest
        constraints = []
        constraints.append( ('project', 'CORDEX') )
        constraints.append( ('time_frequency', 'mon' ) )
        constraints.append( ('variable', 'tas') )
        constraints.append( ('experiment', 'historical') )

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File_Thredds',
            limit=10,
            offset=0,
            constraints = constraints)

        nose.tools.ok_(len(result) > 1, result)

    @attr('online')
    def test_tds_file_cordex_date(self):
        #raise SkipTest
        constraints = []
        constraints.append( ('project', 'CORDEX') )
        constraints.append( ('time_frequency', 'mon' ) )
        constraints.append( ('variable', 'tas') )
        constraints.append( ('experiment', 'historical') )

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File_Thredds',
            limit=10,
            offset=0,
            temporal=True,
            start='2001-01-01T12:00:00Z',
            end='2005-12-31T12:00:00Z',
            constraints = constraints)

        nose.tools.ok_(len(result) > 1, result)
        nose.tools.ok_(summary['number_of_selected_files'] > 1, result)

    @attr('online')
    def test_tds_file_cmip5(self):
        #raise SkipTest
        constraints = []
        constraints.append( ('project', 'CMIP5') )
        constraints.append( ('time_frequency', 'mon' ) )
        constraints.append( ('variable', 'tas') )
        constraints.append( ('experiment', 'historical') )

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File_Thredds',
            limit=10,
            offset=0,
            constraints = constraints)

        nose.tools.ok_(len(result) > 1, result)
        nose.tools.ok_(summary['number_of_selected_files'] > 1, result)

    @attr('online')
    def test_tds_file_cmip5_date(self):
        constraints = []
        constraints.append( ('project', 'CMIP5') )
        constraints.append( ('time_frequency', 'mon' ) )
        constraints.append( ('variable', 'tas') )
        constraints.append( ('experiment', 'historical') )

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File_Thredds',
            limit=20,
            offset=0,
            constraints = constraints,
            temporal=False,
            start='1960-01-01T12:00:00Z',
            end='1995-12-31T12:00:00Z')

        nose.tools.ok_(len(result) > 1, result)
        nose.tools.ok_(summary['number_of_selected_files'] > 1, result)

    




