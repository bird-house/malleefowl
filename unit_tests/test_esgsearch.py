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
                                  latest=True, replica=False, temporal=True)

    @attr('online')
    def test_dataset(self):
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
    def test_aggregation(self):
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

    




