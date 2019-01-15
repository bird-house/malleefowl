from datetime import datetime
from datetime import date

import pytest

from unittest import TestCase


def test_date_format():
    end_date = datetime(2001, 1, 1, 12)
    timestamp_format = '%Y-%m-%dT%H:%M:%SZ'
    to_timestamp = '{0}Z'.format(end_date.isoformat().strip())
    assert to_timestamp == '2001-01-01T12:00:00Z'
    assert to_timestamp == end_date.strftime(timestamp_format)


def test_date_format_1900():
    # test date < 1900
    start_date = datetime(1899, 1, 1, 12)
    from_timestamp = '{0}Z'.format(start_date.isoformat().strip())
    assert from_timestamp == '1899-01-01T12:00:00Z'
    # assert from_timestamp == start_date.strftime(timestamp_format)


class EsgDistribSearchTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        from malleefowl.esgf.search import ESGSearch
        cls.esgsearch = ESGSearch('http://esgf-data.dkrz.de/esg-search', distrib=True,
                                  latest=True, replica=False)

    @pytest.mark.skipif(reason="no way of currently testing this")
    @pytest.mark.online
    def test_file_cmip5_with_local_replica(self):
        # NOAA-GFDL/GFDL-CM3/historical/mon/atmos/Amon/r1i1p1/v20110601/tasmax/tasmax_Amon_GFDL-CM3_historical_r1i1p1_200501-200512.nc  # noqa
        constraints = []
        constraints.append(('project', 'CMIP5'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tasmax'))
        constraints.append(('experiment', 'historical'))
        constraints.append(('institute', 'NOAA-GFDL'))
        constraints.append(('ensemble', 'r1i1p1'))
        constraints.append(('model', 'GFDL-CM3'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=100,
            offset=0,
            constraints=constraints)

        assert len(result) > 1
        # we want the local replica, not the original file
        assert not ('gfdl.noaa.gov/thredds/fileServer' in result[0])


class EsgSearchTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        from malleefowl.esgf.search import ESGSearch
        cls.esgsearch = ESGSearch('http://esgf-data.dkrz.de/esg-search', distrib=False,
                                  latest=True, replica=False)

    @pytest.mark.online
    def test_dataset(self):
        constraints = []
        constraints.append(('project', 'CORDEX'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tas'))
        constraints.append(('experiment', 'historical'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='Dataset',
            limit=1,
            offset=0,
            constraints=constraints)

        assert len(result) == 1

    @pytest.mark.online
    def test_file(self):
        constraints = []
        constraints.append(('project', 'CORDEX'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tas'))
        constraints.append(('experiment', 'historical'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=1,
            offset=0,
            constraints=constraints)

        assert len(result) > 1

    @pytest.mark.online
    def test_file_cmip5_many(self):
        constraints = []
        constraints.append(('project', 'CMIP5'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tas'))
        constraints.append(('experiment', 'historical'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=100,
            offset=0,
            constraints=constraints)

        assert len(result) > 1

    @pytest.mark.online
    def test_file_more_than_one(self):
        constraints = []
        constraints.append(('project', 'CORDEX'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tas'))
        constraints.append(('experiment', 'historical'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=3,
            offset=0,
            constraints=constraints)

        assert len(result) > 1

    @pytest.mark.online
    def test_aggregation(self):
        constraints = []
        constraints.append(('project', 'CORDEX'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tas'))
        constraints.append(('experiment', 'historical'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='Aggregation',
            limit=1,
            offset=0,
            constraints=constraints)

        assert len(result) == 0

    @pytest.mark.online
    def test_file_cordex(self):
        constraints = []
        constraints.append(('project', 'CORDEX'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tas'))
        constraints.append(('experiment', 'historical'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=10,
            offset=0,
            constraints=constraints)

        assert len(result) > 1

    @pytest.mark.online
    def test_file_cordex_date(self):
        constraints = []
        constraints.append(('project', 'CORDEX'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tas'))
        constraints.append(('experiment', 'historical'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=10,
            offset=0,
            temporal=True,
            start=date(2001, 1, 1),
            end=date(2005, 12, 31),
            constraints=constraints)

        assert len(result) > 1
        assert summary['number_of_selected_files'] > 1

    @pytest.mark.online
    def test_file_cordex_many(self):
        constraints = []
        constraints.append(('project', 'CORDEX'))
        constraints.append(('time_frequency', 'day'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=100,
            offset=0,
            temporal=False,
            constraints=constraints)

        assert len(result) > 1
        assert summary['number_of_selected_files'] > 1

    @pytest.mark.online
    def test_file_cordex_fx(self):
        constraints = []
        constraints.append(('project', 'CORDEX'))
        constraints.append(('time_frequency', 'fx'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=1,
            offset=0,
            temporal=False,
            start='1960-01-01T12:00:00Z',
            end='1970-12-31T12:00:00Z',
            constraints=constraints)

        assert len(result) == 1
        assert summary['number_of_selected_files'] == 1

    @pytest.mark.online
    def test_file_cordex_fly(self):
        constraints = []
        constraints.append(('project', 'CORDEX'))
        constraints.append(('time_frequency', 'day'))
        constraints.append(('variable', 'tas'))
        constraints.append(('variable', 'tasmax'))
        constraints.append(('variable', 'tasmin'))
        constraints.append(('variable', 'pr'))
        constraints.append(('variable', 'prsn'))
        constraints.append(('experiment', 'historical'))
        constraints.append(('experiment', 'rcp26'))
        constraints.append(('experiment', 'rcp45'))
        constraints.append(('experiment', 'rcp85'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=100,
            offset=0,
            temporal=True,
            start=date(1960, 1, 1),
            end=date(1970, 12, 31),
            constraints=constraints)

        assert len(result) >= 1
        assert summary['number_of_selected_files'] >= 1

    @pytest.mark.online
    def test_file_cmip5(self):
        constraints = []
        constraints.append(('project', 'CMIP5'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tas'))
        constraints.append(('experiment', 'historical'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=10,
            offset=0,
            constraints=constraints)

        assert len(result) > 1
        assert summary['number_of_selected_files'] > 1

    @pytest.mark.online
    def test_file_cmip5_date(self):
        constraints = []
        constraints.append(('project', 'CMIP5'))
        constraints.append(('time_frequency', 'mon'))
        constraints.append(('variable', 'tas'))
        constraints.append(('experiment', 'historical'))

        (result, summary, facet_counts) = self.esgsearch.search(
            search_type='File',
            limit=20,
            offset=0,
            constraints=constraints,
            temporal=False,
            start=date(1960, 1, 1),
            end=date(1995, 12, 31),)

        assert len(result) > 1
        assert summary['number_of_selected_files'] > 1
