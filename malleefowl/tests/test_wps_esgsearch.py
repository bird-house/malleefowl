import pytest
from pywps import Service
from pywps.tests import assert_response_success

from .common import TESTDATA, client_for

from malleefowl.processes.wps_esgsearch import ESGSearchProcess


@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.online
def test_dataset():
    client = client_for(Service(processes=[ESGSearchProcess()]))
    datainputs = "url={};search_type={};limit={};offset={};constraints={}".format(
        'http://esgf-data.dkrz.de/esg-search',
        'Dataset', '10', '10',
        'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical')
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='esgsearch',
        datainputs=datainputs)
    print resp.get_data()
    assert_response_success(resp)

"""
    @pytest.mark.online
    def test_dataset_with_spaces(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '10'))
        inputs.append(('offset', '10'))
        inputs.append(
            ('constraints', ' project: CORDEX, time_frequency : mon,variable:tas,  experiment:historical  '))

        output = [('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        assert execution.status == 'ProcessSucceeded'

    @pytest.mark.online
    def test_dataset_out_of_limit(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '100'))
        inputs.append(('offset', '99'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))

        output = [('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        assert execution.status == 'ProcessSucceeded'

    @pytest.mark.online
    def test_dataset_out_of_offset(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '1'))
        inputs.append(('offset', '1000'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))

        output = [('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        assert execution.status == 'ProcessSucceeded'

    @pytest.mark.online
    def test_dataset_latest(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '100'))
        inputs.append(('offset', '0'))
        inputs.append(('latest', 'False'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))

        output = [('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        assert execution.status == 'ProcessSucceeded'

    @pytest.mark.online
    def test_dataset_facet_counts(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '1'))
        inputs.append(('offset', '0'))
        inputs.append(('latest', 'True'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon'))

        output = [('facet_counts', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        assert execution.status == 'ProcessSucceeded'

    @pytest.mark.online
    def test_dataset_query(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '1'))
        inputs.append(('offset', '0'))
        inputs.append(('latest', 'True'))
        inputs.append(('constraints', 'project:CORDEX'))
        inputs.append(('query', 'geopotential'))

        output = [('facet_counts', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        assert execution.status == 'ProcessSucceeded'

    @pytest.mark.online
    def test_aggregation(self):
        inputs = []
        inputs.append(('search_type', 'Aggregation'))
        inputs.append(('limit', '5'))
        inputs.append(('offset', '20'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))
        output = [('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        assert execution.status == 'ProcessSucceeded'

    @pytest.mark.online
    def test_file(self):
        inputs = []
        inputs.append(('search_type', 'File'))
        inputs.append(('limit', '1'))
        inputs.append(('offset', '30'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))
        output = [('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        assert execution.status == 'ProcessSucceeded'
"""
