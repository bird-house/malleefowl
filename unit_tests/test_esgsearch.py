import nose.tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from owslib.wps import monitorExecution

from __init__ import TESTDATA, SERVICE

class WpsTestCase(TestCase):
    """
    Base TestCase class, sets up a wps
    """

    @classmethod
    def setUpClass(cls):
        from owslib.wps import WebProcessingService
        cls.wps = WebProcessingService(SERVICE, verbose=False, skip_caps=False)

class WgetTestCase(WpsTestCase):

    @attr('online')
    def test_esgsearch_datasets(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '10'))
        inputs.append(('offset', '10'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))

        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)

    @attr('online')
    def test_esgsearch_datasets_with_spaces(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '10'))
        inputs.append(('offset', '10'))
        inputs.append(
            ('constraints', ' project: CORDEX, time_frequency : mon,variable:tas,  experiment:historical  '))

        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)

    @attr('online')
    def test_esgsearch_datasets_out_of_limit(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '100'))
        inputs.append(('offset', '99'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))

        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)

    @attr('online')
    def test_esgsearch_datasets_out_of_offset(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '1'))
        inputs.append(('offset', '1000'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))

        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)

    @attr('online')
    def test_esgsearch_datasets_replica(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '100'))
        inputs.append(('offset', '0'))
        inputs.append(('replica', 'True'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))

        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)
        #nose.tools.ok_(False, "TODO: check replica")

    @attr('online')
    def test_esgsearch_datasets_latest(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '100'))
        inputs.append(('offset', '0'))
        inputs.append(('latest', 'False'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))

        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)
        #nose.tools.ok_(False, "TODO: check latest")


    @attr('online')
    def test_esgsearch_datasets_facet_counts(self):
        inputs = []
        inputs.append(('search_type', 'Dataset'))
        inputs.append(('limit', '1'))
        inputs.append(('offset', '0'))
        inputs.append(('latest', 'True'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon'))

        output=[('facet_counts', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)
        #nose.tools.ok_(False, "TODO: check facets")

        
    @attr('online')
    def test_esgsearch_aggregations(self):
        inputs = []
        inputs.append(('search_type', 'Aggregation'))
        inputs.append(('limit', '5'))
        inputs.append(('offset', '20'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))
        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)

    @attr('online')
    def test_esgsearch_files(self):
        inputs = []
        inputs.append(('search_type', 'File'))
        inputs.append(('limit', '1'))
        inputs.append(('offset', '30'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))
        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)
    




