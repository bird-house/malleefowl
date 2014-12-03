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
        inputs.append(('type', 'datasets'))
        inputs.append(('limit', '10'))
        inputs.append(('offset', '10'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))

        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)

    @attr('online')
    def test_esgsearch_aggregations(self):
        inputs = []
        inputs.append(('type', 'aggregations'))
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
        inputs.append(('type', 'files'))
        inputs.append(('limit', '1'))
        inputs.append(('offset', '30'))
        inputs.append(
            ('constraints', 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical'))
        output=[('output', True), ('summary', True)]
        execution = self.wps.execute(identifier="esgsearch", inputs=inputs, output=output)
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)
    




