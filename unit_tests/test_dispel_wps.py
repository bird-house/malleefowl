import nose.tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from owslib.wps import monitorExecution

from __init__ import TESTDATA, SERVICE, CREDENTIALS

class WpsTestCase(TestCase):
    """
    Base TestCase class, sets up a wps
    """

    @classmethod
    def setUpClass(cls):
        from owslib.wps import WebProcessingService
        cls.wps = WebProcessingService(SERVICE, verbose=False, skip_caps=False)
        cls.nodes = cls.setup_nodes()

    @classmethod
    def setup_nodes(cls):
        source = dict(
            service = SERVICE,
            credentials = CREDENTIALS,
            )
        worker = dict(
            service = SERVICE,
            identifier = "dummy",
            inputs = [],
            resource = 'resource')
        NODES = dict(source=source, worker=worker)

        # TODO: fix json encoding to unicode
        import yaml
        raw = yaml.dump(NODES)
        nodes = yaml.load(raw)
        return nodes

class DispelTestCase(WpsTestCase):

    @attr('online')
    def test_dispel_cdo_sinfo(self):
        inputs = [('nodes', str(self.nodes))]
        execution = self.wps.execute(identifier="dispel", inputs=inputs, output=[('output', True)])
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)
    



