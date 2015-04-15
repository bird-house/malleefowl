import nose.tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from owslib.wps import monitorExecution

from __init__ import TESTDATA, SERVICE, CREDENTIALS

class EsgfWorkflowTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        from owslib.wps import WebProcessingService
        cls.wps = WebProcessingService(SERVICE, verbose=False, skip_caps=False)
        cls.nodes = cls.setup_nodes()

    @classmethod
    def setup_nodes(cls):
        constraints = [('project', 'CORDEX'),
                   ('experiment', 'historical'),
                   ('variable', 'tasmax'),
                   ('time_frequency', 'day'),
                   ('ensemble', 'r1i1p1'),
                   ('institute', 'MPI-CSC'),
                   ('domain', 'WAS-44')]
        cs_str = ','.join( ['%s:%s' % (key, value) for key, value in constraints] )
        # TODO: change facets keyword
        esgsearch = dict(
            facets=cs_str,
            limit=1,
            temporal=True,
            distrib=False,
            latest=True,
            replica=False,
            start='2001-01-01T12:00:00Z',
            end='2005-12-31T12:00:00Z',
        )
        source = dict(
            service = SERVICE,
            credentials = CREDENTIALS,
            )
        worker = dict(
            service = SERVICE,
            identifier = "dummy",
            inputs = [],
            resource = 'resource')
        NODES = dict(esgsearch=esgsearch, source=source, worker=worker)

        # TODO: fix json encoding to unicode
        import yaml
        raw = yaml.dump(NODES)
        nodes = yaml.load(raw)
        return nodes

    @attr('online')
    def test_esgf_workflow_dummy(self):
        inputs = [('nodes', str(self.nodes))]
        execution = self.wps.execute(identifier="dispel", inputs=inputs, output=[('output', True)])
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)
    



