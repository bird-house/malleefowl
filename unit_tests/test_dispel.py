from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process

from malleefowl.dispel import GenericWPS, EsgSearch, Wget

from nose import tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from __init__ import SERVICE, TESTDATA, CREDENTIALS

@attr('online')
@attr('security')
def test_esgsearch():
    # TODO: set environ with credentials
    graph = WorkflowGraph()
    esgsearch = EsgSearch(url=SERVICE)
    download = Wget(url=SERVICE,
                    credentials=CREDENTIALS)
    doit = GenericWPS(
        url='http://localhost:8092/wps',
        identifier='cdo_sinfo',
        resource='netcdf_file',
        )
    graph.connect(esgsearch, 'output', download, 'resource')
    graph.connect(download, 'output', doit, 'resource')
    results = simple_process.process(graph, {
        esgsearch: [{'constraints' : 'project:CORDEX,experiment:historical,variable:tas,time_frequency:mon'}]})
    tools.ok_(False, results)
    
    
    




