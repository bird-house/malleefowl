from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process

from malleefowl.dispel import WebProcessingServicePE

from nose import tools

from __init__ import SERVICE, TESTDATA

def test_esgsearch():
    graph = WorkflowGraph()
    print 'before esgsearch'
    esgsearch = WebProcessingServicePE(
        url=SERVICE,
        identifier='esgsearch',
        inputs=[('constraints', 'project:CORDEX')]
        )
    print 'before download'
    download = WebProcessingServicePE(
        url=SERVICE,
        identifier='wget',
        inputs=[('resource',
                 'http://localhost:8090/wpscache/tasmax_WAS-44_MPI-M-MPI-ESM-LR_historical_r1i1p1_MPI-CSC-REMO2009_v1_day_20010101-20051231.nc' )]
        )
    print 'before doit'
    doit = WebProcessingServicePE(
        url='http://localhost:8092/wps',
        identifier='cdo_sinfo',
        resource='netcdf_file',
        )
    graph.connect(esgsearch, 'output', download, 'ignore')
    graph.connect(download, 'output', doit, 'resource_url')
    results = simple_process.process(graph, {esgsearch: [{}]})
    tools.ok_(False, results)
    
    
    




