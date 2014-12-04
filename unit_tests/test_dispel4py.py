from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process
from dispel4py.examples.graph_testing.testing_PEs import TestOneInOneOut

from malleefowl.dispel import WebProcessingServicePE

from nose import tools

from __init__ import SERVICE

def test_esgsearch():
    graph = WorkflowGraph()
    esgsearch = WebProcessingServicePE(
        url=SERVICE,
        identifier='esgsearch',
        inputs=[('constraints', 'project:CORDEX')]
        )
    donothing = TestOneInOneOut()
    graph.connect(esgsearch, 'output', donothing, 'input')
    results = simple_process.process(graph, {esgsearch: [{}]})
    tools.ok_(False, results)
    
    
    




