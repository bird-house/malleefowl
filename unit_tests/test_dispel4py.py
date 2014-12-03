from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process
from dispel4py.examples.graph_testing.testing_PEs \
    import TestProducer, TestOneInOneOut, TestTwoInOneOut
from nose import tools

def test_tee():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(prod, 'output', cons2, 'input')
    results = simple_process.process(graph, {prod: [{}, {}, {}, {}, {}]})
    tools.eq_({(cons1.id, 'output'): [1, 2, 3, 4, 5],
               (cons2.id, 'output'): [1, 2, 3, 4, 5]}, results)




