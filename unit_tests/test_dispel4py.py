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
    
from dispel4py.core import GenericPE, NAME, TYPE, GROUPING
    
class ESGSearchProducer(GenericPE):
    def __init__(self, monitor, constraints="project:CORDEX"):
        GenericPE.__init__(self)
        self.monitor = monitor
        self.constraints = constraints
        self.outputconnections["output"] = dict(NAME="output", TYPE=['string'])
        
    def process(self, inputs=None):
        self.monitor('doing esgsearch')
        from owslib.wps import WebProcessingService, monitorExecution
        wps = WebProcessingService("http://localhost:8091/wps")
        execution = wps.execute(identifier='esgsearch', inputs=[('constraints', self.constraints)], output=[('output', True)])
        monitorExecution(execution)
        outputs = { 'output' : [execution.processOutputs[0].reference] }
        return outputs

class WPSWorker(GenericPE):
    def __init__(self, url, identifier, inputs, outputs, monitor):
        GenericPE.__init__(self)
        from owslib.wps import WebProcessingService, monitorExecution
        self.wps = WebProcessingService(self.url)
        self.identifier = identifier
        self.inputs = inputs
        self.outputs = outputs
        self.monitor = monitor
        self.inputconnections['resource'] = { NAME : 'resource' }
        self.outputconnections['output'] = { NAME : 'output'}
    
    def process(self, inputs):
        self.monitor('doing wps')
        execution = wps.execute(identifier=selfidentifier,
                                inputs=self.inputs,
                                output=self.outputs)
        monitorExecution(execution)
        outputs = { 'output' : [execution.processOutputs[0].reference] }
        return outputs

class ShowResultConsumer(GenericPE):
    def __init__(self, monitor):
        self.monitor = monitor
        GenericPE.__init__(self)
        self.inputconnections['input'] = { NAME : 'input'}
        self.outputconnections['output'] = { NAME : 'output'}

    def process(self, inputs):
        self.monitor('very busy today')
        print inputs['input']
        outputs = {'output' : inputs['input']}
        return outputs
    
def monitor(message):
    print message
    
def test_esgsearch():
    graph = WorkflowGraph()
    esgsearch = ESGSearchProducer(monitor)
    show = ShowResultConsumer(monitor)
    graph.connect(esgsearch, 'output', show, 'input')
    results = simple_process.process(graph, {esgsearch: [{}]})
    tools.ok_(False, results)
    
    
    




