from dispel4py.core import GenericPE, NAME, TYPE, GROUPING

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class WebProcessingServicePE(GenericPE):
    def __init__(self, url, identifier, inputs, output='output'):
        GenericPE.__init__(self)
        from owslib.wps import WebProcessingService
        self.wps = WebProcessingService(url)
        self.identifier = identifier
        self.inputs = inputs
        self.output = output
        self.inputconnections['resource'] = { NAME : 'resource' }
        self.outputconnections['output'] = { NAME : 'output'}
    
    def process(self, inputs):
        from owslib.wps import monitorExecution
        execution = self.wps.execute(
            identifier=self.identifier,
            inputs=self.inputs,
            output=[(self.output, True)])
        monitorExecution(execution)
        outputs = { 'output' : [execution.processOutputs[0].reference] }
        return outputs
