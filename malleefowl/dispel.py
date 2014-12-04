from dispel4py.core import GenericPE, NAME, TYPE, GROUPING

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class BaseWPS(GenericPE):
    def __init__(self, url, identifier, resource='resource', inputs=[], output='output'):
        GenericPE.__init__(self)
        from owslib.wps import WebProcessingService
        self.wps = WebProcessingService(url)
        self.identifier = identifier
        self.wps_resource = resource
        self.wps_inputs = inputs
        self.wps_output = output
        self.inputconnections['resource'] = { NAME : 'resource' }
        self.outputconnections['output'] = { NAME : 'output'}

    def execute(self):
        from owslib.wps import monitorExecution
        execution = self.wps.execute(
            identifier=self.identifier,
            inputs=self.wps_inputs,
            output=[(self.wps_output, True)])
        monitorExecution(execution)
        result = { 'output' : [execution.processOutputs[0].reference] }
        return result
    
    def process(self, inputs):
        #print inputs.keys()
        #print inputs.values()
        if inputs.has_key('resource'):
            for value in inputs['resource']:
                self.wps_inputs.append((self.wps_resource, str(value)))
        return self._process(inputs)

class GenericWPS(BaseWPS):
    def _process(self, inputs):
        return self.execute()

class EsgSearch(BaseWPS):
    def __init__(self, url):
        BaseWPS.__init__(self, url, 'esgsearch')
        self.inputconnections['constraints'] = { NAME : 'constraints' }

    def _process(self, inputs):
        if inputs.has_key('constraints'):
            self.wps_inputs.append( ('constraints', inputs['constraints']) )
        self.wps_inputs.append( ('limit', '1') )
        self.wps_inputs.append( ('type', 'files') )
        result = self.execute()

        # read json document with list of urls
        import json
        import urllib2
        urls = json.load(urllib2.urlopen(result['output'][0]))
        result['output'] = urls
        return result

class Wget(BaseWPS):
    def __init__(self, url, credentials=None):
        BaseWPS.__init__(self, url, 'wget')
        self.credentials = credentials

    def _process(self, inputs):
        if self.credentials:
            self.wps_inputs.append( ('credentials', self.credentials) )
        result = self.execute()

        # read json document with list of urls
        import json
        import urllib2
        urls = json.load(urllib2.urlopen(result['output'][0]))
        result['output'] = urls
        return result


