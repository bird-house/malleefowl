from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process
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
        self.outputconnections['status'] = { NAME : 'status'}
        self.outputconnections['status_location'] = { NAME : 'status_location'}
        self.monitor = None

    def set_monitor(self, monitor):
        self.monitor = monitor

    def monitor_execution(self, execution):
        logger.debug("status_location = %s", execution.statusLocation)
        
        while execution.isComplete() == False:
            execution.checkStatus(sleepSecs = 3)
            logger.info('Execution status=%s, progress=%s',
                        execution.status, execution.percentCompleted )
            if self.monitor is not None:
                self.monitor(execution)
        
        if execution.isSucceded():
            for output in execution.processOutputs:               
                if output.reference is not None:
                    logger.info('%s=%s (%s)', output.identifier, output.reference, output.mimeType)
                else:
                    logger.info('%s=%s', output.identifier, ", ".join(output.data))
        else:
            for ex in execution.errors:
                logger.error('code=%s, locator=%s, text=%s', ex.code, ex.locator, ex.text)

    def execute(self):
        execution = self.wps.execute(
            identifier=self.identifier,
            inputs=self.wps_inputs,
            output=[(self.wps_output, True)])
        self.monitor_execution(execution)

        result = {}
        result['output'] = execution.processOutputs[0].reference
        result['status'] = execution.status
        result['status_location'] = execution.statusLocation
        return result
    
    def process(self, inputs):
        if inputs.has_key('resource'):
            for value in inputs['resource']:
                self.wps_inputs.append((self.wps_resource, str(value)))
        return self._process(inputs)

class GenericWPS(BaseWPS):
    def _process(self, inputs):
        print inputs
        result = self.execute()
        print result
        return result

class EsgSearch(BaseWPS):
    def __init__(self, url,
                 constraints='project:CORDEX',
                 limit=1,
                 type='files',
                 distrib=False,
                 replica=False,):
        BaseWPS.__init__(self, url, 'esgsearch')
        self.constraints = constraints
        self.distrib = distrib
        self.replica = replica
        self.limit = limit
        self.type = type

    def _process(self, inputs):
        self.wps_inputs.append( ('constraints', self.constraints) )
        self.wps_inputs.append( ('limit', str(self.limit)) )
        self.wps_inputs.append( ('type', self.type) )
        self.wps_inputs.append( ('distrib', str(self.distrib)) )
        self.wps_inputs.append( ('replica', str(self.replica)) )
        result = self.execute()

        # read json document with list of urls
        import json
        import urllib2
        urls = json.load(urllib2.urlopen(result['output']))
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
        urls = json.load(urllib2.urlopen(result['output']))
        result['output'] = urls
        return result

def esgsearch_workflow(url, esgsearch_params, wget_params, doit_params, monitor=None):
    graph = WorkflowGraph()

    esgsearch = EsgSearch(url, **esgsearch_params)
    esgsearch.set_monitor(monitor)
    download = Wget(url, **wget_params)
    download.set_monitor(monitor)
    doit = GenericWPS(**doit_params)
    doit.set_monitor(monitor)

    graph.connect(esgsearch, 'output', download, 'resource')
    graph.connect(download, 'output', doit, 'resource')

    result = simple_process.process(graph, inputs={ esgsearch : [{}] })
    
    return result[(doit.id, 'output')]

