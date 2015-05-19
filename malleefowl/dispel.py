from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process
from dispel4py.core import GenericPE, NAME, TYPE, GROUPING

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class BaseWPS(GenericPE):
    def __init__(self, url, identifier, resource='resource', inputs=[], output=None):
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
        output = [] # default: all outputs
        if self.wps_output is not None:
            output.append((self.wps_output, True))
        execution = self.wps.execute(
            identifier=self.identifier,
            inputs=self.wps_inputs,
            output=output)
        self.monitor_execution(execution)

        result = {}
        # only set output if specific output was requested
        # TODO: use generic dispel outputs matching process description?
        if self.wps_output is not None:
            if len(execution.processOutputs) > 0:
                result['output'] = execution.processOutputs[0].reference
            else:
                raise Exception('Process %s could not produce output %s' % (self.identifier, self.wps_output))
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
        result = self.execute()
        return result

class EsgSearch(BaseWPS):
    def __init__(self, url,
                 constraints='project:CORDEX',
                 limit=100,
                 search_type='File_Thredds',
                 distrib=False,
                 replica=False,
                 latest=True,
                 temporal=False,
                 start=None,
                 end=None,):
        BaseWPS.__init__(self, url, 'esgsearch', output='output')
        self.constraints = constraints
        self.distrib = distrib
        self.replica = replica
        self.latest = latest
        self.limit = limit
        self.temporal = temporal
        self.start = start
        self.end = end
        self.search_type = search_type

    def _process(self, inputs):
        self.wps_inputs.append( ('constraints', self.constraints) )
        self.wps_inputs.append( ('limit', str(self.limit)) )
        self.wps_inputs.append( ('search_type', self.search_type) )
        self.wps_inputs.append( ('distrib', str(self.distrib)) )
        self.wps_inputs.append( ('replica', str(self.replica)) )
        self.wps_inputs.append( ('latest', str(self.latest)) )
        self.wps_inputs.append( ('temporal', str(self.temporal)) )
        if self.start != None and self.end != None:
            self.wps_inputs.append( ('start', str(self.start)) )
            self.wps_inputs.append( ('end', str(self.end)) )
        result = self.execute()

        # read json document with list of urls
        import json
        import urllib2
        urls = json.load(urllib2.urlopen(result['output']))
        if len(urls) == 0:
            raise Exception('Could not retrieve any files')
        result['output'] = urls
        return result

class Download(BaseWPS):
    def __init__(self, url, credentials=None):
        BaseWPS.__init__(self, url, 'download', output='output')
        self.credentials = credentials

    def _process(self, inputs):
        if self.credentials:
            self.wps_inputs.append( ('credentials', self.credentials) )
        result = self.execute()

        # read json document with list of urls
        import json
        import urllib2
        urls = json.load(urllib2.urlopen(result['output']))
        if len(urls) == 0:
            raise Exception('Could not retrieve any files')
        result['output'] = urls
        return result

class SwiftDownload(BaseWPS):
    def __init__(self, url, storage_url, auth_token, container, prefix):
        BaseWPS.__init__(self, url, 'swift_download', output='output')
        self.storage_url = storage_url
        self.auth_token = auth_token
        self.container = container
        self.prefix = prefix

    def _process(self, inputs):
        self.wps_inputs.append( ('storage_url', self.storage_url) )
        self.wps_inputs.append( ('auth_token', self.auth_token) )
        self.wps_inputs.append( ('container', self.container) )
        self.wps_inputs.append( ('prefix', self.prefix) )
        result = self.execute()

        # read json document with list of urls
        import json
        import urllib2
        urls = json.load(urllib2.urlopen(result['output']))
        if len(urls) == 0:
            raise Exception('Could not retrieve any files')
        result['output'] = urls
        return result

def esgsearch_workflow(url, esgsearch_params, download_params, doit_params, monitor=None):
    graph = WorkflowGraph()

    esgsearch = EsgSearch(url, **esgsearch_params)
    esgsearch.set_monitor(monitor)
    download = Download(url, **download_params)
    download.set_monitor(monitor)
    doit = GenericWPS(**doit_params)
    doit.set_monitor(monitor)

    # TODO: handle exceptions ... see dispel docs
    graph.connect(esgsearch, 'output', download, 'resource')
    graph.connect(download, 'output', doit, 'resource')

    result = simple_process.process(graph, inputs={ esgsearch : [{}] })
    wf_result = dict(source=result[(download.id, 'status_location')],
                     worker=result[(doit.id, 'status_location')])
    
    return wf_result

def swift_workflow(url, download_params, doit_params, monitor=None):
    graph = WorkflowGraph()

    logger.debug("download params = %s", download_params)

    download = SwiftDownload(url, **download_params)
    download.set_monitor(monitor)
    doit = GenericWPS(**doit_params)
    doit.set_monitor(monitor)

    graph.connect(download, 'output', doit, 'resource')

    result = simple_process.process(graph, inputs={ download : [{}] })
    wf_result = dict(source=result[(download.id, 'status_location')],
                     worker=result[(doit.id, 'status_location')])
    
    return wf_result

