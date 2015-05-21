from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process
from dispel4py.core import NAME, TYPE, GROUPING
from dispel4py.base import BasePE

from malleefowl.config import wps_url

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class GenericWPS(BasePE):
    INPUT_NAME = 'input'
    OUTPUT_NAME = 'output'
    STATUS_NAME = 'status'
    STATUS_LOCATION_NAME = 'status_location'
    
    def __init__(self, url, identifier, resource='resource', inputs=[], output=None):
        BasePE.__init__(self)
        self._add_input(GenericWPS.INPUT_NAME)
        self._add_output(GenericWPS.OUTPUT_NAME)
        self._add_output(GenericWPS.STATUS_NAME)
        self._add_output(GenericWPS.STATUS_LOCATION_NAME)
        
        from owslib.wps import WebProcessingService
        self.wps = WebProcessingService(url=url, skip_caps=True)
        self.identifier = identifier
        self.wps_resource = resource
        self.wps_inputs = inputs
        self.wps_output = output

        def log(message):
            if self._monitor:
                self._monitor("{0}: {1}".format(self.identifier, message), self._progress)
            else:
                logger.info('STATUS ({0}: {2}/100) - {1}'.format(self.identifier, message, self._progress))
        self.log = log
        self._monitor = None
        self._progress = 0
        self._pstart = 0
        self._pend = 100

    def set_monitor(self, monitor, start_progress=0, end_progress=100):
        self._monitor = monitor
        self._pstart = start_progress
        self._pend = end_progress

    def update_progress(self, execution):
        self._progress = int( self._pstart + ( (self._pend - self._pstart) / 100.0 * execution.percentCompleted ) )
            
    def monitor_execution(self, execution):
        self.update_progress(execution)
        self.log("status_location={0.statusLocation}".format(execution))
        
        while execution.isComplete() == False:
            execution.checkStatus(sleepSecs=1)
            self.update_progress(execution)
            self.log(execution.statusMessage)

        if execution.isSucceded():
            for output in execution.processOutputs:               
                if output.reference is not None:
                    self.log( '{0.identifier}={0.reference} ({0.mimeType})'.format(output) )
                else:
                    self.log( '{0}={1}'.format(output.identifier, ", ".join(output.data) ) )
        else:
            msg = '\n'.join(['code={0.code}, locator={0.locator}, text={0.text}'.format(ex) for ex in execution.errors])
            self.log(msg)
            raise Exception(msg)

    def execute(self):
        output = [] # default: all outputs
        if self.wps_output is not None:
            output.append((self.wps_output, True))
        execution = self.wps.execute(
            identifier=self.identifier,
            inputs=self.wps_inputs,
            output=output)
        self.monitor_execution(execution)

        result = dict(status=execution.status, status_location=execution.statusLocation)
        # only set output if specific output was requested
        # TODO: use generic dispel outputs matching process description?
        if self.wps_output is not None:
            if len(execution.processOutputs) > 0:
                result[self.OUTPUT_NAME] = execution.processOutputs[0].reference
            else:
                raise Exception('Process %s could not produce output %s' % (self.identifier, self.wps_output))
        return result
    
    def process(self, inputs):
        if inputs.has_key(self.INPUT_NAME):
            for value in inputs[self.INPUT_NAME]:
                self.wps_inputs.append((self.wps_resource, str(value)))
        try:
            result = self._process(inputs)
            if result is not None:
                return result
        except Exception:
            logger.exception('Failed to execute process.')
            pass

    def _process(self, inputs):
        return self.execute()

class EsgSearch(GenericWPS):
    def __init__(self, url,
                 constraints='project:CORDEX',
                 limit=100,
                 search_type='File_Thredds',
                 distrib=False,
                 replica=False,
                 latest=True,
                 temporal=False,
                 start=None,
                 end=None):
        GenericWPS.__init__(self, url, 'esgsearch', output='output')
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
            raise Exception('Could not find any files.')
        result['output'] = urls
        return result

class Download(GenericWPS):
    def __init__(self, url, credentials=None):
        GenericWPS.__init__(self, url, 'download', output='output')
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

class SwiftDownload(GenericWPS):
    def __init__(self, url, storage_url, auth_token, container, prefix):
        GenericWPS.__init__(self, url, 'swift_download', output='output')
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

def esgf_workflow(source, worker, monitor=None):
    graph = WorkflowGraph()
    
    esgsearch = EsgSearch(
        url=wps_url(),
        constraints=source.get('facets'),
        limit=source.get('limit', 100),
        search_type='File_Thredds',
        distrib=source.get('distrib'),
        replica=source.get('replica'),
        latest=source.get('latest'),
        temporal=source.get('temporal'),
        start=source.get('start'),
        end=source.get('end'))
    esgsearch.set_monitor(monitor, 0, 10)
    download = Download(url=wps_url(), credentials=source.get('credentials'))
    download.set_monitor(monitor, 10, 50)
    doit = GenericWPS(**worker)
    doit.set_monitor(monitor, 50, 100)

    # TODO: handle exceptions ... see dispel docs
    graph.connect(esgsearch, esgsearch.OUTPUT_NAME, download, download.INPUT_NAME)
    graph.connect(download, download.OUTPUT_NAME, doit, doit.INPUT_NAME)

    result = simple_process.process(graph, inputs={ esgsearch : [{}] })
    if not result.has_key( (esgsearch.id, esgsearch.STATUS_LOCATION_NAME) ):
        raise Exception("Failed to find files on ESGF.")
    if not result.has_key( (download.id, download.STATUS_LOCATION_NAME) ):
        raise Exception("Failed to download files.")
    if not result.has_key( (doit.id, doit.STATUS_LOCATION_NAME) ):
        raise Exception("Failed to run process.")
    return dict(source=result.get( (download.id, download.STATUS_LOCATION_NAME) ),
                worker=result.get( (doit.id, doit.STATUS_LOCATION_NAME) ))

def swift_workflow(source, worker, monitor=None):
    graph = WorkflowGraph()

    download = SwiftDownload(url=wps_url(), **source)
    download.set_monitor(monitor, 0, 50)
    doit = GenericWPS(**worker)
    doit.set_monitor(monitor, 50, 100)

    graph.connect(download, download.OUTPUT_NAME, doit, doit.INPUT_NAME)

    result = simple_process.process(graph, inputs={ download : [{}] })
    if not result.has_key( (download.id, 'status_location') ):
        raise Exception("Failed to download files.")
    if not result.has_key( (doit.id, 'status_location') ):
        raise Exception("Failed to run process.")
    return dict(source=result[(download.id, 'status_location')],
                worker=result[(doit.id, 'status_location')])

def run(workflow, monitor=None):
    if workflow['source'].has_key('swift'):
        result = swift_workflow(source=workflow['source']['swift'], worker=workflow['worker'], monitor=monitor)
    else:
        result = esgf_workflow(source=workflow['source']['esgf'], worker=workflow['worker'], monitor=monitor)
    return result
    

