from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process
from dispel4py.base import BasePE

from malleefowl.config import wps_url

import logging
logger = logging.getLogger(__name__)

class MonitorPE(BasePE):
    INPUT_NAME = 'input'
    OUTPUT_NAME = 'output'

    def __init__(self, output=None):
        BasePE.__init__(self)
        self._add_input(self.INPUT_NAME)  
        self._add_output(self.OUTPUT_NAME)

        def log(message, progress):
            if self._monitor:
                self._monitor("{0}: {1}".format(self.identifier, message), progress)
            else:
                logger.info('STATUS ({0}: {2}/100) - {1}'.format(self.identifier, message, progress))
        self.monitor = log
        self._monitor = None
        self._pstart = 0
        self._pend = 100

    def set_monitor(self, monitor, start_progress=0, end_progress=100):
        self._monitor = monitor
        self._pstart = start_progress
        self._pend = end_progress


class GenericWPS(MonitorPE):
    STATUS_NAME = 'status'
    STATUS_LOCATION_NAME = 'status_location'
    
    def __init__(self, url, identifier, resource='resource', inputs=[], output=None):
        MonitorPE.__init__(self)
        self._add_output(self.STATUS_NAME)
        self._add_output(self.STATUS_LOCATION_NAME)
        
        from owslib.wps import WebProcessingService
        self.wps = WebProcessingService(url=url, skip_caps=True, verify=False)
        self.identifier = identifier
        self.wps_resource = resource
        self.wps_inputs = inputs
        self.wps_output = output

    def progress(self, execution):
        return int( self._pstart + ( (self._pend - self._pstart) / 100.0 * execution.percentCompleted ) )
            
    def monitor_execution(self, execution):
        progress = self.progress(execution)
        self.monitor("status_location={0.statusLocation}".format(execution), progress)
        
        while execution.isNotComplete():
            try:
                execution.checkStatus(sleepSecs=3)
            except:
                logger.exception("Could not read status xml document.")
            else:
                progress = self.progress(execution)
                self.monitor(execution.statusMessage, progress)

        if execution.isSucceded():
            for output in execution.processOutputs:               
                if output.reference is not None:
                    self.monitor( '{0.identifier}={0.reference} ({0.mimeType})'.format(output), progress )
                else:
                    self.monitor( '{0}={1}'.format(output.identifier, ", ".join(output.data) ), progress )
        else:
            self.monitor('\n'.join(['ERROR: {0.text} (code={0.code}, locator={0.locator})'.format(ex) for ex in execution.errors]), progress )

    def execute(self):
        output = []
        if self.wps_output is not None:
            output=[ (self.wps_output, True) ]
        logger.info("execute inputs=%s", self.wps_inputs)
        execution = self.wps.execute(
            identifier=self.identifier,
            inputs=self.wps_inputs,
            output=output)
        self.monitor_execution(execution)

        result = {self.STATUS_NAME:execution.status,
                  self.STATUS_LOCATION_NAME:execution.statusLocation}
        if execution.isSucceded():
            # NOTE: only set workflow output if specific output was requested
            if self.wps_output is not None:
                for output in execution.processOutputs:
                    if self.wps_output == output.identifier:
                        result[self.OUTPUT_NAME] = output.reference
                        break
            return result
        else:
            failure_msg = '\n'.join(['{0.text}'.format(ex) for ex in execution.errors])
            raise Exception(failure_msg)
    
    def process(self, inputs):
        if inputs.has_key(self.INPUT_NAME):
            for value in inputs[self.INPUT_NAME]:
                self.wps_inputs.append((self.wps_resource, str(value)))
        try:
            result = self._process(inputs)
            if result is not None:
                return result
        except Exception:
            import traceback
            logger.error(traceback.format_exc())
            raise

    def _process(self, inputs):
        return self.execute()

    
class EsgSearch(GenericWPS):
    def __init__(self, url,
                 search_url='https://esgf-data.dkrz.de/esg-search',
                 constraints='project:CORDEX',
                 limit=100,
                 search_type='File',
                 distrib=False,
                 replica=False,
                 latest=True,
                 temporal=False,
                 start=None,
                 end=None):
        GenericWPS.__init__(self, url, 'esgsearch', output='output')
        self.search_url = search_url
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
        self.wps_inputs.append( ('url', self.search_url) )
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
        result[self.OUTPUT_NAME] = urls
        return result


class SolrSearch(MonitorPE):
    """
    Run search against birdhouse solr index and return a list of download urls.
    """
    def __init__(self, url, query, filter_query=None):
        MonitorPE.__init__(self)
        self.url = url
        self.query= query
        self.filter_query = filter_query

        
    def process(self, inputs):
        import pysolr
        solr = pysolr.Solr(self.url, timeout=60)
        options = {'start':0, 'rows':1024}
        if self.filter_query:
            options['fq'] = self.filter_query
        search_result = solr.search(self.query, **options)
        urls = []
        for item in search_result:
            if 'url' in item:
                urls.append(item['url'])
        if len(urls) == 0:
            raise Exception('Could not find any files.')
        elif len(urls) != search_result.hits:
            logger.warn('Not all found items from solr search have a download url.')
        result = {}
        result[self.OUTPUT_NAME] = urls
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
            raise Exception('Could not download any files.')
        result['output'] = urls
        return result

    
class SwiftDownload(GenericWPS):
    def __init__(self, url, storage_url, auth_token, container, prefix=None):
        GenericWPS.__init__(self, url, 'swift_download', output='output')
        self.storage_url = storage_url
        self.auth_token = auth_token
        self.container = container
        self.prefix = prefix

    def _process(self, inputs):
        self.wps_inputs.append( ('storage_url', self.storage_url) )
        self.wps_inputs.append( ('auth_token', self.auth_token) )
        self.wps_inputs.append( ('container', self.container) )
        if self.prefix is not None:
            self.wps_inputs.append( ('prefix', self.prefix) )
        result = self.execute()

        # read json document with list of urls
        import json
        import urllib2
        urls = json.load(urllib2.urlopen(result['output']))
        if len(urls) == 0:
            raise Exception('Could not download any files.')
        result['output'] = urls
        return result

    
class ThreddsDownload(GenericWPS):
    def __init__(self, url, catalog_url):
        GenericWPS.__init__(self, url, 'thredds_download', output='output')
        self.catalog_url = catalog_url

    def _process(self, inputs):
        self.wps_inputs.append( ('url', self.catalog_url) )
        result = self.execute()

        # read json document with list of urls
        import json
        import urllib2
        urls = json.load(urllib2.urlopen(result['output']))
        if len(urls) == 0:
            raise Exception('Could not download any files.')
        result['output'] = urls
        return result

    
def esgf_workflow(source, worker, monitor=None):
    graph = WorkflowGraph()

    # TODO: configure limit
    esgsearch = EsgSearch(
        url=wps_url(),
        search_url=source.get('url', 'https://esgf-data.dkrz.de/esg-search'),
        constraints=source.get('facets'),
        limit=source.get('limit', 100),
        search_type='File',
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

    graph.connect(esgsearch, esgsearch.OUTPUT_NAME, download, download.INPUT_NAME)
    graph.connect(download, download.OUTPUT_NAME, doit, doit.INPUT_NAME)

    result = simple_process.process(graph, inputs={ esgsearch : [{}] })
    
    status_location = result.get( (doit.id, doit.STATUS_LOCATION_NAME) )[0]
    status = result.get( (doit.id, doit.STATUS_NAME) )[0]
    return dict(worker=dict(status_location=status_location, status=status))


def swift_workflow(source, worker, monitor=None):
    graph = WorkflowGraph()

    download = SwiftDownload(url=wps_url(), **source)
    download.set_monitor(monitor, 0, 50)
    doit = GenericWPS(**worker)
    doit.set_monitor(monitor, 50, 100)

    graph.connect(download, download.OUTPUT_NAME, doit, doit.INPUT_NAME)

    result=simple_process.process(graph, inputs={ download : [{}] })
    
    status_location = result.get( (doit.id, doit.STATUS_LOCATION_NAME) )[0]
    status = result.get( (doit.id, doit.STATUS_NAME) )[0]
    return dict(worker=dict(status_location=status_location, status=status))


def thredds_workflow(source, worker, monitor=None):
    graph = WorkflowGraph()
    
    download = ThreddsDownload(url=wps_url(), **source)
    download.set_monitor(monitor, 10, 50)
    doit = GenericWPS(**worker)
    doit.set_monitor(monitor, 50, 100)

    graph.connect(download, download.OUTPUT_NAME, doit, doit.INPUT_NAME)

    result = simple_process.process(graph, inputs={ download : [{}] })
    
    status_location = result.get( (doit.id, doit.STATUS_LOCATION_NAME) )[0]
    status = result.get( (doit.id, doit.STATUS_NAME) )[0]
    return dict(worker=dict(status_location=status_location, status=status))


def solr_workflow(source, worker, monitor=None):
    graph = WorkflowGraph()

    solrsearch = SolrSearch(
        url = source.get('url'),
        query=source.get('query'),
        filter_query=source.get('filter_query'))
    solrsearch.set_monitor(monitor, 0, 10)
    download = Download(url=wps_url())
    download.set_monitor(monitor, 10, 50)
    doit = GenericWPS(**worker)
    doit.set_monitor(monitor, 50, 100)

    graph.connect(solrsearch, solrsearch.OUTPUT_NAME, download, download.INPUT_NAME)
    graph.connect(download, download.OUTPUT_NAME, doit, doit.INPUT_NAME)

    result = simple_process.process(graph, inputs={ solrsearch : [{}] })
    
    status_location = result.get( (doit.id, doit.STATUS_LOCATION_NAME) )[0]
    status = result.get( (doit.id, doit.STATUS_NAME) )[0]
    return dict(worker=dict(status_location=status_location, status=status))


def run(workflow, monitor=None):
    if 'swift' in workflow['source']:
        return swift_workflow(source=workflow['source']['swift'], worker=workflow['worker'], monitor=monitor)
    elif 'thredds' in workflow['source']:
        return thredds_workflow(source=workflow['source']['thredds'], worker=workflow['worker'], monitor=monitor)
    elif 'esgf' in workflow['source']:
        return esgf_workflow(source=workflow['source']['esgf'], worker=workflow['worker'], monitor=monitor)
    elif 'solr' in workflow['source']:
        return solr_workflow(source=workflow['source']['solr'], worker=workflow['worker'], monitor=monitor)
    else:
        raise Exception("Unknown workflow type")

    

