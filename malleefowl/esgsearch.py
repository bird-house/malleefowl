from datetime import datetime
from dateutil import parser as date_parser

import threading
from Queue import Queue
import time

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


def monitor(message, progress):
    logger.info('%s: progess %d/100', message, progress)

def date_from_filename(filename):
    """Example cordex:
    tas_EUR-44i_ECMWF-ERAINT_evaluation_r1i1p1_HMS-ALADIN52_v1_mon_200101-200812.nc
    """
    logger.debug('filename=%s', filename)
    value = filename.split('.')
    value.pop() # remove .nc
    value = value[-1] # part with date
    value = value.split('_')[-1] # only date part
    logger.debug('date part = %s', value)
    value = value.split('-') # split start-end
    start_year = int(value[0][:4]) # keep only the year
    end_year = int(value[1][:4])
    return (start_year, end_year)

def variable_filter(constraints, variables):
    """return True if variable fulfills contraints"""
    var_types = ['variable', 'cf_standard_name', 'variable_long_name']

    success = True
    cs = constraints.mixed()
    # check different types of variables
    for var_type in var_types:
        # is there a constrain for this variable type?
        if cs.has_key(var_type):
            # at least one variable constraint must be fulfilled
            success = False
            # do we have this variable type?
            if variables.has_key(var_type):
                # do we have an allowed value?
                allowed_values = cs.get(var_type)
                if variables[var_type] in allowed_values:
                    # if one variables matches then we are ok
                    return True
    return success

def temporal_filter(filename, start_date=None, end_date=None):
    """return True if file is in timerange start/end"""
    # TODO: keep fixed fields fx ... see esgsearch.js
    """
    // fixed fields are always in time range
    if ($.inArray("fx", doc.time_frequency) >= 0) {
    return true;
    }
    """

    logger.debug('filename=%s, start_date=%s, end_date=%s', filename, start_date, end_date)
            
    if start_date is None or end_date is None:
        return True
    start_year, end_year = date_from_filename(filename)
    if start_year > end_date.year:
        logger.debug('skip: %s > %s', start_year, end_date.year)
        return False
    if end_year < start_date.year:
        logger.debug('skip: %s < %s', end_year, start_date.year)
        return False
    return True

class ESGSearch(object):
    """
    wrapper for esg search.

    TODO: bbox constraint for datasets
    """

    def __init__(self,
            url='http://localhost:8081/esg-search',
            distrib=False,
            replica=False,
            latest=True,
            monitor=monitor):
        # replica is  boolean defining whether to return master records
        # or replicas, or None to return both.
        self.replica = False
        if replica == True:
            self.replica = True

        # latest: A boolean defining whether to return only latest versions
        #    or only non-latest versions, or None to return both.
        self.latest = True
        if latest == False:
            self.latest= None
        self.monitor = monitor

        from pyesgf.search import SearchConnection
        self.conn = SearchConnection(url, distrib=distrib)

        self.fields = 'id,number_of_files,number_of_aggregations,size,url'

    def search(self, constraints=[('project', 'CORDEX')], query='*',
               start=None, end=None, limit=1, offset=0,
               search_type='Dataset',
               temporal=False):
        self.monitor("Starting ...", 0)

        from pyesgf.multidict import MultiDict
        my_constraints = MultiDict()
        for key,value in constraints:
            my_constraints.add(key, value)

        logger.debug('constraints=%s', my_constraints)

        logger.debug('query: %s', query)
        if query is None or len(query.strip()) == 0:
            query = '*'
            
        # TODO: check type of start, end
        logger.debug('start=%s, end=%s', start, end)

        start_date = end_date = None
        if start is not None and end is not None:
            start_date = date_parser.parse(start)
            end_date = date_parser.parse(end)
        
        ctx = None
        if temporal == True:
            logger.debug("using dataset search with time constraints")
            ctx = self.conn.new_context(fields=self.fields,
                                   replica=self.replica,
                                   latest=self.latest,
                                   query=query,
                                   from_timestamp=start,
                                   to_timestamp=end)
        else:
            ctx = self.conn.new_context(fields=self.fields,
                                   replica=self.replica,
                                   latest=self.latest,
                                   query=query)
        if len(my_constraints) > 0:
            ctx = ctx.constrain(**my_constraints.mixed())

        logger.debug('ctx: facet_constraints=%s, replica=%s, latests=%s', ctx.facet_constraints, ctx.replica, ctx.latest)
        
        self.monitor("Datasets found=%d" % ctx.hit_count, 5)
        
        self.summary = dict(total_number_of_datasets=ctx.hit_count,
                       number_of_datasets=0,
                       number_of_files=0,
                       number_of_aggregations=0,
                       size=0)

        self.result = []
        
        self.count = 0
        # search datasets
        # we always do this to get the summary document
        datasets = ctx.search()

        (self.start_index, self.stop_index, self.max_count) = self._index(datasets, limit, offset)
        self.summary['number_of_datasets'] = max(0, self.max_count)

        t0 = datetime.now()
        for i in range(self.start_index, self.stop_index):
            ds = datasets[i]
            progress = int( ((10.0 - 5.0) / self.max_count) * self.count )
            self.count = self.count + 1
            self.result.append(ds.json)
            for key in ['number_of_files', 'number_of_aggregations', 'size']:
                #logger.debug(ds.json)
                self.summary[key] = self.summary[key] + ds.json.get(key, 0)

        self.summary['ds_search_duration_secs'] = (datetime.now() - t0).seconds
        self.summary['size_mb'] = self.summary.get('size', 0) / 1024 / 1024
        self.summary['size_gb'] = self.summary.get('size_mb', 0) / 1024

        logger.debug('search_type = %s ', search_type)
            
        # search aggregations (optional)
        if search_type == 'Aggregation':
            self._aggregation_search(datasets, my_constraints)

        # search files (optional)
        elif search_type == 'File':
            self._file_search(datasets, my_constraints, start_date, end_date)

        # search files on thredds (optional)
        elif search_type == 'File_Thredds':
            self._tds_file_search(datasets, my_constraints, start_date, end_date)
            
        logger.debug('summary=%s', self.summary)
        self.monitor('Done', 100)

        return (self.result, self.summary, ctx.facet_counts)

    def _index(self, datasets, limit, offset):
        start_index = min(offset, len(datasets))
        stop_index = min(offset+limit, len(datasets))
        max_count = stop_index - start_index

        return (start_index, stop_index, max_count)

    # The threader thread pulls an worker from the queue and processes it
    def threader(self):
        while True:
            # gets an worker from the queue
            worker = self.job_queue.get()

            # Run the example job with the avail worker in queue (thread)
            try:
                self._file_search_job(**worker)
            except Exception:
                logger.exception('Search job failed! Could not retrieve files/aggregations.')
                progress = 10 + int( ((95.0 - 10.0) / self.max_count) * self.count )
                self.monitor('Query for Dataset failed.', progress)

            # completed with the job
            self.job_queue.task_done()

    def _file_search_job(self, f_ctx, start_date, end_date):
        #logger.debug('num files: %d', f_ctx.hit_count)
        logger.debug('facet constraints=%s', f_ctx.facet_constraints)
        for f in f_ctx.search():
            if not temporal_filter(f.filename, start_date, end_date):
                continue
            with self.result_lock:
                logger.debug('add file %s', f.filename)
                if f.download_url == 'null':
                    self.summary['number_of_invalid_files'] = self.summary['number_of_invalid_files'] + 1
                else:
                    self.summary['number_of_selected_files'] = self.summary['number_of_selected_files'] + 1
                    self.summary['file_size'] = self.summary['file_size'] + f.size
                    self.result.append(f.download_url)
        progress = 10 + int( ((95.0 - 10.0) / self.max_count) * self.count )
        self.monitor("Dataset %d/%d" % (self.count, self.max_count), progress)
        self.count = self.count + 1

    def _file_search(self, datasets, constraints, start_date, end_date):
        self.monitor("file search ...", 10)
        
        t0 = datetime.now()
        self.summary['file_size'] = 0
        self.summary['number_of_selected_files'] = 0
        self.summary['number_of_invalid_files'] = 0
        # lock for parallel search
        self.result_lock = threading.Lock()
        self.result = []
        self.count = 0
        # init threading
        self.job_queue = Queue()
        # using 5 thredds
        for x in range(5):
            t = threading.Thread(target=self.threader)
            # classifying as a daemon, so they will die when the main dies
            t.daemon = True
            # begins, must come after daemon definition
            t.start()

        for i in range(self.start_index, self.stop_index):
            f_ctx = datasets[i].file_context()
            f_ctx = f_ctx.constrain(**constraints.mixed())
            # fill job queue
            self.job_queue.put(dict(f_ctx=f_ctx, start_date=start_date, end_date=end_date))

        # wait until the thread terminates.
        self.job_queue.join()
            
        self.summary['file_search_duration_secs'] = (datetime.now() - t0).seconds
        self.summary['file_size_mb'] = self.summary['file_size'] / 1024 / 1024
        self.monitor("Files found=%d" % len(self.result), 95)

    def _aggregation_search(self, datasets, constraints):
        self.monitor("aggregation search ...", 10)
        
        t0 = datetime.now()
        self.summary['aggregation_size'] = 0
        self.summary['number_of_selected_aggregations'] = 0
        self.summary['number_of_invalid_aggregations'] = 0
        self.result = []
        self.count = 0
        for i in range(self.start_index, self.stop_index):
            ds = datasets[i]
            progress = 10 + int( ((95.0 - 10.0) / self.max_count) * self.count )
            self.monitor("Dataset %d/%d" % (self.count, self.max_count), progress)
            self.count = self.count + 1
            agg_ctx = ds.aggregation_context()
            agg_ctx = agg_ctx.constrain(**constraints.mixed())
            #logger.debug('num aggregations: %d', agg_ctx.hit_count)
            logger.debug('facet constraints=%s', agg_ctx.facet_constraints)
            if agg_ctx.hit_count == 0:
                logger.warn('dataset %s has no aggregations!', ds.dataset_id)
                continue
            for agg in agg_ctx.search():
                self.summary['number_of_selected_aggregations'] = self.summary['number_of_selected_aggregations'] + 1
                self.summary['aggregation_size'] = self.summary['aggregation_size'] + agg.size
                self.result.append(agg.opendap_url)
        self.summary['agg_search_duration_secs'] = (datetime.now() - t0).seconds
        self.summary['aggregation_size_mb'] = self.summary['aggregation_size'] / 1024 / 1024
        self.monitor("Aggregations found=%d" % len(self.result), 95)

    def _tds_url_of_dataset(self, dataset):
        tds_url = None
        for url in dataset.json.get('url', []):
            (tds_url, mime_type, service) = url.split('|')
            if service == 'Catalog':
                if '#' in tds_url:
                    tds_url = tds_url.split('#')[0]
                logger.debug('found tds_url =%s', tds_url)
                break
        if tds_url is None:
            logger.warn('no thredds url found in dataset %s', dataset.dataset_id)
        return tds_url

    def _tds_parse_dataset_urls(self, tds_url, constraints, start_date, end_date):
        from urllib2 import urlparse
        from os.path import basename
        
        from lxml import etree
        ns = etree.FunctionNamespace("http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0")
        ns.prefix = 'tds'
        tree = None
        try:
            tree=etree.parse(tds_url)
        except Exception:
            logger.exception('could not load thredds url %s', tds_url)

        if tree is not None:
            for el in tree.xpath('/tds:catalog/tds:dataset/tds:dataset'):
                try:
                    url_path = el.attrib.get('urlPath')
                    if url_path is None or 'aggregation' in url_path:
                        logger.debug('skip aggregation %s', url_path)
                        continue
                    if not temporal_filter(basename(url_path), start_date, end_date):
                        continue
                    property = {}
                    for p in el.xpath('tds:property'):
                        property[p.attrib.get('name')] = p.attrib.get('value')
                    variables = {}
                    for v in el.xpath('tds:variables/tds:variable'):
                        variables['variable'] = v.attrib.get('name')
                        variables['cf_standard_name'] = v.attrib.get('vocabulary_name')
                        variables['variable_long_name'] = v.text
                    if not variable_filter(constraints, variables):
                        logger.debug('skip variables %s', variables)
                        continue
                    self.summary['file_size'] = self.summary['file_size'] + int(property.get('size', 0))
                    url_parts = urlparse.urlparse(tds_url)
                    url = url_parts.scheme + '://' + url_parts.netloc + '/thredds/fileServer/' + url_path
                    self.result.append(url)
                except Exception:
                    logger.exception('could not load parse urlPath')
        
        
    def _tds_file_search(self, datasets, constraints, start_date, end_date):
        self.monitor("thredds file search ...", 10)

        t0 = datetime.now()
        self.summary['file_size'] = 0
        self.summary['number_of_selected_files'] = 0
        self.summary['number_of_invalid_files'] = 0
        self.result = []
        self.count = 0
        total_files = 0
        for i in range(self.start_index, self.stop_index):
            progress = 10 + int( ((95.0 - 10.0) / self.max_count) * self.count )
            self.monitor("Dataset %d/%d" % (self.count, self.max_count), progress)
            self.count = self.count + 1

            ds = datasets[i]
            total_files = total_files + ds.number_of_files
            tds_url = self._tds_url_of_dataset(ds)
            if tds_url is None:
                logger.warn('skipping dataset %s. No thredds catalog found', ds.dataset_id)
            else:
                self._tds_parse_dataset_urls(tds_url, constraints, start_date, end_date)
        self.summary['number_of_selected_files'] = len(self.result)
        self.summary['number_of_invalid_files'] = total_files - len(self.result)
        self.summary['tds_file_search_duration_secs'] = (datetime.now() - t0).seconds
        self.summary['file_size_mb'] = self.summary['file_size'] / 1024 / 1024
        self.monitor("Files found=%d" % len(self.result), 95)
            
            
            
