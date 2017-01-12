from datetime import datetime

import threading
from Queue import Queue

import logging
logger = logging.getLogger(__name__)


def date_from_filename(filename):
    """Example cordex:
    tas_EUR-44i_ECMWF-ERAINT_evaluation_r1i1p1_HMS-ALADIN52_v1_mon_200101-200812.nc
    """
    logger.debug('filename=%s', filename)
    result = None
    value = filename.split('.')
    value.pop()  # remove .nc
    value = value[-1]  # part with date
    value = value.split('_')[-1]  # only date part
    logger.debug('date part = %s', value)
    if value != 'fx':
        value = value.split('-')  # split start-end
        start_year = int(value[0][:4])  # keep only the year
        end_year = int(value[1][:4])
        result = (start_year, end_year)
    return result


def variable_filter(constraints, variables):
    """return True if variable fulfills contraints"""
    var_types = ['variable', 'cf_standard_name', 'variable_long_name']

    success = True
    cs = constraints.mixed()
    # check different types of variables
    for var_type in var_types:
        # is there a constrain for this variable type?
        if var_type in cs:
            # at least one variable constraint must be fulfilled
            success = False
            # do we have this variable type?
            if var_type in variables:
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
    start_end = date_from_filename(filename)
    if start_end is None:  # fixed field
        return True
    start_year = start_end[0]
    end_year = start_end[1]
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

    def __init__(
            self,
            url='http://localhost:8081/esg-search',
            distrib=False,
            replica=False,
            latest=True,
            monitor=None):
        # replica is  boolean defining whether to return master records
        # or replicas, or None to return both.
        self.replica = False
        if replica is True:
            self.replica = True

        # latest: A boolean defining whether to return only latest versions
        #    or only non-latest versions, or None to return both.
        self.latest = True
        if latest is False:
            self.latest = None
        self.monitor = monitor

        from pyesgf.search import SearchConnection
        self.conn = SearchConnection(url, distrib=distrib)
        self.fields = 'id,instance_id,number_of_files,number_of_aggregations,size,url'
        # local context has *all* local datasets
        local_conn = SearchConnection(url, distrib=False)
        self.local_ctx = local_conn.new_context(fields=self.fields, replica=True, latest=None)

    def show_status(self, message, progress):
        if self.monitor is None:
            logger.info("%s, progress=%d/100", message, progress)
        else:
            self.monitor(message, progress)

    def search(self, constraints=[('project', 'CORDEX')], query='*',
               start=None, end=None, limit=1, offset=0,
               search_type='Dataset',
               temporal=False):
        self.show_status("Starting ...", 0)

        from pyesgf.multidict import MultiDict
        my_constraints = MultiDict()
        for key, value in constraints:
            my_constraints.add(key, value)

        logger.debug('constraints=%s', my_constraints)

        if query is None or len(query.strip()) == 0:
            query = '*:*'
        logger.debug('query: %s', query)

        # TODO: check type of start, end
        logger.debug('start=%s, end=%s', start, end)

        ctx = None
        if temporal is True:
            logger.debug("using dataset search with time constraints")
            # TODO: handle timestamps in a better way
            timestamp_format = '%Y-%m-%dT%H:%M:%SZ'
            if start:
                from_timestamp = start.strftime(timestamp_format)
            else:
                from_timestamp = None
            if end:
                to_timestamp = end.strftime(timestamp_format)
            else:
                to_timestamp = None
            ctx = self.conn.new_context(fields=self.fields,
                                        replica=self.replica,
                                        latest=self.latest,
                                        query=query,
                                        from_timestamp=from_timestamp,
                                        to_timestamp=to_timestamp)
        else:
            ctx = self.conn.new_context(fields=self.fields,
                                        replica=self.replica,
                                        latest=self.latest,
                                        query=query)
        if len(my_constraints) > 0:
            ctx = ctx.constrain(**my_constraints.mixed())

        logger.debug('ctx: facet_constraints=%s, replica=%s, latests=%s',
                     ctx.facet_constraints, ctx.replica, ctx.latest)

        self.show_status("Datasets found=%d" % ctx.hit_count, 0)

        self.summary = dict(total_number_of_datasets=ctx.hit_count,
                            number_of_datasets=0,
                            number_of_files=0,
                            number_of_aggregations=0,
                            size=0)

        self.result = []

        self.count = 0
        # search datasets
        # we always do this to get the summary document
        datasets = ctx.search(ignore_facet_check=True)

        (self.start_index, self.stop_index, self.max_count) = self._index(datasets, limit, offset)
        self.summary['number_of_datasets'] = max(0, self.max_count)

        t0 = datetime.now()
        for i in range(self.start_index, self.stop_index):
            ds = datasets[i]
            # progress = self.count * 100.0 / self.max_count
            self.count = self.count + 1
            self.result.append(ds.json)
            for key in ['number_of_files', 'number_of_aggregations', 'size']:
                # logger.debug(ds.json)
                self.summary[key] = self.summary[key] + ds.json.get(key, 0)

        self.summary['ds_search_duration_secs'] = (datetime.now() - t0).seconds
        self.summary['size_mb'] = self.summary.get('size', 0) / 1024 / 1024
        self.summary['size_gb'] = self.summary.get('size_mb', 0) / 1024

        logger.debug('search_type = %s ', search_type)

        if search_type == 'Dataset':
            pass
        # search files (optional)
        elif search_type == 'File':
            self._file_search(datasets, my_constraints, start, end)
        # search aggregations (optional)
        elif search_type == 'Aggregation':
            self._aggregation_search(datasets, my_constraints)
        else:
            raise Exception('unknown search type: %s', search_type)

        logger.debug('summary=%s', self.summary)
        self.show_status('Done', 100)

        return (self.result, self.summary, ctx.facet_counts)

    def _index(self, datasets, limit, offset):
        start_index = min(offset, len(datasets))
        stop_index = min(offset + limit, len(datasets))
        max_count = stop_index - start_index

        return (start_index, stop_index, max_count)

    def _file_context(self, dataset):
        logger.debug('file_context: checking for local replica')
        f_ctx = dataset.file_context()
        # if distrib search check if we have a replica locally
        if self.conn.distrib:
            ctx = self.local_ctx.constrain(instance_id=dataset.json.get('instance_id'))
            if ctx.hit_count == 1:
                logger.info('found local replica')
                datasets = ctx.search(ignore_facet_check=True)
                f_ctx = datasets[0].file_context()
            else:
                logger.info('no local replica found')
        return f_ctx

    def _aggregation_context(self, dataset):
        logger.debug('aggregation_context: checking for local replica')
        agg_ctx = dataset.aggregation_context()
        # if distrib search check if we have a replica locally
        if self.conn.distrib:
            ctx = self.local_ctx.constrain(instance_id=dataset.json.get('instance_id'))
            if ctx.hit_count == 1:
                logger.info('found local replica')
                datasets = ctx.search(ignore_facet_check=True)
                agg_ctx = datasets[0].aggregation_context()
            else:
                logger.info('no local replica found')
        return agg_ctx

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
            # completed with the job
            self.job_queue.task_done()

    def _file_search_job(self, f_ctx, start_date, end_date):
        # logger.debug('num files: %d', f_ctx.hit_count)
        logger.debug('facet constraints=%s', f_ctx.facet_constraints)
        for f in f_ctx.search(ignore_facet_check=True):
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
        progress = self.count * 100.0 / self.max_count
        self.show_status("Dataset %d/%d" % (self.count, self.max_count), progress)
        self.count = self.count + 1

    def _file_search(self, datasets, constraints, start_date, end_date):
        self.show_status("file search ...", 0)

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
        # using max 2 threads
        num_threads = min(2, self.max_count)
        for x in range(num_threads):
            t = threading.Thread(target=self.threader)
            # classifying as a daemon, so they will die when the main dies
            t.daemon = True
            # begins, must come after daemon definition
            t.start()

        for i in range(self.start_index, self.stop_index):
            # f_ctx = datasets[i].file_context()
            f_ctx = self._file_context(datasets[i])
            f_ctx = f_ctx.constrain(**constraints.mixed())
            f_ctx.freetext_constraint = "*:*"
            # fill job queue
            self.job_queue.put(dict(f_ctx=f_ctx, start_date=start_date, end_date=end_date))

        # wait until the thread terminates.
        self.job_queue.join()

        self.summary['file_search_duration_secs'] = (datetime.now() - t0).seconds
        self.summary['file_size_mb'] = self.summary['file_size'] / 1024 / 1024
        self.show_status("Files found=%d" % len(self.result), 100)

    def _aggregation_search(self, datasets, constraints):
        self.show_status("aggregation search ...", 0)

        t0 = datetime.now()
        self.summary['aggregation_size'] = 0
        self.summary['number_of_selected_aggregations'] = 0
        self.summary['number_of_invalid_aggregations'] = 0
        self.result = []
        self.count = 0
        for i in range(self.start_index, self.stop_index):
            ds = datasets[i]
            progress = self.count * 100.0 / self.max_count
            self.show_status("Dataset %d/%d" % (self.count, self.max_count), progress)
            self.count = self.count + 1
            # agg_ctx = ds.aggregation_context()
            agg_ctx = self._aggregation_context(ds)
            agg_ctx = agg_ctx.constrain(**constraints.mixed())
            agg_ctx.freetext_constrain = "*:*"
            # logger.debug('num aggregations: %d', agg_ctx.hit_count)
            logger.debug('facet constraints=%s', agg_ctx.facet_constraints)
            if agg_ctx.hit_count == 0:
                logger.warn('dataset %s has no aggregations!', ds.dataset_id)
                continue
            for agg in agg_ctx.search(ignore_facet_check=True):
                self.summary['number_of_selected_aggregations'] = self.summary['number_of_selected_aggregations'] + 1
                self.summary['aggregation_size'] = self.summary['aggregation_size'] + agg.size
                self.result.append(agg.opendap_url)
        self.summary['agg_search_duration_secs'] = (datetime.now() - t0).seconds
        self.summary['aggregation_size_mb'] = self.summary['aggregation_size'] / 1024 / 1024
        self.show_status("Aggregations found=%d" % len(self.result), 100)
