from datetime import datetime
from dateutil import parser as date_parser

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


def monitor(message, progress):
    logger.info('%s: progess %d/100', message, progress)

def date_from_filename(filename):
    """Example cordex:
    tas_EUR-44i_ECMWF-ERAINT_evaluation_r1i1p1_HMS-ALADIN52_v1_mon_200101-200812.nc
    """
    value = filename.split('.')
    value.pop() # remove .nc
    value = value[-1] # part with date
    value = value.split('_')[-1] # only date part
    value = value.split('-') # split start-end
    start_year = int(value[0][:4]) # keep only the year
    end_year = int(value[1][:4])
    return (start_year, end_year)

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
            temporal=False,
            monitor=monitor):
        self.replica = replica
        self.latest = latest
        self.temporal = temporal
        self.monitor = monitor

        from pyesgf.search import SearchConnection
        self.conn = SearchConnection(url, distrib=distrib)

        self.fields = 'id,number_of_files,number_of_aggregations,size'
    
    def search(self, constraints=[('project', 'CORDEX')], query='*',
               start=None, end=None, limit=1, offset=0,
               search_type='Dataset'):
        self.monitor("Starting ...", 1)

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
        if self.temporal == True:
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
        
        summary = dict(total_number_of_datasets=ctx.hit_count,
                       number_of_datasets=0,
                       number_of_files=0,
                       number_of_aggregations=0,
                       size=0)
       
        result = []
        count = 0
        # search datasets
        # we always do this to get the summary document
        datasets = ctx.search()
        (start_index, stop_index, max_count) = self._index(datasets, limit, offset)
        
        summary['number_of_datasets'] = max(0, max_count)

        t0 = datetime.now()
        for i in range(start_index, stop_index):
            ds = datasets[i]
            progress = int( ((10.0 - 5.0) / max_count) * count )
            count = count + 1
            self.monitor("Dataset %d/%d" % (count, max_count), progress)
            result.append(ds.json)
            for key in ['number_of_files', 'number_of_aggregations', 'size']:
                logger.debug(ds.json)
                summary[key] = summary[key] + ds.json.get(key, 0)

        summary['ds_search_duration_secs'] = (datetime.now() - t0).seconds
        summary['size_mb'] = summary.get('size', 0) / 1024 / 1024
        summary['size_gb'] = summary.get('size_mb', 0) / 1024
            
        # search aggregations (optional)
        if search_type == 'Aggregation':
            self._aggregation_search(datasets, my_constraints, limit, offset, summary)

        # search files (optional)
        elif search_type == 'File':
            self._file_search(datasets, my_constraints, start_date, end_date, limit, offset, summary)
            
        logger.debug('summary=%s', summary)
        self.monitor('Done', 100)

        return (result, summary, ctx.facet_counts)

    def _index(self, datasets, limit, offset):
        start_index = min(offset, len(datasets))
        stop_index = min(offset+limit, len(datasets))
        max_count = stop_index - start_index

        return (start_index, stop_index, max_count)

    def _file_search(self, datasets, constraints, start_date, end_date, limit, offset, summary={}):
        t0 = datetime.now()
        summary['file_size'] = 0
        summary['number_of_selected_files'] = 0
        summary['number_of_invalid_files'] = 0
        result = []
        count = 0
        (start_index, stop_index, max_count) = self._index(datasets, limit, offset)
        for i in range(start_index, stop_index):
            ds = datasets[i]
            progress = 10 + int( ((95.0 - 10.0) / max_count) * count )
            self.monitor("Dataset %d/%d" % (count, max_count), progress)
            count = count + 1
            f_ctx = ds.file_context()
            f_ctx = f_ctx.constrain(**constraints.mixed())
            logger.debug('num files: %d', f_ctx.hit_count)
            logger.debug('facet constraints=%s', f_ctx.facet_constraints)
            for f in f_ctx.search():
                if not temporal_filter(f.filename, start_date, end_date):
                    continue
                if f.download_url == 'null':
                    summary['number_of_invalid_files'] = summary['number_of_invalid_files'] + 1
                else:
                    summary['number_of_selected_files'] = summary['number_of_selected_files'] + 1
                    summary['file_size'] = summary['file_size'] + f.size
                    result.append(f.download_url)
        summary['file_search_duration_secs'] = (datetime.now() - t0).seconds
        summary['file_size_mb'] = summary['file_size'] / 1024 / 1024
        self.monitor("Files found=%d" % len(result), 95)

    def _aggregation_search(self, datasets, constraints, limit, offset, summary={}):
        t0 = datetime.now()
        summary['aggregation_size'] = 0
        summary['number_of_selected_aggregations'] = 0
        summary['number_of_invalid_aggregations'] = 0
        result = []
        count = 0
        (start_index, stop_index, max_count) = self._index(datasets, limit, offset)
        for i in range(start_index, stop_index):
            ds = datasets[i]
            progress = 10 + int( ((95.0 - 10.0) / max_count) * count )
            self.monitor("Dataset %d/%d" % (count, max_count), progress)
            count = count + 1
            agg_ctx = ds.aggregation_context()
            agg_ctx = agg_ctx.constrain(**constraints.mixed())
            logger.debug('num aggregations: %d', agg_ctx.hit_count)
            logger.debug('facet constraints=%s', agg_ctx.facet_constraints)
            if agg_ctx.hit_count == 0:
                logger.warn('dataset %s has no aggregations!', ds.dataset_id)
                continue
            for agg in agg_ctx.search():
                summary['number_of_selected_aggregations'] = summary['number_of_selected_aggregations'] + 1
                summary['aggregation_size'] = summary['aggregation_size'] + agg.size
                result.append(agg.opendap_url)
        summary['agg_search_duration_secs'] = (datetime.now() - t0).seconds
        summary['aggregation_size_mb'] = summary['aggregation_size'] / 1024 / 1024
        self.monitor("Aggregations found=%d" % len(result), 95)
        
