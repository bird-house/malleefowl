from datetime import datetime

from malleefowl.process import WPSProcess
from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class ESGSearch(WPSProcess):
    """
    wps wrapper for esg search.

    TODO: time constraints for datasets and files
    TODO: bbox constraint for datasets
    """
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "esgsearch",
            title = "ESGF Search",
            version = "0.1",
            abstract="Search ESGF datasets, files and aggreations.")

        self.url = self.addLiteralInput(
            identifier="url",
            title="URL",
            abstract="URL of ESGF Search Index. Example: http://esgf-data.dkrz.de/esg-search",
            default='http://localhost:8081/esg-search',
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        self.distrib = self.addLiteralInput(
            identifier = "distrib",
            title = "Distributed",
            abstract = "If flag is set then a distributed search will be run.",
            default = True,
            minOccurs=1,
            maxOccurs=1,
            type=type(True)
            )

        self.replica = self.addLiteralInput(
            identifier = "replica",
            title = "Replica",
            abstract = "If flag is set then search will include replicated datasets.",
            default = False,
            minOccurs=1,
            maxOccurs=1,
            type=type(True)
            )

        self.latest = self.addLiteralInput(
            identifier = "latest",
            title = "Latest",
            abstract = "If flag is set then search will include only latest datasets.",
            default = True,
            minOccurs=1,
            maxOccurs=1,
            type=type(True)
            )

        self.temporal = self.addLiteralInput(
            identifier = "temporal",
            title = "Temporal",
            abstract = "If flag is set then search will use temporal filter.",
            default = False,
            minOccurs=1,
            maxOccurs=1,
            type=type(True)
            )

        self.search_type = self.addLiteralInput(
            identifier = "search_type",
            title = "Search Type",
            abstract = "Search on Datasets, Files or Aggregations",
            default = 'Dataset',
            minOccurs=1,
            maxOccurs=1,
            type=type(''),
            allowedValues=['Dataset', 'File', 'Aggregation']
            )

        self.constraints = self.addLiteralInput(
            identifier = "constraints",
            title = "Constraints",
            abstract = "Constraints as list of key/value pairs. Example: project:CORDEX, time_frequency:mon, variable:tas",
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        self.query = self.addLiteralInput(
            identifier = "query",
            title = "Query",
            abstract = "Freetext query. For Example: temperatue",
            default = '*',
            minOccurs=0,
            maxOccurs=1,
            type=type('')
            )

        self.start = self.addLiteralInput(
            identifier="start",
            title="Start",
            abstract="Startime: 2000-01-11T12:00:00Z",
            type=type(datetime(year=2000, month=1, day=1)),
            minOccurs=0,
            maxOccurs=1,
            )

        self.end = self.addLiteralInput(
            identifier="end",
            title="End",
            abstract="Endtime: 2005-12-31T12:00:00Z",
            type=type(datetime(year=2005, month=12, day=31)),
            minOccurs=0,
            maxOccurs=1,
            )
        
        self.limit = self.addLiteralInput(
            identifier = "limit",
            title = "Limit",
            abstract = "Maximum number of datasets in search result",
            default = 10,
            minOccurs=1,
            maxOccurs=1,
            type=type(1),
            allowedValues=[0,1,5,10,20,50,100]
            )

        self.offset = self.addLiteralInput(
            identifier = "offset",
            title = "Offset",
            abstract = "Start search of datasets at offset.",
            default = 0,
            minOccurs=1,
            maxOccurs=1,
            type=type(1),
            )
        
        self.output = self.addComplexOutput(
            identifier="output",
            title="Search Result",
            abstract="JSON document with search result",
            metadata=[],
            formats=[{"mimeType":"test/json"}],
            asReference=True,
            )

        self.summary = self.addComplexOutput(
            identifier="summary",
            title="Search Result Summary",
            abstract="JSON document with search result summary",
            metadata=[],
            formats=[{"mimeType":"test/json"}],
            asReference=True,
            )

        self.facet_counts = self.addComplexOutput(
            identifier="facet_counts",
            title="Facet Counts",
            abstract="JSON document with facet counts for contstaints.",
            metadata=[],
            formats=[{"mimeType":"test/json"}],
            asReference=True,
            )
        
    def execute(self):
        self.show_status("Starting ...", 1)

        from pyesgf.search import SearchConnection
        conn = SearchConnection(self.url.getValue(),
                                distrib=self.distrib.getValue())
            

        constraints = {}
        for constrain in self.constraints.getValue().strip().split(','):
            key, value = constrain.split(':')
            constraints[key.strip()] = value.strip()

        logger.debug('constraints=%s', constraints)

        # replica is  boolean defining whether to return master records
        # or replicas, or None to return both.
        replica = None
        if self.replica.getValue() == True:
            replica = True

        # latest: A boolean defining whether to return only latest versions
        #    or only non-latest versions, or None to return both.
        latest = True
        if self.replica.getValue() == False:
            latest= None

        fields = 'id,number_of_files,number_of_aggregations,size'
        query = self.query.getValue()
        logger.debug('query: %s', query)
        if query is None or len(query.strip()) == 0:
            query = '*'
        # TODO: check type of start, end
        start = self.start.getValue()
        end = self.end.getValue()
        logger.debug('start=%s, end=%s', start, end)
        #if start is not None:
        #    from_timestamp = start.strftime(format="%Y-%m-%dT%H:%M:%SZ")
        #if end is not None:
        #    to_timestamp = end.strftime(format="%Y-%m-%dT%H:%M:%SZ")
        # TODO: update esgf-pyclient to constrain with timestamps
        ctx = None
        if self.temporal.getValue() == True:
             ctx = conn.new_context(fields=fields,
                                   replica=replica,
                                   latest=latest,
                                   query=query,
                                   from_timestamp=start,
                                   to_timestamp=end)
        else:
            ctx = conn.new_context(fields=fields,
                                   replica=replica,
                                   latest=latest,
                                   query=query)
        if len(constraints) > 0:
            ctx = ctx.constrain(**constraints)
                
        self.show_status("Datasets found=%d" % ctx.hit_count, 5)
        
        search_type = self.search_type.getValue()
        limit = self.limit.getValue()
        offset = self.offset.getValue()

        summary = dict(total_number_of_datasets=ctx.hit_count,
                       number_of_datasets=0,
                       number_of_files=0,
                       number_of_aggregations=0,
                       number_of_invalid_aggregations=0,
                       size=0)
       
        result = []
        count = 0
        # search datasets
        # we always do this to get the summary document
        datasets = ctx.search()
        start_index = min(offset, len(datasets))
        stop_index = min(offset+limit, len(datasets))
        max_count = stop_index - start_index
        
        summary['number_of_datasets'] = max(0, stop_index - start_index)
        
        for i in range(start_index, stop_index):
            ds = datasets[i]
            count = count + 1
            progress = int( ((10.0 - 5.0) / ctx.hit_count) * count )
            self.show_status("Dataset %d/%d" % (count, max_count), progress)
            result.append(ds.json)
            for key in ['number_of_files', 'number_of_aggregations', 'size']:
                logger.debug(ds.json)
                summary[key] = summary[key] + ds.json.get(key, 0)

        summary['size_mb'] = summary.get('size', 0) / 1024 / 1024
        summary['size_gb'] = summary.get('size_mb', 0) / 1024
        summary['size_tb'] = summary.get('size_gb', 0) / 1024

        # search aggregations (optional)
        if search_type == 'Aggregation':
            result = []
            count = 0
            for i in range(start_index, stop_index):
                ds = datasets[i]
                count = count + 1
                progress = int( ((95.0 - 10.0) / ctx.hit_count) * count )
                self.show_status("Dataset %d/%d" % (count, max_count), progress)
                agg_ctx = ds.aggregation_context()
                for agg in agg_ctx.search():
                    result.append(agg.opendap_url)
            self.show_status("Aggregations found=%d" % len(result), 95)

        # search files (optional)
        elif search_type == 'File':
            result = []
            count = 0
            for i in range(start_index, stop_index):
                ds = datasets[i]
                count = count + 1
                progress = int( ((95.0 - 10.0) / ctx.hit_count) * count )
                self.show_status("Dataset %d/%d" % (count, max_count), progress)
                for f in ds.file_context().search():
                    if f.download_url == 'null':
                        summary['number_of_invalid_aggregations'] = summary['number_of_invalid_aggregations'] + 1
                    else:
                        result.append(f.download_url)
            self.show_status("Files found=%d" % len(result), 95)

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=result, fp=fp, indent=4, sort_keys=True)
            self.output.setValue( outfile )

        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=summary, fp=fp, indent=4, sort_keys=True)
            self.summary.setValue( outfile )

        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=ctx.facet_counts, fp=fp, indent=4, sort_keys=True)
            self.facet_counts.setValue( outfile )

        self.show_status("Done.", 100)

        
