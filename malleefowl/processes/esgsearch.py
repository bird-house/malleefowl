from malleefowl.process import WPSProcess
from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class ESGSearch(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "esgsearch",
            title = "ESGF Search",
            version = "0.1",
            abstract="Search ESGF datasets, files and aggreations.")

        self.url = self.addLiteralInput(
            identifier="url",
            title="URL",
            abstract="URL of ESGF Search Index",
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

        self.type = self.addLiteralInput(
            identifier = "type",
            title = "Result Type",
            abstract = "Datasets, Files or Aggregations",
            default = 'datasets',
            minOccurs=1,
            maxOccurs=1,
            type=type(''),
            allowedValues=['datasets', 'files', 'aggregations']
            )

        self.constraints = self.addLiteralInput(
            identifier = "constraints",
            title = "Constraints",
            abstract = "Constraints as list of key/value pairs. Example: project:CORDEX, time_frequency:mon, variable:tas, experiment:historical, domain:EUR-11",
            default = 'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical,domain:EUR-11',
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        self.query = self.addLiteralInput(
            identifier = "query",
            title = "Query",
            abstract = "Query search string",
            default = 'CORDEX',
            minOccurs=0,
            maxOccurs=1,
            type=type('')
            )
        
        self.from_timestamp = self.addLiteralInput(
            identifier = "from",
            title = "From",
            abstract = "From Timestamp",
            default = '2000',
            minOccurs=0,
            maxOccurs=1,
            type=type(''),
            allowedValues=['1990', '2000', '2010', '2020']
            )

        self.to_timestamp = self.addLiteralInput(
            identifier = "to",
            title = "To",
            abstract = "To Timestamp",
            default = '2010',
            minOccurs=0,
            maxOccurs=1,
            type=type(''),
            allowedValues=['1990', '2000', '2010', '2020']
            )

        self.limit = self.addLiteralInput(
            identifier = "limit",
            title = "Limit",
            abstract = "Maximum number of datasets in search result",
            default = 10,
            minOccurs=1,
            maxOccurs=1,
            type=type(1),
            allowedValues=[1, 5, 10, 20, 50, 100]
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

        fields = 'id,number_of_files,number_of_aggregations,size'
        ctx = conn.new_context(fields=fields, **constraints)
                
        self.show_status("Datasets found=%d" % ctx.hit_count, 5)

        result_type = self.type.getValue()
        limit = self.limit.getValue()

        summary = dict(number_of_datasets=ctx.hit_count,
                       number_of_files=0,
                       number_of_aggregations=0,
                       size=0)
        result = []
        count = 0
        if result_type == 'datasets':
            for ds in ctx.search():
                count = count + 1
                if count > limit:
                    logger.warning('dataset limit %d reached, skip the rest', limit)
                    break
                progress = int( ((95.0 - 5.0) / ctx.hit_count) * count )
                self.show_status("Dataset %d/%d" % (count, ctx.hit_count), progress)
                result.append(ds.json)
                for key in ['number_of_files', 'number_of_aggregations', 'size']:
                    logger.debug(ds.json)
                    summary[key] = summary[key] + ds.json.get(key, 0)

        elif result_type == 'aggregations':
            for ds in ctx.search():
                count = count + 1
                if count > limit:
                    logger.warning('dataset limit %d reached, skip the rest', limit)
                    break
                progress = int( ((95.0 - 5.0) / ctx.hit_count) * count )
                self.show_status("Dataset %d/%d" % (count, ctx.hit_count), progress)
                agg_ctx = ds.aggregation_context()
                for agg in agg_ctx.search():
                    result.append(agg.opendap_url)
            self.show_status("Aggregations found=%d" % len(result), 95)

        elif result_type == 'files':
            for ds in ctx.search():
                count = count + 1
                if count > limit:
                    logger.warning('dataset limit %d reached, skip the rest', limit)
                    break
                progress = int( ((95.0 - 5.0) / ctx.hit_count) * count )
                self.show_status("Dataset %d/%d" % (count, ctx.hit_count), progress)
                for f in ds.file_context().search():
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

        self.show_status("Done.", 100)

        
