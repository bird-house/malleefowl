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
        
        self.output = self.addComplexOutput(
            identifier="output",
            title="Search Result",
            abstract="JSON document with search result",
            metadata=[],
            formats=[{"mimeType":"test/json"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("Starting ...", 1)

        from pyesgf.search import SearchConnection
        conn = SearchConnection(self.url.getValue(),
                                distrib=self.distrib.getValue())

        ctx = conn.new_context(
            project='CMIP5',
            model='HadCM3',
            experiment='decadal2000',
            time_frequency='day',
            realm='ocean',
            ensemble='r1i2p1')
                
        self.show_status("Datasets found=%d" % ctx.hit_count, 5)

        type = self.type.getValue()

        result = []
        if type == 'datasets':
            keys = ['id', 'number_of_files', 'number_of_aggregations', 'size']
            for ds in ctx.search():
                ds_dict = {}
                for key in keys:
                    ds_dict[key] = ds.json.get(key)
                result.append(ds_dict)

        elif type == 'aggregations':
            count = 0
            for ds in ctx.search():
                count = count + 1
                progress = int( ((95.0 - 5.0) / ctx.hit_count) * count )
                self.show_status("Dataset %d/%d" % (count, ctx.hit_count), progress)
                agg_ctx = ds.aggregation_context()
                for agg in agg_ctx.search():
                    result.append(agg.opendap_url)
            self.show_status("Aggregations found=%d" % len(result), 95)

        elif type == 'files':
            count = 0
            for ds in ctx.search():
                count = count + 1
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

        self.show_status("Done.", 100)

        
