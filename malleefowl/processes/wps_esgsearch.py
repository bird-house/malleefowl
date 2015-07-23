from datetime import datetime
from dateutil import parser as date_parser

from malleefowl.process import WPSProcess
from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class ESGSearch(WPSProcess):
    """
    wps wrapper for esg search.

    TODO: bbox constraint for datasets
    """
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "esgsearch",
            title = "ESGF Search",
            version = "0.3",
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
            default = False,
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
            allowedValues=[0,1,2,5,10,20,50,100,200]
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
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

        self.summary = self.addComplexOutput(
            identifier="summary",
            title="Search Result Summary",
            abstract="JSON document with search result summary",
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

        self.facet_counts = self.addComplexOutput(
            identifier="facet_counts",
            title="Facet Counts",
            abstract="JSON document with facet counts for contstaints.",
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )
        
    def execute(self):
        from malleefowl.esgf.search import ESGSearch
        esgsearch = ESGSearch(
            url = self.url.getValue(),
            distrib = self.distrib.getValue(),
            replica = self.replica.getValue(),
            latest = self.replica.getValue(),
            monitor = self.show_status,
        )

        constraints = []
        for constrain in self.constraints.getValue().strip().split(','):
            key, value = constrain.split(':')
            constraints.append( (key.strip(), value.strip() ) )

        (result, summary, facet_counts) = esgsearch.search(
            constraints = constraints,
            query = self.query.getValue(),
            start = self.start.getValue(),
            end = self.end.getValue(),
            search_type = self.search_type.getValue(),
            limit = self.limit.getValue(),
            offset = self.offset.getValue(),
            temporal = self.temporal.getValue())

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
            json.dump(obj=facet_counts, fp=fp, indent=4, sort_keys=True)
            self.facet_counts.setValue( outfile )


        
