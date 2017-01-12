import json
from datetime import datetime
from dateutil import parser as date_parser

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput
from pywps import LiteralOutput
from pywps import ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata

from malleefowl.esgf.search import ESGSearch


class ESGSearchProcess(Process):
    """
    wps wrapper for esg search.

    TODO: bbox constraint for datasets
    """
    def __init__(self):
        inputs = [
            LiteralInput('url', 'URL',
                         data_type='string',
                         abstract="URL of ESGF Search Index. Example: http://esgf-data.dkrz.de/esg-search",
                         min_occurs=1,
                         max_occurs=1,
                         ),
            LiteralInput('distrib', 'Distributed',
                         data_type='boolean',
                         abstract="If flag is set then a distributed search will be run.",
                         min_occurs=1,
                         max_occurs=1,
                         default=False,
                         ),
            LiteralInput('replica', 'Replica',
                         data_type='boolean',
                         abstract="If flag is set then search will include replicated datasets.",
                         min_occurs=1,
                         max_occurs=1,
                         default=False,
                         ),
            LiteralInput('latest', 'Latest',
                         data_type='boolean',
                         abstract="If flag is set then search will include only latest datasets.",
                         min_occurs=1,
                         max_occurs=1,
                         default=True,
                         ),
            LiteralInput('temporal', 'Temporal',
                         data_type='boolean',
                         abstract="If flag is set then search will use temporal filter.",
                         min_occurs=1,
                         max_occurs=1,
                         default=True,
                         ),
            LiteralInput('search_type', 'Search Type',
                         data_type='string',
                         abstract="Search on Datasets, Files or Aggregations.",
                         min_occurs=1,
                         max_occurs=1,
                         default='Dataset',
                         allowed_values=['Dataset', 'File', 'Aggregation']
                         ),
            LiteralInput('constraints', 'Constraints',
                         data_type='string',
                         abstract="Constraints as list of key/value pairs."
                                  "Example: project:CORDEX, time_frequency:mon, variable:tas",
                         min_occurs=1,
                         max_occurs=1,
                         default="project:CORDEX, time_frequency:mon, variable:tas",
                         ),
            LiteralInput('query', 'Query',
                         data_type='string',
                         abstract="Freetext query. For Example: temperatue",
                         min_occurs=0,
                         max_occurs=1,
                         default='*',
                         ),
            LiteralInput('start', 'Start',
                         data_type='dateTime',
                         abstract="Startime: 2000-01-11T12:00:00Z",
                         min_occurs=0,
                         max_occurs=1,
                         ),
            LiteralInput('end', 'End',
                         data_type='dateTime',
                         abstract="Endtime: 2005-12-31T12:00:00Z",
                         min_occurs=0,
                         max_occurs=1,
                         ),
            LiteralInput('limit', 'Limit',
                         data_type='integer',
                         abstract="Maximum number of datasets in search result",
                         min_occurs=1,
                         max_occurs=1,
                         default=10,
                         allowed_values=[0, 1, 2, 5, 10, 20, 50, 100, 200]
                         ),
            LiteralInput('offset', 'Offset',
                         data_type='integer',
                         abstract="Start search of datasets at offset.",
                         min_occurs=1,
                         max_occurs=1,
                         default=0,
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'Search Result',
                          abstract="JSON document with search result",
                          as_reference=True,
                          supported_formats=[Format('application/json')]),
            ComplexOutput('summary', 'Search Result Summary',
                          abstract="JSON document with search result summary",
                          as_reference=True,
                          supported_formats=[Format('application/json')]),
            ComplexOutput('facet_counts', 'Facet Counts',
                          abstract="JSON document with facet counts for constraints.",
                          as_reference=True,
                          supported_formats=[Format('application/json')]),

        ]

        super(ESGSearchProcess, self).__init__(
            self._handler,
            identifier="esgsearch",
            title="ESGF Search",
            version="0.6",
            abstract="Search ESGF datasets, files and aggreations.",
            metadata=[
                Metadata('Birdhouse', 'http://bird-house.github.io/'),
                Metadata('User Guide', 'http://malleefowl.readthedocs.io/en/latest/'),
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )

    def _handler(self, request, response):
        esgsearch = ESGSearch(
            url=request.inputs['url'][0].data,
            distrib=request.inputs['distrib'][0].data,
            replica=request.inputs['replica'][0].data,
            latest=request.inputs['latest'][0].data,
        )

        constrains_str = request.inputs['constraints'][0].data.strip()
        constraints = []
        for constrain in constrains_str.split(','):
            key, value = constrain.split(':')
            constraints.append((key.strip(), value.strip()))

        (result, summary, facet_counts) = esgsearch.search(
            constraints=constraints,
            query=request.inputs['query'][0].data,
            start=request.inputs['start'][0].data,
            end=request.inputs['end'][0].data,
            search_type=request.inputs['search_type'][0].data,
            limit=request.inputs['limit'][0].data,
            offset=srequest.inputs['offset'][0].data,
            temporal=request.inputs['temporal'][0].data)

        with open('out.json', 'w') as fp:
            json.dump(obj=result, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name

        with open('summary.json', 'w') as fp:
            json.dump(obj=summary, fp=fp, indent=4, sort_keys=True)
            response.outputs['summary'].file = fp.name

        with open('counts.json', 'w') as fp:
            json.dump(obj=facet_counts, fp=fp, indent=4, sort_keys=True)
            response.outputs['facet_counts'].file = fp.name
        return response
