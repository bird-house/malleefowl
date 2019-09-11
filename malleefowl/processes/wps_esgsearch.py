import os
import json

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexOutput
from pywps import FORMATS
from pywps.app.Common import Metadata

from malleefowl.esgf.search import ESGSearch

import logging
LOGGER = logging.getLogger(__name__)


class ESGSearchProcess(Process):
    """
    The ESGF search process runs a ESGF search request with constraints (project, experiment, ...)
    to get a list of matching files on ESGF data nodes.
    It is using `esgf-pyclient <https://github.com/ESGF/esgf-pyclient>`_ Python client
    for the ESGF search API.

    In addition to the esgf-pyclient the process checks if local replicas are available
    and would return the replica files instead of the original one.

    The result is a JSON document with a list of ``http://`` URLs to files on ESGF data nodes.

    TODO: bbox constraint for datasets
    """
    def __init__(self):
        inputs = [
            LiteralInput('url', 'URL',
                         data_type='string',
                         abstract="URL of ESGF Search Index which is used for search queries."
                                  " Example: http://esgf-data.dkrz.de/esg-search",
                         min_occurs=0,
                         max_occurs=1,
                         default="http://esgf-data.dkrz.de/esg-search",
                         ),
            LiteralInput('distrib', 'Distributed',
                         data_type='boolean',
                         abstract="If flag is set then a distributed search will be run.",
                         min_occurs=0,
                         max_occurs=1,
                         default='0',
                         ),
            LiteralInput('replica', 'Replica',
                         data_type='boolean',
                         abstract="If flag is set then search will include replicated datasets.",
                         min_occurs=0,
                         max_occurs=1,
                         default='False',
                         ),
            LiteralInput('latest', 'Latest',
                         data_type='boolean',
                         abstract="If flag is set then search will include only latest datasets.",
                         min_occurs=0,
                         max_occurs=1,
                         default='True',
                         ),
            LiteralInput('temporal', 'Temporal',
                         data_type='boolean',
                         abstract="If flag is set then search will use temporal filter.",
                         min_occurs=0,
                         max_occurs=1,
                         default='1',
                         ),
            LiteralInput('search_type', 'Search Type',
                         data_type='string',
                         abstract="Search on Datasets, Files or Aggregations.",
                         min_occurs=0,
                         max_occurs=1,
                         default='Dataset',
                         allowed_values=['Dataset', 'File', 'Aggregation']
                         ),
            LiteralInput('constraints', 'Constraints',
                         data_type='string',
                         abstract="Constraints as list of key/value pairs."
                                  "Example: project:CORDEX, time_frequency:mon, variable:tas",
                         min_occurs=0,
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
                         data_type='date',
                         abstract="Startime: 2000-01-11",
                         min_occurs=0,
                         max_occurs=1,
                         default='2000-01-11'
                         ),
            LiteralInput('end', 'End',
                         data_type='date',
                         abstract="Endtime: 2005-12-31",
                         min_occurs=0,
                         max_occurs=1,
                         default='2005-12-31'
                         ),
            LiteralInput('limit', 'Limit',
                         data_type='integer',
                         abstract="Maximum number of datasets in search result",
                         min_occurs=0,
                         max_occurs=1,
                         default='10',
                         allowed_values=[0, 1, 2, 5, 10, 20, 50, 100, 200]
                         ),
            LiteralInput('offset', 'Offset',
                         data_type='integer',
                         abstract="Start search of datasets at offset.",
                         min_occurs=0,
                         max_occurs=1,
                         default='0',
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'Search Result',
                          abstract="JSON document with search result,"
                                   " a list of URLs to files on ESGF archive nodes.",
                          as_reference=True,
                          supported_formats=[FORMATS.JSON]),
            ComplexOutput('summary', 'Search Result Summary',
                          abstract="JSON document with search result summary",
                          as_reference=True,
                          supported_formats=[FORMATS.JSON]),
            ComplexOutput('facet_counts', 'Facet Counts',
                          abstract="JSON document with facet counts for constraints.",
                          as_reference=True,
                          supported_formats=[FORMATS.JSON]),

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
        distrib = False
        if 'distrib' in request.inputs:
            distrib = request.inputs['distrib'][0].data
        replica = False
        if 'replica' in request.inputs:
            replica = request.inputs['replica'][0].data
        latest = True
        if 'latest' in request.inputs:
            latest = request.inputs['latest'][0].data
        esgsearch = ESGSearch(
            url=request.inputs['url'][0].data,
            distrib=distrib,
            replica=replica,
            latest=latest,
        )

        constrains_str = request.inputs['constraints'][0].data.strip()
        constraints = []
        for constrain in constrains_str.split(','):
            key, value = constrain.split(':')
            constraints.append((key.strip(), value.strip()))

        if 'start' in request.inputs:
            start = request.inputs['start'][0].data
        else:
            start = None
        if 'end' in request.inputs:
            end = request.inputs['end'][0].data
        else:
            end = None
        if 'offset' in request.inputs:
            offset = request.inputs['offset'][0].data
        else:
            offset = 0
        if 'limit' in request.inputs:
            limit = request.inputs['limit'][0].data
        else:
            limit = 10
        if 'query' in request.inputs:
            query = request.inputs['query'][0].data
        else:
            query = '*'
        if 'search_type' in request.inputs:
            search_type = request.inputs['search_type'][0].data
        else:
            search_type = 'Dataset'
        temporal = True
        if 'temporal' in request.inputs:
            temporal = request.inputs['temporal'][0].data

        (result, summary, facet_counts) = esgsearch.search(
            constraints=constraints,
            query=query,
            start=start, end=end,
            search_type=search_type,
            limit=limit,
            offset=offset,
            temporal=temporal)

        with open(os.path.join(self.workdir, 'out.json'), 'w') as fp:
            json.dump(obj=result, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name

        with open(os.path.join(self.workdir, 'summary.json'), 'w') as fp:
            json.dump(obj=summary, fp=fp, indent=4, sort_keys=True)
            response.outputs['summary'].file = fp.name

        with open(os.path.join(self.workdir, 'counts.json'), 'w') as fp:
            json.dump(obj=facet_counts, fp=fp, indent=4, sort_keys=True)
            response.outputs['facet_counts'].file = fp.name
        return response
