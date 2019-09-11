import os
import yaml

from pywps import Process
from pywps import ComplexInput
from pywps import ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata

from malleefowl.workflow import run

import logging
LOGGER = logging.getLogger("PYWPS")


class DispelWorkflow(Process):
    """
    The workflow process is usually called by the `Phoenix`_ WPS web client to
    run WPS process for climate data (like cfchecker, climate indices with ocgis, ...)
    with a given selection of input data (currently NetCDF files from ESGF data nodes).

    Currently the `Dispel4Py <https://github.com/dispel4py/dispel4py>`_ workflow engine is used.

    The Workflow for ESGF input data is as follows::

    Search ESGF files -> Download ESGF files -> Run choosen process on local (downloaded) ESGF files.
    """
    def __init__(self):
        inputs = [
            ComplexInput('workflow', 'Workflow description',
                         abstract='Workflow description in YAML.',
                         min_occurs=1,
                         max_occurs=1,
                         supported_formats=[Format('text/yaml')]),
        ]
        outputs = [
            ComplexOutput('output', 'Workflow result',
                          abstract="Workflow result document in YAML.",
                          as_reference=True,
                          supported_formats=[Format('text/yaml')]),
            ComplexOutput('logfile', 'Workflow log file',
                          abstract="Workflow log file.",
                          as_reference=True,
                          supported_formats=[FORMATS.TEXT]),
        ]

        super(DispelWorkflow, self).__init__(
            self._handler,
            identifier="workflow",
            title="Workflow",
            version="0.7",
            abstract="Runs Workflow with dispel4py.",
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
        def monitor(message, progress):
            response.update_status(message, progress)

        response.update_status("starting workflow ...", 0)

        workflow = yaml.load(request.inputs['workflow'][0].stream)
        workflow_name = workflow.get('name', 'unknown')

        response.update_status("workflow {0} prepared.".format(workflow_name), 0)

        result = run(workflow, monitor=monitor, headers=request.http_request.headers)

        with open(os.path.join(self.workdir, 'output.txt'), 'w') as fp:
            yaml.dump(result, stream=fp)
            response.outputs['output'].file = fp.name
        with open(os.path.join(self.workdir, 'logfile.txt'), 'w') as fp:
            fp.write("workflow log file")
            response.outputs['logfile'].file = fp.name
        response.update_status("workflow {0} done.".format(workflow_name), 100)
        return response
