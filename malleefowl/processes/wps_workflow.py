import yaml
from datetime import datetime

from pywps import Process
from pywps import ComplexInput
from pywps import ComplexOutput
from pywps import Format
from pywps.app.Common import Metadata

from malleefowl.custom_workflow import run

import logging
LOGGER = logging.getLogger("PYWPS")


class DispelWorkflow(Process):
    def __init__(self):
        inputs = [
            ComplexInput('workflow', 'Workflow description',
                         abstract='Workflow description in YAML.',
                         metadata=[Metadata('Info')],
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
                          supported_formats=[Format('text/plain')]),
        ]

        super(DispelWorkflow, self).__init__(
            self._handler,
            identifier="workflow",
            title="Workflow",
            version="0.8",
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
        # Open the log file from the beginning so that we can write to it any progress
        with open('logfile.txt', 'w') as fp:

            # Callback monitoring function that update the workflow WPS status and the log file
            def monitor(message, progress):
                response.update_status(message, progress)
                fp.write('{timestamp}{progress:>4}%: {msg}\n'.format(
                    timestamp=datetime.now().strftime('%H:%M:%S'),
                    progress=progress,
                    msg=message))

            monitor("starting workflow ...", 0)

            # Load the workflow
            workflow = yaml.load(request.inputs['workflow'][0].stream)
            workflow_name = workflow.get('name', 'unknown')

            monitor("workflow {0} prepared.".format(workflow_name), 0)

            # Log the workflow structure
            yaml.dump(workflow, stream=fp)
            fp.write('\n')

            # prepare headers
            headers = {}
            if 'X-X509-User-Proxy' in request.http_request.headers:
                headers['X-X509-User-Proxy'] = request.http_request.headers['X-X509-User-Proxy']
            if 'Access-Token' in request.http_request.headers:
                headers['Access-Token'] = request.http_request.headers['Access-Token']

            try:
                result = run(workflow, monitor=monitor, headers=headers)
            except Exception as e:
                fp.close()
                full_msg = '{0}\nExecution log :\n'.format(str(e))
                with open('logfile.txt', 'r') as log_fp:
                    for log_line in log_fp:
                        full_msg += log_line
                # Augment the exception message by appending the full log but conserve the full exception stack
                e.args = (full_msg,)
                raise
            else:
                monitor("workflow {0} done.".format(workflow_name), 100)

                # dump the result into the log file
                fp.write('\nWorkflow result:\n')
                yaml.dump(result, stream=fp)

                response.outputs['logfile'].file = fp.name

        with open('output.txt', 'w') as fp:
            yaml.dump(result, stream=fp)
            response.outputs['output'].file = fp.name

        return response
