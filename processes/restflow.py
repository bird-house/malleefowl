"""
Processes for testing wps data types

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

from os import path
import tempfile

import yaml

from malleefowl.process import WPSProcess

class Run(WPSProcess):
    """This process runs a restflow workflow description"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.restflow",
            title = "Run restflow workflow",
            version = "0.1",
            metadata=[
                {"title":"Restflow","href":"https://github.com/restflow-org"},
                ],
            abstract="Runs given workflow with yaml description")


        self.restflow_command_in = self.addLiteralInput(
            identifier="command",
            title="Restflow Command",
            abstract="Choose Restflow Command",
            default="execute",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['execute', 'validate', 'visualize']
            )

        self.workflow_description_in = self.addComplexInput(
            identifier="workflow_description",
            title="Workflow description",
            abstract="Workflow description in yaml",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            maxmegabites=2,
            formats=[{"mimeType":"text/yaml"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Workflow Report",
            abstract="Workflow Report",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

        self.work_output = self.addLiteralOutput(
            identifier="work_output",
            title="Process Result",
            abstract="Process Result",
            type=type(''),
            )

        self.work_status = self.addLiteralOutput(
            identifier="work_status",
            title="Process Status",
            abstract="Process Status",
            type=type(''),
            )

    def execute(self):
        self.status.set(msg="starting restflow workflow", percentDone=5, propagate=True)

        wf_filename = path.abspath(self.workflow_description_in.getValue(asFile=False))

        options = ''
        
        if self.restflow_command_in.getValue() == "validate":
            options = '--validate'
        elif self.restflow_command_in.getValue() == "visualize":
            options = '--to-dot'
        
        result = self.cmd(
            cmd=["restflow", options, "--enable-trace", "-f", wf_filename, "--run", "restflow"],
            stdout=True)

        products = yaml.load( open(path.join(self.working_dir, "restflow", "_metadata", "products.yaml")) )
        self.work_output.setValue(products.get('/wps/work/output/1'))
        self.work_status.setValue(products.get('/wps/work/status/1'))
       
        self.status.set(msg="workflow done", percentDone=90, propagate=True)

        (_, out_filename) = tempfile.mkstemp(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(result)
            fp.close()
            self.output.setValue( out_filename )
