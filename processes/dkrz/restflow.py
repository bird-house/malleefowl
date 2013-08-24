"""
Processes for testing wps data types

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

from os import path
import tempfile

from malleefowl.process import WPSProcess

class Run(WPSProcess):
    """This process runs a restflow workflow description"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "de.dkrz.restflow.run",
            title = "Run restflow workflow",
            version = "0.1",
            metadata=[
                {"title":"Restflow","href":"https://github.com/restflow-org"},
                ],
            abstract="Runs given workflow with yaml description")


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

        self.text_out = self.addComplexOutput(
            identifier="output",
            title="Workflow result",
            abstract="Workflow result",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting restflow workflow", percentDone=5, propagate=True)

        wf_filename = path.abspath(self.workflow_description_in.getValue(asFile=False))

        result = self.cmd(cmd=["restflow", "-t", "-f", wf_filename], stdout=True)
       
        self.status.set(msg="workflow done", percentDone=90, propagate=True)

        (_, out_filename) = tempfile.mkstemp(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(result)
            fp.close()
            self.text_out.setValue( out_filename )
