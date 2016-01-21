import yaml
import tempfile

from pywps.Process import WPSProcess
from malleefowl import config
from malleefowl.workflow import run

class DispelWorkflow(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier="workflow",
            title="Workflow",
            version="0.4",
            abstract="Runs Workflow with dispel4py.",
            statusSupported=True,
            storeSupported=True)

        self.workflow= self.addComplexInput(
            identifier="workflow",
            title="Workflow description",
            abstract="Workflow description in YAML",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[{"mimeType":"text/yaml"}],
            maxmegabites=20
            )
        
        self.output = self.addComplexOutput(
            identifier="output",
            title="Workflow result",
            formats=[{"mimeType":"text/yaml"}],
            asReference=True,
            )
        
        self.logfile = self.addComplexOutput(
            identifier="logfile",
            title="Workflow log file",
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )
       
    def execute(self):
        def monitor(message, progress):
            self.status.set(message, progress)
        
        self.status.set("starting workflow ...", 0)

        fp = open(self.workflow.getValue())
        workflow = yaml.load(fp)
        workflow_name = workflow.get('name', 'unknown')
        
        self.status.set("workflow {0} prepared.".format(workflow_name), 0)

        result = run(workflow, monitor=monitor)

        _,outfile = tempfile.mkstemp(suffix='.txt')
        with open(outfile, 'w') as fp:
            yaml.dump(result, stream=fp)
            self.output.setValue(outfile)
        _,outfile = tempfile.mkstemp(suffix='.txt')
        with open(outfile, 'w') as fp:
            fp.write("workflow log file")
            self.logfile.setValue(outfile)
        self.status.set("workflow {0} done.".format(workflow_name), 100)


        

    
        


