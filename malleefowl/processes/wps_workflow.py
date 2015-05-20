import yaml

from malleefowl.process import WPSProcess
from malleefowl import config
from malleefowl.workflow import run

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class DispelWorkflow(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "workflow",
            title = "Workflow",
            version = "0.3",
            metadata=[],
            abstract="Runs Workflow with dispel4py.")

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
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )
       
    def execute(self):
        self.show_status("starting workflow ...", 0)

        # TODO: handle multiple values (fix in pywps)
        # http://pymotw.com/2/json/
        result = None
        with open(self.workflow.getValue()) as fp:
            workflow = yaml.load(fp)
            self.show_status("workflow prepared", 0)
            result = run(workflow, monitor=self.show_status)

        if result is None:
            raise Exception("could not process workflow.")

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=result, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )
    
        self.show_status("workflow done", 100)

    
        


