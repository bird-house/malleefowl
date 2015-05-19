import yaml

from malleefowl.process import WPSProcess
from malleefowl import config
from malleefowl.dispel import run

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class DispelWorkflow(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "dispel",
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
        def monitor(execution):
            self.show_status(execution.statusMessage,execution.percentCompleted)
        
        self.show_status("Starting ...", 0)

        # TODO: handle multiple values (fix in pywps)
        # http://pymotw.com/2/json/
        workflow = None
        with open(self.workflow.getValue()) as fp:
            workflow = yaml.load(fp)
            logger.debug('workflow=%s', workflow)

        self.show_status("Prepared ...", 5)
        result = run(workflow, monitor)

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=result, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("workflow ... done", 100)

    
        


