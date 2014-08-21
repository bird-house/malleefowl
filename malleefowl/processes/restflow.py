"""
Processes for testing wps data types

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os.path
import types

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

from malleefowl.process import WPSProcess
from malleefowl import restflow, config

class Generate(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "restflow_generate",
            title = "Generate Restflow Workflow Description",
            version = "0.1",
            metadata=[
                {"title":"Restflow","href":"https://github.com/restflow-org"},
                ],
            abstract="Generates Restflow workflow description in yaml from given json input parameters.")

        self.name = self.addLiteralInput(
            identifier="name",
            title="Workflow",
            abstract="Choose Workflow",
            default="simpleWorkflow",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['simpleWorkflow']
            )

        self.nodes= self.addComplexInput(
            identifier="nodes",
            title="Workflow Nodes",
            abstract="Workflow Nodes in JSON",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[{"mimeType":"text/json"}],
            maxmegabites=2
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Workflow Description",
            abstract="Workflow Description in YAML",
            metadata=[],
            formats=[{"mimeType":"text/yaml"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("Generate workflow ...", 0)

        # TODO: handle multiple values (fix in pywps)
        # http://pymotw.com/2/json/
        wf = ''
        with open(self.nodes.getValue()) as fp:
            import yaml
            # TODO: fix json encode to unicode
            nodes = yaml.load(fp)
            logger.debug("nodes: %s", nodes)
   
            wf = restflow.generate(self.name.getValue(), nodes)
            logger.debug("generated wf: %s", wf)
        
        outfile = self.mktempfile(suffix='.yaml')
        restflow.write( outfile, wf )

        self.output.setValue( outfile )

        self.show_status("Generate workflow ... Done", 100)

class Run(WPSProcess):
    """This process runs a restflow workflow"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "restflow_run",
            title = "Run Restflow Workflow",
            version = "0.1",
            metadata=[
                {"title":"Restflow","href":"https://github.com/restflow-org"},
                ],
            abstract="Runs given workflow with yaml description")


        self.command = self.addLiteralInput(
            identifier="command",
            title="Command",
            abstract="Choose Restflow Command",
            default="execute",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['execute', 'validate', 'visualize']
            )

        self.workflow_description = self.addComplexInput(
            identifier="workflow_description",
            title="Workflow description",
            abstract="Workflow description in YAML",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            maxmegabites=2,
            formats=[{"mimeType":"text/yaml"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Workflow Result",
            abstract="Workflow Result",
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )
        
    def execute(self):
        self.show_status("starting workflow", 0)
        
        wf_description = self.workflow_description.getValue()

        status = lambda msg, percent: self.show_status(msg, percent)
        result = restflow.run(
            wf_description, 
            timeout=config.timeout(), 
            status_callback=status)

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=result, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("starting workflow", 100)


