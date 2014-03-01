"""
Processes for testing wps data types

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os.path
import types

import logging
logging.debug('check logging')

from malleefowl.process import WPSProcess
from malleefowl import restflow

class GenerateAndRun(WPSProcess):
    """Generates workflow description document in yaml and runs it"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.restflow.genrun",
            title = "Generate and run Restflow Workflow",
            version = "0.1",
            metadata=[
                {"title":"Restflow","href":"https://github.com/restflow-org"},
                ],
            abstract="Generate YAML workflow description and run it")

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
            title="Workflow Result",
            abstract="Workflow Result",
            metadata=[],
            formats=[{"mimeType":"text/txt"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="Generate workflow ...", percentDone=5, propagate=True)

        # TODO: handle multiple values (fix in pywps)
        # http://pymotw.com/2/json/
        logging.debug('json doc: %s', self.nodes.getValue())
        fp = open(self.nodes.getValue())
        
        import yaml
        # TODO: fix json encode to unicode
        nodes = yaml.load(fp)
        logging.debug("nodes: %s", nodes)
        self.message("nodes: %s" % (nodes))
   
        wf = restflow.generate(self.name.getValue(), nodes)
        logging.debug("generated wf: %s", wf)
        self.message("generatd wf: %s" % (wf))
        
        outfile = self.mktempfile(suffix='.txt')
        restflow.write( outfile, wf )

        self.status.set(msg="Run workflow ...", percentDone=10, propagate=True)

        (result_file, retcode, stdoutdata, stderrdata) = restflow.run(outfile, verbose=True)
        self.message("result %s %s %s %s" % (result_file, retcode, stdoutdata, stderrdata))
        
        self.status.set(msg="Workflow ... Done", percentDone=90, propagate=True)

        self.output.setValue( result_file )

class Generate(WPSProcess):
    """Generates workflow description document in yaml for restflow"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.restflow.generate",
            title = "Generate Restflow Workflow",
            version = "0.1",
            metadata=[
                {"title":"Restflow","href":"https://github.com/restflow-org"},
                ],
            abstract="Generate YAML workflow description for restflow")

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
        self.status.set(msg="Generate workflow ...", percentDone=5, propagate=True)

        # TODO: handle multiple values (fix in pywps)
        # http://pymotw.com/2/json/
        logging.debug('json doc: %s', self.nodes.getValue())
        fp = open(self.nodes.getValue())
        
        import yaml
        # TODO: fix json encode to unicode
        nodes = yaml.load(fp)
        logging.debug("nodes: %s", nodes)
   
        wf = restflow.generate(self.name.getValue(), nodes)
        logging.debug("generated wf: %s", wf)
        
        outfile = self.mktempfile(suffix='.txt')
        restflow.write( outfile, wf )

        self.status.set(msg="Generate workflow ... Done", percentDone=90, propagate=True)

        self.output.setValue( outfile )

class Run(WPSProcess):
    """This process runs a restflow workflow description"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.restflow.run",
            title = "Run restflow workflow",
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
            formats=[{"mimeType":"text/txt"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="Starting Workflow", percentDone=5, propagate=True)

        filename = os.path.abspath(self.workflow_description.getValue(asFile=False))
        logging.debug("filename = %s", filename)

        result = restflow.run(filename, verbose=True)

        self.status.set(msg="Workflow done", percentDone=90, propagate=True)

        #import time
        #time.sleep(20)

        self.output.setValue( result )
