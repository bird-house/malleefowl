"""
Processes for testing wps data types

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

from os import path
import time
import yaml
import json
import types

import logging
log = logging.getLogger(__name__)

from malleefowl.process import WPSProcess
from malleefowl import restflow

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
        log.debug('json doc: %s', self.nodes.getValue())
        fp = open(self.nodes.getValue())
        nodes = json.load(fp)
        log.debug("nodes: %s", nodes)
   
        wf = restflow.generate(self.name.getValue(), nodes)
        log.debug("generated wf: %s", wf)
        
        outfile = self.mktempfile(suffix='.yaml')
        restflow.write( outfile, wf )

        self.status.set(msg="Generate workflow ... Done", percentDone=90, propagate=True)

        self.output.setValue( outfile )

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
            maxmegabites=20,
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
        self.status.set(msg="Starting restflow workflow", percentDone=5, propagate=True)

        wf_filename = path.abspath(self.workflow_description_in.getValue(asFile=False))
        log.debug("wf_filename = %s", wf_filename)

        options = ''
        
        if self.restflow_command_in.getValue() == "validate":
            options = '--validate'
        elif self.restflow_command_in.getValue() == "visualize":
            options = '--to-dot'

        # run async command
        import subprocess
        from subprocess import PIPE
        cmd = ["restflow", options, "-t", "-f", wf_filename, "--run", "restflow"]
        p = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
                
        products_path = path.join(self.working_dir, "restflow", "_metadata", "products.yaml")
        endstate_path = path.join(self.working_dir, "restflow", "_metadata", "endstate.yaml")

        log.debug("products_path = %s", products_path)

        self.status.set(msg="Downloading ...", percentDone=10, propagate=True)

        while p.poll() == None:   
            time.sleep(1)

            if path.isfile(endstate_path):
                break
            
            if not path.isfile(products_path):
                continue
            products = yaml.load( open(products_path) )
            if products == None:
                continue
            
            if products.has_key('/wps/download/file_identifier/1'):
                self.status.set(msg="First file downloaded", percentDone=20, propagate=True)

            if products.has_key('/wps/download/file_identifiers/1'):
                self.status.set(msg="All files downloaded", percentDone=55, propagate=True)

        # wait till finished
        (stdoutdata, stderrdata) = p.communicate()
        self.status.set(msg=stdoutdata + stderrdata, percentDone=80, propagate=True)

        products = yaml.load( open(products_path) )
        self.work_output.setValue(products.get('/wps/work/output/1'))
        self.work_status.setValue(products.get('/wps/work/status/1'))
       
        self.status.set(msg="Workflow done", percentDone=90, propagate=True)

        self.output.setValue( path.join(self.working_dir, "restflow", "_metadata", "stdout.txt") )
