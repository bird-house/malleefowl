import yaml

from pywps.Process import WPSProcess
from malleefowl import config
from malleefowl.workflow import run

class DispelWorkflow(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier="workflow",
            title="Workflow",
            version="0.5",
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

        outfile = 'output.txt'
        with open(outfile, 'w') as fp:
            yaml.dump(result, stream=fp)
            self.output.setValue(outfile)
        outfile = 'logfile.txt'
        with open(outfile, 'w') as fp:
            fp.write("workflow log file")
            self.logfile.setValue(outfile)
        self.status.set("workflow {0} done.".format(workflow_name), 100)

class DummyProcess(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier="dummy",
            title="Dummy Process",
            version="1.0",
            abstract="Dummy Process used by Workflow Tests.",
            statusSupported=True,
            storeSupported=True)

        self.dataset = self.addComplexInput(
            identifier="dataset",
            title="Dataset (NetCDF)",
            minOccurs=1,
            maxOccurs=10,
            maxmegabites=5000,
            formats=[{"mimeType":"application/x-netcdf"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Output",
            abstract="Output",
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )


    def execute(self):
        self.status.set(self, "starting ...", 0)
        datasets = self.getInputValues(identifier='dataset')
        outfile = 'out.txt'
        with open(outfile, 'w') as fp:
            fp.write('we got %d files.' % len(datasets))
        self.output.setValue( outfile )

        self.status.set("done", 100)

    
        


