from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

from malleefowl.process import WPSProcess
from malleefowl import restflow, config

class DispelWorkflow(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "dispel",
            title = "Run Dispel Workflow",
            version = "0.1",
            metadata=[],
            abstract="Runs Workflow with dispel4py.")

        self.name = self.addLiteralInput(
            identifier="name",
            title="Workflow",
            abstract="Choose Workflow",
            default="esgsearch_workflow",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['esgsearch_workflow']
            )

        self.nodes= self.addComplexInput(
            identifier="nodes",
            title="Workflow Nodes",
            abstract="Workflow Nodes in JSON",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[{"mimeType":"text/json"}],
            maxmegabites=20
            )
        
        self.output = self.addComplexOutput(
            identifier="output",
            title="Workflow Result",
            abstract="Workflow Result",
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )
       
    def execute(self):
        def monitor(execution):
            self.show_status(execution.statusMessage,execution.percentCompleted)
        
        self.show_status("Starting ...", 0)

        import os
        logger.debug('environ: %s', os.environ)

        # TODO: handle multiple values (fix in pywps)
        # http://pymotw.com/2/json/
        nodes = None
        with open(self.nodes.getValue()) as fp:
            import yaml
            # TODO: fix json encode to unicode
            nodes = yaml.load(fp)

        self.show_status("Prepared ...", 5)

        esgsearch = nodes['esgsearch']
        constraints = esgsearch['facets']
        distrib = esgsearch['distrib'] == 'true'
        latest = esgsearch['latest'] == 'true'
        replica = esgsearch['replica'] == 'true'
        from malleefowl.dispel import esgsearch_workflow
        logger.debug('nodes=%s', nodes)
        result = esgsearch_workflow(
            url=nodes['source']['service'],
            esgsearch_params=dict(constraints=constraints, limit=100, search_type='File', distrib=distrib),
            wget_params=dict(credentials=nodes['source']['credentials']),
            doit_params=dict(url=nodes['worker']['service'],
                             identifier=nodes['worker']['identifier'],
                             resource=nodes['worker']['resource'],
                             inputs=nodes['worker']['inputs']),
            monitor = monitor,
            )
        
        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=result, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("workflow ... done", 100)


