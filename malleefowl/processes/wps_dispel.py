import yaml

from malleefowl.process import WPSProcess
from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def esgsearch_workflow(nodes, monitor):
    from malleefowl.dispel import esgsearch_workflow as esgwf
    esgsearch = nodes['esgsearch']
    logger.debug('nodes=%s', nodes)

    result = esgwf(
        url=nodes['source']['service'],
        esgsearch_params=dict(
            constraints=esgsearch['facets'],
            limit=100,
            #search_type='File_Thredds',
            search_type='File',
            distrib=esgsearch['distrib'],
            latest=esgsearch['latest'],
            replica=esgsearch['replica'],
            temporal=esgsearch['temporal'],
            start=esgsearch['start'],
            end=esgsearch['end']),
        download_params=dict(credentials=nodes['source']['credentials']),
        doit_params=dict(url=nodes['worker']['service'],
                         identifier=nodes['worker']['identifier'],
                         resource=nodes['worker']['resource'],
                         inputs=nodes['worker']['inputs']),
        monitor = monitor,
        )
    return result

def swift_workflow(nodes, monitor):
    from malleefowl.dispel import swift_workflow as swiftwf
    logger.debug('nodes=%s', nodes)

    result = swiftwf(
        # TODO: fix parameter providing
        url=nodes['source']['service'],
        download_params=dict(storage_url=nodes['source']['storage_url'],
                             auth_token=nodes['source']['auth_token'],
                             container=nodes['source']['container'],
                             prefix=nodes['source']['prefix']),
        doit_params=dict(url=nodes['worker']['service'],
                         identifier=nodes['worker']['identifier'],
                         resource=nodes['worker']['resource'],
                         inputs=nodes['worker']['inputs']),
        monitor = monitor,
        )
    return result

class DispelWorkflow(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "dispel",
            title = "Run Dispel Workflow",
            version = "0.3",
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
            allowedValues=['esgsearch_workflow', 'swift_workflow']
            )

        self.nodes= self.addComplexInput(
            identifier="nodes",
            title="Workflow Nodes",
            abstract="Workflow Nodes in JSON",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[{"mimeType":"text/yaml"}],
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

        # TODO: handle multiple values (fix in pywps)
        # http://pymotw.com/2/json/
        nodes = None
        with open(self.nodes.getValue()) as fp:
            nodes = yaml.load(fp)

        self.show_status("Prepared ...", 5)

        result = None
        if self.name.getValue() == 'swift_workflow':
            result = swift_workflow(nodes, monitor)
        else:
            result = esgsearch_workflow(nodes, monitor)

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=result, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("workflow ... done", 100)

    
        


