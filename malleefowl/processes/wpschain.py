import os.path
import types

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

from malleefowl.process import WPSProcess

class SimpleWPSChain(WPSProcess):
    """runs simple wps chain (source+worker)"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.wpschain",
            title = "Run simple WPS chain",
            version = "0.1",
            metadata=[
                ],
            abstract="Run simple WPS chain")


        self.nodes= self.addComplexInput(
            identifier="nodes",
            title="Workflow Nodes",
            abstract="Workflow Nodes in JSON",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[{"mimeType":"text/json"}],
            maxmegabites=2,
            upload=True,
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Workflow Result",
            abstract="Workflow Result",
            formats=[{"mimeType":"text/txt"}],
            asReference=True,
            )
        
    def execute(self):
        self.status.set(msg="Run workflow ...", percentDone=5, propagate=True)

        # TODO: handle multiple values (fix in pywps)
        # http://pymotw.com/2/json/
        logger.debug('json doc: %s', self.nodes.getValue())
        fp = open(self.nodes.getValue())
        
        import yaml
        # TODO: fix json encode to unicode
        nodes = yaml.load(fp)
        logger.debug("nodes: %s", nodes)
   
        #wf = restflow.generate(self.name.getValue(), nodes)
                
        outfile = self.mktempfile(suffix='.txt')
        with open(outfile, 'w') as fp:
            fp.write('just testing ...')
        
        self.status.set(msg="Workflow ... Done", percentDone=90, propagate=True)

        self.output.setValue( outfile )
