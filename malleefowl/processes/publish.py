from malleefowl.process import WPSProcess

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


class Publish(WPSProcess):
    """
    TODO: combine publish with wget ... input needs to be a file list to keep the filenames
    """
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "publish",
            title = "Publish",
            version = "1.0",
            metadata=[],
            abstract="Publish files.",
            )

        self.resource = self.addComplexInput(
            identifier="resource",
            title="Resource",
            abstract="Files",
            minOccurs=0,
            maxOccurs=1024,
            maxmegabites=50000,
            formats=[{"mimeType":"application/x-netcdf"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="URls",
            abstract="URls of published resources.",
            metadata=[],
            formats=[{"mimeType":"text/json"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("starting ...", 0)

        files = self.getInputValues(identifier='resource')

        from malleefowl.publish import publish
        urls = publish(files)

        import json
        outfile = self.mktempfile(suffix='.txt')
        with open(outfile, 'w') as fp:
            json.dump(obj=urls, fp=fp, indent=4, sort_keys=True)
            self.output.setValue(outfile)
            
        self.show_status("done", 100)

        
