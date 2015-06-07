from malleefowl.process import WPSProcess
from malleefowl import thredds

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class ThreddsDownload(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "thredds_download",
            title = "Download files from Thredds Catalog",
            version = "0.1",
            abstract="Downloads files from Thredds Catalog and provides file list as JSON Document.")       

        self.url = self.addLiteralInput(
            identifier = "url",
            title = "URL",
            abstract = "URL of the catalog",
            minOccurs=1,
            maxOccurs=1,
            type=type(''),
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Downloaded files",
            abstract="JSON document with list of downloaded files with file url.",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def execute(self):
        files = thredds.download(url=self.url.getValue(), monitor=self.show_status)

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )


