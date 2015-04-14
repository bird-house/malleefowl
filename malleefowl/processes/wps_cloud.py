from malleefowl.process import WPSProcess
from malleefowl import cloud

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Download(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "cloud_download",
            title = "Download files from Swift Cloud",
            version = "1.0",
            abstract="Downloads files from Swift Cloud and provides file list as json document.")

        self.storage_url = self.addLiteralInput(
            identifier="storage_url",
            title="Storage URL",
            abstract="Storage URL",
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        self.auth_token = self.addLiteralInput(
            identifier = "auth_token",
            title = "Auth Token",
            abstract = "Auth Token",
            minOccurs=1,
            maxOccurs=1,
            type=type(''),
            )

        self.container = self.addLiteralInput(
            identifier = "container",
            title = "Container",
            abstract = "Container",
            minOccurs=1,
            maxOccurs=1,
            type=type(''),
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Downloaded files",
            abstract="JSON document with list of downloaded files with file url.",
            metadata=[],
            formats=[{"mimeType":"test/json"}],
            asReference=True,
            )

    def execute(self):
        files = cloud.download(
            self.storage_url.getValue(),
            self.auth_token.getValue(),
            self.container.getValue())

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )


