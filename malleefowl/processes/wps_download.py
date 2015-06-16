from malleefowl.process import WPSProcess
from malleefowl.download import download_files

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Download(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "download",
            title = "Download files",
            version = "0.4",
            abstract="Downloads files and provides file list as json document.")

        self.resource = self.addLiteralInput(
            identifier="resource",
            title="Resource",
            abstract="URL of your resource ...",
            minOccurs=1,
            maxOccurs=2048,
            type=type('')
            )

        self.credentials = self.addComplexInput(
            identifier = "credentials",
            title = "X509 Certificate",
            abstract = "X509 proxy certificate to access ESGF data.",
            minOccurs=0,
            maxOccurs=1,
            maxmegabites=1,
            formats=[{"mimeType":"application/x-pkcs7-mime"}],
            )

        self.cookies = self.addComplexInput(
            identifier = "cookies",
            title = "Cookie with credentials",
            abstract = "Cookie with credentials to access ESGF data.",
            minOccurs=0,
            maxOccurs=1,
            maxmegabites=1,
            formats=[{"mimeType":"text/plain"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Downloaded files",
            abstract="Json document with list of downloaded files with file url.",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def execute(self):
        files = download_files(
            urls = self.getInputValues(identifier='resource'),
            credentials = self.credentials.getValue(),
            cookies = self.cookies.getValue(),
            monitor=self.show_status)

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )



