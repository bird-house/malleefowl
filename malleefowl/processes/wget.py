from malleefowl.process import WPSProcess
from malleefowl.download import download_files

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Wget(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "wget",
            title = "wget download",
            version = "0.4",
            abstract="Downloads files with wget and provides file list as json document.")

        self.resource = self.addLiteralInput(
            identifier="resource",
            title="Resource",
            abstract="URL of your resource ...",
            minOccurs=1,
            maxOccurs=1024,
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

        self.output = self.addComplexOutput(
            identifier="output",
            title="Downloaded files for local access",
            abstract="Json document with list of downloaded files with file url.",
            metadata=[],
            formats=[{"mimeType":"test/json"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("Downloading ...", 0)

        files = download_files(
            urls = self.getInputValues(identifier='resource'),
            credentials = self.credentials.getValue(),
            monitor=self.show_status)

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("Downloading ... done", 100)


