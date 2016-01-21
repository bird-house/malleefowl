from pywps.Process import WPSProcess

from malleefowl import download
from malleefowl import config

class ThreddsDownload(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier="thredds_download",
            title="Download files from Thredds Catalog",
            version="0.3",
            abstract="Downloads files from Thredds Catalog and provides file list as JSON Document.",
            statusSupported=True,
            storeSupported=True)       

        self.url = self.addLiteralInput(
            identifier="url",
            title="URL",
            abstract="URL of the catalog",
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

    def monitor(self, msg, percent_done):
        self.status.set(msg, percent_done)

    def execute(self):
        files = download.download_files_from_thredds(
            url=self.url.getValue(),
            monitor=self.monitor)

        import json
        _,outfile = self.mkstemp(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )


