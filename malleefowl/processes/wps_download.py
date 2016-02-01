import json
from pywps.Process import WPSProcess

from malleefowl.download import download_files

class Download(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier="download",
            title="Download files",
            version="0.5",
            abstract="Downloads files and provides file list as json document.",
            statusSupported=True,
            storeSupported=True)

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

        self.openid = self.addLiteralInput(
            identifier = "openid",
            title = "ESGF OpenID",
            abstract = "For example: https://esgf-data.dkrz.de/esgf-idp/openid/username",
            minOccurs = 0,
            maxOccurs = 1,
            type = type('')
            )

        self.password = self.addLiteralInput(
            identifier = "password",
            title = "OpenID Password",
            abstract = "Enter your Password",
            minOccurs = 0,
            maxOccurs = 1,
            type = type('')
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Downloaded files",
            abstract="Json document with list of downloaded files with file url.",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def monitor(self, msg, progress):
        self.status.set(msg, progress)

    def execute(self):
        files = download_files(
            urls=self.getInputValues(identifier='resource'),
            credentials=self.credentials.getValue(),
            openid=self.openid.getValue(),
            password=self.password.getValue(),
            monitor=self.monitor)

        outfile = 'out.json'
        with open(outfile, 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )



