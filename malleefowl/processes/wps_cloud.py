from malleefowl.process import WPSProcess
from malleefowl import cloud

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Login(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "cloud_login",
            title = "Login to Swift Cloud",
            version = "1.0",
            abstract="Login to Swift Cloud and get Token.")

        self.username = self.addLiteralInput(
            identifier="username",
            title="Username",
            minOccurs=1,
            maxOccurs=1,
            type=type(''),
            )

        self.password = self.addLiteralInput(
            identifier = "password",
            title = "Password",
            minOccurs=1,
            maxOccurs=1,
            type=type(''),
            )

        self.auth_token = self.addLiteralOutput(
            identifier="auth_token",
            title="Auth Token",
            type=type(''),
            )

        self.storage_url = self.addLiteralOutput(
            identifier="storage_url",
            title="Storage URL",
            type=type(''),
            )

    def execute(self):
        (storage_url, auth_token) = cloud.login(
            self.username.getValue(),
            self.password.getValue())

        self.storage_url.setValue( storage_url )
        self.auth_token.setValue( auth_token )

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
            minOccurs=1,
            maxOccurs=1,
            type=type(''),
            )

        self.auth_token = self.addLiteralInput(
            identifier = "auth_token",
            title = "Auth Token",
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


