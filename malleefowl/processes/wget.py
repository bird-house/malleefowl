from malleefowl.process import WPSProcess
from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Wget(WPSProcess):
    """This process downloads files form esgf data node via wget and http"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "esgf_wget",
            title = "ESGF wget download",
            version = "0.2",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Download files from esgf data node with wget")

        self.source = self.addLiteralInput(
            identifier="source",
            title="Source",
            abstract="URL of your source ...",
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        self.credentials = self.addComplexInput(
            identifier = "credentials",
            title = "X509 Certificate",
            abstract = "X509 proxy certificate to access ESGF data.",
            minOccurs=1,
            maxOccurs=1,
            maxmegabites=1,
            formats=[{"mimeType":"application/x-pkcs7-mime"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="NetCDF File",
            abstract="NetCDF file downloaded with wget.",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("Starting wget download ...", 5)

        credentials = self.credentials.getValue()
        source = self.source.getValue()

        self.show_status("Downloading %s" % (source), 10)

        try:
            cmd = ["wget",
                   "--certificate", credentials, 
                   "--private-key", credentials, 
                   "--no-check-certificate", 
                   "-N",
                   "-P", config.cache_path(),
                   "--progress", "dot:mega",
                   source]
            self.cmd(cmd, stdout=True)
        except Exception:
            msg = "wget failed ..."
            self.show_status(msg, 20)
            logger.exception(msg)
            raise

        from os.path import join, basename
        outfile = join(config.cache_path(), basename(source))
        logger.debug('result file=%s', outfile)

        self.output.setValue(outfile)
        self.show_status("Downloading... done", 90)

