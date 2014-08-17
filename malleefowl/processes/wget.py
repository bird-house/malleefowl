from malleefowl.process import WPSProcess
from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Wget(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "esgf_wget",
            title = "wget download",
            version = "0.3",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Downloads files with wget.")

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
            minOccurs=0,
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

        self.output_path = self.addLiteralOutput(
            identifier="output_path",
            title="Output path",
            abstract="Path to downloaded NetCDF file.",
            type=type('')
            )

    def execute(self):
        self.show_status("Starting wget download ...", 5)

        credentials = self.credentials.getValue()
        source = self.source.getValue()

        self.show_status("Downloading %s" % (source), 10)

        try:
            cmd = ["wget"]
            if credentials is not None:
                cmd.append("--certificate")
                cmd.append(credentials) 
                cmd.append("--private-key")
                cmd.append(credentials)
            cmd.append("--no-check-certificate") 
            cmd.append("-N")
            cmd.append("-P")
            cmd.append(config.cache_path())
            cmd.append("--progress")
            cmd.append("dot:mega")
            cmd.append(source)
            self.cmd(cmd, stdout=True)
        except Exception, e:
            msg = "wget failed ..."
            self.show_status(msg + str(e), 20)
            logger.exception(msg)
            raise

        from os.path import join, basename
        outfile = join(config.cache_path(), basename(source))
        logger.debug('result file=%s', outfile)

        self.output.setValue(outfile)
        self.output_path.setValue("file://" + outfile)
        self.show_status("Downloading... done", 100)

