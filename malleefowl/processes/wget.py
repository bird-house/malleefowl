from malleefowl.process import WPSProcess
from malleefowl import config
from malleefowl.download import download_with_archive

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

        credentials = self.credentials.getValue()
        resources = self.getInputValues(identifier='resource')

        local_files = []

        count = 0
        max_count = len(resources)
        for url in resources:
            count = count + 1
            progress = (count-1) * 100.0 / max_count
            self.show_status("Downloading %d/%d" % (count, max_count), progress)

            try:
                local_files.append(download_with_archive(url, credentials))
            except:
                logger.exception("Failed to download %s", url)

        if max_count > len(local_files):
            logger.warn('Could not retrieve all files: %d from %d', len(local_files), max_count)
            if len(local_files) == 0:
                raise Exception("Could not retrieve any file. Check your permissions!")

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=local_files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("Downloading ... done", 100)


