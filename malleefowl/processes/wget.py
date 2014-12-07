from malleefowl.process import WPSProcess
from malleefowl import config

from malleefowl.utils import esgf_archive_path

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
            maxOccurs=300,
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

        #self.output_external = self.addComplexOutput(
        #    identifier="output_external",
        #    title="Downloaded files for external access",
        #    abstract="Json document with list of downloaded files with http url for external use.",
        #    metadata=[],
        #   formats=[{"mimeType":"test/json"}],
        #    asReference=True,
        #    )

    def _wget(self, url, credentials=None):
        from os.path import basename
        resource_name = basename(url)
        logger.debug('downloading %s', url)
        #logger.debug('credentials %s', credentials)
        
        try:
            cmd = ["wget"]
            if credentials is not None:
                logger.debug('using credentials')
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
            cmd.append(url)
            self.cmd(cmd, stdout=True)
        except Exception, e:
            msg = "wget failed on %s. Maybe not authorized? " % (resource_name)
            logger.exception(msg)
            raise Exception(msg + str(e))

        from os.path import join
        cached_file = join(config.cache_path(), resource_name)
        local_url = "file://" + cached_file
        #external_url = config.cache_url() + '/' + resource_name

        return local_url

    def execute(self):
        self.show_status("Downloading ...", 0)

        credentials = self.credentials.getValue()
        resource = self.resource.getValue()

        logger.debug('credentials: %s', credentials)

        import types
        if type(resource) != types.ListType:
            resource = [resource]

        local_files = []
        #external_files = []

        count = 0
        max_count = len(resource)
        for url in resource:
            count = count + 1
            progress = (count-1) * 100.0 / max_count
            self.show_status("Downloading %d/%d" % (count, max_count), progress)

            #external_url = None
            local_url = esgf_archive_path(url)
            if local_url is None:
                local_url = self._wget(url, credentials=credentials)
            
            local_files.append(local_url)
            #if external_url is not None:
            #    external_files.append(external_url)

        if max_count > len(local_files):
            logger.warn('Could not retrieve all files: %d from %d', len(local_files), max_count)
            if len(local_files) == 0:
                raise Exception("Could not retrieve any file. Check your permissions!")

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=local_files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        #outfile = self.mktempfile(suffix='.json')
        #with open(outfile, 'w') as fp:
        #    json.dump(obj=external_files, fp=fp, indent=4, sort_keys=True)
        #self.output_external.setValue( outfile )

        self.show_status("Downloading ... done", 100)


