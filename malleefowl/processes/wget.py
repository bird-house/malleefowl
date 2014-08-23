from malleefowl.process import WPSProcess
from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Wget2(WPSProcess):
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
            maxOccurs=100,
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
            title="Downloaded files",
            abstract="Json document with list of downloaded files.",
            metadata=[],
            formats=[{"mimeType":"test/json"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("Starting wget download ...", 0)

        credentials = self.credentials.getValue()
        resource = self.resource.getValue()

        import types
        if type(resource) != types.ListType:
            resource = [resource]

        files = {'local': [], 'external': []}
        count = 0
        for url in resource:
            count = count + 1

            from os.path import basename
            resource_name = basename(url)
            self.show_status("Downloading %s" % resource_name, count)

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
                cmd.append(url)
                self.cmd(cmd, stdout=True)
            except Exception, e:
                msg = "wget failed ..."
                logger.exception(msg)
                raise Exception(msg + str(e))

            from os.path import join
            cached_file = join(config.cache_path(), resource_name)
            local_url = "file://" + cached_file
            external_url = "http://localhost/cash" + cached_file
            files['local'].append(local_url)
            files['external'].append(external_url)

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("Downloading... done", 100)


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
            title="Downloaded file",
            abstract="File downloaded with wget.",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )

        self.output_path = self.addLiteralOutput(
            identifier="output_path",
            title="File path",
            abstract="Local Path to downloaded file.",
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

