import os
import json

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput
from pywps import LiteralOutput
from pywps import ComplexOutput
from pywps import FORMATS
from pywps.app.Common import Metadata

from malleefowl.download import download_files

import logging
LOGGER = logging.getLogger("PYWPS")


class Download(Process):
    """
    The download process gets as input a list of URLs pointing to NetCDF files
    which should be downloaded.

    The downloader first checks if the file is available in the local ESGF archive or cache.
    If not then the file will be downloaded and stored in a local cache.
    As a result it provides a list of local ``file://`` paths to the requested files.

    The downloader does not download files if they are already in the
    ESGF archive or in the local cache.
    """

    def __init__(self):
        inputs = [
            LiteralInput('resource', 'Resource',
                         data_type='string',
                         abstract="URL pointing to your resource which should be downloaded.",
                         min_occurs=1,
                         max_occurs=1024,
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'Downloaded files',
                          abstract="Json document with list of downloaded files with file url.",
                          as_reference=True,
                          supported_formats=[FORMATS.JSON]),
        ]

        super(Download, self).__init__(
            self._handler,
            identifier="download",
            title="Download files",
            version="0.9",
            abstract="Downloads files and provides file list as json document.",
            metadata=[
                Metadata('Birdhouse', 'http://bird-house.github.io/'),
                Metadata('User Guide', 'http://malleefowl.readthedocs.io/en/latest/'),
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )

    def _handler(self, request, response):
        response.update_status("starting download ...", 0)

        urls = [resource.data for resource in request.inputs['resource']]
        credentials = os.path.join(self.workdir, 'cert.pem')
        if not os.path.isfile(credentials):
            credentials = None

        def monitor(msg, progress):
            LOGGER.info("%s - (%d/100)", msg, progress)
            # response.update_status(msg, progress)

        files = download_files(
            urls=urls,
            credentials=credentials,
            tempdir=self.workdir,
            monitor=monitor)

        with open(os.path.join(self.workdir, 'out.json'), 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name

        response.update_status("download done", 100)
        return response
