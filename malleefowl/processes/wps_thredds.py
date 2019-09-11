import os
import json

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexOutput
from pywps import FORMATS
from pywps.app.Common import Metadata

from malleefowl import download


class ThreddsDownload(Process):
    def __init__(self):
        inputs = [
            LiteralInput('url', 'URL',
                         data_type='string',
                         abstract="URL of the catalog.",
                         min_occurs=1,
                         max_occurs=1,
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'Downloaded files',
                          abstract="JSON document with list of downloaded files with file url.",
                          as_reference=True,
                          supported_formats=[FORMATS.JSON]),
        ]

        super(ThreddsDownload, self).__init__(
            self._handler,
            identifier="thredds_download",
            title="Download files from Thredds Catalog",
            version="0.5",
            abstract="Downloads files from Thredds Catalog and provides file list as JSON Document.",
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
        def monitor(message, progress):
            response.update_status(message, progress)
        files = download.download_files_from_thredds(
            url=request.inputs['url'][0].data,
            tempdir=self.workdir,
            monitor=monitor)

        with open(os.path.join(self.workdir, 'out.json'), 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name
        return response
