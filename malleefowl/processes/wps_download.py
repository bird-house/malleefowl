import json

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput
from pywps import LiteralOutput
from pywps import ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata

from malleefowl.download import download_files


class Download(Process):

    def __init__(self):
        inputs = [
            LiteralInput('resource', 'Resource',
                         data_type='string',
                         abstract="URL of your resource.",
                         min_occurs=1,
                         max_occurs=1024,
                         ),
            ComplexInput('credentials', 'X509 Certificate',
                         abstract='Optional X509 proxy certificate to access ESGF data.',
                         metadata=[Metadata('Info')],
                         min_occurs=0,
                         max_occurs=1,
                         supported_formats=[Format('application/x-pkcs7-mime')]),
        ]
        outputs = [
            ComplexOutput('output', 'Downloaded files',
                          abstract="Json document with list of downloaded files with file url.",
                          as_reference=True,
                          supported_formats=[Format('application/json')]),
        ]

        super(Download, self).__init__(
            self._handler,
            identifier="download",
            title="Download files",
            version="0.7",
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
        urls = [resource.data for resource in request.inputs['resource']]
        if 'credentials' in reqest.inputs:
            credentials = request.inputs['credentials'].data
        else:
            credentials = None

        def monitor(msg, progress):
            response.update_status(msg, progress)

        files = download_files(
            urls=urls,
            credentials=credentials,
            monitor=monitor)

        with open('out.json', 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name
