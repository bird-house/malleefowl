from pywps import Process
from pywps import LiteralInput
from pywps import LiteralOutput
from pywps import ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata

from datetime import date

from malleefowl.esgf import logon


class MyProxyLogon(Process):
    def __init__(self):
        inputs = [
            LiteralInput('openid', 'ESGF OpenID',
                         data_type='string',
                         abstract="For example: https://esgf-data.dkrz.de/esgf-idp/openid/username.",
                         min_occurs=1,
                         max_occurs=1,
                         ),
            LiteralInput('password', 'OpenID Password',
                         data_type='string',
                         abstract="Enter your Password.",
                         min_occurs=1,
                         max_occurs=1,
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'X509 Certificate',
                          abstract="X509 Proxy Certificate",
                          as_reference=True,
                          supported_formats=[Format('application/x-pkcs7-mime')]),
            LiteralOutput('expires', 'Expires', data_type='date',
                          abstract="Proxy certificate expires at date.",
                          ),
        ]

        super(MyProxyLogon, self).__init__(
            self._handler,
            identifier="esgf_logon",
            title="ESGF MyProxy Logon",
            version="0.5",
            abstract="Run MyProxy Logon to retrieve an ESGF certificate.",
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
        openid = request.inputs['openid'][0].data
        password = request.inputs['password'][0].data

        certfile = logon.myproxy_logon_with_openid(openid=openid, password=password, interactive=False)

        response.outputs['output'].file = certfile
        response.outputs['expires'].data = logon.cert_infos(certfile)['expires']

        response.update_status("logon successful", 100)
        return response
