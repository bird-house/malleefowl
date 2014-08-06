"""
Processes for ESGF access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
from datetime import datetime, date
import json
import types
import StringIO

from netCDF4 import Dataset

from malleefowl.process import WPSProcess, SourceProcess, WorkerProcess
from malleefowl import utils, publish, config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Logon(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "esgf_logon",
            title = "Logon with ESGF OpenID",
            version = "1.0",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Logon with ESGF OpenID")

        self.openid = self.addLiteralInput(
            identifier = "openid",
            title = "ESGF OpenID",
            abstract = "Enter ESGF OpenID",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.password = self.addLiteralInput(
            identifier = "password",
            title = "OpenID Password",
            abstract = "Enter your Password",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="X509 Certificate",
            abstract="X509 Proxy Certificate",
            metadata=[],
            formats=[{"mimeType":"application/x-pkcs7-mime"}],
            asReference=True,
            )

        self.expires = self.addLiteralOutput(
            identifier="expires",
            title="Expires",
            abstract="Proxy Certificate will expire on given date",
            type=type(date(2014,4,8)),
            )

    def execute(self):
        from malleefowl import myproxy
        
        self.show_status("start logon ...", 5)

        openid=self.openid.getValue()
        password=self.password.getValue()
        
        logger.debug('openid=%s' % (openid))

        certfile = myproxy.logon_with_openid(openid=openid, password=password, interactive=False)
        
        self.show_status("logon successful", 90)

        self.output.setValue( certfile )
        self.expires.setValue(myproxy.cert_infos(certfile)['expires'])
        

class Wget(SourceProcess):
    """This process downloads files form esgf data node via wget and http"""

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "esgf_wget",
            title = "ESGF wget download",
            version = "0.2",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Download files from esgf data node with wget")

        self.credentials = self.addComplexInput(
            identifier = "credentials",
            title = "X509 Certificate",
            abstract = "X509 Proxy Certificate",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            maxmegabites=1,
            formats=[{"mimeType":"application/x-pkcs7-mime"}],
            )

    def execute(self):
        self.show_status("Starting wget download ...", 5)

        credentials = self.credentials.getValue()
        file_url = self.file_identifier.getValue()
        filename = os.path.basename(file_url)
        logger.debug('file url = %s', file_url)

        self.show_status("Downloading %s" % (filename), 10)

        try:
            cmd = ["wget",
                   "--certificate", credentials, 
                   "--private-key", credentials, 
                   "--no-check-certificate", 
                   "-N",
                   "-P", config.cache_path(),
                   "--progress", "dot:mega",
                   file_url]
            self.cmd(cmd, stdout=True)
        except Exception:
            msg = "wget failed ..."
            self.show_status(msg, 20)
            logger.exception(msg)
            raise

        outfile = os.path.join(config.cache_path(), filename)
        logger.debug('result file=%s', outfile)

        self.output.setValue(outfile)
        self.show_status("Downloading... done", 90)

class OpenDAP(SourceProcess):
    """This process downloads files form esgf data node via opendap"""

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "esgf_opendap",
            title = "ESGF OpenDAP download",
            version = "0.2",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Download files from esgf data node via OpenDAP")


        self.credentials = self.addComplexInput(
            identifier = "credentials",
            title = "X509 Certificate",
            abstract = "X509 Proxy Certificate",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            maxmegabites=1,
            formats=[{"mimeType":"application/x-pkcs7-mime"}],
            )

        self.startindex = self.addLiteralInput(
            identifier = "startindex",
            title = "Start Index",
            minOccurs = 1,
            maxOccurs = 1,
            default="1",
            type=type(1)
            )

        self.endindex = self.addLiteralInput(
            identifier = "endindex",
            title = "End Index",
            minOccurs = 1,
            maxOccurs = 1,
            default="1",
            type=type(1)
            )

    def execute(self):
        self.show_status("starting esgf opendap ...", 5)

        credentials = self.credentials.getValue()
        logger.debug('credentials = %s', credentials)
        dap_config = '.dodsrc'

        with open(dap_config, 'w') as fp:
            fp.write("""\
HTTP.VERBOSE=0
HTTP.COOKIEJAR=./.dods_cookies
HTTP.SSL.VALIDATE=0
HTTP.SSL.CERTIFICATE={credentials_pem}
HTTP.SSL.KEY={credentials_pem}
HTTP.SSL.CAPATH=./
""".format(credentials_pem=credentials,
           ))

        self.show_status("prepared opendap access", 7)

        opendap_url = self.file_identifier.getValue()        
        nc_filename = self.mktempfile(suffix='.nc')

        istart = self.startindex.getValue() - 1
        istop = self.endindex.getValue()
        utils.nc_copy(source=opendap_url, target=nc_filename, istart=istart, istop=istop)
        
        self.show_status("retrieved netcdf file", 90)

        self.output.setValue(nc_filename)
        


        
