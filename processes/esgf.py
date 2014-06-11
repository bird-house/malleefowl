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
from malleefowl import utils, publish

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Logon(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "org.malleefowl.esgf.logon",
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

        myproxy.logon(openid=openid, password=password, interactive=False)
        
        self.show_status("logon successful", 90)

        self.output.setValue( "cert.pem" )
        self.expires.setValue(utils.cert_infos("cert.pem")['expires'])
        

class Wget(SourceProcess):
    """This process downloads files form esgf data node via wget and http"""

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "org.malleefowl.esgf.wget",
            title = "ESGF wget download",
            version = "0.2",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Download files from esgf data node with wget")

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            abstract = "Your unique token to publish data",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

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

        self.sidecar = self.addComplexOutput(
            identifier="sidecar",
            title="Metadata",
            abstract="Metadata of downloaded files",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("starting esgf wget ...", 5)

        credentials = self.credentials.getValue()
        logger.debug('credentials = %s', credentials)
        file_url = self.file_identifier.getValue()
        filename = os.path.basename(file_url)
        logger.debug('file url = %s', file_url)

        token = self.token.getValue()

        from malleefowl import tokenmgr
        userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)

        self.show_status("download %s" % (filename), 10)

        try:
            cmd = ["wget",
                   "--certificate", credentials, 
                   "--private-key", credentials, 
                   "--no-check-certificate", 
                   "-N",
                   "-P", self.cache_path,
                   "--progress", "dot:mega",
                   file_url]
            self.cmd(cmd, stdout=True)
        except Exception as e:
            raise RuntimeError("wget failed (%s)." % (e.message))

        outfile = os.path.join(self.cache_path, filename)
        logger.debug('result file=%s', outfile)

        publish.link_to_local_store(files=[outfile], userid=userid)
        
        self.output.setValue(outfile)
        self.show_status("esgf wget ... done", 90)
        
        metadata = dict(url=file_url, filename=filename)
        sidecar_file = self.mktempfile(suffix='.json')
        with open(sidecar_file, 'w') as fp:
            json.dump(obj=metadata, fp=fp, indent=4, sort_keys=True)
        self.sidecar.setValue(sidecar_file)


class OpenDAP(SourceProcess):
    """This process downloads files form esgf data node via opendap"""

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "org.malleefowl.esgf.opendap",
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
        
class Metadata(WorkerProcess):
    """This process downloads files form esgf data node via opendap"""

    def __init__(self):
        WorkerProcess.__init__(self,
            identifier = "org.malleefowl.esgf.metadata",
            title = "Retrieve Metadata of NetCDF File",
            version = "0.1",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Retrieve Metadata of NetCDF File")

        # complex output
        # -------------

        self.output = self.addComplexOutput(
            identifier="output",
            title="NetCDF Metadata Output",
            abstract="NetCDF Metadata Output",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("starting netcdf metadata retrieval", 5)

        nc_file = self.get_nc_files()[0]

        ds = Dataset(nc_file)
        metadata = {}
        metadata['global_attributes'] = {}
        for att_name in ["contact", "experiment", "institute_id", "title"]:
            if hasattr(ds, att_name):
                metadata['global_attributes'][att_name] = getattr(ds, att_name)
        metadata['dimensions'] = ds.dimensions.keys()
        metadata['variables'] = {}
        for var_name in ds.variables.keys():
            metadata['variables'][var_name] = {}
            for att_name in ["axis", "bounds", "calendar", "long_name", "standard_name", "units", "shape"]:
                if hasattr(ds.variables[var_name], att_name):
                    metadata['variables'][var_name][att_name] = getattr(ds.variables[var_name], att_name)
        
        self.show_status("retrieved netcdf metadata", 80)

        out_filename = self.mktempfile(suffix='.json')
        with open(out_filename, 'w') as fp:
            json.dump(obj=metadata, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( out_filename )
        
        self.show_status("netcdf metadata written", 90)


        
