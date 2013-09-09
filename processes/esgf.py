"""
Processes for ESGF access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
from datetime import datetime, date
import json
import types
import StringIO

from pyesgf.logon import LogonManager

from malleefowl.process import WPSProcess, SourceProcess
from malleefowl import utils

def logon(openid, password):
    # TODO: unset x509 env
    #del os.environ['X509_CERT_DIR']
    #del os.environ['X509_USER_PROXY']
    
    esgf_dir = os.path.abspath(os.curdir)
    # NetCDF DAP support looks in CWD for configuration
    dap_config = os.path.join(esgf_dir, '.dodsrc')
    esgf_credentials = os.path.join(esgf_dir, 'credentials.pem')
        
    lm = LogonManager(esgf_dir, dap_config=dap_config)
    lm.logoff()
    
    lm.logon_with_openid(
        openid=openid,
        password=password,
        bootstrap=True, 
        update_trustroots=True, 
        interactive=False)

    if not lm.is_logged_on():
        raise Exception("Logon failed")

    return esgf_credentials

class Wget(SourceProcess):
    """This process downloads files form esgf data node via wget and http"""

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "org.malleefowl.esgf.wget",
            title = "Download files from esgf data node via wget",
            version = "0.1",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Download files from esgf data node via wget")

        # openid
        # -----------

        self.openid_in = self.addLiteralInput(
            identifier = "openid",
            title = "ESGF OpenID",
            abstract = "Enter ESGF OpenID",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.password_in = self.addLiteralInput(
            identifier = "password",
            title = "OpenID Password",
            abstract = "Enter your Password",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

    def execute(self):
        self.status.set(msg="starting esgf download", percentDone=5, propagate=True)

        esgf_credentials = logon(
            openid=self.openid_in.getValue(), 
            password=self.password_in.getValue())
        
        self.status.set(msg="logon successful", percentDone=10, propagate=True)

        netcdf_url = self.file_identifier.getValue()
        
        self.cmd(cmd=["wget", 
                      "--certificate", esgf_credentials, 
                      "--private-key", esgf_credentials, 
                      "--no-check-certificate", 
                      "-N",
                      "-P", self.cache_path,
                      "--progress", "dot:mega",
                      netcdf_url], stdout=True)
        
        out = os.path.join(self.cache_path, os.path.basename(netcdf_url))
        self.message('out path=%s' % (out), force=True)
        self.status.set(msg="retrieved netcdf file", percentDone=90, propagate=True)
        
        self.output.setValue(out)


class OpenDAP(SourceProcess):
    """This process downloads files form esgf data node via opendap"""

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "org.malleefowl.esgf.opendap",
            title = "Download files from esgf data node via OpenDAP",
            version = "0.1",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Download files from esgf data node via OpenDAP")

        self.openid_in = self.addLiteralInput(
            identifier = "openid",
            title = "ESGF OpenID",
            abstract = "Enter ESGF OpenID",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.password_in = self.addLiteralInput(
            identifier = "password",
            title = "OpenID Password",
            abstract = "Enter your Password",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.startindex_in = self.addLiteralInput(
            identifier = "startindex",
            title = "Start Index",
            minOccurs = 1,
            maxOccurs = 1,
            default="1",
            type=type(1)
            )

        self.endindex_in = self.addLiteralInput(
            identifier = "endindex",
            title = "End Index",
            minOccurs = 1,
            maxOccurs = 1,
            default="1",
            type=type(1)
            )

    def execute(self):
        self.status.set(msg="starting esgf download", percentDone=5, propagate=True)

        logon(
            openid=self.openid_in.getValue(), 
            password=self.password_in.getValue())

        self.status.set(msg="logon successful", percentDone=10, propagate=True)

        opendap_url = self.file_identifier.getValue()
        
        nc_filename = self.mktempfile(suffix='.nc')

        istart = self.startindex_in.getValue() - 1
        istop = self.endindex_in.getValue()
        utils.nc_copy(source=opendap_url, target=nc_filename, istart=istart, istop=istop)
        
        self.status.set(msg="retrieved netcdf file", percentDone=90, propagate=True)

        self.output.setValue(nc_filename)
        
class Metadata(WPSProcess):
    """This process downloads files form esgf data node via opendap"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.esgf.metadata",
            title = "Retrieve Metadata of NetCDF File",
            version = "0.1",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Retrieve Metadata of NetCDF File")

        self.openid_in = self.addLiteralInput(
            identifier = "openid",
            title = "ESGF OpenID",
            abstract = "Enter ESGF OpenID",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.password_in = self.addLiteralInput(
            identifier = "password",
            title = "OpenID Password",
            abstract = "Enter your Password",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.netcdf_url_in = self.addLiteralInput(
            identifier="file_identifier",
            title="NetCDF URL",
            abstract="NetCDF URL",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

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
        from Scientific.IO.NetCDF import NetCDFFile
        
        self.status.set(msg="starting netcdf metadata retrieval", percentDone=5, propagate=True)

        logon(
            openid=self.openid_in.getValue(), 
            password=self.password_in.getValue())

        self.status.set(msg="logon successful", percentDone=20, propagate=True)

        netcdf_url = self.netcdf_url_in.getValue()
        self.message(msg='NetCDF URL is %s' % netcdf_url, force=True)

        ds = NetCDFFile(netcdf_url)
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
        
        self.status.set(msg="retrieved netcdf metadata", percentDone=80, propagate=True)

        out_filename = self.mktempfile(suffix='.json')
        with open(out_filename, 'w') as fp:
            json.dump(obj=metadata, fp=fp)
            fp.close()
            self.output.setValue( out_filename )
        
        self.status.set(msg="netcdf metadata written", percentDone=90, propagate=True)


        
