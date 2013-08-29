"""
Processes for ESGF access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
from datetime import datetime, date
import tempfile
import json
import types
import StringIO

from pyesgf.logon import LogonManager

from malleefowl.process import WPSProcess

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

class Wget(WPSProcess):
    """This process downloads files form esgf data node via wget and http"""

    def __init__(self):
        WPSProcess.__init__(self,
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

        self.netcdf_url_in = self.addLiteralInput(
            identifier="file_url",
            title="NetCDF URL",
            abstract="NetCDF URL",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        # complex output
        # -------------

        self.netcdf_out = self.addComplexOutput(
            identifier="output",
            title="NetCDF Output",
            abstract="NetCDF Output",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting esgf download", percentDone=5, propagate=True)

        esgf_credentials = logon(
            openid=self.openid_in.getValue(), 
            password=self.password_in.getValue())
        
        self.status.set(msg="logon successful", percentDone=10, propagate=True)

        netcdf_url = self.netcdf_url_in.getValue()
        cache_path = self.get_cache_path()
        
        self.cmd(cmd=["wget", 
                      "--certificate", esgf_credentials, 
                      "--private-key", esgf_credentials, 
                      "--no-check-certificate", 
                      "-N",
                      "-P", cache_path,
                      "--progress", "dot:mega",
                      netcdf_url], stdout=True)
        
        out = os.path.join(cache_path, os.path.basename(netcdf_url))
        self.message('out path=%s' % (out), force=True)
        self.status.set(msg="retrieved netcdf file", percentDone=90, propagate=True)
        
        self.netcdf_out.setValue(out)


class OpenDAP(WPSProcess):
    """This process downloads files form esgf data node via opendap"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.esgf.opendap",
            title = "Download files from esgf data node via OpenDAP",
            version = "0.1",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Download files from esgf data node via OpenDAP")

        # opendap url
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

        self.opendap_url_in = self.addLiteralInput(
            identifier="file_url",
            title="OpenDAP URL",
            abstract="OpenDAP URL",
            metadata=[],
            minOccurs=1,
            maxOccurs=10,
            type=type('')
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

        # output
        # ------

        self.num_merged_files_out = self.addLiteralOutput(
            identifier="num_merged_files",
            title="Number of merged Files",
            abstract="Number of merged Files",
            default="1",
            type=type(1),
            )

        # complex output
        # -------------

        self.output = self.addComplexOutput(
            identifier="output",
            title="NetCDF Output",
            abstract="NetCDF Output",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )
        
    def execute(self):
        from Scientific.IO.NetCDF import NetCDFFile
        
        self.status.set(msg="stating esgf download", percentDone=5, propagate=True)

        logon(
            openid=self.openid_in.getValue(), 
            password=self.password_in.getValue())

        self.status.set(msg="logon successful", percentDone=10, propagate=True)

        opendap_urls = []
        value = self.opendap_url_in.getValue()
        if value != None:
            if type(value) == types.ListType:
                opendap_urls = value
            else:
                opendap_urls = [value]

        percent_done = 10
        percent_per_step = 40.0 / len(opendap_urls)
        step = 0
        nc_files = []
        for opendap_url in opendap_urls:
            ds = NetCDFFile(opendap_url)
            var_str = ','.join(ds.variables.keys())

            time_dim = 'time,%d,%d' % (int(self.startindex_in.getValue()), int(self.endindex_in.getValue()))
        
            percent_done += percent_per_step
            self.status.set(msg="retrieved netcdf metadata", percentDone=percent_done, propagate=True)

            (_, nc_filename) = tempfile.mkstemp(suffix='.nc')
            cmd = ["ncks", "-O", "-v", var_str, "-d", time_dim, "-o", nc_filename, opendap_url]
            self.cmd(cmd=cmd, stdout=True)
            nc_files.append(nc_filename)
          
            percent_done += percent_per_step
            self.status.set(msg="retrieved netcdf file", percentDone=percent_done, propagate=True)

            step += 1

        # merge output files
        if len(nc_files) > 1:
            cmd = ['cdo', 'merge']
            cmd.extend(nc_files)
            (_, nc_filename) = tempfile.mkstemp(suffix='.nc')
            cmd.append(nc_filename)
            self.cmd(cmd=cmd, stdout=True)
            self.output.setValue(nc_filename)
        else:
            self.output.setValue(nc_files[0])
        self.num_merged_files_out.setValue(len(nc_files))

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
            identifier="file_url",
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

        (_, out_filename) = tempfile.mkstemp(suffix='.json')
        with open(out_filename, 'w') as fp:
            json.dump(obj=metadata, fp=fp)
            fp.close()
            self.output.setValue( out_filename )
        
        self.status.set(msg="netcdf metadata written", percentDone=90, propagate=True)


        
