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
from malleefowl import utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


class Login(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "org.malleefowl.esgf.login",
            title = "Login to esgf node with openid",
            version = "1.0",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Login to esgf node")

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

    def execute(self):
        self.show_status("login to esgf", 5)

        try:
            esgf_credentials = utils.logon(
                openid=self.openid.getValue(), 
                password=self.password.getValue())
        except Exception, err:
            raise RuntimeError("logon failed (%s)." % (err.message))
        
        self.show_status("logon successful", 90)

        

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
        self.show_status("starting esgf download", 5)

        try:
            esgf_credentials = utils.logon(
                openid=self.openid_in.getValue(), 
                password=self.password_in.getValue())
        except Exception, err:
            raise RuntimeError("logon failed (%s)." % (err.message))
        
        self.show_status("logon successful", 10)

        netcdf_url = self.file_identifier.getValue()

        try:
            cmd = ["wget",
                   "--certificate", esgf_credentials, 
                   "--private-key", esgf_credentials, 
                   "--no-check-certificate", 
                   "-N",
                   "-P", self.cache_path,
                   "--progress", "dot:mega",
                   netcdf_url]
            self.cmd(cmd, stdout=True)
        except Exception, err:
            raise RuntimeError("wget failed (%s)." % (err.message))

        outfile = os.path.join(self.cache_path, os.path.basename(netcdf_url))
        logger.debug('result file=%s', outfile)
        
        self.show_status("retrieved netcdf file", 90)

        self.output.setValue(outfile)


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
        self.show_status("starting esgf download", 5)

        utils.logon(
            openid=self.openid_in.getValue(), 
            password=self.password_in.getValue())

        self.show_status("logon successful", 10)

        opendap_url = self.file_identifier.getValue()
        
        nc_filename = self.mktempfile(suffix='.nc')

        istart = self.startindex_in.getValue() - 1
        istop = self.endindex_in.getValue()
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
            fp.close()
            self.output.setValue( out_filename )
        
        self.show_status("netcdf metadata written", 90)


        
