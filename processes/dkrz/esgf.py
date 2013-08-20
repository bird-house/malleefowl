"""
Processes for ESGF access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
from datetime import datetime, date
import tempfile

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

class Search(WPSProcess):
    """
    This process is a WPS wrapper for ESGF metadata search.
    """

    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "de.dkrz.esgf.search",
            title = "Search ESGF Metadata",
            version = "0.1",
            metadata = [
                {"title": "ESGF", "href": "http://esgf.org"},
                {"title": "ESGF Search API", "href": "www.esgf.org/wiki/ESGF_Search_REST_API"}
            ],
            abstract = "Search ESGF Metadata")

        self.constraints_in = self.addLiteralInput(
            identifier = "constraints",
            title = "Search Constraints",
            abstract = "Enter Search Constraints (e.a: institute=MPI-M experiment=decadal1960)",
            minOccurs = 0,
            maxOccurs = 1,
            type = type('')
            )

        self.json_out = self.addComplexOutput(
            identifier="output",
            title="JSON Output",
            abstract="JSON Output",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def execute(self):
        from pyesgf.search import SearchConnection
        import json

        self.status.set(msg="starting esgf search", percentDone=5, propagate=True)

        conn = SearchConnection('http://localhost:8090/esg-search', distrib=False)
        ctx = conn.new_context(
            project='CMIP5', product='output1',
            replica=False, latest=True)
       
        (_, out_filename) = tempfile.mkstemp(suffix='.json')
        with open(out_filename, 'w') as fp:
            json.dump(obj=ctx.facet_counts, fp=fp)
            fp.close()
            self.json_out.setValue( out_filename )

class Wget(WPSProcess):
    """This process downloads files form esgf data node via wget and http"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "de.dkrz.esgf.wget",
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
            identifier="netcdf_url",
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

        cache_dir = "/tmp/cache/wget"
        
        self.cmd(cmd=["wget", 
                      "--certificate", esgf_credentials, 
                      "--private-key", esgf_credentials, 
                      "--no-check-certificate", 
                      "-N",
                      "-P", cache_dir,
                      "--progress", "dot:mega",
                      netcdf_url], stdout=True)
        
        out = os.path.join(cache_dir, os.path.basename(netcdf_url))
        self.message('out path=%s' % (out), force=True)
        self.status.set(msg="retrieved netcdf file", percentDone=90, propagate=True)
        
        self.netcdf_out.setValue(out)


class OpenDAP(WPSProcess):
    """This process downloads files form esgf data node via opendap"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "de.dkrz.esgf.opendap",
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

        self.input = self.addLiteralInput(
            identifier="input",
            title="OpenDAP URL",
            abstract="OpenDAP URL",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        self.starttime_in = self.addLiteralInput(
            identifier = "starttime",
            title = "Start time",
            minOccurs = 1,
            maxOccurs = 1,
            default="2010-01-01",
            type=type(date(2010,1,1))
            )

        self.endtime_in = self.addLiteralInput(
            identifier = "endtime",
            title = "End time",
            minOccurs = 1,
            maxOccurs = 1,
            default="2010-12-31",
            type=type(date(2010,12,31))
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

        self.status.set(msg="logon successful", percentDone=20, propagate=True)

        opendap_url = self.input.getValue()
        self.message(msg='OPeNDAP URL is %s' % opendap_url, force=True)

        ds = NetCDFFile(opendap_url)
        #variables = []
        #dimensions = []
        #for var in ds.variables.keys():
        #    if not var in ds.dimensions.keys():
        #        variables.append(var)
        #    else:
        #        dimensions.append(var)
        var_str = ','.join(ds.variables.keys())

        #dim_str = ''
        #for dim in dimensions:
        #    dim_str = dim_str + ' -d ' + dim + ',1,1'

        self.status.set(msg="retrieved netcdf metadata", percentDone=40, propagate=True)

        (_, out_filename) = tempfile.mkstemp(suffix='.txt')
        (_, nc_filename) = tempfile.mkstemp(suffix='.nc')
        self.cmd(cmd=["ncks", "-O", "-v", var_str, "-d", "time,1,1", "-o", nc_filename, opendap_url], stdout=True)

        self.output.setValue(nc_filename)
            
        self.status.set(msg="retrieved netcdf file", percentDone=90, propagate=True)


        
