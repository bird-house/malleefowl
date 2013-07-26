"""
Processes for ESGF access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

# TODO: fix python sys path


from malleefowl.process import WPSProcess
from datetime import datetime, date

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

        self.username_in = self.addLiteralInput(
            identifier = "username",
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
            identifier="opendap_url",
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

        self.netcdf_out = self.addComplexOutput(
            identifier="output",
            title="NetCDF Output",
            abstract="NetCDF Output",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )

    def execute(self):
        self.message(msg='exec download form opendap', force=True)

        from pyesgf.logon import LogonManager
        lm = LogonManager()
        lm.logoff()
        #lm.is_logged_on()
        lm.logon_with_openid(
            openid=self.username_in.getValue(),
            password=self.password_in.getValue(),
            bootstrap=True, update_trustroots=True, interactive=False)

        opendap_url = self.opendap_url_in.getValue()
        self.message(msg='OPeNDAP URL is %s' % opendap_url, force=True)

        import netCDF4
        ds = netCDF4.Dataset(opendap_url)
        print ds.variables.keys()

        # opendap with contraints
        dap_access = "%s?time[0:1:1]" % (opendap_url)
 
        import tempfile
        from os import curdir, path
        (fp, nc_filename) = tempfile.mkstemp(suffix='.nc')
        result = self.cmd(cmd=["ncks", "-O", dap_access, nc_filename], stdout=True)

        self.netcdf_out.setValue(nc_filename)
