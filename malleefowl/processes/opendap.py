# TODO: old code for esgf opendap access

from netCDF4 import Dataset

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
        


        
