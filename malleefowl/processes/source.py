"""
Processes for data source access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import json

from malleefowl.process import SourceProcess

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class GetFileFromCSW(SourceProcess):
    """This process retrieves files from catalog service.
    TODO: this is just a dummy process for the old wizard ... will be removed!
    """

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "org.malleefowl.csw",
            title = "Get file from Catalog Service",
            version = "0.1",
            metadata=[
                ],
            abstract="This is just a dummy process for the old wizard ... will be removed!")

    def execute(self):
        self.show_status("get file from csw ...", 5)

        file_url = self.file_identifier.getValue()
        import urllib
        urllib.urlretrieve(file_url, 'out.nc')
        self.output.setValue( 'out.nc' )

        self.show_status("get file from csw ... done", 90)





