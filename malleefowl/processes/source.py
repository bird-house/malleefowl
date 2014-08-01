"""
Processes for data source access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import json

from malleefowl.process import WPSProcess, SourceProcess
from malleefowl import source

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class ListLocalFiles(WPSProcess):
    """This process lists files from local filesystem."""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.listfiles",
            title = "List Files in Malleefowl Storage",
            version = "0.2",
            metadata=[
                ],
            abstract="List Files in Malleefowl Storage")

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            abstract = "Your unique token to recieve data",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.filter = self.addLiteralInput(
            identifier = "filter",
            title = "Filter",
            abstract = "Filter for file selection",
            minOccurs = 1,
            maxOccurs = 1,
            type = type(''),
            )

        self.output = self.addLiteralOutput(
            identifier="output",
            title="Filelist as json",
            abstract="This is a filelist as json",
            type=type('')
            )

    def execute(self):
        self.show_status("list files ...", 5)

        token = self.token.getValue()
        filter = self.filter.getValue()

        files = source.list_files(token, filter)
        
        self.output.setValue(json.dumps(files))

        self.show_status("list files ... done", 90)

class GetLocalFiles(SourceProcess):
    """This process retrieves files from malleefowl storage."""

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "org.malleefowl.storage.filesystem",
            title = "Get files from malleefowl storage",
            version = "0.2",
            metadata=[
                ],
            abstract="Get file from malleefowl storage")

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            abstract = "Your unique token to recieve data",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

    def execute(self):
        self.show_status("get file ...", 5)

        token = self.token.getValue()
        file_id = self.file_identifier.getValue()
        
        self.output.setValue( source.get_files(token, file_id) )

        self.show_status("get file ... done", 90)

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





