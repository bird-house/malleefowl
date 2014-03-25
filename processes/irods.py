import json

from malleefowl.process import WPSProcess
from malleefowl import irodsmgr

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class ListFiles(WPSProcess):
    """This process lists files in irods."""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.irods.list",
            title = "List Files in iRods",
            version = "0.1",
            metadata=[
                ],
            abstract="List Files in iRods")

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
            default = 'nc',
            type = type(''),
            )
        self.collection = self.addLiteralInput(
            identifier = "collection",
            title = "Collection",
            abstract = "iRods Collection containing your Files",
            minOccurs = 1,
            maxOccurs = 1,
            default = '/DKRZ_CORDEX_Zone/home/public/wps/test1',
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

        files = irodsmgr.list_files(
            token=self.token.getValue(),
            filter=self.filter.getValue(),
            collection=self.collection.getValue())
        
        self.output.setValue(json.dumps(files))

        self.show_status("list files ... done", 90)
