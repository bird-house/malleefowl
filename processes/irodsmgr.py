import json

from malleefowl.process import WPSProcess
from malleefowl import irodsmgr, tokenmgr

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class ListFiles(WPSProcess):
    """This process calls irods ils command."""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.irods.ls",
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
        self.collection = self.addLiteralInput(
            identifier = "collection",
            title = "Collection",
            abstract = "iRods Collection containing your Files",
            minOccurs = 1,
            maxOccurs = 1,
            default = '/DKRZ_CORDEX_Zone/home/public/wps/test1',
            type = type(''),
            )
        self.output = self.addComplexOutput(
            identifier="output",
            title="Files in iRods Collection",
            abstract="Files in iRods Collection as JSON",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("list files ...", 5)

        userid = tokenmgr.get_userid(
            tokenmgr.sys_token(),
            self.token.getValue())

        files = irodsmgr.list_files(
            collection=self.collection.getValue())

        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("list files ... done", 90)


class Rsync(WPSProcess):
    """This process calls irods irsync command."""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.irods.rsync",
            title = "Rsync irods collections",
            version = "0.1",
            metadata=[
                ],
            abstract="Rsync irods collections")

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            abstract = "Your token to access this process",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )
        self.src = self.addLiteralInput(
            identifier = "src",
            title = "Source Collection",
            abstract = "iRods Source Collection",
            minOccurs = 1,
            maxOccurs = 1,
            default = 'i:/DKRZ_CORDEX_Zone/home/public/wps/test1',
            type = type(''),
            )
        self.dest = self.addLiteralInput(
            identifier = "dest",
            title = "Destination Collection",
            abstract = "iRods Destination Collection",
            minOccurs = 1,
            maxOccurs = 1,
            default = 'i:/tmp/test1',
            type = type(''),
            )

    def execute(self):
        self.show_status("start rsync ...", 5)

        userid = tokenmgr.get_userid(
            tokenmgr.sys_token(),
            self.token.getValue())

        irodsmgr.rsync(
            src=self.src.getValue(),
            dest=self.dest.getValue())

        self.show_status("rsync ... done", 90)
