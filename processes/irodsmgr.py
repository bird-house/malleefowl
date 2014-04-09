import json
import os

from malleefowl.process import WPSProcess
from malleefowl import irodsmgr, tokenmgr

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class List(WPSProcess):
    """This process calls irods ils command."""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.irods.ls",
            title = "iRods ls",
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
        self.home = self.addLiteralInput(
            identifier = "home",
            title = "Home",
            abstract = "Choose iRods Home",
            minOccurs = 1,
            maxOccurs = 1,
            default = 'DKRZ', #self.irods_home().keys()[0],
            type = type(''),
            allowedValues=['DKRZ'] #self.irods_home().keys()
            )
        self.collection = self.addLiteralInput(
            identifier = "collection",
            title = "Collection",
            abstract = "Enter Collection in iRods Home",
            minOccurs = 1,
            maxOccurs = 1,
            default = "/",
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
        self.show_status("start irods ls ...", 5)

        userid = tokenmgr.get_userid(
            tokenmgr.sys_token(),
            self.token.getValue())

        collection = self.collection.getValue().strip()
        if os.path.isabs(collection):
            collection = collection[1:]
        root = self.irods_home().get(self.home.getValue())
        collection = os.path.join(root, collection)
        (files, subcolls) = irodsmgr.ls(collection)

        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=dict(files=files, subcolls=subcolls), fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("irods ls ... done", 90)


class Rsync(WPSProcess):
    """This process calls irods irsync command."""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.irods.rsync",
            title = "iRods rsync",
            version = "0.1",
            metadata=[
                ],
            abstract="Rsync irods collection")

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            abstract = "Your token to access this process",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )
        self.collection = self.addLiteralInput(
            identifier = "collection",
            title = "Source Collection",
            abstract = "Enter Collection in iRods home %s" % (self.irods_home),
            minOccurs = 1,
            maxOccurs = 1,
            default = 'test1',
            type = type(''),
            )
        self.output = self.addLiteralOutput(
            identifier="output", 
            title="Path to Destination Collection")

    def execute(self):
        self.show_status("start rsync ...", 5)

        userid = tokenmgr.get_userid(
            tokenmgr.sys_token(),
            self.token.getValue())
        
        src = self.collection.getValue().strip()
        if os.path.isabs(src):
            src = src[1:]
        src = os.path.join(self.irods_home, src)
        dest = os.path.join(self.files_path, userid, 'irods', os.path.basename(src))
        if not os.path.exists(dest):
            os.makedirs(dest)

        irodsmgr.rsync(
            src="i:%s" % (src),
            dest=dest)

        self.output.setValue(dest)
        self.show_status("rsync ... done", 90)
