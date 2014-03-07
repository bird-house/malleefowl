"""
Processes for data source access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
import json

import malleefowl
from malleefowl import utils, tokenmgr

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class ListLocalFiles(malleefowl.process.WPSProcess):
    """This process lists files from local filesystem."""

    def __init__(self):
        malleefowl.process.WPSProcess.__init__(self,
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
        self.show_status("starting ...", 5)

        token = self.token.getValue()
        userid = tokenmgr.get_userid(tokenmgr.sys_token, token)
        
        files_path = os.path.join(self.files_path, userid)
        utils.mkdir(files_path)

        search_filter = self.filter.getValue()

        files = [f for f in os.listdir(files_path) if search_filter in f]

        self.show_status("retrieved file list", 90)
        
        self.output.setValue(json.dumps(files))

class GetFileFromFilesystem(malleefowl.process.SourceProcess):
    """This process retrieves files from local filesystem."""

    def __init__(self):
        malleefowl.process.SourceProcess.__init__(self,
            identifier = "org.malleefowl.storage.filesystem",
            title = "Get files from filesystem storage",
            version = "0.2",
            metadata=[
                ],
            abstract="Get file from filesystem storage")

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            abstract = "Your unique token to recieve data",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

    def execute(self):
        self.show_status("starting ...", 5)

        token = self.token.getValue()
        userid = tokenmgr.get_userid(tokenmgr.sys_token, token)
        
        files_path = os.path.join(self.files_path, userid)
        utils.mkdir(files_path)

        file_id = self.file_identifier.getValue()

        files = [f for f in os.listdir(files_path) if file_id in f]
        file_path = os.path.join(files_path, files[0])

        self.show_status("retrieved file", 90)
        
        self.output.setValue(file_path)


class GetTestFiles(malleefowl.process.SourceProcess):
    """This process retrieves test files."""

    def __init__(self):
        malleefowl.process.SourceProcess.__init__(self,
            identifier = "org.malleefowl.storage.testfiles",
            title = "Get files for testing",
            version = "0.1",
            metadata=[
                ],
            abstract="Get files for testing")

    def execute(self):
        self.show_status("starting ...", 5)

        # TODO: configure user id
        userid = "test@malleefowl.org"
        logger.warn('TODO: configure test user %s' % (userid))
        
        file_path = os.path.join(self.files_path, userid, self.file_identifier.getValue())
        logger.debug('test file: %s', file_path)

        self.show_status("retrieved file", 90)
        
        self.output.setValue(file_path)


