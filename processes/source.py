"""
Processes for data source access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os

from malleefowl.process import SourceProcess


class Filesystem(SourceProcess):
    """This process provides files from local filesystem."""

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "org.malleefowl.files",
            title = "Choose local files",
            version = "0.1",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Choose local files")

        # openid
        # -----------

        files = [f for f in os.listdir(self.files_path)]

        self.file_in = self.addLiteralInput(
            identifier = "file",
            title = "File",
            abstract = "Choose File",
            minOccurs = 1,
            maxOccurs = 10,
            type = type(''),
            allowedValues=files
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=5, propagate=True)

        file = self.file_in.getValue()

        self.status.set(msg="retrieved file", percentDone=90, propagate=True)
        
        self.output.setValue(path)


