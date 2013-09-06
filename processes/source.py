"""
Processes for data source access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
import json

from malleefowl.process import SourceProcess, WPSProcess


class ListLocalFiles(WPSProcess):
    """This process lists files from local filesystem."""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.listfiles",
            title = "List local files",
            version = "0.1",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="List local files")

        # openid
        # -----------

        self.filter_in = self.addLiteralInput(
            identifier = "filter",
            title = "Filter",
            abstract = "Filter for file selection",
            minOccurs = 1,
            maxOccurs = 1,
            type = type(''),
            )

        self.filelist_out = self.addLiteralOutput(
            identifier="output",
            title="Filelist as json",
            abstract="This is a filelist as json",
            type=type('')
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=5, propagate=True)

        filter = self.filter_in.getValue()

        self.status.set(msg="retrieved file", percentDone=90, propagate=True)

        files = [f for f in os.listdir(self.files_path) if filter in f]
        
        self.filelist_out.setValue(json.dumps(files))

class GetLocalFile(SourceProcess):
    """This process retrieves files from local filesystem."""

    def __init__(self):
        SourceProcess.__init__(self,
            identifier = "org.malleefowl.storage.filesystem.getfile",
            title = "Get files from filesystem storage",
            version = "0.1",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Get local file")

        # openid
        # -----------

        self.filter_in = self.addLiteralInput(
            identifier = "filter",
            title = "Filter",
            abstract = "Filter for file selection",
            minOccurs = 1,
            maxOccurs = 1,
            type = type(''),
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=5, propagate=True)

        filter = self.filter_in.getValue()

        self.status.set(msg="retrieved file", percentDone=90, propagate=True)

        files = [f for f in os.listdir(self.files_path) if filter in f]
        file_path = os.path.join(self.files_path, files[0])
        
        self.output.setValue(file_path)


