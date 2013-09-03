from pywps.Process import WPSProcess as PyWPSProcess
from pywps import config

import tempfile
import json

class ProcessMetadata(PyWPSProcess):
    """This process provides additional metadata for workflow processes."""

    def __init__(self):
        metadata = []
        metadata.append(
            {"title":"ClimDaPs", "href":"http://www.dkrz.de"}
            )
        metadata.append(
            {"title":"Hardworking Bird Malleefowl", "href":"http://en.wikipedia.org/wiki/Malleefowl"}
            )
        
        PyWPSProcess.__init__(
            self,
            identifier = "org.malleefowl.metadata",
            title = "Additional Metadata for Workflow Processes",
            version = "0.1",
            storeSupported = "true",   # async
            statusSupported = "true",  # retrieve status, needs to be true for async 
            metadata = metadata,
            abstract="Additional Metadata for Workflow Processes.",
            grassLocation = False)

        # input data
        # ----------

        self.processid = self.addLiteralInput(
            identifier="processid",
            title="Process Identifier",
            abstract="Process Identifier",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        # output data
        # -----------

        self.output = self.addLiteralOutput(
            identifier="output",
            title="Process Metadata",
            abstract="Process Metadata as JSON",
            default=None,
            type=type(''),
            )

    def execute(self):
        metadata = {'esgfilter': 'variable:tas,variable:huss,variable:psl'}

        self.status.set(msg="metadata retrieved", percentDone=90, propagate=True)

        self.output.setValue( json.dumps(metadata) )

