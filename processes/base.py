from pywps.Process import WPSProcess as PyWPSProcess
from pywps import config

import tempfile
import json
from malleefowl import database

class ProcessMetadata(PyWPSProcess):
    """This process provides additional metadata for malleefowl processes."""

    def __init__(self):
        PyWPSProcess.__init__(
            self,
            identifier = "org.malleefowl.metadata",
            title = "Additional Metadata for Malleefowl Processes",
            version = "0.1",
            storeSupported = "true",   # async
            statusSupported = "true",  # retrieve status, needs to be true for async 
            metadata = [],
            abstract="Additional Metadata for Malleefowl Processes.",
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
            default='{}',
            type=type(''),
            )

    def execute(self):
        self.status.set(msg="starting to retrieve process metadata", percentDone=10, propagate=True)
        
        metadata = database.retrieve_process_metadata(self.processid.getValue())

        self.status.set(msg="metadata retrieved", percentDone=90, propagate=True)

        self.output.setValue( json.dumps(metadata) )

