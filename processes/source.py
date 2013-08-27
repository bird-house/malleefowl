"""
Processes for data source access

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

from malleefowl.process import WPSProcess


class Filesystem(WPSProcess):
    """This process downloads files form esgf data node via wget and http"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.source.filesystem",
            title = "Choose files from filesystem",
            version = "0.1",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Choose files from filesystem")

        # openid
        # -----------

        self.path_in = self.addLiteralInput(
            identifier = "path",
            title = "Filepath",
            abstract = "Filepath",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        # complex output
        # -------------

        self.netcdf_out = self.addComplexOutput(
            identifier="output",
            title="NetCDF Output",
            abstract="NetCDF Output",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=5, propagate=True)

        path = self.path_in.getValue()

        self.status.set(msg="retrieved file", percentDone=90, propagate=True)
        
        self.netcdf_out.setValue(path)


