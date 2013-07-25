"""
Processes with cdo commands

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import types

from malleefowl.process import WorkflowProcess


class CDOInfo(WorkflowProcess):
    """This process calls cdo sinfo for given netcdf file"""

    def __init__(self):
        WorkflowProcess.__init__(
            self,
            identifier = "de.dkrz.cdo.sinfo",
            title = "cdo sinfo",
            version = "0.1",
            metadata=[
                {"title":"CDO","href":"https://code.zmaw.de/projects/cdo"},
                ],
            abstract="calling cdo sinfo ...",
            )

        # complex input
        # -------------

        # comes from workflow process ...

        # complex output
        # -------------

        self.text_out = self.addComplexOutput(
            identifier="output",
            title="CDO sinfo result",
            abstract="CDO sinfo result",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

    def execute(self):
        self.message(msg='exec cdo sinfo', force=True)

        # complex
        # write cdo result
        self.message(msg='cdo result', force=True)
        
        from os import curdir, path
        nc_filename = path.abspath(self.netcdf_in.getValue(asFile=False))
        result = self.cmd(cmd=["cdo", "sinfo", nc_filename], stdout=True)

        with open('/tmp/cdo-result.txt', 'w') as fp:
            fp.write(result)
            fp.close()
            self.text_out.setValue( fp.name )
