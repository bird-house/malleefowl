"""
Processes with cdo commands

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import types
import tempfile

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
        self.status.set(msg="starting cdo sinfo", percentDone=0, propagate=True)

        from os import curdir, path
        nc_filename = path.abspath(self.netcdf_in.getValue(asFile=False))
        self.message(msg='nc_filename = %s' % (nc_filename), force=True)

        result = self.cmd(cmd=["cdo", "sinfo", nc_filename], stdout=True)

        self.status.set(msg="cdo sinfo done", percentDone=90, propagate=True)

        (_, out_filename) = tempfile.mkstemp(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(result)
            fp.close()
            self.text_out.setValue( out_filename )
