import nose.tools
from nose import SkipTest

import tempfile

from malleefowl import restflow

def test_generate():
    wf = restflow.generate()
    nose.tools.ok_("WpsExecute" in wf, wf)

def test_run():
    wf = restflow.generate()

    (fp, filename) = tempfile.mkstemp(suffix=".yaml", prefix="restflow-")
    restflow.write(filename, wf)

    restflow.run(filename, basedir=tempfile.mkdtemp(), verbose=True)
