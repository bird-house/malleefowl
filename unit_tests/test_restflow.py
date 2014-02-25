import nose.tools
from nose import SkipTest

import tempfile

from malleefowl import restflow

def test_generate():
    wf = restflow.generate()

def test_run():
    wf = restflow.generate_workflow()

    (fp, filename) = tempfile.mkstemp(suffix=".yaml", prefix="restflow")
    restflow.write(filename, wf)

    restflow.run(filename, verbose=True)
    nose.tools.ok_(False)
