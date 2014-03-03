import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

import os
import tempfile

from malleefowl import restflow

service="http://localhost:8090/wps"

# set path to buildout/bin to have access to restflow binary
os.environ['PATH'] = '%s:%s' % (
    os.path.join(os.path.dirname(restflow.__file__), '..', '..', '..', 'bin'),
    os.environ['PATH'])


def test_generate_simple():
    source = dict(
        service = service,
        identifier = "org.malleefowl.storage.testfiles.source",
        input = [],
        output = ['output'],
        sources = []
        )
    worker = dict(
        service=service,
        identifier="org.malleefowl.test.dummyprocess",
        input=['input1=1', 'input2=2'],
        output=['output1', 'output2']
        )
    nodes = dict(source=source, worker=worker)
    
    wf = restflow.generate("simpleWorkflow", nodes)
    nose.tools.ok_("WpsExecute" in wf, wf)
    nose.tools.ok_(service in wf, wf)
    nose.tools.ok_('input2' in wf, wf)
    nose.tools.ok_('output2' in wf, wf)

@attr('online')
def test_run_simple():
    source = dict(
        service = service,
        identifier = "org.malleefowl.storage.testfiles.source",
        input = [],
        output = ['output'],
        sources = [['test1.nc'], ['test2.nc']]
        )
    worker = dict(
        service = service,
        identifier = "de.dkrz.cdo.sinfo.worker",
        input = [],
        output = ['output'])
    nodes = dict(source=source, worker=worker)

    # TODO: fix json encoding to unicode
    import yaml
    raw = yaml.dump(nodes)
    nodes = yaml.load(raw)
    print nodes
    
    wf = restflow.generate("simpleWorkflow", nodes)

    (fp, filename) = tempfile.mkstemp(suffix=".yaml", prefix="restflow-")
    restflow.write(filename, wf)

    (result_file, retcode, stdoutdata, stderrdata) = restflow.run(filename, basedir=tempfile.mkdtemp(), verbose=True)
    with open(result_file) as fp:
        line = fp.readline()
        nose.tools.ok_('wpsoutputs' in line, line)

