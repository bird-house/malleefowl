from nose.tools import ok_, with_setup
from nose import SkipTest
from nose.plugins.attrib import attr

import os
import tempfile

from malleefowl import (
    restflow,
    wpsclient,
    )

import yaml

service="http://localhost:8090/wps"

# set path to buildout/bin to have access to restflow binary
os.environ['PATH'] = '%s:%s' % (
    os.path.join(os.path.dirname(restflow.__file__), '..', '..', '..', 'bin'),
    os.environ['PATH'])

NODES = None
def setup_nodes():
    global NODES
    
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
    NODES = dict(source=source, worker=worker)

    # TODO: fix json encoding to unicode
    raw = yaml.dump(NODES)
    NODES = yaml.load(raw)

@with_setup(setup_nodes)
def test_generate_simple():
    global NODES
    
    wf = restflow.generate("simpleWorkflow", NODES)
    ok_("WpsExecute" in wf, wf)
    ok_(service in wf, wf)
    ok_('output' in wf, wf)

@attr('online')
@with_setup(setup_nodes)
def test_run_simple():
    global NODES
    wf = restflow.generate("simpleWorkflow", NODES)

    (fp, filename) = tempfile.mkstemp(suffix=".yaml", prefix="restflow-")
    restflow.write(filename, wf)

    result_file = restflow.run(filename, basedir=tempfile.mkdtemp(), verbose=True)
    with open(result_file) as fp:
        line = fp.readline()
        ok_('wpsoutputs' in line, line)


@attr('online')
@with_setup(setup_nodes)
def test_run_simple_with_wps():
    global NODES
    gen_result = wpsclient.execute(
        service = service,
        identifier = "org.malleefowl.restflow.generate",
        inputs = [('nodes', yaml.dump(NODES))],
        outputs = [('output', True)]
        )
    ok_(len(gen_result) == 1, gen_result)
    ok_('http' in gen_result[0]['reference'], gen_result)
    wf_url =  gen_result[0]['reference'].encode('ascii', 'ignore')
    print wf_url

    run_result = wpsclient.execute(
        service = service,
        identifier = "org.malleefowl.restflow.run",
        inputs = [('workflow_description', wf_url)],
        outputs = [('output', True)]
        )
    result_url = run_result[0]['reference']
    ok_('wpsoutputs' in result_url, result_url)

    import urllib
    fp = urllib.urlopen(result_url)
    content = fp.read()
    ok_('wpsoutputs' in content, content)
    
