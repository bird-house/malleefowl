from nose.tools import ok_, with_setup
from nose import SkipTest
from nose.plugins.attrib import attr

import os
import tempfile
import yaml

from malleefowl import (
    #restflow,
    wpsclient,
    )

from pywps import config
import __init__ as base

# set path to buildout/bin to have access to restflow binary
## os.environ['PATH'] = '%s:%s' % (
##     os.path.join(os.path.dirname(restflow.__file__), '..', '..', '..', 'bin'),
##     os.environ['PATH'])

NODES = None
ESGF_NODES = None
def setup_nodes():
    global NODES
    
    source = dict(
        service = base.SERVICE,
        identifier = "org.malleefowl.storage.filesystem.source",
        input = [],
        output = ['output'],
        sources = [['test1.nc'], ['test2.nc']]
        )
    worker = dict(
        service = base.SERVICE,
        identifier = "de.dkrz.cdo.sinfo.worker",
        input = [],
        output = ['output'])
    NODES = dict(source=source, worker=worker)

    # TODO: fix json encoding to unicode
    raw = yaml.dump(NODES)
    NODES = yaml.load(raw)

def setup_esgf_nodes():
    global ESGF_NODES
    
    source = dict(
        service = base.SERVICE,
        identifier = "org.malleefowl.esgf.wget.source",
        input = ['openid=' + config.getConfigValue("tests", "user"), 'password=' + config.getConfigValue("tests", "password")],
        output = ['output'],
        sources = [['http://bmbf-ipcc-ar5.dkrz.de/thredds/fileServer/cmip5/output1/MPI-M/MPI-ESM-LR/rcp26/mon/atmos/Amon/r1i1p1/v20120315/tas/tas_Amon_MPI-ESM-LR_rcp26_r1i1p1_200601-210012.nc']]
        )
    worker = dict(
        service = base.SERVICE,
        identifier = "de.dkrz.cdo.sinfo.worker",
        input = [],
        output = ['output'])
    ESGF_NODES = dict(source=source, worker=worker)

    # TODO: fix json encoding to unicode
    raw = yaml.dump(ESGF_NODES)
    ESGF_NODES = yaml.load(raw)

@with_setup(setup_nodes)
def test_generate_simple():
    raise SkipTest
    
    global NODES
    
    wf = restflow.generate("simpleWorkflow", NODES)
    ok_("WpsExecute" in wf, wf)
    ok_(base.SERVICE in wf, wf)
    ok_('output' in wf, wf)

@attr('online')
@with_setup(setup_nodes)
def test_run_simple():
    raise SkipTest

    global NODES
    wf = restflow.generate("simpleWorkflow", NODES)

    (fp, filename) = tempfile.mkstemp(suffix=".yaml", prefix="restflow-")
    restflow.write(filename, wf)

    result_file = restflow.run(filename, basedir=tempfile.mkdtemp())
    with open(result_file) as fp:
        line = fp.readline()
        ok_('wpsoutputs' in line, line)


@attr('online')
@with_setup(setup_nodes)
def test_run_simple_with_wps():
    raise SkipTest

    global NODES
    gen_result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "org.malleefowl.restflow.generate",
        inputs = [('nodes', yaml.dump(NODES))],
        outputs = [('output', True)]
        )
    ok_(len(gen_result) == 1, gen_result)
    ok_('http' in gen_result[0]['reference'], gen_result)
    wf_url =  gen_result[0]['reference'].encode('ascii', 'ignore')
    print wf_url

    run_result = wpsclient.execute(
        service = base.SERVICE,
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

## @attr('esgf')
## @attr('online')
## @with_setup(setup_esgf_nodes)
## def test_run_simple_esgf():
##     raise SkipTest

##     global ESGF_NODES
##     gen_result = wpsclient.execute(
##         service = base.SERVICE,
##         identifier = "org.malleefowl.restflow.generate",
##         inputs = [('nodes', yaml.dump(ESGF_NODES))],
##         outputs = [('output', True)]
##         )
##     wf_url = gen_result[0]['reference'].encode('ascii', 'ignore')
##     print wf_url

##     run_result = wpsclient.execute(
##         service = base.SERVICE,
##         identifier = "org.malleefowl.restflow.run",
##         inputs = [('workflow_description', wf_url)],
##         outputs = [('output', True)]
##         )
##     result_url = run_result[0]['reference']
##     ok_('wpsoutputs' in result_url, result_url)

##     import urllib
##     fp = urllib.urlopen(result_url)
##     content = fp.read()
##     ok_('wpsoutputs' in content, content)
  
