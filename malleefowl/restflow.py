import os
import tempfile

from mako.template import Template
from mako.lookup import TemplateLookup

mylookup = TemplateLookup(directories=[os.path.join(os.path.dirname(__file__), 'templates')],
                          module_directory=os.path.join(tempfile.gettempdir(), 'mako_cache'))

import logging
logger = logging.getLogger(__name__)

def generate(name, service, identifier, input=[], output=[]):
    mytemplate = mylookup.get_template(name + '.yaml')
    return  mytemplate.render(service=service, identifier=identifier, input=input, output=output)

def write(filename, workflow):
    with open(filename, 'w') as fp:
        fp.write(workflow)

def run(filename, basedir=None, verbose=False):
    logger.debug("filename = %s", filename)

    basedir = basedir if basedir is not None else os.curdir()
    
    cmd = ["restflow", "-f", filename, "--run", "restflow"]
    if verbose:
        cmd.append('-t')
    cmd.append('--base')
    cmd.append(basedir)
    cmd.append('--outfile')
    cmd.append('result.yaml')
        
    import subprocess
    from subprocess import PIPE
    p = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, cwd=basedir)

    (stdoutdata, stderrdata) = p.communicate()

    result = ''
    result_file = os.path.join(basedir, 'wps_output.txt')
    with open(result_file, 'r') as fp:
        result = fp.readline()

    return result
    
