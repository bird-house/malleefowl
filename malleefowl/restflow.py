import os
import tempfile

import logging
log = logging.getLogger(__name__)

def generate(name, nodes):
    from mako.template import Template
    from mako.lookup import TemplateLookup
    
    mylookup = TemplateLookup(directories=[os.path.join(os.path.dirname(__file__), 'templates')],
                              output_encoding='ascii', input_encoding='utf-8', encoding_errors='replace',
                              module_directory=os.path.join(tempfile.gettempdir(), 'mako_cache'))
    mytemplate = mylookup.get_template(name + '.yaml')
    return  mytemplate.render(nodes=nodes)

def write(filename, workflow):
    with open(filename, 'w') as fp:
        fp.write(workflow)

def run(filename, basedir=None, verbose=False):
    log.debug("filename = %s", filename)

    basedir = basedir if basedir is not None else os.curdir
    
    cmd = ["restflow", "-f", filename, "--run", "restflow"]
    if verbose:
        cmd.append('-t')
    cmd.append('--base')
    cmd.append(basedir)
        
    import subprocess
    from subprocess import PIPE
    p = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, cwd=basedir)

    (stdoutdata, stderrdata) = p.communicate()
    log.debug("stdoutdata: %s", stdoutdata)
    log.debug("stderrdata: %s", stderrdata)

    result_file = os.path.join(basedir, 'restflow_output.txt')

    return result_file
    
