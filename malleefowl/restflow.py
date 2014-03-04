import os
import tempfile

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

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

def status(msg, percent_done):
    logger.info('STATUS - percent done: %d, status: %s', percent_done, msg)

def run(filename, basedir=None, timeout=300, status_callback=status, verbose=False):
    logger.debug("filename = %s", filename)

    basedir = basedir if basedir is not None else os.curdir
    
    cmd = ["restflow", "-f", filename, "--run", "restflow"]
    if verbose:
        cmd.append('-t')
    cmd.append('--base')
    cmd.append(basedir)
        
    import subprocess
    from subprocess import PIPE
    p = subprocess.Popen(cmd, cwd=basedir)

    status_file = os.path.join(basedir, 'restflow_status.txt')
    result_file = os.path.join(basedir, 'restflow_output.txt')
    
    logger.debug("before process call")
    import time
    count = 0
    status_callback('workflow is started', 5)
    while p.poll() is None:
        time.sleep(1)
        if os.path.exists(status_file):
            with open(status_file, 'r') as fp:
                msg = fp.read()
                status_callback(msg, 20)
        logger.debug("still running: count=%s, returncode=%s", count, p.returncode)
        if timeout > 0 and count > timeout:
            msg = 'Killed workflow due to timeout of %d secs' % ( timeout)
            logger.error(msg)
            p.kill()
            raise Exception(msg)
        count = count + 1
        if os.path.exists(result_file):
            logger.warn('terminated workflow. No exit code but result exists.')
            #p.terminate()

    if not os.path.exists(result_file):
        msg = "No result file found %s" % (result_file)
        logger.error(msg)
        raise Exception(msg)

    status_callback('workflow is done', 95)
            
    logger.debug("after process call")
    #logger.debug("stdoutdata: %s", stdoutdata)
    #logger.debug("stderrdata: %s", stderrdata)

    logger.debug("workflow done, output=%s", result_file)

    return result_file
    
