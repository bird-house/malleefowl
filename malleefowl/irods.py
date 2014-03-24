from pywps import config
from malleefowl import tokenmgr, utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def list_files(token, filter, folder):
    userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)

    from subprocess import Popen, PIPE
    p = Popen(['/home/pingu/opt/iRODS/clients/icommands/bin/ils', folder], stdout=PIPE, stderr=PIPE)
    (stdoutdata, stderrdata) = p.communicate()

    logger.debug("stdout=%s", stdoutdata)
    files = []
    for line in stdoutdata.splitlines():
        line = line.strip()
        if line.endswith('.nc'):
            files.append(line)
    return files
