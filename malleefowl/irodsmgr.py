from pywps import config
from malleefowl import tokenmgr, utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def list_files(token, filter, folder):
    userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)

    logger.debug('userid=%s, filter=%s, folder=%s' % (userid, filter, folder))

    from subprocess import Popen, PIPE
    p = Popen(['/home/pingu/opt/iRODS/clients/icommands/bin/ils', folder], stdout=PIPE, stderr=PIPE)
    (stdoutdata, stderrdata) = p.communicate()

    logger.debug("stdout=%s, stderr=%s", stdoutdata, stderrdata)
    files = []
    for line in stdoutdata.splitlines():
        line = line.strip()
        if line.endswith('.nc'):
            files.append(line)
    return files
