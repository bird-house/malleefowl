"""
This module is used to get esgf logon credentials. There are two choices:

* a proxy certificate from a myproxy server with an ESGF openid.
* OpenID login as used in browsers.

Some of the code is taken from esgf-pyclient:
https://github.com/stephenpascoe/esgf-pyclient

See also:

* open climate workbench: https://github.com/apache/climate
* MyProxyLogon: https://pypi.python.org/pypi/MyProxyClient
"""

import os
from malleefowl.exceptions import MyProxyLogonError

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


def _consumer(provider, url):
    consumer = provider
    if url:
        from urlparse import urlparse
        consumer = urlparse(url).netloc
    return consumer

def _password(interactive, password):
    if interactive:
        if password is None:
            from getpass import getpass
            password = getpass('Enter password for %s: ' % username)
    return password

def _outdir(outdir):
    if outdir == None:
        outdir = os.curdir
    return outdir

def openid_logon(openid, password=None, interactive=False, outdir=None, url=None):
    """
    Uses the OpenID logon at an ESGF identity provider to get the credentials (cookies)

    TODO: move this code to esgf pyclient

    :return: cookies file
    """
    (username, provider, port) = parse_openid(openid)
    consumer = _consumer(provider, url)
    password = _password(interactive, password)
    outdir = _outdir(outdir)

    import requests
    from requests.auth import HTTPBasicAuth
    from cookielib import MozillaCookieJar

    url = 'https://{0}/esg-orp/j_spring_openid_security_check.htm'.format(consumer)
    data = dict(openid_identifier='https://{0}/esgf-idp/openid/'.format(provider), rememberOpenid='on')
    auth = HTTPBasicAuth(username, password)
    headers = {'esgf-idea-agent-type': 'basic_auth'}

    session = requests.Session()
    cookies = os.path.join(outdir, 'cookies.txt')
    session.cookies = MozillaCookieJar(cookies)
    if not os.path.exists(cookies):
        # Create a new cookies file and set our Session's cookies
        logger.debug("setting cookies")
        session.cookies.save()
    else:
        # Load saved cookies from the file and use them in a request
        logger.debug("loading saved cookies")
        session.cookies.load(ignore_discard=True)
    response = session.post(url, auth=auth, data=data, headers=headers, verify=True)
    logger.debug("openid logon: status=%s", response.status_code)
    response.raise_for_status()
    session.cookies.save(ignore_discard=True)
   
    return cookies


def openid_logon_with_wget(openid, password=None, interactive=False, outdir=None, url=None):
    (username, provider, port) = parse_openid(openid)
    consumer = _consumer(provider, url)
    password = _password(interactive, password)
    outdir = _outdir(outdir)
     
    cmd = ['wget']
    cmd.append('--post-data')
    cmd.append('"openid_identifier=https://{0}/esgf-idp/openid/&rememberOpenid=on"'.format(provider))
    cmd.append('--header="esgf-idea-agent-type:basic_auth"')
    cmd.append('--http-user="{0}"'.format(username))
    cmd.append('--http-password="{0}"'.format(password))
    #certs = os.path.join(outdir, 'certificates')
    #cmd.append('--ca-directory={0}'.format(certs))
    cmd.append('--cookies=on')
    cmd.append('--keep-session-cookies')
    cmd.append('--save-cookies')
    cookies = os.path.join(outdir, 'wcookies.txt')
    cmd.append(cookies)
    cmd.append('--load-cookies')
    cmd.append(cookies)
    #cmd.append('-v')
    cmd.append('-O-')
    cmd.append('https://{0}/esg-orp/j_spring_openid_security_check.htm'.format(consumer))
    cmd_str = ' '.join(cmd)
    logger.debug('execute: %s', cmd_str)
    
    import subprocess
    subprocess.check_output(cmd_str, shell=True)

    return cookies


def myproxy_logon_with_openid(openid, password=None, interactive=False, outdir=None):
    """
    Trys to get MyProxy parameters from OpenID and calls :meth:`logon`.

    :param openid: OpenID used to login at ESGF node.
    """
    (username, hostname, port) = parse_openid(openid)
    return myproxy_logon(username, hostname, port, password, interactive, outdir)

def myproxy_logon(username, hostname, port=7512, password=None, interactive=False, outdir=None):
    """
    Runs myproxy logon with username and password.

    :param outdir: path used for retrieved files (certificates, ...).
    :param interactive: if true user is prompted for parameters.

    :return: certfile, proxy certificate.
    """
    if interactive:
        if hostname is None:
            print 'Enter myproxy hostname:',
            hostname = raw_input()
        if username is None:
            print 'Enter myproxy username:',
            username = raw_input()
        if password is None:
            from getpass import getpass
            password = getpass('Enter password for %s: ' % username)

    if outdir == None:
        outdir = os.curdir
            
    try:
        import subprocess
        from subprocess import PIPE
        env = os.environ.copy()
        env['X509_CERT_DIR'] = outdir
        env['LD_LIBRARY_PATH'] = ''
        logger.debug("env=%s", env)
        logger.debug("env PATH=%s", env.get('PATH'))
        logger.debug("env LD_LIBRARY_PATH=%s", env.get('LD_LIBRARY_PATH'))
        certfile = os.path.join(outdir, "cert.pem")
        cmd=["myproxy-logon", "-l", username, "-s", hostname, "-p", port, "-b", "-T", "-S", "-o", certfile, "-v"]
        logger.debug("cmd=%s", cmd)
        p = subprocess.Popen(
            cmd,
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            env=env)
        (stdoutstr, stderrstr) = p.communicate(password)
        if p.returncode != 0:
            raise MyProxyLogonError("logon failed! %s %s" % (stdoutstr, stderrstr))
    except Exception as e:
        logger.exception("myproxy logon failed")
        raise MyProxyLogonError("myproxy-logon process failed! %s" % (e.message))
    return certfile
    
def parse_openid(openid):
    """
    parse openid document to get myproxy service
    """
    from xml.etree import ElementTree
    import urllib2
    import re

    XRI_NS = 'xri://$xrd*($v*2.0)'
    MYPROXY_URN = 'urn:esg:security:myproxy-service'
    ESGF_OPENID_REXP = r'https://.*/esgf-idp/openid/(.*)'
    MYPROXY_URI_REXP = r'socket://([^:]*):?(\d+)?'
    
    openid_doc = urllib2.urlopen(openid).read()
    xml = ElementTree.fromstring(openid_doc)

    hostname = None
    port = None
    username = None

    services = xml.findall('.//{%s}Service' % XRI_NS)
    for service in services:
        try:
            service_type = service.find('{%s}Type' % XRI_NS).text
        except AttributeError:
            continue

        # Detect myproxy hostname and port
        if service_type == MYPROXY_URN:
            myproxy_uri = service.find('{%s}URI' % XRI_NS).text
            mo = re.match(MYPROXY_URI_REXP, myproxy_uri)
            if mo:
                hostname, port = mo.groups()

    # If the OpenID matches the standard ESGF pattern assume it contains
    # the username, otherwise prompt or raise an exception
    mo = re.match(ESGF_OPENID_REXP, openid)
    if mo:
        username = mo.group(1)
                
    if port is None:
        port == "7512"
            
    return username, hostname, port

def cert_infos(filename):
    import OpenSSL
    from dateutil import parser as date_parser
    with open(filename) as fh:
        data = fh.read()
        cert = OpenSSL.crypto.load_certificate(OpenSSL.SSL.FILETYPE_PEM, data)
    expires = date_parser.parse(cert.get_notAfter())
    return dict(expires=expires)
