"""
This module is used to get esgf logon credentials. There are two choices:

* a proxy certificate from a myproxy server with an ESGF openid.
* OpenID login as used in browsers.

Some of the code is taken from esgf-pyclient:
https://github.com/ESGF/esgf-pyclient

See also:

* open climate workbench: https://github.com/apache/climate
* MyProxyLogon: https://github.com/cedadev/MyProxyClient
"""

import os
import requests
from requests.auth import HTTPBasicAuth
from cookielib import MozillaCookieJar
import re
from lxml import etree
from io import BytesIO
import OpenSSL
from dateutil import parser as date_parser

import logging
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
            password = getpass('Enter password: ')
    return password


def openid_logon(openid, password=None, interactive=False, outdir=None, url=None):
    """
    Uses the OpenID logon at an ESGF identity provider to get the credentials (cookies)

    TODO: move this code to esgf pyclient

    :return: cookies file
    """
    (username, provider, port) = parse_openid(openid)
    consumer = _consumer(provider, url)
    password = _password(interactive, password)
    outdir = outdir or os.path.curdir

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

    if outdir is None:
        outdir = os.curdir

    from myproxy.client import MyProxyClient
    myproxy_clnt = MyProxyClient(hostname=hostname, port=port, caCertDir=outdir, proxyCertLifetime=43200)
    creds = myproxy_clnt.logon(username, password, bootstrap=True)

    outfile = os.path.join(outdir, 'cert.pem')
    with open('cert.pem', 'w') as fout:
        for cred in creds:
            fout.write(cred)

    return outfile


def parse_openid(openid, ssl_verify=False):
    """
    parse openid document to get myproxy service
    """
    XRI_NS = 'xri://$xrd*($v*2.0)'
    MYPROXY_URN = 'urn:esg:security:myproxy-service'
    ESGF_OPENID_REXP = r'https://.*/esgf-idp/openid/(.*)'
    MYPROXY_URI_REXP = r'socket://([^:]*):?(\d+)?'

    kwargs = {'verify': ssl_verify}
    response = requests.get(openid, **kwargs)
    xml = etree.parse(BytesIO(response.content))

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
        port = "7512"

    return username, hostname, port


def cert_infos(filename):
    with open(filename) as fh:
        data = fh.read()
        cert = OpenSSL.crypto.load_certificate(OpenSSL.SSL.FILETYPE_PEM, data)
    expires = date_parser.parse(cert.get_notAfter())
    return dict(expires=expires)
