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
import re
from lxml import etree
from io import BytesIO
import OpenSSL
from dateutil import parser as date_parser

from pyesgf.logon import LogonManager, ESGF_CREDENTIALS

import logging
logger = logging.getLogger(__name__)


def myproxy_logon_with_openid(openid, password=None, interactive=False, outdir=None):
    """
    Tries to get MyProxy parameters from OpenID and calls :meth:`logon`.

    :param openid: OpenID used to login at ESGF node.
    """
    outdir = outdir or os.curdir
    username, hostname, port = parse_openid(openid)
    lm = LogonManager(esgf_dir=outdir, dap_config=os.path.join(outdir, 'dodsrc'))
    lm.logoff()
    lm.logon(username=username, password=password, hostname=hostname,
             bootstrap=True, update_trustroots=False, interactive=interactive)
    return os.path.join(outdir, ESGF_CREDENTIALS)


def parse_openid(openid, ssl_verify=False):
    """
    parse openid document to get myproxy service
    """
    XRI_NS = 'xri://$xrd*($v*2.0)'
    MYPROXY_URN = 'urn:esg:security:myproxy-service'
    ESGF_OPENID_REXP = r'https://.*/esgf-idp/openid/(.*)'
    MYPROXY_URI_REXP = r'socket://([^:]*):?(\d+)?'

    response = requests.get(openid, verify=ssl_verify)
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

    port = port or "7512"

    return username, hostname, port


def cert_infos(filename):
    expires = None
    with open(filename) as fh:
        data = fh.read()
        cert = OpenSSL.crypto.load_certificate(OpenSSL.SSL.FILETYPE_PEM, data)
        expires = date_parser.parse(cert.get_notAfter())
    return dict(expires=expires)
