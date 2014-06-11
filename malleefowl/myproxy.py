"""
This module is used to get a proxy certificate from a myproxy server with an esgf openid.

Some of the code is taken from esgf-pyclient:
https://github.com/stephenpascoe/esgf-pyclient
"""

import os

def logon_with_openid(openid, password=None, interactive=True, workdir=None):
    (username, hostname, port) = parse(openid)
    return logon(username, hostname, port, password, interactive, workdir)

def logon(username, hostname, port=7512, password=None, interactive=True, workdir=None):
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

    if workdir == None:
        workdir = os.curdir
            
    success = False
    try:
        import subprocess
        from subprocess import PIPE
        env = os.environ.copy()
        env['X509_CERT_DIR'] = workdir
        certfile = os.path.join(workdir, "cert.pem")
        p = subprocess.Popen(
            ["myproxy-logon", "-l", username, "-s", hostname, "-p", port, "-b", "-T", "-S", "-o", certfile, "-v"],
            stdin=PIPE,
            env=env)
        p.communicate(password)
        success = (p.returncode == 0)
    except:
        pass
    return success
    
def parse(openid):
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
