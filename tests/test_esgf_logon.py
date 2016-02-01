import nose.tools
from nose.plugins.attrib import attr

from malleefowl.esgf import logon

@attr('online')
def test_parse_openid():
    openid="https://esgf-data.dkrz.de/esgf-idp/openid/pingutest1"
    (username, hostname, port) = logon.parse_openid(openid)
    nose.tools.ok_( username == "pingutest1", username)
    nose.tools.ok_( hostname == "esgf-data.dkrz.de", hostname)
    nose.tools.ok_( port == "7512", port)

