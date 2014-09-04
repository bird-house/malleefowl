import nose.tools
from nose import SkipTest

from malleefowl import myproxy

def test_parse():
    openid="https://esgf-data.dkrz.de/esgf-idp/openid/pingutest"
    (username, hostname, port) = myproxy.parse(openid)
    nose.tools.ok_( username == "pingutest", username)
    nose.tools.ok_( hostname == "esgf-data.dkrz.de", hostname)
    nose.tools.ok_( port == "7512", port)

