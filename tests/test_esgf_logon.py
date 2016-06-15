import pytest

from malleefowl.esgf import logon

@pytest.mark.online
def test_parse_openid():
    openid="https://esgf-data.dkrz.de/esgf-idp/openid/pingutest1"
    (username, hostname, port) = logon.parse_openid(openid)
    assert username == "pingutest1"
    assert hostname == "esgf-data.dkrz.de"
    assert port == "7512"

