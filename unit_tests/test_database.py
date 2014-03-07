from nose.tools import ok_, with_setup, raises
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import database
from malleefowl import tokenmgr

TEST_TOKEN = tokenmgr.get_uuid()
TEST_USERID = 'test@malleefowl.org'

def test_add_token():
    database.add_token(token=TEST_TOKEN, userid=TEST_USERID)

def test_get_token_by_userid():
    token = database.get_token_by_userid( userid=TEST_USERID)
    ok_(TEST_TOKEN == token, token)

def test_get_userid_by_token():
    userid = database.get_userid_by_token( token=TEST_TOKEN)
    ok_(TEST_USERID == userid, userid)
