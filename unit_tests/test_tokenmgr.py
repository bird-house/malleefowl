from nose.tools import ok_, with_setup, raises
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import tokenmgr
from malleefowl.tokenmgr import AccessDeniedError

TEST_TOKEN = tokenmgr.get_uuid()

def setup():
    tokenmgr.init()
    tokenmgr._add(TEST_TOKEN, 'amelie@montematre.org')
    
def test_token_valid():
    ok_(tokenmgr.is_token_valid('abc', 'abc'))
    ok_(not tokenmgr.is_token_valid('abc', 'dfe'))

@raises(AccessDeniedError)
def test_token_valid():
    ok_(tokenmgr.is_token_valid('abc123', 'abc'))

@raises(AccessDeniedError)
def test_token_valid_with_exception():
    ok_(tokenmgr.is_token_valid('abc123', 'abc'))

def test_is_sys_token():
    ok_(tokenmgr._is_sys_token('abc'))
    ok_(not tokenmgr._is_sys_token('dfe'))

def test_gen_token_for_userid():
    token = tokenmgr.gen_token_for_userid('abc', 'adelie@somewhere.org')
    ok_(token is not None)

@raises(AccessDeniedError)
def test_gen_token_with_exception():
    token = tokenmgr.gen_token_for_userid('abc123', 'adelie@somewhere.org')

def test_get_token():
    token = tokenmgr.get_token('abc', 'amelie@montematre.org')
    ok_(token == TEST_TOKEN)

def test_get_uuid():
    uuid = tokenmgr.get_uuid()
    ok_(len(uuid) == 22, uuid)
    
