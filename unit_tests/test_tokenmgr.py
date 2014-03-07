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
    ok_(tokenmgr.is_token_valid(tokenmgr.sys_token(), TEST_TOKEN))
    ok_(not tokenmgr.is_token_valid(tokenmgr.sys_token(), 'dos_is_nix'))

@raises(AccessDeniedError)
def test_token_valid_with_exception():
    ok_(tokenmgr.is_token_valid(TEST_TOKEN, 'abc'))

def test_is_sys_token():
    ok_(tokenmgr._is_sys_token(tokenmgr.sys_token()))
    ok_(not tokenmgr._is_sys_token('abc'))

def test_gen_token_for_userid():
    token = tokenmgr.gen_token_for_userid(tokenmgr.sys_token(), 'adelie@somewhere.org')
    ok_(token is not None)

@raises(AccessDeniedError)
def test_gen_token_with_exception():
    token = tokenmgr.gen_token_for_userid(TEST_TOKEN, 'adelie@somewhere.org')

def test_get_token():
    token = tokenmgr.get_token(tokenmgr.sys_token(), 'amelie@montematre.org')
    ok_(token == TEST_TOKEN)

def test_get_uuid():
    uuid = tokenmgr.get_uuid()
    ok_(len(uuid) == 22, uuid)
    
