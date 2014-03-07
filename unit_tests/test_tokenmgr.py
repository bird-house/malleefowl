from nose.tools import ok_, with_setup, raises
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import tokenmgr
from malleefowl.tokenmgr import AccessDeniedError

def setup():
    tokenmgr.init()
    tokenmgr._add('abc123', 'amelie@montematre.org')
    
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

def test_gen_token():
    token = tokenmgr.gen_token('abc', 'adelie@somewhere.org')

@raises(AccessDeniedError)
def test_gen_token_with_exception():
    token = tokenmgr.gen_token('abc123', 'adelie@somewhere.org')

def test_get_token():
    token = tokenmgr.get_token('abc', 'amelie@montematre.org')
    ok_(token == 'abc123')
    
