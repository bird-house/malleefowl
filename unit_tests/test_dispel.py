import nose.tools
from nose import SkipTest

from malleefowl import dispel

def test_generate():
    nose.tools.ok_(False, dispel.generate({}))
