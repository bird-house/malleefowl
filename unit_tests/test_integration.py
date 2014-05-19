import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import wpsclient

import __init__ as base

import json
import tempfile

@attr('integration')
def test_inout():
    result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "org.malleefowl.test.inout",
        inputs = [('intRequired', '2')],
        )
    nose.tools.ok_(len(result) == 12, len(result))
    nose.tools.ok_('intRequired' in str(result), result)


