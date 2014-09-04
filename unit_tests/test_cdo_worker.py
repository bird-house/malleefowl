import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import wpsclient

import __init__ as base

@attr('online')
def test_cdo_sinfo():
    result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "de.dkrz.cdo.sinfo.worker",
        inputs = [('file_identifier', base.TEST1_NC),
                  ('file_identifier', base.TEST2_NC)],
        outputs = [('output', True)]
        )
    nose.tools.ok_(len(result) == 1, result)
    nose.tools.ok_('http' in result[0]['reference'])

@attr('online')
def test_cdo_merge():
    result = wpsclient.execute(
        service = base.SERVICE,
        identifier = "de.dkrz.cdo.operation.worker",
        inputs = [('file_identifier', base.TEST1_NC),
                  ('file_identifier', base.TEST2_NC),
                  ('operator', 'merge')],
        outputs = [('output', True)]
        )
    nose.tools.ok_(len(result) == 1, result)
    nose.tools.ok_('http' in result[0]['reference'])


