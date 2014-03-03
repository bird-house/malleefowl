import nose.tools
from nose import SkipTest
from nose.plugins.attrib import attr

from malleefowl import wpsclient

service = "http://localhost:8090/wps"

@attr('online')
def test_cdo_sinfo():
    result = wpsclient.execute(
        service = service,
        identifier = "de.dkrz.cdo.sinfo.worker",
        inputs = [('file_identifier', 'http://localhost:8090/files/test1.nc'),
                  ('file_identifier', 'http://localhost:8090/files/test2.nc')],
        outputs = [('output', True)]
        )
    nose.tools.ok_(len(result) == 1, result)
    nose.tools.ok_('http' in result[0]['reference'])

@attr('online')
def test_cdo_merge():
    result = wpsclient.execute(
        service = service,
        identifier = "de.dkrz.cdo.operation.worker",
        inputs = [('file_identifier', 'http://localhost:8090/files/test1.nc'),
                  ('file_identifier', 'http://localhost:8090/files/test2.nc'),
                  ('operator', 'merge')],
        outputs = [('output', True)]
        )
    nose.tools.ok_(len(result) == 1, result)
    nose.tools.ok_('http' in result[0]['reference'])


