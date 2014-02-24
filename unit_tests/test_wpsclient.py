import nose.tools
from nose import SkipTest

from malleefowl import wpsclient

service = "http://localhost:8090/wps"

import json
import tempfile

def test_execute():
    result = wpsclient.execute(
        service = service,
        identifier = "org.malleefowl.test.dummyprocess",
        inputs = [('input1', '2'), ('input2', '3')],
        outputs = [('output1', True), ('output2', True)]
        )
    nose.tools.ok_(len(result) == 2, result)
    nose.tools.ok_('http' in result[0]['reference'])
    nose.tools.ok_('http' in result[1]['reference'])

    fp = tempfile.NamedTemporaryFile(mode='w+')
    fp.write(json.dumps(result))
    fp.flush()
    fp.seek(0)

    result2 = json.load(fp)
    nose.tools.ok_(len(result2) == 2, result2)
    nose.tools.ok_('http' in result2[0]['reference'])
    nose.tools.ok_('http' in result2[1]['reference'])

