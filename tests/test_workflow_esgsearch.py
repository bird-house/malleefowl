#from malleefowl.dispel import esgsearch_workflow

from nose import tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from __init__ import SERVICE, TESTDATA, CREDENTIALS

def my_monitor(execution):
    print execution.status
    print execution.percentCompleted
    print execution.statusMessage

@attr('online')
@attr('security')
def test_esgsearch_workflow():
    # TODO: set environ with credentials
    # export TEST_CREDENTIALS=http://localhost:8081/mycreds.pem 
    raise SkipTest 
    constraints = [('project', 'CORDEX'),
                   ('experiment', 'historical'),
                   ('variable', 'tasmax'),
                   ('time_frequency', 'day'),
                   ('ensemble', 'r1i1p1'),
                   ('institute', 'MPI-CSC'),
                   ('domain', 'WAS-44')]
    cs_str = ','.join( ['%s:%s' % (key, value) for key, value in constraints])
    result = esgsearch_workflow(
        SERVICE,
        esgsearch_params=dict(constraints=cs_str,
                              limit=1, search_type='File', distrib=False, temporal=False,
                              start='2001-01-01T12:00:00Z', end='2005-12-31T12:00:00Z',
                          ),
        wget_params=dict(credentials=CREDENTIALS),
        doit_params=dict(url=SERVICE,
                         identifier='dummy', resource='resource', inputs=[]),
        monitor = my_monitor,
        )
    tools.ok_( len(result['source']) == 1, result)
    tools.ok_('malleefowl' in result['source'][0], result)
    tools.ok_( len(result['worker']) == 1, result)
    tools.ok_('malleefowl' in result['worker'][0], result)
    #tools.ok_(False, result)



    
    
    




