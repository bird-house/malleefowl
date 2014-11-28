import logging

SERVICE = "http://localhost:8091/wps"
TESTDATA = {}


from os.path import join, dirname
__testdata_filename__ = join(dirname(__file__), 'testdata.json')

try:
    from os.path import join
    import json
    with open(__testdata_filename__, 'r') as fp:
        TESTDATA = json.load(fp)
except:
    logging.error('could not read testdata! %s', __testdata_filename__ )
