import nose.tools
from tests.common import WpsTestClient

def test_caps():
    wps = WpsTestClient()
    resp = wps.get(service='wps', request='getcapabilities')
    names = resp.xpath_text('/wps:Capabilities'
                            '/wps:ProcessOfferings'
                            '/wps:Process'
                            '/ows:Identifier')
    assert sorted(names.split()) == ['download', 'esgf_logon', 'esgsearch', 'swift_download', 'swift_download_urls', 'swift_login', 'swift_upload', 'thredds_download', 'workflow']

