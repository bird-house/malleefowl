from malleefowl.tests.common import WpsTestClient


def test_caps():
    wps = WpsTestClient()
    resp = wps.get(service='wps', request='getcapabilities')
    names = resp.xpath_text('/wps:Capabilities'
                            '/wps:ProcessOfferings'
                            '/wps:Process'
                            '/ows:Identifier')
    assert sorted(names.split()) == ['download', 'dummy', 'esgf_logon', 'esgsearch', 'thredds_download', 'workflow']
