from malleefowl.process import WPSProcess

from datetime import date

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class Logon(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "esgf_logon",
            title = "Logon with ESGF OpenID",
            version = "1.0",
            metadata=[
                {"title":"ESGF","href":"http://esgf.org"},
                ],
            abstract="Logon with ESGF OpenID")

        self.openid = self.addLiteralInput(
            identifier = "openid",
            title = "ESGF OpenID",
            abstract = "Enter ESGF OpenID",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.password = self.addLiteralInput(
            identifier = "password",
            title = "OpenID Password",
            abstract = "Enter your Password",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="X509 Certificate",
            abstract="X509 Proxy Certificate",
            formats=[{"mimeType":"application/x-pkcs7-mime"}],
            asReference=True,
            )

        self.expires = self.addLiteralOutput(
            identifier="expires",
            title="Expires",
            abstract="Proxy Certificate will expire on given date",
            type=type(date(2014,4,8)),
            )

    def execute(self):
        from malleefowl import myproxy
        
        self.show_status("start logon ...", 5)

        openid=self.openid.getValue()
        password=self.password.getValue()
        
        logger.debug('openid=%s' % (openid))

        certfile = myproxy.logon_with_openid(openid=openid, password=password, interactive=False)
        
        self.show_status("logon successful", 90)

        self.output.setValue( certfile )
        self.expires.setValue(myproxy.cert_infos(certfile)['expires'])
        
