"""
Managing user access tokens

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

from malleefowl.process import WPSProcess
from malleefowl import tokenmgr

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class GenerateToken(WPSProcess):
    """Generates token for userid"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.token.generate",
            title = "Generates token for userid",
            version = "0.1",
            metadata=[
                ],
            abstract="Generates token for userid")

        self.sys_token = self.addLiteralInput(
            identifier = "sys_token",
            title = "System token",
            abstract = "System User Token",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.userid = self.addLiteralInput(
            identifier = "userid",
            title = "User ID",
            abstract = "Your User ID",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.output = self.addLiteralOutput(
            identifier="output",
            title="Token",
            abstract="Token for your user id",
            type=type('')
            )

    def execute(self):
        self.show_status("generate token ...", 10)

        userid = self.userid.getValue()
        sys_token = self.sys_token.getValue()

        logger.debug("generate token for userid=%s" % (userid))
        
        token = tokenmgr.gen_token_for_userid(sys_token, userid)

        self.output.setValue(token)

        self.show_status("generate token ... done", 90)
