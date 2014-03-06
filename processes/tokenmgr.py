"""
Managing user access tokens

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import malleefowl
from malleefowl.process import WPSProcess

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class GetToken(WPSProcess):
    """Gets token for userid"""

    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "org.malleefowl.token.get",
            title = "Get token for userid",
            version = "0.1",
            metadata=[
                ],
            abstract="Get token for userid")

        logger.debug('init token')

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
        self.show_status("get token ...", 10)

        userid = self.userid.getValue()
        token = userid.replace('@', '_')

        self.output.setValue(token)

        self.show_status("get token ... done", 90)
