import os
from pywps.app.Service import Service

from malleefowl.processes import processes

__version__ = "0.6.4"


def application(environ, start_response):
    app = Service(processes, [os.environ.get('PYWPS_CFG')])
    return app(environ, start_response)
