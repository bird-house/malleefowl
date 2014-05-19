from malleefowl.process import WPSProcess

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


class IntegrationCheckProcess(WPSProcess):
    """
    run integration tests ...
    """
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier="org.malleefowl.base.integration", 
            title="Run integration tests",
            version = "1.0",
            metadata = [],
            abstract="Run integration tests with nosetests ...",
            )

        self.output = self.addComplexOutput(
            identifier = "output",
            title = "nosetests result",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )
                                           
    def execute(self):
        self.show_status("Starting ...", 5)

        from pywps import config
        import os.path
        integration_tests = os.path.join(
            config.getConfigValue("malleefowl", "integration_tests"),
            "test_integration.py")

        from subprocess import Popen, PIPE
        try:
            # nosetests --verbosity 3 -x --with-xunit -a 'integration'
            p = Popen(['nosetests', '--verbosity', '3', integration_tests],
                      stdout=PIPE, stderr=PIPE)
            (stdoutdata,stderrdata) = p.communicate()

            with open("nose.txt", "w") as fp:
                fp.write(stdoutdata)
                fp.write(stderrdata)
        except Exception as e:
            logger.exception('nosetest failed!')
            raise
        
        self.output.setValue("nose.txt")
        self.show_status("Done", 95)
    
