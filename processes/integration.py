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
            formats=[{"mimeType":"text/xml"}],
            asReference=True,
            )
                                           
    def execute(self):
        self.show_status("Starting ...", 5)

        from pywps import config
        import os.path
        integration_tests = os.path.join(
            config.getConfigValue("malleefowl", "integration_tests"),
            "test_integration.py")

        import nose
        nose.run(argv=['nosetests', '-v', '--with-xunit', integration_tests])
        
        self.output.setValue("nosetests.xml")
        self.show_status("Done", 95)
    
