
#from malleefowl.process import WPSProcess
import malleefowl.process

class BaseOAIMetadata(malleefowl.process.WPSProcess):
    """This process lists all available oai identifiers for iso metadata."""
    def __init__(self, identifier, title, version, metadata=[], abstract=None ):
        malleefowl.process.WPSProcess.__init__(
            self,
            identifier = identifier,
            title = title,
            version = version,
            metadata = metadata,
            abstract = abstract,
            )

        self.oai_service = self.addLiteralInput(
            identifier="oai_service",
            title="OAI Service",
            abstract="Choose OAI Service",
            type=type(''),
            default='http://c3grid1.dkrz.de:8080/oai/provider',
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['http://c3grid1.dkrz.de:8080/oai/provider']
            )

        self.metadata_prefix = self.addLiteralInput(
            identifier="metadata_prefix",
            title="Metadata Prefix",
            abstract="Choose Metadata Prefix",
            type=type(''),
            default='iso',
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['iso']
            )

class BaseISOMetadata(malleefowl.process.WPSProcess):
    """Base class for iso metadata processes."""
    def __init__(self, identifier, title, version, metadata=[], abstract=None):
        metadata.extend([
            {"title": "C3Grid", "href": "http://www.c3grid.de"},
            {"title": "ISO 19139 Metadata", "href": "https://geo-ide.noaa.gov/wiki/index.php?title=ISO_19139_Identifiers"}
            ])
        
        malleefowl.process.WPSProcess.__init__(
            self,
            identifier = identifier,
            title = title,
            version = version,
            metadata= metadata,
            abstract=abstract,
            )

        self.input = self.addComplexInput(
            identifier="input",
            title="Input",
            abstract="URL of Input Metadata document",
            metadata=[],
            minOccurs=0,
            maxOccurs=1,
            formats=[{"mimeType": "text/xml"}, {"mimeType": "application/json"}],
            maxmegabites=100,
            )

        self.input_format = self.addLiteralInput(
            identifier="input_format",
            title="Input Format",
            abstract="Choose Input Format",
            default="oai",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['json', 'isoxml']
            )
