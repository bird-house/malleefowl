"""
Processes for c3grid iso metadata

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

from c3meta import tools

from malleefowl.process import WPSProcess

class C3MetaConverter(WPSProcess):
    """This process converts c3 iso metadata to json and yaml"""
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "de.c3grid.isometa.converter",
            title = "Convert C3Grid ISO Metadata",
            version = "0.1",
            metadata=[
                {"title": "C3Grid", "href": "http://www.c3grid.de"},
                {"title": "ISO 19139 Metadata", "href": "https://geo-ide.noaa.gov/wiki/index.php?title=ISO_19139_Identifiers"}
                ],
            abstract="Convert C3Grid ISO Metadata to JSON and YAML",
            )

        self.input = self.addComplexInput(
            identifier="input",
            title="Input",
            abstract="URL of Input Metadata document",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[{"mimeType": "text/xml"}, {"mimeType": "application/json"}],
            maxmegabites=10
            )

        self.input_format = self.addLiteralInput(
            identifier="input_format",
            title="Input Format",
            abstract="Choose Input Format",
            default="iso",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['json', 'iso']
            )

        self.output_format = self.addLiteralInput(
            identifier="output_format",
            title="Output Format",
            abstract="Choose Output Format",
            default="json",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['json', 'iso']
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Output",
            abstract="Converted Metadata document",
            metadata=[],
            formats=[ {"mimeType": "text/xml"}, {"mimeType": "application/json"}],
            asReference=True,
            )

    def execute(self):
        pass
