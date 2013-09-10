"""
Processes for c3grid iso metadata

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

from c3meta import tools

from malleefowl.process import WPSProcess

class ConvertISOMetadata(WPSProcess):
    """This process converts c3 iso metadata to json and yaml"""
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "de.c3grid.iso19139.convert",
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
            minOccurs=0,
            maxOccurs=1,
            formats=[{"mimeType": "text/xml"}, {"mimeType": "application/json"}],
            maxmegabites=10
            )

        self.oai_identifier =  self.addLiteralInput(
            identifier="oai_identifier",
            title="OAI Identifier",
            abstract="Enter OAI Identifier",
            default="de.dkrz.wdcc.iso2139553",
            type=type(''),
            minOccurs=0,
            maxOccurs=1,
            )

        self.input_format = self.addLiteralInput(
            identifier="input_format",
            title="Input Format",
            abstract="Choose Input Format",
            default="oai",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['oai', 'json', 'isoxml']
            )

        self.output_format = self.addLiteralInput(
            identifier="output_format",
            title="Output Format",
            abstract="Choose Output Format",
            default="json",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['json', 'isoxml']
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
        self.status.set(msg="starting isometa converter", percentDone=10, propagate=True)

        input_format = self.input_format.getValue()
        source = None
        if input_format == 'oai':
            source = self.oai_identifier.getValue()
        else:
            source = self.input.getValue()

        output_format = self.output_format.getValue()
        ouput_ext = None
        if output_format == 'json':
            output_ext = '.txt'
        else:
            output_ext = '.xml'
        
        exp = tools.read(format=input_format, source=source)
        self.status.set(msg="file converted", percentDone=50, propagate=True)

        out_filename = self.mktempfile(suffix='.' + output_ext)
        tools.write(exp, target=out_filename, format=output_format)
        self.status.set(msg="file written", percentDone=80, propagate=True)
        
        self.output.setValue( out_filename )
        self.status.set(msg="conversion finished", percentDone=90, propagate=True)
        
        
