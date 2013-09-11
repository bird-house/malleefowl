"""
Processes for c3grid iso metadata

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
import json

from c3meta import tools

from malleefowl.process import WPSProcess

class BaseOAIMetadata(WPSProcess):
    """This process lists all available oai identifiers for iso metadata."""
    def __init__(self, identifier, title, version, metadata=[], abstract=None ):
        WPSProcess.__init__(
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

class ListOAIIdentifier(BaseOAIMetadata):
    """This process lists all available oai identifiers for iso metadata."""
    def __init__(self):
        BaseOAIMetadata.__init__(
            self,
            identifier = "de.c3grid.oai.iso19139.list",
            title = "List OAI Identifier of ISO Metadata",
            version = "0.1",
            metadata = [],
            abstract = "This process lists all available oai identifiers for iso metadata."
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Output",
            abstract="List of OAI Identifier",
            metadata=[],
            formats=[ {"mimeType": "application/json"} ],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting listing of oai identifier", percentDone=10, propagate=True)

        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(json.dumps(tools.list_sources(format='oai'), indent=4))
            fp.close()
            self.output.setValue( out_filename )
        self.status.set(msg="file written", percentDone=80, propagate=True)
        
        self.output.setValue( out_filename )
        self.status.set(msg="listing finished", percentDone=90, propagate=True)

class SummaryOAIMetadata(BaseOAIMetadata):
    """This process shows a summary c3 iso metadata."""

    def __init__(self):
        BaseOAIMetadata.__init__(
            self,
            identifier = "de.c3grid.oai.iso19139.summary",
            title = "Show Summary of C3Grid ISO Metadata",
            version = "0.1",
            abstract="Convert C3Grid ISO Metadata to JSON and YAML",
            )

        self.oai_identifier =  self.addLiteralInput(
            identifier="input",
            title="OAI Identifier",
            abstract="Enter OAI Identifier",
            default="de.dkrz.wdcc.iso2139553",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Output",
            abstract="Metadata Summary Document",
            metadata=[],
            formats=[ {"mimeType": "application/json"} ],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting isometa summary", percentDone=10, propagate=True)

        source = self.oai_identifier.getValue()
        
        exp = tools.read(format='oai', source=source)
        self.status.set(msg="file read", percentDone=50, propagate=True)

        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(json.dumps(tools.summary(exp), indent=4))
            fp.close()
            self.output.setValue( out_filename )
        self.status.set(msg="file written", percentDone=80, propagate=True)
        
        self.output.setValue( out_filename )
        self.status.set(msg="summary finished", percentDone=90, propagate=True)

class ConvertOAIMetadata(BaseOAIMetadata):
    """This process converts c3 iso metadata to json and yaml."""
    def __init__(self):
        BaseOAIMetadata.__init__(
            self,
            identifier = "de.c3grid.oai.iso19139.convert",
            title = "Convert C3Grid ISO Metadata",
            version = "0.1",
            abstract="Convert C3Grid ISO Metadata to JSON and YAML",
            )

        self.oai_identifier =  self.addLiteralInput(
            identifier="input",
            title="OAI Identifier",
            abstract="Enter OAI Identifier",
            default="de.dkrz.wdcc.iso2139553",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
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

        source = self.oai_identifier.getValue()
        
        output_format = self.output_format.getValue()
        ouput_ext = None
        if output_format == 'json':
            output_ext = '.txt'
        else:
            output_ext = '.xml'
            
        self.status.set(msg="start reading", percentDone=20, propagate=True)
        exp = tools.read(format='oai', source=source)
        self.status.set(msg="file read", percentDone=50, propagate=True)

        out_filename = self.mktempfile(suffix='.' + output_ext)
        tools.write(exp, target=out_filename, format=output_format)
        self.status.set(msg="file written", percentDone=80, propagate=True)
        
        self.output.setValue( out_filename )
        self.status.set(msg="conversion finished", percentDone=90, propagate=True)
        
        
class BaseISOMetadata(WPSProcess):
    """Base class for iso metadata processes."""
    def __init__(self, identifier, title, version, metadata=[], abstract=None):
        metadata.extend([
            {"title": "C3Grid", "href": "http://www.c3grid.de"},
            {"title": "ISO 19139 Metadata", "href": "https://geo-ide.noaa.gov/wiki/index.php?title=ISO_19139_Identifiers"}
            ])
        
        WPSProcess.__init__(
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

class SummaryISOMetadata(BaseISOMetadata):
    """This process shows a summary c3 iso metadata."""

    def __init__(self):
        BaseISOMetadata.__init__(
            self,
            identifier = "de.c3grid.iso19139.summary",
            title = "Show Summary of C3Grid ISO Metadata",
            version = "0.1",
            abstract="Convert C3Grid ISO Metadata to JSON and YAML",
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Output",
            abstract="Metadata Summary Document",
            metadata=[],
            formats=[ {"mimeType": "application/json"} ],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting isometa summary", percentDone=10, propagate=True)

        input_format = self.input_format.getValue()
        source = os.path.abspath(self.input.getValue())

        exp = tools.read(format=input_format, source=source)
        self.status.set(msg="file read", percentDone=50, propagate=True)

        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(json.dumps(tools.summary(exp), indent=4))
            fp.close()
            self.output.setValue( out_filename )
        self.status.set(msg="file written", percentDone=80, propagate=True)
        
        self.output.setValue( out_filename )
        self.status.set(msg="summary finished", percentDone=90, propagate=True)

        
class ConvertISOMetadata(BaseISOMetadata):
    """This process converts c3 iso metadata to json and yaml."""
    def __init__(self):
        BaseISOMetadata.__init__(
            self,
            identifier = "de.c3grid.iso19139.convert",
            title = "Convert C3Grid ISO Metadata",
            version = "0.1",
            abstract="Convert C3Grid ISO Metadata to JSON and YAML",
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
        source = os.path.abspath(self.input.getValue())
        
        output_format = self.output_format.getValue()
        ouput_ext = None
        if output_format == 'json':
            output_ext = '.txt'
        else:
            output_ext = '.xml'
            
        self.status.set(msg="start reading", percentDone=20, propagate=True)
        exp = tools.read(format=input_format, source=source)
        self.status.set(msg="file read", percentDone=50, propagate=True)

        out_filename = self.mktempfile(suffix='.' + output_ext)
        tools.write(exp, target=out_filename, format=output_format)
        self.status.set(msg="file written", percentDone=80, propagate=True)
        
        self.output.setValue( out_filename )
        self.status.set(msg="conversion finished", percentDone=90, propagate=True)
        
        
