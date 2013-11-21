from malleefowl.process import WPSProcess

from owslib.wms import WebMapService

import json
from datetime import datetime, date
from dateutil import parser as date_parser
import types
import os

class GetWMSLayers(WPSProcess):
    """Retrieve layers from thredds ncwms service."""

    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "org.malleefowl.wms.layer",
            title = "Get WMS Layers",
            version = "0.1",
            metadata = [],
            abstract = "Get all Layers from Thredds ncWMS Service",
            )

        self.start_in = self.addLiteralInput(
            identifier="start",
            title="Start Date",
            abstract="Start Date: 2006-01-01",
            default="2006-01-01",
            type=type(date(2013,7,11)),
            minOccurs=0,
            maxOccurs=1,
            )

        self.end_in = self.addLiteralInput(
            identifier="end",
            title="End Date",
            abstract="End Date: 2006-12-31",
            default="2006-12-31",
            type=type(date(2013,7,11)),
            minOccurs=0,
            maxOccurs=1,
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="WMS Layers",
            abstract="WMS Layers as JSON",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=10, propagate=True)

        files = [f for f in os.listdir(self.files_path) if f.endswith(".nc")]

        layers = []
        count = 0
        percent_done = 10
        for file_name in files:
            service_url = self.thredds_url + '/wms/test/' + file_name
            try:
                wms = WebMapService(service_url, version='1.1.1')
            except:
                self.message("could not access wms %s" % (service_url))
                continue

            for layerid,layer in wms.items():
                if layerid in ['lat', 'lon']:
                    continue
                timesteps = map(str.strip, layer.timepositions)
                layers.append(dict(service=service_url,
                                   name=layer.name,
                                   title=layer.title,
                                   timesteps=timesteps))
            count = count + 1
            percent_done = int(percent_done + count * 80 / len(files))
            self.status.set(msg="adding layer ...", percentDone=percent_done, propagate=True)

        self.status.set(msg="done", percentDone=90, propagate=True)
        
        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(json.dumps(layers))
            fp.close()
            self.output.setValue( out_filename )

class AnimateWMSLayer(WPSProcess):
    """Create gif animation of wms layer for timesteps."""

    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "org.malleefowl.wms.animate",
            title = "Animate WMS Layer",
            version = "0.1",
            metadata = [],
            abstract = "Create gif animation of wms layer for timesteps.",
            )

        self.delay_in = self.addLiteralInput(
            identifier="delay",
            title="Delay",
            abstract="Animation Delay 1/100 seconds",
            default=10,
            type=type(1),
            minOccurs=1,
            maxOccurs=1,
            )

        self.max_frames_in = self.addLiteralInput(
            identifier="max_frames",
            title="Max. Frames",
            abstract="Maximum Number of Animation Frames",
            default=500,
            type=type(1),
            minOccurs=1,
            maxOccurs=1,
            )

        self.start_in = self.addLiteralInput(
            identifier="start",
            title="Start Date",
            abstract="Start Date of Animation: 2006-01-01",
            default="2006-01-01",
            type=type(date(2013,7,11)),
            minOccurs=0,
            maxOccurs=1,
            )

        self.end_in = self.addLiteralInput(
            identifier="end",
            title="End Date",
            abstract="End Date of Animation: 2006-12-31",
            default="2006-12-31",
            type=type(date(2013,7,11)),
            minOccurs=0,
            maxOccurs=1,
            )

        self.resolution_in = self.addLiteralInput(
            identifier="resolution",
            title="Temporal Resolution",
            abstract="Temporal Resolution for Animation",
            default="monthly",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['hourly', 'daily', 'weekly', 'monthly', 'yearly']
            )

        self.layer_in = self.addLiteralInput(
            identifier="layer",
            title="Layer",
            abstract="Layer Name",
            default="tas",
            type=type(''),
            minOccurs=1,
            maxOccurs=10,
            )

        self.srs_in = self.addLiteralInput(
            identifier="srs",
            title="SRS",
            abstract="Coordinate System: EPSG:4326",
            default="EPSG:4326",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        self.width_in = self.addLiteralInput(
            identifier="width",
            title="Width",
            abstract="Image Width",
            default=300,
            type=type(1),
            minOccurs=1,
            maxOccurs=1,
            )

        self.height_in = self.addLiteralInput(
            identifier="height",
            title="Height",
            abstract="Image Height",
            default=200,
            type=type(1),
            minOccurs=1,
            maxOccurs=1,
            )

        self.bbox_in = self.addLiteralInput(
            identifier="bbox",
            title="Bounding Box",
            abstract="Bounding Box: (minx,miny,maxx,maxy)",
            default="-180,-90,180,90",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Animated WMS Layer",
            abstract="Animated gif of WMS Layer",
            metadata=[],
            formats=[{"mimeType":"image/gif"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=10, propagate=True)

        service_url = self.thredds_url + '/wms/test/cordex-eur-tas-year-pywpsInputoZXCTG.nc'
        wms = WebMapService(service_url, version='1.1.1')

        layer = wms.contents['tas']
        timesteps = map(str.strip, layer.timepositions)

        layers = self.layer_in.getValue()
        if type(layers) != types.ListType:
            layers = [layers]
        layers = map(str.strip, layers)
        width = int(self.width_in.getValue())
        height = int(self.height_in.getValue())
        bbox = tuple(map(float, self.bbox_in.getValue().split(",")))

        images = []
        percent_done = 10
        count = 0
        for time in timesteps:
            if time < self.start_in.getValue():
                continue
            if time > self.end_in.getValue():
                break
            
            img_filename = self.mktempfile(suffix='.gif')
            img = wms.getmap(layers=layers,
                             bbox=bbox,
                             size=(width, height),
                             format='image/gif',
                             srs=self.srs_in.getValue(),
                             time=time)
            out = open(img_filename, 'wb')
            out.write(img.read())
            out.close()
            images.append(img_filename)
            count = count + 1
            if (count % 10 == 0):
                percent_done = int(percent_done + count * 70 / len(timesteps))
                self.status.set(
                    msg="wms image %d/%d generated" % (count, len(timesteps)),
                    percentDone=percent_done, propagate=True)
            if count == self.max_frames_in.getValue():
                break

        self.status.set(msg="wms images generated", percentDone=80, propagate=True)
        
        out_filename = self.mktempfile(suffix='.gif')
        try:
            cmd = ["gifsicle"]
            cmd.append("--delay=%d" % self.delay_in.getValue())
            cmd.append("--loop")
            cmd.extend(images)
            cmd.append("--output")
            cmd.append(out_filename)
            self.cmd(cmd=cmd, stdout=True)
        except:
            self.message(msg='gifsicle failed', force=True)
            raise

        self.status.set(msg="done", percentDone=90, propagate=True)

        self.output.setValue( out_filename )

        

        
