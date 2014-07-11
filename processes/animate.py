from malleefowl.process import WPSProcess
from malleefowl import utils

from owslib.wms import WebMapService

import json
from datetime import datetime, date
from dateutil import parser as date_parser
import types
import os
import shutil

from malleefowl import config
from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

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

        self.token = self.addLiteralInput(
            identifier = "token",
            title = "Token",
            abstract = "Your unique access token",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.start = self.addLiteralInput(
            identifier="start",
            title="Start Date",
            abstract="Start Date: 2006-01-01",
            default="2006-01-01",
            type=type(date(2013,7,11)),
            minOccurs=0,
            maxOccurs=1,
            )

        self.end = self.addLiteralInput(
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
        # TODO: needs to be protected by token
        self.show_status("starting ...", 10)

        token = self.token.getValue()

        from malleefowl import tokenmgr
        userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)
        
        files_path = os.path.join(self.files_path, userid)
        utils.mkdir(files_path)

        files = [f for f in os.listdir(files_path) if f.endswith(".nc")]

        layers = []
        count = 0
        percent_done = 10
        for file_name in files:
            service_url = config.thredds_url() + '/wms/test/' + userid + '/' +file_name
            try:
                wms = WebMapService(service_url, version='1.1.1')
            except:
                logger.warn("could not access wms %s" % (service_url))
                continue

            for layerid,layer in wms.items():
                if layerid in ['lat', 'lon']:
                    continue
                if (layer.timepositions == None):
                    continue
                timesteps = map(str.strip, layer.timepositions)
                layers.append(dict(service=service_url,
                                   service_name=file_name,
                                   name=layer.name,
                                   title=layer.title,
                                   timesteps=timesteps))
            count = count + 1
            percent_done = int(percent_done + count * 80 / len(files))
            self.show_status("adding layer ...", percent_done)

        self.show_status("done", 90)
        
        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(json.dumps(layers))
            fp.close()
            self.output.setValue( out_filename )

class GetTimestepsForAnimation(WPSProcess):
    """Get timesteps for animation."""

    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "org.malleefowl.wms.animate.timesteps",
            title = "Timesteps for Animation",
            version = "0.1",
            metadata = [],
            abstract = "Get timesteps for Animation.",
            )

        self.max_frames_in = self.addLiteralInput(
            identifier="max_frames",
            title="Max. Frames",
            abstract="Maximum Number of Animation Frames",
            default=60,
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
            allowedValues=['all', 'hourly', 'daily', 'weekly', 'monthly', 'yearly']
            )

        self.service_url_in = self.addLiteralInput(
            identifier="service_url",
            title="WMS Service",
            abstract="URL of WMS Service",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        self.layer_in = self.addLiteralInput(
            identifier="layer",
            title="Layer",
            abstract="Layer Name",
            default="tas",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Timesteps for Animation",
            abstract="Timesteps for Animation as JSON",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=10, propagate=True)

        wms = WebMapService(self.service_url_in.getValue(), version='1.1.1')

        layer_name = self.layer_in.getValue().strip()
        wms_layer = wms.contents[layer_name]
        timesteps = map(str.strip, wms_layer.timepositions)

        filtered_timesteps = utils.filter_timesteps(timesteps,
                                                    start = self.start_in.getValue(),
                                                    end = self.end_in.getValue(),
                                                    aggregation=self.resolution_in.getValue())
        wms_time = reduce(lambda t1, t2: str(t1) + ',' + str(t2), filtered_timesteps)

        percent_done = 10
        max_frames = min(len(filtered_timesteps), self.max_frames_in.getValue())

        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(json.dumps(filtered_timesteps[:max_frames]))
            fp.close()
            self.output.setValue( out_filename )

        self.status.set(msg="done", percentDone=90, propagate=True)

        self.output.setValue( out_filename )

            
class GetAnimationAsKML(WPSProcess):
    """Create animation as KML (GoogleEarch) of wms layer for timesteps."""

    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "org.malleefowl.wms.animate.kml",
            title = "Animate WMS Layer as KML (GoogleEarth)",
            version = "0.1",
            metadata = [],
            abstract = "Create animation of wms layer for timesteps as KML file (GoogleEarth).",
            )

        self.max_frames_in = self.addLiteralInput(
            identifier="max_frames",
            title="Max. Frames",
            abstract="Maximum Number of Animation Frames",
            default=50,
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
            allowedValues=['all', 'hourly', 'daily', 'weekly', 'monthly', 'yearly']
            )

        self.service_url_in = self.addLiteralInput(
            identifier="service_url",
            title="WMS Service",
            abstract="URL of WMS Service",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        self.layer_in = self.addLiteralInput(
            identifier="layer",
            title="Layer",
            abstract="Layer Name",
            default="tas",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
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
            title="KML File",
            abstract="Animated WMS Layer as KML",
            metadata=[],
            formats=[{"mimeType":"application/vnd.google-earth.kmz"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=10, propagate=True)

        wms = WebMapService(self.service_url_in.getValue(), version='1.1.1')

        layer_name = self.layer_in.getValue().strip()
        wms_layer = wms.contents[layer_name]
        timesteps = map(str.strip, wms_layer.timepositions)
        width = int(self.width_in.getValue())
        height = int(self.height_in.getValue())
        bbox = tuple(map(float, self.bbox_in.getValue().split(",")))

        filtered_timesteps = utils.filter_timesteps(timesteps,
                                              start = self.start_in.getValue(),
                                              end = self.end_in.getValue(),
                                              aggregation=self.resolution_in.getValue())
       
        percent_done = 10
        max_frames = min(len(filtered_timesteps), self.max_frames_in.getValue())
        wms_time = reduce(lambda t1, t2: str(t1) + ',' + str(t2),
                          filtered_timesteps[:max_frames])

        out_filename = self.mktempfile(suffix='.kmz')
            
        # get wms image for timestep
        img = wms.getmap(layers=[layer_name],
                         bbox=bbox,
                         size=(width, height),
                         format='application/vnd.google-earth.kmz',
                         srs=self.srs_in.getValue(),
                         time=wms_time)
        out = open(out_filename, 'wb')
        out.write(img.read())
        out.flush()
        out.close()

        self.status.set(msg="done", percentDone=90, propagate=True)

        self.output.setValue( out_filename )

        
