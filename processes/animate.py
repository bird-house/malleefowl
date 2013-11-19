from malleefowl.process import WPSProcess

from owslib.wms import WebMapService
import json

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

        service_url = self.thredds_url + '/wms/test/cordex-eur-tas-year-pywpsInputoZXCTG.nc'
        wms = WebMapService(service_url, version='1.1.1')

        layers = []
        for layerid,layer in wms.items():
            if layerid in ['lat', 'lon']:
                continue
            timesteps = map(str.strip, layer.timepositions)
            layers.append(dict(name=layer.name, title=layer.title, timesteps=timesteps))

        self.status.set(msg="done", percentDone=90, propagate=True)
        
        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(json.dumps(layers))
            fp.close()
            self.output.setValue( out_filename )

        
