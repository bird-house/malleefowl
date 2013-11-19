from malleefowl.process import WPSProcess


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

        self.output = self.addLiteralOutput(
            identifier="output",
            title="WMS Layers",
            abstract="WMS Layers as JSON",
            default='{}',
            type=type(''),
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=10, propagate=True)
