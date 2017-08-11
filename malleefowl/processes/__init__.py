from .wps_esgsearch import ESGSearchProcess
from .wps_download import Download
from .wps_thredds import ThreddsDownload
from .wps_workflow import DispelWorkflow

processes = [
    ESGSearchProcess(),
    Download(),
    ThreddsDownload(),
    DispelWorkflow(),
]
