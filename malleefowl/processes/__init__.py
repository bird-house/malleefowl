from .wps_esgflogon import MyProxyLogon
from .wps_esgsearch import ESGSearchProcess
from .wps_download import Download
from .wps_thredds import ThreddsDownload
from .wps_workflow import DispelWorkflow

processes = [
    MyProxyLogon(),
    ESGSearchProcess(),
    Download(),
    ThreddsDownload(),
    DispelWorkflow(),
]
