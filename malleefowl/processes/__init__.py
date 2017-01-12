from .wps_esgflogon import MyProxyLogon
from .wps_download import Download
from .wps_thredds import ThreddsDownload
from .wps_workflow import DispelWorkflow
from .wps_workflow import DummyProcess

processes = [
    MyProxyLogon(),
    Download(),
    ThreddsDownload(),
    DispelWorkflow(),
    DummyProcess(),
]
