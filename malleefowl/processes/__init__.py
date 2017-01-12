from .wps_esgflogon import MyProxyLogon
from .wps_download import Download
from .wps_workflow import DispelWorkflow

processes = [
    MyProxyLogon(),
    Download(),
    DispelWorkflow(),
]
