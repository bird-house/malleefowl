from .wps_esgflogon import MyProxyLogon
from .wps_download import Download

processes = [
    MyProxyLogon(),
    Download(),
]
