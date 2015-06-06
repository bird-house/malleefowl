import threddsclient

from malleefowl.download import download_files

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def download(url, recursive=False, monitor=None):
    catalog = threddsclient(url)
    return download_files(urls=threddsclient.download_urls(), monitor=monitor)

