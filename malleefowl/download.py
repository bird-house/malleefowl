import os

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


def download(url):
    from os.path import basename, join
    resource_name = basename(url)
    filename = join(config.cache_path(), resource_name)

    if not os.path.exists(filename):
        import wget
        filename = wget.download(url, out=config.cache_path(), bar=None)
    return filename


