import os

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


def download(url, cache_enabled=True):
    import wget
    from os.path import join
    filename = wget.filename_from_url(url)
    filename = join(config.cache_path(), filename)

    # if caching is enabled download only if file does not exist
    if not cache_enabled or not os.path.exists(filename):
        filename = wget.download(url, out=config.cache_path(), bar=None)
    return filename


