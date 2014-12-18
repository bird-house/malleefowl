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

def wget_download_with_archive(url, credentials=None):
    from .utils import esgf_archive_path
    local_url = esgf_archive_path(url)
    if local_url is None:
        local_url = wget_download(url, credentials)
    return local_url

def wget_download(url, credentials=None):
    from os.path import basename
    resource_name = basename(url)
    logger.debug('downloading %s', url)

    from subprocess import check_call
    try:
        cmd = ["wget"]
        if credentials is not None:
            logger.debug('using credentials')
            cmd.append("--certificate")
            cmd.append(credentials) 
            cmd.append("--private-key")
            cmd.append(credentials)
        cmd.append("--no-check-certificate") 
        cmd.append("-N")
        cmd.append("-P")
        cmd.append(config.cache_path())
        cmd.append("--progress")
        cmd.append("dot:mega")
        cmd.append(url)
        check_call(cmd)
    except:
        msg = "wget failed on %s. Maybe not authorized? " % (resource_name)
        logger.exception(msg)
        raise Exception(msg)

    from os.path import join
    cached_file = join(config.cache_path(), resource_name)
    local_url = "file://" + cached_file
    #external_url = config.cache_url() + '/' + resource_name

    return local_url


