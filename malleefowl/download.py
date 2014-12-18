from malleefowl import config
from .exceptions import DownloadException

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


def download_files(urls=[], credentials=None, monitor=None):
    files = []

    count = 0
    max_count = len(urls)
    for url in urls:
        progress = count * 100.0 / max_count
        if monitor is not None:
            monitor("Downloading %d/%d" % (count+1, max_count), progress)
        count = count + 1

        try:
            files.append(download_with_archive(url, credentials))
        except:
            logger.exception("Failed to download %s", url)

    if max_count > len(files):
        logger.warn('Could not retrieve all files: %d from %d', len(files), max_count)
        if len(files) == 0:
            raise DownloadException("Could not retrieve any file. Check your permissions!")

    return files

def download_with_archive(url, credentials=None):
    """
    Downloads file. Checks before downloading if file is already in local esgf archive.
    """
    from .utils import esgf_archive_path
    local_url = esgf_archive_path(url)
    if local_url is None:
        local_url = download(url, use_file_url=True, credentials=credentials)
    return local_url

def download(url, use_file_url=False, credentials=None):
    """
    Downloads url and returns local filename.

    :param url: url of file
    :param use_file_url: True if result should be a file url "file://", otherwise use system path.
    :param credentials: path to credentials if security is needed to download file
    returns downloaded file with either file:// or system path
    """
    from os.path import basename
    resource_name = basename(url)
    logger.debug('downloading %s', url)

    from subprocess import check_output
    try:
        cmd = ["wget"]
        if credentials is not None:
            logger.debug('using credentials')
            cmd.append("--certificate")
            cmd.append(credentials) 
            cmd.append("--private-key")
            cmd.append(credentials)
        cmd.append("--no-check-certificate")
        if not logger.isEnabledFor(logging.DEBUG):
            cmd.append("--quiet")
        cmd.append("-N")
        cmd.append("-P")
        cmd.append(config.cache_path())
        cmd.append(url)
        check_output(cmd)
    except:
        msg = "wget failed on %s. Maybe not authorized? " % (resource_name)
        logger.exception(msg)
        raise Exception(msg)

    from os.path import join
    result = join(config.cache_path(), resource_name)
    if use_file_url == True:
        result = "file://" + result
    return result


