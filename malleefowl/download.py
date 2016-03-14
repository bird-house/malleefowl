import threading
from Queue import Queue
import subprocess

from malleefowl import config

import logging
logger = logging.getLogger(__name__)

def download_with_archive(url, credentials=None, openid=None, password=None):
    """
    Downloads file. Checks before downloading if file is already in local esgf archive.
    """
    from .utils import esgf_archive_path
    file_url = esgf_archive_path(url)
    if file_url is None:
        file_url = download(url, use_file_url=True, credentials=credentials, openid=openid, password=password)
    return file_url

def download(url, use_file_url=False, credentials=None, openid=None, password=None):
    """
    Downloads url and returns local filename.

    :param url: url of file
    :param use_file_url: True if result should be a file url "file://", otherwise use system path.
    :param credentials: path to credentials if security is needed to download file
    :param openid: download with you openid and password
    :returns: downloaded file with either file:// or system path
    """
    import urlparse
    parsed_url = urlparse.urlparse(url)
    if parsed_url.scheme == 'file':
        result = url
    else:
        result = wget(url=url, use_file_url=use_file_url, credentials=credentials, openid=openid, password=password)
    return result

def wget(url, use_file_url=False, credentials=None, openid=None, password=None):
    """
    Downloads url and returns local filename.

    :param url: url of file
    :param use_file_url: True if result should be a file url "file://", otherwise use system path.
    :param credentials: path to credentials if security is needed to download file
    :param openid: download with you openid and password
    :returns: downloaded file with either file:// or system path
    """
    logger.debug('downloading %s', url)

    try:
        cmd = ["wget"]
        if credentials is not None:
            logger.debug('using credentials')
            cmd.append("--certificate")
            cmd.append(credentials) 
            cmd.append("--private-key")
            cmd.append(credentials) 
            cmd.append("--ca-certificate")
            cmd.append(credentials)
        elif openid:
            from .esgf.logon import openid_logon_with_wget
            cookies = openid_logon_with_wget(openid, password, url=url)
            logger.error(cookies)
            #cmd.append('--ca-directory={0}'.format('certificates'))
            cmd.append('--cookies=on')
            cmd.append('--keep-session-cookies')
            cmd.append('--save-cookies')
            cmd.append(cookies)
            cmd.append('--load-cookies')
            cmd.append(cookies)
        cmd.append("--no-check-certificate")
        if not logger.isEnabledFor(logging.DEBUG):
            cmd.append("--quiet")
        cmd.append("-N")           # turn on timestamping
        cmd.append("--continue")   # continue partial downloads
        cmd.append("-x")           # force creation of directories
        cmd.append("-P")           # directory prefix
        cmd.append(config.cache_path())
        cmd.append(url)
        logger.debug("cmd: %s", cmd)
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        logger.debug("Output: %s", output)
    except Exception as e:
        msg = "wget failed on {0}.".format(url)
        logger.exception(msg)
        if hasattr(e, 'output'):
            logger.error("Ouptut: %s", e.output)
        raise Exception(msg)

    import urlparse
    parsed_url = urlparse.urlparse(url)
    from os.path import join
    filename = join(config.cache_path(), parsed_url.netloc, parsed_url.path.strip('/'))
    if use_file_url == True:
        filename = "file://" + filename
    return filename

def download_files(urls=[], credentials=None, openid=None, password=None, monitor=None):
    dm = DownloadManager(monitor)
    return dm.download(urls, credentials, openid, password)

def download_files_from_thredds(url, recursive=False, monitor=None):
    import threddsclient
    return download_files(urls=threddsclient.download_urls(url), monitor=monitor)

class DownloadManager(object):
    def __init__(self, monitor=None):
        self.files = []
        self.count = 0
        self.monitor = monitor
        
    def show_status(self, message, progress):
        if self.monitor is None:
            logger.info("%s, progress=%d/100", message, progress)
        else:
            self.monitor(message, progress)

    # The threader thread pulls an worker from the queue and processes it
    def threader(self):
        while True:
            # gets an worker from the queue
            worker = self.job_queue.get()

            # Run the example job with the avail worker in queue (thread)
            try:
                self.download_job(**worker)
            except Exception:
                logger.exception('download failed!')
            # completed with the job
            self.job_queue.task_done()

    def download_job(self, url, credentials, openid, password):
        file_url = download_with_archive(url, credentials, openid, password)
        with self.result_lock:
            self.files.append(file_url)
            self.count = self.count + 1
        progress = self.count * 100.0 / self.max_count
        self.show_status('Downloaded %d/%d' % (self.count, self.max_count), progress)

    def download(self, urls, credentials=None, openid=None, password=None):
        # start ...
        from datetime import datetime
        t0 = datetime.now()
        self.show_status("start downloading of %d files" % len(urls), 0)
        # lock for parallel search
        self.result_lock = threading.Lock()
        self.files = []
        self.count = 0
        self.max_count = len(urls)
        # init threading
        self.job_queue = Queue()
        # using max 8 thredds
        num_threads = min(8, len(urls))
        logger.info('starting %d download threads', num_threads)
        for x in range(num_threads):
            t = threading.Thread(target=self.threader)
            # classifying as a daemon, so they will die when the main dies
            t.daemon = True
            # begins, must come after daemon definition
            t.start()
        for url in urls:
            # fill job queue
            self.job_queue.put(dict(url=url, credentials=credentials, openid=openid, password=password))

        # wait until the thread terminates.
        self.job_queue.join()
        # how long?
        duration = (datetime.now() - t0).seconds
        self.show_status("downloaded %d files in %d seconds" % (len(urls), duration), 100)
        if len(self.files) != len(urls):
            raise Exception("could not download all files %d/%d" % (len(self.files), len(urls)))
        # done
        return self.files




