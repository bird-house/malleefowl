import threading
from Queue import Queue

from malleefowl import config
from .exceptions import DownloadException

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def download_with_archive(url, credentials=None):
    """
    Downloads file. Checks before downloading if file is already in local esgf archive.
    """
    from .utils import esgf_archive_path
    file_url = esgf_archive_path(url)
    if file_url is None:
        file_url = download(url, use_file_url=True, credentials=credentials)
    return file_url

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
        cmd.append("-N")           # turn on timestamping
        cmd.append("--continue")   # continue partial downloads
        cmd.appned("-x")           # force creation of directories
        cmd.append("-P")           # directory prefix
        cmd.append(config.cache_path())
        cmd.append(url)
        check_output(cmd)
    except:
        msg = "wget failed on %s. Maybe not authorized? " % (resource_name)
        logger.exception(msg)
        raise Exception(msg)

    from os.path import join
    filename = join(config.cache_path(), resource_name)
    if use_file_url == True:
        filename = "file://" + filename
    return filename

def download_files(urls=[], credentials=None, monitor=None):
    dm = DownloadManager(monitor)
    return dm.download(urls, credentials)


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

    def download_job(self, url, credentials):
        file_url = download_with_archive(url, credentials)
        with self.result_lock:
            self.files.append(file_url)
            self.count = self.count + 1
        progress = self.count * 100.0 / self.max_count
        self.show_status('Downloaded %d/%d' % (self.count, self.max_count), progress)

    def download(self, urls, credentials):
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
            self.job_queue.put(dict(url=url, credentials=credentials))

        # wait until the thread terminates.
        self.job_queue.join()
        # how long?
        duration = (datetime.now() - t0).seconds
        self.show_status("downloaded %d files in %d seconds" % (len(urls), duration), 100)
        # done
        return self.files




