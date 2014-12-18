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

class DownloadManager(object):
    def __init__(self, monitor=None):
        self.result = []
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
                logger.exception('download job failed!')
                progress = self.count * 100.0 / self.max_count
                self.show_status('Query for Dataset failed.', progress)

        # completed with the job
        self.job_queue.task_done()

    def download_job(self):
        with self.result_lock:
            self.result.append(f.download_url)

    def download(urls, credentials):
        t0 = datetime.now()
        # lock for parallel search
        self.result_lock = threading.Lock()
        self.result = []
        self.count = 0
        # init threading
        self.job_queue = Queue()
        # using 5 thredds
        for x in range(5):
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

        duration = (datetime.now() - t0).seconds




