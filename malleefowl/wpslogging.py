import logging

# https://pypi.python.org/pypi/ConcurrentLogHandler/0.8.3
try:
    from cloghandler import ConcurrentRotatingFileHandler as RFHandler
except ImportError:
    # Next 2 lines are optional:  issue a warning to the user
    from warnings import warn
    warn("ConcurrentLogHandler package not installed.  Using builtin log handler")
    from logging.handlers import RotatingFileHandler as RFHandler


@property
def DEBUG():
    return logging.DEBUG

def getLogger(name):
    from pywps import config as wpsconfig
    log_file = wpsconfig.getConfigValue("server", "logFile").replace('.log', '_trace.log')
    if log_file == '':
        log_file = "pywps_trace.log"
    log_level = wpsconfig.getConfigValue("server", "logLevel")
    
    logger = logging.getLogger(name)
    if 'DEBUG' in log_level:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # capture security warnings
    # https://urllib3.readthedocs.org/en/latest/security.html#insecureplatformwarning
    logging.captureWarnings(True)

    formatter = logging.Formatter('%(asctime)-15s - %(process)d - %(levelname)-8s %(message)s')

    # log handler
    fh = RFHandler(log_file, "a", 2*1024*1024, 5) 
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
    
