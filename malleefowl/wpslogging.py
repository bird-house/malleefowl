import logging
from pywps import config as wpsconfig

@property
def DEBUG():
    return logging.DEBUG

def getLogger(name):
    # create logger
    logger = logging.getLogger(name)
    if 'DEBUG' ==  wpsconfig.getConfigValue("server", "logLevel"):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # capture security warnings
    # https://urllib3.readthedocs.org/en/latest/security.html#insecureplatformwarning
    logging.captureWarnings(True)

    # create stream handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)-15s - %(process)s - %(levelname)-8s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger
    
