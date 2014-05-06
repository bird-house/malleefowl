import logging

@property
def DEBUG():
    return logging.DEBUG

class TraceLogger(object):
    def __init__(self, filename):
        self.logger = open(filename, "a")

    def write(self, message):
        self.logger.write("- TRACE - %s -" % (message))

def getLogger(name):
    from pywps import config
    log_path = config.getConfigValue("malleefowl", "logPath")
    log_level = config.getConfigValue("malleefowl", "logLevel")
    
    logger = logging.getLogger(name)
    if 'DEBUG' in log_level:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    import os.path

    #formatter = logging.Formatter('%(asctime)-15s %(name)-5s %(levelname)-8s IP: %(ip)-15s User: %(user)-8s %(message)s')
    formatter = logging.Formatter('%(asctime)-15s - %(name)-5s - %(levelname)-8s %(message)s')

    # warn
    fh = logging.FileHandler(os.path.join(log_path, 'malleefowl_warn.log'))
    fh.setLevel(logging.WARN)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # info
    fh = logging.FileHandler(os.path.join(log_path, 'malleefowl_info.log'))
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # debug
    fh = logging.FileHandler(os.path.join(log_path, 'malleefowl_debug.log'))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # log stdout to trace
    import sys
    sys.stdout = TraceLogger(os.path.join(log_path, 'malleefowl_trace.log'))

    return logger
    
