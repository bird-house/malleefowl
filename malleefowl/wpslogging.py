import logging

@property
def DEBUG():
    return logging.DEBUG

class TraceLogger(object):
    def __init__(self, filename="trace.log"):
        self.logger = open(filename, "a")

    def write(self, message):
        import datetime 
        log_message = '{asctime} - TRACE - {message}'.format(
            asctime=str(datetime.datetime.now())[:19],
            message=message)
        self.logger.write(log_message + '\n')

def getLogger(name):
    from pywps import config
    log_path = config.getConfigValue("malleefowl", "logPath")
    log_level = config.getConfigValue("malleefowl", "logLevel")
    
    logger = logging.getLogger(name)
    if 'DEBUG' in log_level:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)-15s - %(name)-5s - %(levelname)-8s %(message)s')

    # warn
    import os.path
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
    
