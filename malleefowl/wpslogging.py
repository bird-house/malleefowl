import logging

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

    # debug
    fh = logging.FileHandler(os.path.join(log_path, 'malleefowl_debug.log'))
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # info
    fh = logging.FileHandler(os.path.join(log_path, 'malleefowl_info.log'))
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # warn
    fh = logging.FileHandler(os.path.join(log_path, 'malleefowl_warn.log'))
    fh.setLevel(logging.WARN)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
    
