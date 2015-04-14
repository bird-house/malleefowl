import os

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

from swiftclient import client, ClientException
from swiftclient.service import SwiftService

def login(username, password):
    storage_url = auth_token = None
    
    try:
        (storage_url, auth_token) = client.get_auth(config.swift_auth_url(), username, password,
                                                    auth_version=config.swift_auth_version())
    except ClientException:
        logger.exception('swift login failed for user %s', username)
        raise
    return storage_url, auth_token

def download(storage_url, auth_token, container):
    options = dict(
        os_storage_url = storage_url,
        os_auth_token = auth_token,
        skip_identical = True,
        prefix = None,
        marker = '',
        header = [],
        object_thredds = 10,
        object_dd_thredds = 10,
        container_thredds = 10,
        no_download = False,
        insecure = True,
        all = False)

    # TODO: need a better download path
    prevdir = os.getcwd()
    download_path = os.path.join(config.cache_path(), container)
    if not os.path.exists(download_path):
        os.mkdir(download_path)
    os.chdir(download_path)

    files = []
    
    try:
        swift = SwiftService(options=options)
        down_iter = swift.download(container=container)
        for down in down_iter:
            if down['path'].endswith('/'):
                continue
            if down['success']:
                logger.info('success: %s %s', down['path'], down['container'])
                file_path = 'file://' + os.path.join(download_path, down['path'])
                files.append(file_path)
            else:
                error = down['error']
                if isinstance(error, ClientException):
                    if error.http_status == 304:
                        logger.info('skipped: %s', down['path'])
                        file_path = 'file://' + os.path.join(download_path, down['path'])
                        files.append(file_path)
    finally:
        os.chdir(prevdir)

    return files
