import os

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

from swiftclient import client, ClientException
from swiftclient.service import SwiftService

def login(username, password, auth_url=None, auth_version=None):
    storage_url = auth_token = None

    if auth_url is None:
        auth_url = config.swift_auth_url()
    if auth_version is None:
        auth_version = config.swift_auth_version()
    
    try:
        (storage_url, auth_token) = client.get_auth(auth_url, username, password, auth_version=auth_version)
    except ClientException:
        logger.exception('swift login failed for user %s', username)
        raise
    return storage_url, auth_token

def download(storage_url, auth_token, container, prefix=None):
    options = dict(
        os_storage_url = storage_url,
        os_auth_token = auth_token,
        skip_identical = True,
        prefix = prefix,
        marker = '',
        header = [],
        object_thredds = 10,
        object_dd_thredds = 10,
        container_thredds = 10,
        no_download = False,
        insecure = True,
        all = False)

    # TODO: need a better download path
    from urlparse import urlparse
    parsed_url = urlparse(storage_url)
    
    prevdir = os.getcwd()
    download_path = os.path.join(config.cache_path(),
                                 os.path.basename(parsed_url.path),
                                 container)
    if not os.path.exists(download_path):
        os.makedirs(download_path)
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

def get_temp_key(storage_url, auth_token):
    """ Tries to get meta-temp-url key from account.
    If not set, generate tempurl and save it to acocunt.
    This requires at least account owner rights. """
    import string
    import random
    
    try:
        account = client.get_account(storage_url, auth_token)
    except ClientException:
        logger.exception("get_account failed.")
        return None

    key = account[0].get('x-account-meta-temp-url-key')

    if not key:
        chars = string.ascii_lowercase + string.digits
        key = ''.join(random.choice(chars) for x in range(32))
        headers = {'x-account-meta-temp-url-key': key}
        try:
            client.post_account(storage_url, auth_token, headers)
        except ClientException as e:
            logger.exception("post_account failed.")
            return None
    return key

def get_temp_url(storage_url, auth_token, container, objectname, expires=600):
    import time
    import urlparse
    import hmac
    from hashlib import sha1
    
    key = get_temp_key(storage_url, auth_token)
    if not key:
        return None

    expires += int(time.time())
    url_parts = urlparse.urlparse(storage_url)
    path = "%s/%s/%s" % (url_parts.path, container, objectname)
    base = "%s://%s" % (url_parts.scheme, url_parts.netloc)
    hmac_body = 'GET\n%s\n%s' % (expires, path)
    sig = hmac.new(key, hmac_body.encode("utf-8"), sha1).hexdigest()
    url = '%s%s?temp_url_sig=%s&temp_url_expires=%s' % (
        base, path, sig, expires)
    return url

def download_urls(storage_url, auth_token, container, prefix=None):
    options = dict(
        os_storage_url = storage_url,
        os_auth_token = auth_token,
        skip_identical = True,
        prefix = prefix,
        marker = '',
        header = [],
        object_thredds = 10,
        object_dd_thredds = 10,
        container_thredds = 10,
        no_download = True,
        insecure = True,
        all = False)

    urls = []
    
    try:
        swift = SwiftService(options=options)
        down_iter = swift.download(container=container)
        for down in down_iter:
            if down['path'].endswith('/'):
                continue
            if down['success']:
                logger.info('success: %s %s', down['path'], down['container'])
                urls.append(get_temp_url(storage_url, auth_token, container, down['path']))
    except:
        logger.exception('download failed.')
    return urls

     
