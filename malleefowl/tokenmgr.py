##
## This module manages tokens to access wps processes.
##
##
## TODO: use a standard python module ... maybe for oauth2?

from pywps import config
from malleefowl import database

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class AccessDeniedError(Exception):
    pass

class UnknownUserIDError(Exception):
    pass

class UnknownTokenError(Exception):
    pass

class checkAccess(object):

    def __init__(self, f):
        self.f = f

    def __call__(self, access_token, other):
        if not _is_sys_token(access_token):
            logger.warn('function "%s" called with invalid access token "%s"' % (self.f.__name__, access_token))
            raise AccessDeniedError
        return self.f(access_token, other)

def sys_userid():
    return config.getConfigValue("malleefowl", "sysUserID")

def sys_token():
    return config.getConfigValue("malleefowl", "sysToken")

def init():
    _add(sys_token(), sys_userid())

@checkAccess
def get_token(access_token, userid):
    token = database.get_token_by_userid(userid)
    if token is None:
        raise UnknownUserIDError
    return token

@checkAccess
def gen_token_for_userid(access_token, userid):
    token = get_uuid()
    _add(token, userid)
    return token

@checkAccess
def get_userid(access_token, token):
    userid = database.get_userid_by_token(token)
    if userid is None:
        raise UnknownTokenError
    return userid

@checkAccess
def is_token_valid(access_token, token):
    valid = False
    try:
        userid = database.get_userid_by_token(token)
        valid = userid is not None
    except Exception as e:
        logger.warn("is_token_valid raised exception. err msg=%s" % (e.message))
    return valid

def _is_sys_token(token):
    valid = False
    try:
        userid = database.get_userid_by_token(token)
        valid = userid == sys_userid()
    except Exception as e:
        logger.warn("is_sys_token raised exception. err msg=%s" % (e.message))
    return valid

def _add(token, userid):
    database.add_token(token=token, userid=userid)

def get_uuid():
    """
    get a UUID - URL safe, Base64
    """
    import base64
    import uuid
    
    r_uuid = base64.urlsafe_b64encode(uuid.uuid4().bytes)
    return r_uuid.replace('=', '')



