##
## This module manages tokens to access wps processes.
##
##
## TODO: use a standard python module ... maybe for oauth2?

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class AccessDeniedError(Exception):
    pass

class UnknownUserIDError(Exception):
    pass

class UnknownTokenError(Exception):
    pass

TOKEN_MAP = {}
USERID_MAP = {}

class checkAccess(object):

    def __init__(self, f):
        self.f = f

    def __call__(self, access_token, other):
        if not _is_sys_token(access_token):
            logger.warn('function "%s" called with invalid access token "%s"' % (self.f.__name__, access_token))
            raise AccessDeniedError
        return self.f(access_token, other)

@property
def sys_user():
    return 'malleefowl'

def init(token):
    _add(token, sys_user)

@checkAccess
def get_token(access_token, userid):
    token = USERID_MAP.get(userid, None)
    if token is None:
        raise UnknownUserIDError
    return token

@checkAccess
def gen_token(access_token, userid):
    token = userid.replace('@', '_')
    _add(token, userid)
    return token

@checkAccess
def get_userid(access_token, token):
    userid= TOKEN_MAP.get(token, None)
    if userid is None:
        raise UnknownTokenError
    return userid

@checkAccess
def is_token_valid(access_token, token):
    return TOKEN_MAP.has_key(token)

def _is_sys_token(token):
    return TOKEN_MAP.get(token, None) == sys_user

def _add(token, userid):
    global TOKEN_MAP, USERID_MAP
    TOKEN_MAP[token] = userid
    USERID_MAP[userid] = token


