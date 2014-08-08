##
## This module manages the mongo database used by malleefowl
##

import json

import pymongo

from malleefowl import config
from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def database():
    dburi = config.mongodb_url()
    conn = pymongo.Connection(dburi)
    return conn.malleefowl_db

# TODO: maybe move these methods to tokenmgr or model ...
def add_token(token, userid):
    db = database()
    db.token.update(
        {'userid': userid},
        dict(token=token, userid=userid),
        True)

def get_token_by_userid(userid):
    token = None
    db = database()
    entry = db.token.find_one(dict(userid=userid))
    if entry is not None:
        token = entry.get('token', None)
    return token

def get_userid_by_token(token):
    userid = None
    db=database()
    entry = db.token.find_one(dict(token=token))
    if entry is not None:
        userid = entry.get('userid', None)
    return userid
