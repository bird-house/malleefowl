##
## This module manages the mongo database used by malleefowl
##

import pymongo
from pywps import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def database():
    dburi = config.getConfigValue("malleefowl", "mongodbUrl")
    conn = pymongo.Connection(dburi)
    return conn.malleefowl_db

def register_process_metadata(identifier, metadata):
    if identifier != None and metadata != None:
        db = database()
        process_metadata = { 'identifier': identifier, 'metadata': json.dumps(metadata) }
        db.metadata.update(
            {'identifier': process_metadata['identifier']},
            process_metadata,
            True)

def retrieve_process_metadata(identifier):
    db = database()
    process_metadata = db.metadata.find_one({'identifier': identifier})
    metadata = {}
    if process_metadata != None:
        metadata = json.loads(process_metadata.get('metadata'))
    return metadata

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
    db=database()
    entry = db.token.find_one(dict(token=token))
    return entry.get('userid', None)
