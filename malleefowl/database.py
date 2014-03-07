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
