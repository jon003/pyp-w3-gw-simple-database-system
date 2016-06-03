import os

from . import ezdb
from .config import BASE_DB_FILE_PATH
from .exceptions import ValidationError

def create_database(db_name):
    filepath = os.path.join(BASE_DB_FILE_PATH, db_name)
    # this should satisfy test on line 53.
    if os.path.exists(filepath):
        raise ValidationError('Database with name "{}" already exists.'.format(db_name))
        
    if not os.path.exists(BASE_DB_FILE_PATH):
        os.mkdir(BASE_DB_FILE_PATH)
    
    db_name = ezdb.Database(db_name, filepath)
    db_name.write()
    return db_name

def connect_database(db_name):
    # Look for file on disk, raise error if it isn't there.
    filepath = os.path.join(BASE_DB_FILE_PATH, db_name)
    if not os.path.exists(filepath):
        raise ValidationError('Database with name "{}" does not exists.'.format(db_name))
    # Load data.
    db_name = ezdb.Database(db_name, filepath)
    db_name.load()
    return db_name


# def create_database(db_name):
#     # instantiates a new DB object, associates file on disk with it.
#     # returns the new database object
#     db_name = ezdb.Database(db_name)
#     return db_name

