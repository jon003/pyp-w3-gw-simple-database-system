
class Database(object):
    def __init__(self, db_name):
        # should initialize the list of tables to be empty.
        # create a file on disk, and write out the empty list of tables.
        # might look like:
        # if there is a file, then load_data_from_file()
        # otherwise, init empty one and write it out
        self.tables = []
        
    def show_tables():
        # should return an empty list with no tables created.
        # otherwise, just go read all of the table names from file.
        # returns a list of the table names.
        return [getattr(table, 'name') for tables in self.tables]
        
    def create_table(table_name, columns=None):
        # should set it to an attribute  of the database.
        # should write out the existance of the table to disk.
        new_table = Table(table_name, columns)
        setattr(self, table_name, new_table)
        self.tables.append(new_table)
        self.write()

    def write():
        pass
        # should create a new db file if it doesn't exist.
        # iterate through all of the tables, and write them out to disk.
    
    def load():
        pass
        # should load an existing db file
        with open(path, 'r') as fh:
            db_as_dict = fh.read()
            
        # TODO: turn our on-disk data representation into actual lists of dicts of lists of dicts.
        
        for table in db_as_dict:
            create_table(table)
        # iterate through all tables in file and load those into the object
        
        
class Table(object):
    def __init__(self, table_name, columns=None):
        # create a new table object.
        # add the object as an attribute of the db object
        # call Database.write() to write it out to disk.
        self.name = table_name
        self.schema = columns
        self.rows = []

    def count(self):
        return len(self.rows)

    def _validate_against_schema(new_row):
        # New_row is a list, in order.
        # self.scheme is a list of dicts.
        if not len(new_row) == len(self.schema):
            return False

        # http://stackoverflow.com/questions/1549801/differences-between-isinstance-and-type-in-python
        # https://docs.python.org/2/library/types.html

        typecheck = {
            'str': types.StringType,
            'int': types.IntType,
            'bool': types.BoolType
            'date': date_object
        }
        
        for arg, column in zip(new_row, self.schema):
            #if not isinstance(arg, column['type']):
            if not str(type(arg)) == column['type']):
                return False
                
                



[1, 'Jorge Luis Borges', date(1899, 8, 24), 'ARG', False]

        schema =[
            {'name': 'id', 'type': 'int'},
            {'name': 'name', 'type': 'str'},
            {'name': 'birth_date', 'type': 'date'},
            {'name': 'nationality', 'type': 'str'},
            {'name': 'alive', 'type': 'bool'},
        ],
        
        # look up the schema in the database
        # is new_row even the right length?  If not, raise.
        # schema is a list, we can iterate.
        # check type() against each element in the row.
        # return True or False
    
    def insert(*args):
        # args are the raw values to insert.
        # insert is a method of the table object.
        pass
        # finally, write out the table using Database.write()
        if _validate_against_schema(args):
            for key in self.schema:
                
            
    def describe(self)
        # This should basically just return the schema.
        return self.schema
    
    def query(**kwargs):
        # must be kwargs, because we don't know the format of the schema.
        # returns iteration, but can just be a yield
        # TODO: Match on ALL kwargs before yielding a result.
        for key, value in kwargs.items():
            for row in self.all():
                if getattr(row, key) == value:
                    yield Row(rowdict)
    
    def all():
        # should return a generator object.
        return (Row(rowdict) for rowdict in self.rows)
        

class Row(object):
    def __init__(self, rowdict):
        # iterate over the schema, setting attributes based on key:value
        for k, v in rowdict.items():
            setattr(self, k, v)