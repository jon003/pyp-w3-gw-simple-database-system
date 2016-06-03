import json
import os
from datetime import date
from .exceptions import ValidationError



class Database(object):
    def __init__(self, db_name, filepath):
        self.tables = []
        self.filepath = filepath
        
    def show_tables(self):
        return [getattr(table, 'name') for table in self.tables]
        
    def create_table(self, table_name, columns=None, new=True):
        new_table = Table(table_name, columns)
        setattr(new_table, 'database', self)
        setattr(self, table_name, new_table)
        self.tables.append(new_table)
        
        if new:
            self.write()

    def write(self):
        """
        This method writes all data related to the current table to a file.
        """
        write_file = []
        
        for table in self.tables:
            # table is Table obj
            table_dict = {}
            table_name = getattr(table, 'name')
            table_dict[table_name] = {}
            table_dict[table_name].update({'schema': table.schema})
            table_dict[table_name].update({'rows': []})
            
            # make row loop here
            for row in table.rows:
                table_dict[table_name]['rows'].append(row)
            
            write_file.append(table_dict)
        
        with open(self.filepath, 'w') as fh:
            fh.write(json.dumps(write_file, default=self._json_serial))
            
    def _json_serial(self, obj):
        """JSON serializer for objects not serializable by default json code"""
    
        if isinstance(obj, date):
            serial = obj.isoformat()
            return serial
        raise TypeError ("Type not serializable")
            
    
    def load(self):
        """
        This method parses a JSON file to instantiate the specific objects into memeory (Database, Tables, and Rows)
        """
        with open(self.filepath, 'r') as fh:
            db_as_dict = json.loads(fh.read())
        

        for table in list(db_as_dict):
            table_name = list(table.keys())[0]
            self.create_table(table_name, columns=table[table_name]['schema'], new=False)
            
            for row in table[table_name]['rows']:
                getattr(self, table_name).rows.append(row)
        
class Table(object):
    def __init__(self, table_name, columns=None):.
        self.name = table_name
        self.schema = columns
        self.rows = []

    def count(self):
        return len(self.rows)

    def _validate_against_schema(self, new_row):
        if not len(new_row) == len(self.schema):
            raise ValidationError('Invalid amount of field')

        
        for arg, column in zip(new_row, self.schema):
            if not type(arg) == eval(column['type']):
                formated_type = str(type(arg)).split("\'")
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"'.format(column['name'], formated_type[1], column['type']))
        return True

    
    def insert(self, *args):
        if self._validate_against_schema(args):
            fields = [column['name'] for column in self.schema]
            row = dict(zip(fields, args))
            self.rows.append(row)
            self.database.write()
            
    def describe(self):
        # This should basically just return the schema.
        return self.schema
    
    def query(self, **kwargs):
        # must be kwargs, because we don't know the format of the schema.
        # returns iteration, but can just be a yield
        # TODO: Match on ALL kwargs before yielding a result.
        for key, value in kwargs.items():
            for row in self.rows:
                #print('DEBUG: row is {}, and row[key] is {} and value is: {}').format(row, row[key], value)
                if row[key] == value:
                    yield Row(row)
    
    def all(self):
        """
        This method returns a generator of each row in the table.
        Yielded row is an instance of the Row class.
        """
        return (Row(row) for row in self.rows)
        

class Row(object):
    def __init__(self, rowdict):
        # rowdict is passed a dictionary.
        # iterate over the schema, setting attributes based on key:value
        if not isinstance(rowdict, dict):
            raise ValidationError('rowdict was not passed a dictionary')
        for k, v in rowdict.items():
            setattr(self, str(k), v)